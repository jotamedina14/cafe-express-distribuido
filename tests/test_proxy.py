import socket
import threading
import time
import unittest

import protocolo as p
from patrones.proxy import ErrorCoordinador, ProxyCoordinador, ProxyNodo


class ServidorFalso:
    """Servidor TCP de prueba: cada conexión se atiende con `manejador(canal)`."""

    def __init__(self, manejador, puerto=0):
        self.manejador = manejador
        self.sock = socket.create_server(("127.0.0.1", puerto))
        self.puerto = self.sock.getsockname()[1]
        self.canales = []
        threading.Thread(target=self._aceptar, daemon=True).start()

    def _aceptar(self):
        while True:
            try:
                conexion, _ = self.sock.accept()
            except OSError:
                return
            canal = p.Canal(conexion)
            self.canales.append(canal)
            threading.Thread(target=self._atender, args=(canal,), daemon=True).start()

    def _atender(self, canal):
        try:
            self.manejador(canal)
        except (OSError, p.ErrorProtocolo):
            pass

    def cortar_conexiones(self):
        for canal in self.canales:
            canal.cerrar()
        self.canales.clear()

    def detener(self):
        self.sock.close()
        self.cortar_conexiones()


def esperar(condicion, timeout=3.0):
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if condicion():
            return True
        time.sleep(0.01)
    return False


class CoordinadorFalso:
    """Acepta pedidos, responde con ids y registra lo que recibe."""

    def __init__(self, ignorar_primero=False):
        self.recibidos = []
        self.ignorar_primero = ignorar_primero
        self.siguiente_id = 100
        self.ids_por_ref = {}
        self.servidor = ServidorFalso(self.atender)

    def atender(self, canal):
        while (mensaje := canal.recibir()) is not None:
            self.recibidos.append(mensaje)
            if self.ignorar_primero and len(self.recibidos) == 1:
                continue
            if mensaje["tipo"] == p.PEDIDO_NUEVO:
                if mensaje["producto"] == "cerveza":
                    canal.enviar(p.ERROR, ref=mensaje["ref"], codigo=p.PRODUCTO_INVALIDO, motivo="no hay")
                    continue
                if mensaje["ref"] not in self.ids_por_ref:
                    self.ids_por_ref[mensaje["ref"]] = self.siguiente_id
                    self.siguiente_id += 1
                pid = self.ids_por_ref[mensaje["ref"]]
                canal.enviar(p.PEDIDO_ACEPTADO, ref=mensaje["ref"], pedido_id=pid)
                canal.enviar(p.NOTIFICACION, pedido_id=pid, estado="recibido")
            elif mensaje["tipo"] == p.SUSCRIPCION:
                datos = {"ref": mensaje["ref"]} if "ref" in mensaje else {}
                canal.enviar(p.NOTIFICACION, pedido_id=mensaje["pedido_id"], estado="listo", **datos)


class PruebasProxyCoordinador(unittest.TestCase):
    def test_hacer_pedido_y_recibir_notificacion(self):
        falso = CoordinadorFalso()
        avisos = []
        with ProxyCoordinador("127.0.0.1", falso.servidor.puerto, al_notificar=avisos.append) as proxy:
            pid = proxy.hacer_pedido("latte", 2, sucursal="norte")
            self.assertEqual(pid, 100)
            self.assertTrue(esperar(lambda: avisos))
            self.assertEqual(avisos[0]["estado"], "recibido")
        pedido = falso.recibidos[0]
        self.assertEqual((pedido["producto"], pedido["cantidad"], pedido["sucursal"]),
                         ("latte", 2, "norte"))
        falso.servidor.detener()

    def test_error_del_coordinador_se_convierte_en_excepcion(self):
        falso = CoordinadorFalso()
        with ProxyCoordinador("127.0.0.1", falso.servidor.puerto) as proxy:
            with self.assertRaises(ErrorCoordinador) as ctx:
                proxy.hacer_pedido("cerveza")
            self.assertEqual(ctx.exception.codigo, p.PRODUCTO_INVALIDO)
        falso.servidor.detener()

    def test_reintento_usa_el_mismo_ref_y_no_duplica(self):
        falso = CoordinadorFalso(ignorar_primero=True)
        with ProxyCoordinador("127.0.0.1", falso.servidor.puerto, timeout=0.3) as proxy:
            pid = proxy.hacer_pedido("tinto")
        refs = [m["ref"] for m in falso.recibidos if m["tipo"] == p.PEDIDO_NUEVO]
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0], refs[1])
        self.assertEqual(pid, 100)
        falso.servidor.detener()

    def test_consultar_devuelve_el_estado_actual(self):
        falso = CoordinadorFalso()
        with ProxyCoordinador("127.0.0.1", falso.servidor.puerto) as proxy:
            evento = proxy.consultar(55)
        self.assertEqual((evento["pedido_id"], evento["estado"]), (55, "listo"))
        self.assertTrue(falso.recibidos[-1]["solo_consulta"])
        falso.servidor.detener()

    def test_reconecta_solo_y_se_vuelve_a_suscribir(self):
        falso = CoordinadorFalso()
        estados_conexion = []
        proxy = ProxyCoordinador("127.0.0.1", falso.servidor.puerto,
                                 al_conexion=estados_conexion.append).conectar()
        pid = proxy.hacer_pedido("capuchino")
        falso.recibidos.clear()
        falso.servidor.cortar_conexiones()  # el coordinador "se cae"

        resuscripcion = lambda: any(m["tipo"] == p.SUSCRIPCION and m["pedido_id"] == pid
                                    for m in falso.recibidos)
        self.assertTrue(esperar(resuscripcion, timeout=5))
        self.assertEqual(estados_conexion[:3], [True, False, True])
        proxy.cerrar()
        falso.servidor.detener()

    def test_sin_coordinador_falla_con_error_de_conexion(self):
        libre = socket.create_server(("127.0.0.1", 0))
        puerto = libre.getsockname()[1]
        libre.close()
        proxy = ProxyCoordinador("127.0.0.1", puerto, reintentos=1, timeout=0.5)
        with self.assertRaises(ConnectionError):
            proxy.hacer_pedido("tinto")
        proxy.cerrar()


class PruebasProxyNodo(unittest.TestCase):
    @staticmethod
    def nodo_que_confirma(canal):
        while (mensaje := canal.recibir()) is not None:
            canal.enviar(p.CAMBIO_ESTADO, pedido_id=mensaje["pedido_id"],
                         estado="recibido", nodo_id="nodo-x")

    def test_asignar_devuelve_la_confirmacion(self):
        servidor = ServidorFalso(self.nodo_que_confirma)
        proxy = ProxyNodo("nodo-x", "127.0.0.1", servidor.puerto)
        respuesta = proxy.asignar({"pedido_id": 1, "producto": "latte"})
        self.assertEqual((respuesta["tipo"], respuesta["estado"]), (p.CAMBIO_ESTADO, "recibido"))
        # La conexión se reutiliza entre llamadas.
        proxy.asignar({"pedido_id": 2, "producto": "latte"})
        self.assertEqual(len(servidor.canales), 1)
        proxy.cerrar()
        servidor.detener()

    def test_nodo_reiniciado_se_oculta_con_una_conexion_nueva(self):
        servidor = ServidorFalso(self.nodo_que_confirma)
        proxy = ProxyNodo("nodo-x", "127.0.0.1", servidor.puerto)
        proxy.asignar({"pedido_id": 1})
        servidor.cortar_conexiones()
        time.sleep(0.05)
        respuesta = proxy.asignar({"pedido_id": 2})
        self.assertEqual(respuesta["pedido_id"], 2)
        proxy.cerrar()
        servidor.detener()

    def test_nodo_apagado_lanza_error(self):
        libre = socket.create_server(("127.0.0.1", 0))
        puerto = libre.getsockname()[1]
        libre.close()
        with self.assertRaises(OSError):
            ProxyNodo("nodo-x", "127.0.0.1", puerto).asignar({"pedido_id": 1})

    def test_nodo_colgado_se_detecta_por_timeout(self):
        colgado = threading.Event()
        servidor = ServidorFalso(lambda canal: colgado.wait(10))
        proxy = ProxyNodo("nodo-x", "127.0.0.1", servidor.puerto, timeout=0.3)
        inicio = time.monotonic()
        with self.assertRaises(socket.timeout):
            proxy.asignar({"pedido_id": 1})
        self.assertLess(time.monotonic() - inicio, 1.0)
        colgado.set()
        servidor.detener()


if __name__ == "__main__":
    unittest.main()

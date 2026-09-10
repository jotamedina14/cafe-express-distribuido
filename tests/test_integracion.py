"""Sistema completo (coordinador + nodos + clientes) en un solo proceso, con
puertos asignados por el sistema operativo y tiempos acelerados."""
import socket
import tempfile
import threading
import time
import unittest
from collections import defaultdict

import protocolo as p
from coordinador import ConfigCoordinador, Coordinador
from dominio import EN_PREPARACION, ENTREGADO, LISTO, RECIBIDO
from nodo import ConfigNodo, Nodo
from patrones.breaker import EstadoCircuito
from patrones.proxy import ErrorCoordinador, ProxyCoordinador
from test_proxy import esperar


class Sistema:
    def __init__(self, hilos_por_nodo=(2, 2), estrategia="menor_carga", reasignar=True,
                 factor_tiempo=0.02):
        self.logs = tempfile.TemporaryDirectory()
        self.factor_tiempo = factor_tiempo
        self.coordinador = Coordinador(ConfigCoordinador(
            puerto_clientes=0, puerto_nodos=0, estrategia=estrategia, reasignar=reasignar,
            intervalo_latido=0.1, timeout_latido=0.6, timeout_asignacion=0.3,
            reintento_circuito=1.0, resumen_cada=0, carpeta_logs=self.logs.name, consola=False,
        )).iniciar()
        self.nodos = []
        for hilos in hilos_por_nodo:
            self.agregar_nodo(hilos)
        self.eventos = defaultdict(list)
        self._candado = threading.Lock()

    def agregar_nodo(self, hilos):
        nodo = Nodo(ConfigNodo(nodo_id=f"nodo-{len(self.nodos) + 1}", puerto=0, hilos=hilos,
                               puerto_coordinador=self.coordinador.puerto_nodos,
                               factor_tiempo=self.factor_tiempo, carpeta_logs=self.logs.name,
                               consola=False)).iniciar()
        assert nodo.registrado.wait(3)
        self.nodos.append(nodo)
        return nodo

    def cliente(self):
        return ProxyCoordinador("127.0.0.1", self.coordinador.puerto_clientes,
                                al_notificar=self._anotar).conectar()

    def _anotar(self, evento):
        with self._candado:
            self.eventos[evento["pedido_id"]].append(evento)

    def estados(self, pedido_id):
        with self._candado:
            return [e["estado"] for e in self.eventos[pedido_id]]

    def entregados(self, ids):
        return [pid for pid in ids if ENTREGADO in self.estados(pid)]

    def cerrar(self):
        for nodo in self.nodos:
            nodo.detener()
        self.coordinador.detener()
        self.logs.cleanup()


class PruebasFlujo(unittest.TestCase):
    def setUp(self):
        self.sistema = Sistema()
        self.cliente = self.sistema.cliente()

    def tearDown(self):
        self.cliente.cerrar()
        self.sistema.cerrar()

    def test_todos_los_pedidos_recorren_el_ciclo_completo(self):
        ids = [self.cliente.hacer_pedido("tinto", sucursal="norte") for _ in range(30)]
        self.assertEqual(len(set(ids)), 30)
        self.assertTrue(esperar(lambda: len(self.sistema.entregados(ids)) == 30, timeout=10))
        for pid in ids:
            self.assertEqual(self.sistema.estados(pid), [RECIBIDO, EN_PREPARACION, LISTO, ENTREGADO])
        # Los dos nodos trabajaron.
        nodos = {self.sistema.eventos[pid][-1]["nodo_id"] for pid in ids}
        self.assertEqual(nodos, {"nodo-1", "nodo-2"})

    def test_producto_invalido(self):
        with self.assertRaises(ErrorCoordinador) as ctx:
            self.cliente.hacer_pedido("cerveza")
        self.assertEqual(ctx.exception.codigo, p.PRODUCTO_INVALIDO)
        with self.assertRaises(ErrorCoordinador) as ctx:
            self.cliente.hacer_pedido("tinto", cantidad=99)
        self.assertEqual(ctx.exception.codigo, p.CANTIDAD_INVALIDA)

    def test_otra_sucursal_puede_seguir_un_pedido(self):
        pid = self.cliente.hacer_pedido("mocaccino", suscribir=False)
        vistos = []
        otra = ProxyCoordinador("127.0.0.1", self.sistema.coordinador.puerto_clientes,
                                al_notificar=vistos.append).conectar()
        estado_inicial = otra.suscribir(pid)
        self.assertEqual(estado_inicial["pedido_id"], pid)
        self.assertTrue(esperar(lambda: any(e["estado"] == ENTREGADO for e in vistos), timeout=5))
        self.assertEqual(otra.consultar(pid)["estado"], ENTREGADO)
        otra.cerrar()

    def test_suscripcion_a_pedido_inexistente(self):
        with self.assertRaises(ErrorCoordinador) as ctx:
            self.cliente.suscribir(9999)
        self.assertEqual(ctx.exception.codigo, p.PEDIDO_DESCONOCIDO)


class PruebasRobustez(unittest.TestCase):
    def test_caida_de_un_nodo_reasigna_sus_pedidos(self):
        sistema = Sistema(hilos_por_nodo=(2, 2), factor_tiempo=0.1)
        cliente = sistema.cliente()
        try:
            ids = [cliente.hacer_pedido("latte") for _ in range(24)]
            self.assertTrue(esperar(lambda: len(sistema.entregados(ids)) >= 3, timeout=5))
            sistema.nodos[0].detener()  # caída abrupta de nodo-1
            self.assertTrue(esperar(lambda: len(sistema.entregados(ids)) == 24, timeout=15))
            reasignados = [pid for pid in ids
                           if any(e.get("reasignado_desde") == "nodo-1" for e in sistema.eventos[pid])]
            self.assertGreater(len(reasignados), 0)
            for pid in reasignados:
                self.assertEqual(sistema.eventos[pid][-1]["nodo_id"], "nodo-2")
            self.assertFalse(sistema.coordinador.registro.obtener("nodo-1").vivo)
        finally:
            cliente.cerrar()
            sistema.cerrar()

    def test_sin_reasignacion_se_pierden_pedidos(self):
        sistema = Sistema(hilos_por_nodo=(2, 2), reasignar=False, factor_tiempo=0.1)
        cliente = sistema.cliente()
        try:
            ids = [cliente.hacer_pedido("latte") for _ in range(24)]
            self.assertTrue(esperar(lambda: len(sistema.entregados(ids)) >= 3, timeout=5))
            sistema.nodos[0].detener()
            esperar(lambda: len(sistema.entregados(ids)) == 24, timeout=4)
            self.assertLess(len(sistema.entregados(ids)), 24)
        finally:
            cliente.cerrar()
            sistema.cerrar()

    def test_pedidos_esperan_si_no_hay_nodos(self):
        sistema = Sistema(hilos_por_nodo=())
        cliente = sistema.cliente()
        try:
            pid = cliente.hacer_pedido("tinto")
            time.sleep(0.3)
            self.assertEqual(sistema.estados(pid), [])
            sistema.agregar_nodo(1)
            self.assertTrue(esperar(lambda: ENTREGADO in sistema.estados(pid), timeout=5))
        finally:
            cliente.cerrar()
            sistema.cerrar()


class NodoFalso:
    """Se registra en el coordinador pero nunca responde las asignaciones."""

    def __init__(self, puerto_coordinador, latir=True):
        self.escucha = socket.create_server(("127.0.0.1", 0))
        self.conexiones = []
        threading.Thread(target=self._aceptar, daemon=True).start()
        self.control = p.Canal.conectar("127.0.0.1", puerto_coordinador, timeout=3)
        self.control.enviar(p.REGISTRO, nodo_id="colgado", puerto=self.escucha.getsockname()[1],
                            hilos=8, instancia="x")
        assert self.control.recibir()["tipo"] == p.REGISTRO_OK
        self._fin = threading.Event()
        if latir:
            threading.Thread(target=self._latir, daemon=True).start()

    def _aceptar(self):
        while True:
            try:
                conexion, _ = self.escucha.accept()
            except OSError:
                return
            self.conexiones.append(conexion)  # acepta y se queda callado

    def _latir(self):
        while not self._fin.wait(0.1):
            try:
                self.control.enviar(p.LATIDO, nodo_id="colgado")
            except OSError:
                return

    def cerrar(self):
        self._fin.set()
        p.cerrar_escucha(self.escucha)
        for conexion in self.conexiones:
            conexion.close()
        self.control.cerrar()


class PruebasNodoColgado(unittest.TestCase):
    def test_el_circuit_breaker_aisla_al_nodo_colgado(self):
        sistema = Sistema(hilos_por_nodo=(2,), estrategia="round_robin")
        colgado = NodoFalso(sistema.coordinador.puerto_nodos, latir=True)
        cliente = sistema.cliente()
        try:
            ids = [cliente.hacer_pedido("tinto") for _ in range(12)]
            self.assertTrue(esperar(lambda: len(sistema.entregados(ids)) == 12, timeout=15))
            info = sistema.coordinador.registro.obtener("colgado")
            self.assertTrue(info.vivo)  # sigue latiendo...
            self.assertIs(info.breaker.estado, EstadoCircuito.ABIERTO)  # ...pero no recibe trabajo
            self.assertTrue(all(sistema.eventos[pid][-1]["nodo_id"] == "nodo-1" for pid in ids))
        finally:
            cliente.cerrar()
            colgado.cerrar()
            sistema.cerrar()

    def test_nodo_sin_latidos_se_da_por_caido(self):
        sistema = Sistema(hilos_por_nodo=())
        mudo = NodoFalso(sistema.coordinador.puerto_nodos, latir=False)
        try:
            registro = sistema.coordinador.registro
            self.assertTrue(registro.obtener("colgado").vivo)
            self.assertTrue(esperar(lambda: not registro.obtener("colgado").vivo, timeout=3))
        finally:
            mudo.cerrar()
            sistema.cerrar()


if __name__ == "__main__":
    unittest.main()

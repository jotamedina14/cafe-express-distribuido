"""El nodo se prueba contra un coordinador falso: solo depende del protocolo."""
import tempfile
import threading
import time
import unittest
from collections import defaultdict

import protocolo as p
from dominio import EN_PREPARACION, ENTREGADO, LISTO, RECIBIDO
from nodo import ConfigNodo, Nodo
from patrones.proxy import ProxyNodo
from test_proxy import ServidorFalso, esperar


class CoordinadorDeNodos:
    """Acepta el REGISTRO de un nodo y anota sus latidos y cambios de estado."""

    def __init__(self, intervalo_latido=0.1):
        self.intervalo = intervalo_latido
        self.registros = []
        self.latidos = 0
        self.estados = defaultdict(list)
        self.hilos_por_pedido = {}
        self.servidor = ServidorFalso(self.atender)

    def atender(self, canal):
        registro = canal.recibir()
        self.registros.append(registro)
        canal.enviar(p.REGISTRO_OK, nodo_id=registro["nodo_id"], intervalo_latido=self.intervalo)
        while (mensaje := canal.recibir()) is not None:
            if mensaje["tipo"] == p.LATIDO:
                self.latidos += 1
            elif mensaje["tipo"] == p.CAMBIO_ESTADO:
                self.estados[mensaje["pedido_id"]].append(mensaje["estado"])


class PruebasNodo(unittest.TestCase):
    def setUp(self):
        self.coordinador = CoordinadorDeNodos()
        self.logs = tempfile.TemporaryDirectory()
        config = ConfigNodo(nodo_id="nodo-prueba", puerto=0, hilos=3,
                            puerto_coordinador=self.coordinador.servidor.puerto,
                            factor_tiempo=0.05, carpeta_logs=self.logs.name, consola=False)
        self.nodo = Nodo(config).iniciar()
        self.assertTrue(self.nodo.registrado.wait(3))
        self.proxy = ProxyNodo("nodo-prueba", "127.0.0.1", self.nodo.puerto)

    def tearDown(self):
        self.proxy.cerrar()
        self.nodo.detener()
        self.coordinador.servidor.detener()
        self.logs.cleanup()

    def test_se_registra_con_su_capacidad(self):
        registro = self.coordinador.registros[0]
        self.assertEqual((registro["nodo_id"], registro["hilos"], registro["puerto"]),
                         ("nodo-prueba", 3, self.nodo.puerto))

    def test_envia_latidos(self):
        self.assertTrue(esperar(lambda: self.coordinador.latidos >= 3, timeout=2))

    def test_prepara_y_reporta_todo_el_ciclo(self):
        respuesta = self.proxy.asignar({"pedido_id": 1, "producto": "tinto", "cantidad": 1})
        self.assertEqual(respuesta["estado"], RECIBIDO)
        self.assertTrue(esperar(lambda: ENTREGADO in self.coordinador.estados[1]))
        self.assertEqual(self.coordinador.estados[1], [EN_PREPARACION, LISTO, ENTREGADO])

    def test_los_hilos_trabajan_en_paralelo(self):
        # 6 tintos de ~20 ms con 3 hilos: en paralelo ~40 ms, en serie ~120 ms.
        inicio = time.monotonic()
        for pid in range(10, 16):
            self.proxy.asignar({"pedido_id": pid, "producto": "tinto", "cantidad": 1})
        self.assertTrue(esperar(lambda: all(ENTREGADO in self.coordinador.estados[pid]
                                            for pid in range(10, 16))))
        self.assertLess(time.monotonic() - inicio, 0.11)
        self.assertEqual(self.nodo.procesados, 6)

    def test_asignacion_repetida_no_se_prepara_dos_veces(self):
        for _ in range(3):
            self.proxy.asignar({"pedido_id": 7, "producto": "latte", "cantidad": 1})
        self.assertTrue(esperar(lambda: ENTREGADO in self.coordinador.estados[7]))
        time.sleep(0.1)
        self.assertEqual(self.coordinador.estados[7].count(ENTREGADO), 1)

    def test_rechaza_mensajes_que_no_son_asignaciones(self):
        canal = p.Canal.conectar("127.0.0.1", self.nodo.puerto, timeout=2)
        canal.enviar(p.LATIDO, nodo_id="intruso")
        self.assertEqual(canal.recibir()["codigo"], p.MENSAJE_INVALIDO)
        canal.cerrar()

    def test_se_vuelve_a_registrar_si_el_coordinador_corta(self):
        self.coordinador.servidor.cortar_conexiones()
        self.assertTrue(esperar(lambda: len(self.coordinador.registros) >= 2, timeout=5))
        # La instancia no cambia: el coordinador puede distinguirlo de un reinicio.
        self.assertEqual(self.coordinador.registros[0]["instancia"],
                         self.coordinador.registros[1]["instancia"])


class PruebasTrabajoCpu(unittest.TestCase):
    def test_calibra_y_prepara_con_calculo(self):
        coordinador = CoordinadorDeNodos()
        with tempfile.TemporaryDirectory() as logs:
            nodo = Nodo(ConfigNodo(nodo_id="nodo-cpu", puerto=0, hilos=1, trabajo="cpu",
                                   puerto_coordinador=coordinador.servidor.puerto,
                                   factor_tiempo=0.05, carpeta_logs=logs, consola=False)).iniciar()
            self.assertGreater(nodo._iteraciones_por_segundo, 0)
            self.assertTrue(nodo.registrado.wait(3))
            proxy = ProxyNodo("nodo-cpu", "127.0.0.1", nodo.puerto)
            proxy.asignar({"pedido_id": 1, "producto": "tinto", "cantidad": 1})
            self.assertTrue(esperar(lambda: ENTREGADO in coordinador.estados[1]))
            proxy.cerrar()
            nodo.detener()
        coordinador.servidor.detener()


if __name__ == "__main__":
    unittest.main()

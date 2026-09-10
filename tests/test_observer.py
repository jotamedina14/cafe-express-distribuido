import socket
import unittest

import protocolo as p
from patrones.observer import Observador, ObservadorRemoto, SujetoPedidos


class ObservadorFalso(Observador):
    def __init__(self):
        self.eventos = []

    def notificar(self, evento):
        self.eventos.append(evento)


class PruebasSujeto(unittest.TestCase):
    def setUp(self):
        self.sujeto = SujetoPedidos()
        self.a, self.b = ObservadorFalso(), ObservadorFalso()

    def test_notifica_solo_a_los_suscritos_del_pedido(self):
        self.sujeto.suscribir(1, self.a)
        self.sujeto.suscribir(2, self.b)
        self.assertEqual(self.sujeto.notificar(1, {"pedido_id": 1, "estado": "recibido"}), 1)
        self.assertEqual(len(self.a.eventos), 1)
        self.assertEqual(self.b.eventos, [])

    def test_varios_observadores_del_mismo_pedido(self):
        self.sujeto.suscribir(1, self.a)
        self.sujeto.suscribir(1, self.b)
        self.sujeto.suscribir(1, self.b)  # suscribirse dos veces no duplica avisos
        self.sujeto.notificar(1, {"pedido_id": 1, "estado": "listo"})
        self.assertEqual((len(self.a.eventos), len(self.b.eventos)), (1, 1))

    def test_entregado_cierra_las_suscripciones(self):
        self.sujeto.suscribir(1, self.a)
        self.sujeto.notificar(1, {"pedido_id": 1, "estado": "entregado"})
        self.assertEqual(self.sujeto.suscriptores(1), 0)

    def test_retirar_observador(self):
        self.sujeto.suscribir(1, self.a)
        self.sujeto.suscribir(2, self.a)
        self.sujeto.retirar(self.a)
        self.assertEqual(self.sujeto.notificar(1, {"estado": "listo"}), 0)
        self.assertEqual(self.sujeto.suscriptores(2), 0)

    def test_descarta_observadores_inactivos(self):
        self.sujeto.suscribir(1, self.a)
        self.a.activo = False
        self.assertEqual(self.sujeto.notificar(1, {"estado": "listo"}), 0)


class PruebasObservadorRemoto(unittest.TestCase):
    def test_la_notificacion_viaja_por_el_socket(self):
        a, b = socket.socketpair()
        servidor, cliente = p.Canal(a), p.Canal(b)
        observador = ObservadorRemoto(servidor, "prueba")
        try:
            observador.notificar({"pedido_id": 7, "estado": "listo"})
            mensaje = cliente.recibir()
            self.assertEqual(mensaje, {"tipo": p.NOTIFICACION, "pedido_id": 7, "estado": "listo"})
        finally:
            observador.cerrar()
            servidor.cerrar()
            cliente.cerrar()

    def test_cliente_caido_lo_desactiva(self):
        a, b = socket.socketpair()
        servidor, cliente = p.Canal(a), p.Canal(b)
        observador = ObservadorRemoto(servidor, "prueba")
        cliente.cerrar()
        for i in range(50):
            observador.notificar({"pedido_id": i, "estado": "listo", "relleno": "x" * 2000})
        observador._escritor.join(timeout=5)
        self.assertFalse(observador.activo)
        servidor.cerrar()


if __name__ == "__main__":
    unittest.main()

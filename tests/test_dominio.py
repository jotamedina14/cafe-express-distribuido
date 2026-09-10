import unittest

from dominio import (ENTREGADO, EN_PREPARACION, LISTO, PENDIENTE, RECIBIDO,
                     Pedido, tiempo_preparacion, validar_pedido)


class PruebasPedido(unittest.TestCase):
    def setUp(self):
        self.pedido = Pedido(pedido_id=1, producto="latte")
        self.pedido.asignar("nodo-1")

    def test_ciclo_de_vida_completo(self):
        for estado in (RECIBIDO, EN_PREPARACION, LISTO, ENTREGADO):
            self.assertTrue(self.pedido.avanzar(estado, "nodo-1"))
        self.assertTrue(self.pedido.terminado)
        self.assertEqual([h[0] for h in self.pedido.historial],
                         [RECIBIDO, EN_PREPARACION, LISTO, ENTREGADO])

    def test_no_retrocede(self):
        self.pedido.avanzar(EN_PREPARACION, "nodo-1")
        self.assertFalse(self.pedido.avanzar(RECIBIDO, "nodo-1"))
        self.assertEqual(self.pedido.estado, EN_PREPARACION)

    def test_puede_saltarse_un_estado_que_llego_tarde(self):
        self.assertTrue(self.pedido.avanzar(EN_PREPARACION, "nodo-1"))

    def test_descarta_mensajes_de_otro_nodo(self):
        self.assertFalse(self.pedido.avanzar(RECIBIDO, "nodo-2"))
        self.assertEqual(self.pedido.estado, PENDIENTE)

    def test_liberar_permite_reasignar(self):
        self.pedido.avanzar(EN_PREPARACION, "nodo-1")
        self.pedido.liberar()
        self.assertEqual((self.pedido.estado, self.pedido.nodo_id), (PENDIENTE, None))
        self.assertEqual(self.pedido.reasignaciones, 1)
        # El nodo viejo ya no puede mover el pedido; el nuevo sí.
        self.assertFalse(self.pedido.avanzar(LISTO, "nodo-1"))
        self.pedido.asignar("nodo-2")
        self.assertTrue(self.pedido.avanzar(RECIBIDO, "nodo-2"))

    def test_estado_desconocido(self):
        with self.assertRaises(ValueError):
            self.pedido.avanzar("quemado", "nodo-1")


class PruebasMenu(unittest.TestCase):
    def test_validacion(self):
        self.assertIsNone(validar_pedido("tinto", 1))
        self.assertIsNotNone(validar_pedido("cerveza", 1))
        self.assertIsNotNone(validar_pedido("tinto", 0))
        self.assertIsNotNone(validar_pedido("tinto", "2"))
        self.assertIsNotNone(validar_pedido("tinto", True))

    def test_tiempo_proporcional_a_la_cantidad(self):
        self.assertAlmostEqual(tiempo_preparacion("latte", 3), 3 * tiempo_preparacion("latte"))


if __name__ == "__main__":
    unittest.main()

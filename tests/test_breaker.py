import unittest

from patrones.breaker import CircuitBreaker, CircuitoAbierto, EstadoCircuito


class RelojFalso:
    def __init__(self):
        self.ahora = 1000.0

    def __call__(self):
        return self.ahora

    def avanzar(self, segundos):
        self.ahora += segundos


def falla():
    raise ConnectionRefusedError("nodo caído")


def funciona():
    return "ok"


class PruebasCircuitBreaker(unittest.TestCase):
    def setUp(self):
        self.reloj = RelojFalso()
        self.cambios = []
        self.breaker = CircuitBreaker("nodo-1", umbral_fallos=3, tiempo_reintento=10,
                                      reloj=self.reloj,
                                      al_cambiar=lambda n, a, b, m: self.cambios.append((a, b)))

    def fallar(self, veces):
        for _ in range(veces):
            with self.assertRaises(ConnectionRefusedError):
                self.breaker.llamar(falla)

    def test_empieza_cerrado_y_deja_pasar(self):
        self.assertEqual(self.breaker.llamar(funciona), "ok")
        self.assertIs(self.breaker.estado, EstadoCircuito.CERRADO)

    def test_se_abre_tras_tres_fallos_seguidos(self):
        self.fallar(2)
        self.assertIs(self.breaker.estado, EstadoCircuito.CERRADO)
        self.fallar(1)
        self.assertIs(self.breaker.estado, EstadoCircuito.ABIERTO)
        self.assertFalse(self.breaker.permite())

    def test_un_exito_reinicia_la_cuenta(self):
        self.fallar(2)
        self.breaker.llamar(funciona)
        self.fallar(2)
        self.assertIs(self.breaker.estado, EstadoCircuito.CERRADO)

    def test_abierto_rechaza_sin_llamar(self):
        self.fallar(3)
        llamadas = []
        with self.assertRaises(CircuitoAbierto):
            self.breaker.llamar(lambda: llamadas.append(1))
        self.assertEqual(llamadas, [])

    def test_medio_abierto_tras_el_tiempo_y_se_cierra_si_la_prueba_sale_bien(self):
        self.fallar(3)
        self.reloj.avanzar(9.9)
        self.assertFalse(self.breaker.permite())
        self.reloj.avanzar(0.2)
        self.assertTrue(self.breaker.permite())
        self.assertEqual(self.breaker.llamar(funciona), "ok")
        self.assertIs(self.breaker.estado, EstadoCircuito.CERRADO)
        self.assertEqual(self.cambios, [
            (EstadoCircuito.CERRADO, EstadoCircuito.ABIERTO),
            (EstadoCircuito.ABIERTO, EstadoCircuito.MEDIO_ABIERTO),
            (EstadoCircuito.MEDIO_ABIERTO, EstadoCircuito.CERRADO),
        ])

    def test_si_la_prueba_falla_se_reabre_y_reinicia_la_espera(self):
        self.fallar(3)
        self.reloj.avanzar(10)
        self.fallar(1)
        self.assertIs(self.breaker.estado, EstadoCircuito.ABIERTO)
        self.reloj.avanzar(5)
        self.assertFalse(self.breaker.permite())

    def test_solo_una_prueba_a_la_vez(self):
        self.fallar(3)
        self.reloj.avanzar(10)

        def prueba_lenta():
            # Mientras la prueba está en curso nadie más puede pasar.
            self.assertFalse(self.breaker.permite())
            with self.assertRaises(CircuitoAbierto):
                self.breaker.llamar(funciona)
            return "ok"

        self.assertEqual(self.breaker.llamar(prueba_lenta), "ok")


if __name__ == "__main__":
    unittest.main()

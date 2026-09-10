import socket
import threading
import unittest

import protocolo as p


class PruebasCodificacion(unittest.TestCase):
    def test_ida_y_vuelta(self):
        linea = p.codificar(p.PEDIDO_NUEVO, producto="latte", cantidad=2, sucursal="Café Norte")
        self.assertTrue(linea.endswith(b"\n"))
        self.assertEqual(linea.count(b"\n"), 1)
        mensaje = p.decodificar(linea)
        self.assertEqual(mensaje, {"tipo": p.PEDIDO_NUEVO, "producto": "latte",
                                   "cantidad": 2, "sucursal": "Café Norte"})

    def test_rechaza_tipo_desconocido_al_codificar(self):
        with self.assertRaises(p.ErrorProtocolo):
            p.codificar("HOLA")

    def test_rechaza_json_invalido(self):
        with self.assertRaises(p.ErrorProtocolo):
            p.decodificar(b"{no es json\n")

    def test_rechaza_mensaje_sin_tipo_valido(self):
        with self.assertRaises(p.ErrorProtocolo):
            p.decodificar(b'{"producto":"latte"}\n')
        with self.assertRaises(p.ErrorProtocolo):
            p.decodificar(b'["LATIDO"]\n')

    def test_hay_diez_tipos_de_mensaje(self):
        self.assertEqual(len(p.TIPOS), 10)


class PruebasCanal(unittest.TestCase):
    def setUp(self):
        a, b = socket.socketpair()
        self.emisor, self.receptor = p.Canal(a), p.Canal(b)

    def tearDown(self):
        self.emisor.cerrar()
        self.receptor.cerrar()

    def test_envia_y_recibe_en_orden(self):
        for i in range(5):
            self.emisor.enviar(p.LATIDO, nodo_id="nodo-1", secuencia=i)
        recibidos = [self.receptor.recibir()["secuencia"] for _ in range(5)]
        self.assertEqual(recibidos, list(range(5)))

    def test_envios_concurrentes_no_se_mezclan(self):
        hilos, por_hilo = 8, 200

        def enviar(k):
            for i in range(por_hilo):
                self.emisor.enviar(p.CAMBIO_ESTADO, hilo=k, i=i, relleno="x" * 300)

        lector_resultado = []

        def leer():
            for _ in range(hilos * por_hilo):
                lector_resultado.append(self.receptor.recibir())

        lector = threading.Thread(target=leer)
        lector.start()
        emisores = [threading.Thread(target=enviar, args=(k,)) for k in range(hilos)]
        for h in emisores:
            h.start()
        for h in emisores:
            h.join()
        lector.join(timeout=10)
        self.assertEqual(len(lector_resultado), hilos * por_hilo)
        # Dentro de cada hilo emisor el orden se conserva.
        for k in range(hilos):
            secuencia = [m["i"] for m in lector_resultado if m["hilo"] == k]
            self.assertEqual(secuencia, list(range(por_hilo)))

    def test_recibir_devuelve_none_al_cerrar_el_otro_extremo(self):
        self.emisor.cerrar()
        self.assertIsNone(self.receptor.recibir())

    def test_ignora_lineas_vacias(self):
        self.emisor.sock.sendall(b"\n\n" + p.codificar(p.LATIDO, nodo_id="n"))
        self.assertEqual(self.receptor.recibir()["tipo"], p.LATIDO)

    def test_rechaza_mensajes_demasiado_largos(self):
        def enviar_gigante():
            try:
                self.emisor.sock.sendall(b"a" * (p.LIMITE_MENSAJE + 10) + b"\n")
            except OSError:
                pass

        hilo = threading.Thread(target=enviar_gigante)
        hilo.start()
        with self.assertRaises(p.ErrorProtocolo):
            self.receptor.recibir()
        self.receptor.cerrar()
        hilo.join(timeout=5)


if __name__ == "__main__":
    unittest.main()

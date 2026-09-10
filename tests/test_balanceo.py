import unittest
from collections import Counter
from dataclasses import dataclass

from patrones.balanceo import MenorCarga, RoundRobin, crear_estrategia


@dataclass
class NodoFalso:
    nodo_id: str
    hilos: int
    carga: int = 0


def repartir(estrategia, nodos, pedidos):
    """Simula al despachador: elige un nodo por pedido y le suma carga."""
    conteo = Counter()
    for _ in range(pedidos):
        nodo = estrategia.elegir(nodos)
        nodo.carga += 1
        conteo[nodo.nodo_id] += 1
    return conteo


class PruebasRoundRobin(unittest.TestCase):
    def test_rota_en_orden(self):
        nodos = [NodoFalso("nodo-2", 4), NodoFalso("nodo-1", 4), NodoFalso("nodo-3", 4)]
        rr = RoundRobin()
        self.assertEqual([rr.elegir(nodos).nodo_id for _ in range(6)],
                         ["nodo-1", "nodo-2", "nodo-3"] * 2)

    def test_ignora_la_capacidad(self):
        nodos = [NodoFalso("lento", 2), NodoFalso("rapido-1", 4), NodoFalso("rapido-2", 4)]
        conteo = repartir(RoundRobin(), nodos, 30)
        self.assertEqual(set(conteo.values()), {10})

    def test_sin_nodos(self):
        self.assertIsNone(RoundRobin().elegir([]))


class PruebasMenorCarga(unittest.TestCase):
    def test_reparte_en_proporcion_a_los_hilos(self):
        nodos = [NodoFalso("lento", 2), NodoFalso("rapido-1", 4), NodoFalso("rapido-2", 4)]
        conteo = repartir(MenorCarga(), nodos, 100)
        self.assertEqual(conteo["lento"], 20)
        self.assertEqual(conteo["rapido-1"], 40)
        self.assertEqual(conteo["rapido-2"], 40)

    def test_prefiere_el_nodo_desocupado(self):
        nodos = [NodoFalso("a", 4, carga=3), NodoFalso("b", 4, carga=0)]
        self.assertEqual(MenorCarga().elegir(nodos).nodo_id, "b")


class PruebasFabrica(unittest.TestCase):
    def test_crea_por_nombre(self):
        self.assertIsInstance(crear_estrategia("round_robin"), RoundRobin)
        self.assertIsInstance(crear_estrategia("menor_carga"), MenorCarga)
        with self.assertRaises(ValueError):
            crear_estrategia("azar")


if __name__ == "__main__":
    unittest.main()

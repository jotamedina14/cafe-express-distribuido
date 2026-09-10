"""Patrón Strategy: política de balanceo de carga entre nodos.

El despachador del coordinador solo conoce la interfaz EstrategiaBalanceo;
qué nodo se elige depende de la estrategia inyectada al arrancar
(--estrategia), sin tocar el código del despachador.

Una estrategia recibe la lista de nodos disponibles. De cada nodo usa:
  nodo_id  identificador
  hilos    capacidad (tamaño de su grupo de hilos)
  carga    pedidos asignados que aún no se han entregado
"""
from __future__ import annotations

import threading
from abc import ABC, abstractmethod


class EstrategiaBalanceo(ABC):
    nombre = "abstracta"

    @abstractmethod
    def elegir(self, nodos: list) -> object | None:
        """Devuelve el nodo que debe recibir el siguiente pedido (o None)."""


class RoundRobin(EstrategiaBalanceo):
    """Turno rotatorio: reparte por igual, sin mirar capacidad ni carga.

    Justa cuando todos los nodos son iguales; con nodos de distinta
    capacidad satura al más lento.
    """

    nombre = "round_robin"

    def __init__(self):
        self._turno = 0
        self._candado = threading.Lock()

    def elegir(self, nodos):
        if not nodos:
            return None
        ordenados = sorted(nodos, key=lambda n: n.nodo_id)
        with self._candado:
            elegido = ordenados[self._turno % len(ordenados)]
            self._turno += 1
        return elegido


class MenorCarga(EstrategiaBalanceo):
    """Menor carga relativa: el nodo con menos pedidos pendientes por hilo.

    Normalizar por hilos hace que un nodo de 4 hilos reciba el doble que uno
    de 2, es decir, reparte en proporción a la capacidad real.
    """

    nombre = "menor_carga"

    def elegir(self, nodos):
        if not nodos:
            return None
        return min(nodos, key=lambda n: (n.carga / max(n.hilos, 1), n.carga, n.nodo_id))


ESTRATEGIAS = {cls.nombre: cls for cls in (RoundRobin, MenorCarga)}


def crear_estrategia(nombre: str) -> EstrategiaBalanceo:
    try:
        return ESTRATEGIAS[nombre]()
    except KeyError:
        raise ValueError(f"estrategia desconocida: {nombre!r}. "
                         f"Opciones: {', '.join(ESTRATEGIAS)}") from None

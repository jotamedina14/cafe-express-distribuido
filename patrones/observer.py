"""Patrón Observer distribuido: estado de los pedidos en tiempo real.

El cliente no pregunta en bucle "¿ya está mi pedido?". Se suscribe una vez y
el coordinador le avisa en cada cambio. Es el Observer clásico, pero:

  - el Sujeto (SujetoPedidos) vive en el coordinador,
  - cada Observador remoto (ObservadorRemoto) representa a un cliente que
    está al otro lado de un socket, y la notificación viaja por la red.

Cada ObservadorRemoto tiene su propia cola y su propio hilo escritor, de modo
que un cliente lento o caído nunca bloquea al hilo que notifica.
"""
from __future__ import annotations

import logging
import queue
import threading
from abc import ABC, abstractmethod

import protocolo as p
from dominio import ENTREGADO


class Observador(ABC):
    activo = True

    @abstractmethod
    def notificar(self, evento: dict) -> None:
        """Recibe un cambio de estado de un pedido."""


class ObservadorRemoto(Observador):
    """Un cliente conectado. Todo lo que se le envía pasa por su cola."""

    _FIN = object()

    def __init__(self, canal: p.Canal, nombre: str = "cliente", log: logging.Logger | None = None):
        self.canal = canal
        self.nombre = nombre
        self.activo = True
        self._log = log or logging.getLogger(__name__)
        self._cola: queue.Queue = queue.Queue()
        self._escritor = threading.Thread(target=self._escribir, name=f"escritor-{nombre}", daemon=True)
        self._escritor.start()

    def enviar(self, tipo: str, **datos) -> None:
        if self.activo:
            self._cola.put((tipo, datos))

    def notificar(self, evento: dict) -> None:
        self.enviar(p.NOTIFICACION, **evento)

    def cerrar(self) -> None:
        self.activo = False
        self._cola.put(self._FIN)

    def _escribir(self) -> None:
        while True:
            elemento = self._cola.get()
            if elemento is self._FIN:
                return
            tipo, datos = elemento
            try:
                self.canal.enviar(tipo, **datos)
            except OSError as exc:
                self._log.debug("No se pudo notificar a %s: %s", self.nombre, exc)
                self.activo = False
                return


class SujetoPedidos:
    """Registro de suscripciones: pedido -> observadores interesados."""

    def __init__(self):
        self._suscripciones: dict[int, list[Observador]] = {}
        self._candado = threading.Lock()

    def suscribir(self, pedido_id: int, observador: Observador) -> None:
        with self._candado:
            lista = self._suscripciones.setdefault(pedido_id, [])
            if observador not in lista:
                lista.append(observador)

    def retirar(self, observador: Observador) -> None:
        """Quita todas las suscripciones de un observador (se desconectó)."""
        with self._candado:
            for pedido_id in list(self._suscripciones):
                lista = self._suscripciones[pedido_id]
                if observador in lista:
                    lista.remove(observador)
                if not lista:
                    del self._suscripciones[pedido_id]

    def suscriptores(self, pedido_id: int) -> int:
        with self._candado:
            return len(self._suscripciones.get(pedido_id, ()))

    def notificar(self, pedido_id: int, evento: dict) -> int:
        """Avisa a los observadores del pedido. Devuelve a cuántos avisó."""
        with self._candado:
            lista = [o for o in self._suscripciones.get(pedido_id, ()) if o.activo]
            if evento.get("estado") == ENTREGADO or not lista:
                # Fin del ciclo de vida: no habrá más avisos de este pedido.
                self._suscripciones.pop(pedido_id, None)
            else:
                self._suscripciones[pedido_id] = lista
        for observador in lista:
            observador.notificar(evento)
        return len(lista)

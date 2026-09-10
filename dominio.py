"""Capa de dominio: el menú, los estados de un pedido y sus reglas.

No sabe nada de sockets ni de hilos. La usan el coordinador (para validar y
seguir cada pedido) y los nodos (para saber cuánto tarda cada preparación).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

# --- Estados ------------------------------------------------------------------

PENDIENTE = "pendiente"            # interno del coordinador: aún sin nodo que lo confirme
RECIBIDO = "recibido"              # el nodo lo tiene en su cola
EN_PREPARACION = "en_preparacion"  # un hilo del nodo lo está preparando
LISTO = "listo"                    # preparado, en la barra
ENTREGADO = "entregado"            # entregado al cliente; fin del ciclo de vida

ESTADOS = (RECIBIDO, EN_PREPARACION, LISTO, ENTREGADO)
_ORDEN = {PENDIENTE: 0, RECIBIDO: 1, EN_PREPARACION: 2, LISTO: 3, ENTREGADO: 4}

# --- Menú ---------------------------------------------------------------------

# Segundos base de preparación por unidad de cada producto.
MENU = {
    "tinto": 0.4,
    "americano": 0.5,
    "chocolate": 0.6,
    "capuchino": 0.8,
    "latte": 0.8,
    "mocaccino": 1.0,
}
TIEMPO_ENTREGA = 0.05  # segundos entre "listo" y "entregado"
CANTIDAD_MAXIMA = 20


def tiempo_preparacion(producto: str, cantidad: int = 1) -> float:
    return MENU[producto] * cantidad


def validar_pedido(producto: object, cantidad: object) -> str | None:
    """Devuelve el motivo del rechazo, o None si el pedido es válido."""
    if producto not in MENU:
        return f"producto desconocido: {producto!r}. Menú: {', '.join(MENU)}"
    if not isinstance(cantidad, int) or isinstance(cantidad, bool) \
            or not 1 <= cantidad <= CANTIDAD_MAXIMA:
        return f"la cantidad debe ser un entero entre 1 y {CANTIDAD_MAXIMA}"
    return None


# --- Pedido -------------------------------------------------------------------

@dataclass
class Pedido:
    pedido_id: int
    producto: str
    cantidad: int = 1
    sucursal: str = ""
    estado: str = PENDIENTE
    nodo_id: str | None = None
    reasignaciones: int = 0
    creado: float = field(default_factory=time.time)
    historial: list = field(default_factory=list)  # [(estado, marca de tiempo, nodo)]

    @property
    def terminado(self) -> bool:
        return self.estado == ENTREGADO

    def asignar(self, nodo_id: str) -> None:
        self.nodo_id = nodo_id

    def avanzar(self, nuevo_estado: str, nodo_id: str) -> list[str]:
        """Aplica un cambio de estado reportado por un nodo.

        Solo se acepta si (1) viene del nodo que tiene asignado el pedido y
        (2) mueve el pedido hacia adelante. Así se descartan los mensajes
        tardíos de un nodo al que ya se le quitó el pedido, y los que llegan
        desordenados por viajar en conexiones distintas.

        Los estados son una secuencia estricta: si llega "en_preparacion"
        antes que "recibido", es que "recibido" ya ocurrió. Por eso devuelve
        la lista de estados recorridos, incluidos los que se saltaron, para
        que el cliente reciba siempre el ciclo completo y en orden. Una
        lista vacía significa que el cambio se descartó.
        """
        if nuevo_estado not in ESTADOS:
            raise ValueError(f"estado desconocido: {nuevo_estado!r}")
        if nodo_id != self.nodo_id:
            return []
        actual, objetivo = _ORDEN[self.estado], _ORDEN[nuevo_estado]
        if objetivo <= actual:
            return []
        recorridos = [e for e in ESTADOS if actual < _ORDEN[e] <= objetivo]
        ahora = time.time()
        self.historial.extend((estado, ahora, nodo_id) for estado in recorridos)
        self.estado = nuevo_estado
        return recorridos

    def liberar(self) -> None:
        """Devuelve el pedido a pendiente para reasignarlo a otro nodo."""
        self.estado = PENDIENTE
        self.nodo_id = None
        self.reasignaciones += 1

    def datos_asignacion(self) -> dict:
        """Lo que viaja al nodo en el mensaje ASIGNACION."""
        return {
            "pedido_id": self.pedido_id,
            "producto": self.producto,
            "cantidad": self.cantidad,
            "sucursal": self.sucursal,
        }

    def evento(self, **extra) -> dict:
        """Lo que viaja al cliente en una NOTIFICACION."""
        return {
            "pedido_id": self.pedido_id,
            "producto": self.producto,
            "cantidad": self.cantidad,
            "estado": self.estado,
            "nodo_id": self.nodo_id,
            "reasignaciones": self.reasignaciones,
            **extra,
        }

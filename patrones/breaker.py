"""Patrón Circuit Breaker: deja de insistirle a un nodo que está fallando.

Sin él, cada pedido asignado a un nodo colgado cuesta un timeout completo
y frena al despachador entero. Con él, tras `umbral_fallos` fallos seguidos
el circuito se abre y el nodo deja de recibir trabajo; pasado
`tiempo_reintento` se permite UNA llamada de prueba (medio abierto): si
sale bien el circuito se cierra, si falla se vuelve a abrir.

    CERRADO --(3 fallos seguidos)--> ABIERTO --(10 s)--> MEDIO_ABIERTO
       ^                                ^                     |
       +-------(prueba exitosa)---------+----(prueba falla)---+
"""
from __future__ import annotations

import threading
import time
from enum import Enum


class EstadoCircuito(str, Enum):
    CERRADO = "cerrado"
    ABIERTO = "abierto"
    MEDIO_ABIERTO = "medio_abierto"


class CircuitoAbierto(Exception):
    """Se rechazó la llamada sin intentarla porque el circuito está abierto."""


class CircuitBreaker:
    def __init__(self, nombre: str = "", umbral_fallos: int = 3,
                 tiempo_reintento: float = 10.0, reloj=time.monotonic, al_cambiar=None):
        self.nombre = nombre
        self.umbral_fallos = umbral_fallos
        self.tiempo_reintento = tiempo_reintento
        self._reloj = reloj
        self._al_cambiar = al_cambiar  # callback(nombre, anterior, nuevo, motivo)
        self._estado = EstadoCircuito.CERRADO
        self._fallos = 0
        self._abierto_desde = 0.0
        self._prueba_en_curso = False
        self._candado = threading.Lock()

    @property
    def estado(self) -> EstadoCircuito:
        with self._candado:
            return self._estado

    @property
    def fallos(self) -> int:
        with self._candado:
            return self._fallos

    def permite(self) -> bool:
        """¿Se podría intentar una llamada ahora? Consulta sin cambiar el estado."""
        with self._candado:
            if self._estado is EstadoCircuito.CERRADO:
                return True
            if self._estado is EstadoCircuito.ABIERTO:
                return self._reloj() - self._abierto_desde >= self.tiempo_reintento
            return not self._prueba_en_curso

    def llamar(self, funcion, *args, **kwargs):
        """Ejecuta `funcion` protegida por el circuito.

        Lanza CircuitoAbierto sin ejecutarla si el circuito no lo permite.
        Cualquier excepción de `funcion` cuenta como fallo y se propaga.
        """
        self._antes_de_llamar()
        try:
            resultado = funcion(*args, **kwargs)
        except Exception:
            self.registrar_fallo()
            raise
        self.registrar_exito()
        return resultado

    def registrar_exito(self) -> None:
        with self._candado:
            self._fallos = 0
            self._prueba_en_curso = False
            cambio = self._cambiar(EstadoCircuito.CERRADO, "llamada exitosa")
        self._avisar(cambio)

    def registrar_fallo(self) -> None:
        cambio = None
        with self._candado:
            self._fallos += 1
            self._prueba_en_curso = False
            if self._estado is EstadoCircuito.MEDIO_ABIERTO:
                cambio = self._abrir("falló la llamada de prueba")
            elif self._estado is EstadoCircuito.CERRADO and self._fallos >= self.umbral_fallos:
                cambio = self._abrir(f"{self._fallos} fallos consecutivos")
        self._avisar(cambio)

    # --- internos (se llaman con el candado tomado) ---------------------------

    def _antes_de_llamar(self) -> None:
        cambio = None
        with self._candado:
            if self._estado is EstadoCircuito.ABIERTO:
                restante = self.tiempo_reintento - (self._reloj() - self._abierto_desde)
                if restante > 0:
                    raise CircuitoAbierto(f"circuito hacia {self.nombre} abierto "
                                          f"(reintento en {restante:.1f} s)")
                cambio = self._cambiar(EstadoCircuito.MEDIO_ABIERTO, "fin del tiempo de espera")
            if self._estado is EstadoCircuito.MEDIO_ABIERTO:
                if self._prueba_en_curso:
                    raise CircuitoAbierto(f"circuito hacia {self.nombre}: prueba en curso")
                self._prueba_en_curso = True
        self._avisar(cambio)

    def _abrir(self, motivo: str):
        self._abierto_desde = self._reloj()
        return self._cambiar(EstadoCircuito.ABIERTO, motivo)

    def _cambiar(self, nuevo: EstadoCircuito, motivo: str):
        if nuevo is self._estado:
            return None
        anterior, self._estado = self._estado, nuevo
        return anterior, nuevo, motivo

    def _avisar(self, cambio) -> None:
        # El callback se invoca fuera del candado para que pueda consultar
        # el breaker sin bloquearse.
        if cambio and self._al_cambiar:
            self._al_cambiar(self.nombre, *cambio)

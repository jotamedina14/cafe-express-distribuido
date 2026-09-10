"""Contrato de comunicación de CaféExpress Distribuido.

Cliente, coordinador y nodos se hablan con mensajes JSON, uno por línea,
sobre TCP. Este módulo es lo único que comparten: el cliente y el nodo no se
conocen entre sí, solo conocen este contrato. Por eso cualquiera de los tres
puede reescribirse (incluso en otro lenguaje) sin tocar a los demás.

Formato de un mensaje en el cable:

    {"tipo":"PEDIDO_NUEVO","ref":"9f1c...","producto":"latte","cantidad":1}\\n
"""
from __future__ import annotations

import json
import socket
import threading

# --- Parámetros por defecto de la red ---------------------------------------

PUERTO_CLIENTES = 5000    # el coordinador atiende a los clientes aquí
PUERTO_NODOS = 5001       # el coordinador atiende a los nodos aquí
INTERVALO_LATIDO = 2.0    # segundos entre latidos de un nodo
TIMEOUT_LATIDO = 6.0      # sin latido en este tiempo, el nodo se da por caído
LIMITE_MENSAJE = 64 * 1024  # bytes; una línea más larga se rechaza

# --- Tipos de mensaje ---------------------------------------------------------

REGISTRO = "REGISTRO"                # nodo -> coordinador: me anuncio con mi capacidad
REGISTRO_OK = "REGISTRO_OK"          # coordinador -> nodo: quedaste registrado
LATIDO = "LATIDO"                    # nodo -> coordinador: sigo vivo
PEDIDO_NUEVO = "PEDIDO_NUEVO"        # cliente -> coordinador
PEDIDO_ACEPTADO = "PEDIDO_ACEPTADO"  # coordinador -> cliente, con el id asignado
ASIGNACION = "ASIGNACION"            # coordinador -> nodo elegido
CAMBIO_ESTADO = "CAMBIO_ESTADO"      # nodo -> coordinador
SUSCRIPCION = "SUSCRIPCION"          # cliente -> coordinador: avísame de este pedido
NOTIFICACION = "NOTIFICACION"        # coordinador -> cliente suscrito
ERROR = "ERROR"                      # cualquiera -> cualquiera, con código y motivo

TIPOS = frozenset({
    REGISTRO, REGISTRO_OK, LATIDO, PEDIDO_NUEVO, PEDIDO_ACEPTADO,
    ASIGNACION, CAMBIO_ESTADO, SUSCRIPCION, NOTIFICACION, ERROR,
})

# --- Códigos de error ---------------------------------------------------------

MENSAJE_INVALIDO = "MENSAJE_INVALIDO"
PRODUCTO_INVALIDO = "PRODUCTO_INVALIDO"
CANTIDAD_INVALIDA = "CANTIDAD_INVALIDA"
PEDIDO_DESCONOCIDO = "PEDIDO_DESCONOCIDO"
NODO_NO_REGISTRADO = "NODO_NO_REGISTRADO"


class ErrorProtocolo(ValueError):
    """El mensaje recibido no respeta el contrato."""


def codificar(tipo: str, **datos) -> bytes:
    """Serializa un mensaje a una línea JSON terminada en salto de línea."""
    if tipo not in TIPOS:
        raise ErrorProtocolo(f"tipo de mensaje desconocido: {tipo!r}")
    mensaje = {"tipo": tipo, **datos}
    linea = json.dumps(mensaje, ensure_ascii=False, separators=(",", ":"))
    return (linea + "\n").encode("utf-8")


def decodificar(linea: bytes | str) -> dict:
    """Convierte una línea recibida en un diccionario y valida su tipo."""
    if isinstance(linea, bytes):
        linea = linea.decode("utf-8", errors="replace")
    try:
        mensaje = json.loads(linea)
    except json.JSONDecodeError as exc:
        raise ErrorProtocolo(f"JSON inválido: {exc.msg}") from exc
    if not isinstance(mensaje, dict):
        raise ErrorProtocolo("el mensaje debe ser un objeto JSON")
    if mensaje.get("tipo") not in TIPOS:
        raise ErrorProtocolo(f"tipo de mensaje desconocido: {mensaje.get('tipo')!r}")
    return mensaje


class Canal:
    """Un socket TCP que habla el protocolo.

    enviar() es seguro entre hilos: varios hilos pueden escribir en el mismo
    canal sin que se mezclen los bytes de dos mensajes. recibir() debe
    llamarse desde un solo hilo (el lector de esa conexión).
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        try:
            # Mensajes pequeños de ida y vuelta: sin el algoritmo de Nagle se
            # evitan esperas artificiales de hasta 40 ms por mensaje.
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        self._lector = sock.makefile("rb")
        self._candado = threading.Lock()
        self.cerrado = False

    @classmethod
    def conectar(cls, host: str, puerto: int, timeout: float | None = None) -> "Canal":
        """Abre una conexión. Con timeout, aplica también a envíos y lecturas."""
        return cls(socket.create_connection((host, puerto), timeout=timeout))

    def enviar(self, tipo: str, **datos) -> None:
        datos_cable = codificar(tipo, **datos)
        with self._candado:
            self.sock.sendall(datos_cable)

    def recibir(self) -> dict | None:
        """Bloquea hasta el siguiente mensaje; None si el otro extremo cerró."""
        while True:
            linea = self._lector.readline(LIMITE_MENSAJE + 1)
            if not linea:
                return None
            if len(linea) > LIMITE_MENSAJE:
                raise ErrorProtocolo(f"mensaje de más de {LIMITE_MENSAJE} bytes")
            if linea.strip():
                return decodificar(linea)

    def cerrar(self) -> None:
        """Cierra la conexión y despierta a cualquier hilo bloqueado leyendo."""
        self.cerrado = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        for recurso in (self._lector, self.sock):
            try:
                recurso.close()
            except OSError:
                pass

    def __enter__(self) -> "Canal":
        return self

    def __exit__(self, *_) -> None:
        self.cerrar()

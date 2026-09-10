"""Patrón Proxy remoto: hablar con un componente de la red como si fuera local.

    ProxyCoordinador  lo usa el cliente. hacer_pedido() y suscribir() parecen
                      métodos normales; por dentro hay socket, serialización,
                      correlación de respuestas, timeouts, reintentos y
                      reconexión automática con re-suscripción.

    ProxyNodo         lo usa el coordinador. asignar() es una llamada remota
                      al nodo sobre una conexión persistente que se reabre si
                      el nodo se reinició.

Sin estos proxies, la lógica de red se filtraría a la interfaz del cliente y
al despachador del coordinador.
"""
from __future__ import annotations

import logging
import socket
import threading
import time
import uuid

import protocolo as p
from dominio import ENTREGADO


class ErrorCoordinador(Exception):
    """El coordinador respondió con un mensaje ERROR (p. ej., producto inválido)."""

    def __init__(self, codigo: str | None, motivo: str | None):
        super().__init__(f"{codigo}: {motivo}")
        self.codigo = codigo
        self.motivo = motivo


class _Espera:
    """Una solicitud enviada que aún espera su respuesta."""

    def __init__(self, canal: p.Canal, seguir: bool = False):
        self.canal = canal
        self.seguir = seguir
        self.evento = threading.Event()
        self.respuesta: dict | None = None
        self.error: Exception | None = None


class ProxyCoordinador:
    def __init__(self, host: str = "127.0.0.1", puerto: int = p.PUERTO_CLIENTES,
                 al_notificar=None, al_error=None, al_conexion=None,
                 timeout: float = 5.0, reintentos: int = 3, log: logging.Logger | None = None):
        self.host = host
        self.puerto = puerto
        self.al_notificar = al_notificar  # callback(mensaje NOTIFICACION)
        self.al_error = al_error          # callback(mensaje ERROR sin solicitud asociada)
        self.al_conexion = al_conexion    # callback(conectado: bool)
        self.timeout = timeout
        self.reintentos = reintentos
        self._log = log or logging.getLogger(__name__)
        self._canal: p.Canal | None = None
        self._candado = threading.Lock()
        self._pendientes: dict[str, _Espera] = {}
        self._seguidos: set[int] = set()
        self._candado_pend = threading.Lock()
        self._conectado_antes = False
        self._reconectando = False
        self._cerrado = False

    # --- interfaz "local" -----------------------------------------------------

    def conectar(self) -> "ProxyCoordinador":
        self._asegurar_conexion()
        return self

    def hacer_pedido(self, producto: str, cantidad: int = 1, sucursal: str = "",
                     suscribir: bool = True) -> int:
        """Envía un pedido y devuelve el id asignado por el coordinador."""
        respuesta = self._solicitar(p.PEDIDO_NUEVO, seguir=suscribir, producto=producto,
                                    cantidad=cantidad, sucursal=sucursal, suscribir=suscribir)
        return respuesta["pedido_id"]

    def suscribir(self, pedido_id: int) -> dict:
        """Sigue un pedido (propio o de otra sucursal). Devuelve su estado actual."""
        evento = self._solicitar(p.SUSCRIPCION, pedido_id=pedido_id)
        if evento.get("estado") != ENTREGADO:
            with self._candado_pend:
                self._seguidos.add(pedido_id)
        return evento

    def consultar(self, pedido_id: int) -> dict:
        """Estado actual de un pedido, sin suscribirse."""
        return self._solicitar(p.SUSCRIPCION, pedido_id=pedido_id, solo_consulta=True)

    @property
    def conectado(self) -> bool:
        return self._canal is not None

    def cerrar(self) -> None:
        self._cerrado = True
        with self._candado:
            canal, self._canal = self._canal, None
        if canal:
            canal.cerrar()

    def __enter__(self) -> "ProxyCoordinador":
        return self.conectar()

    def __exit__(self, *_) -> None:
        self.cerrar()

    # --- lo que el proxy oculta -----------------------------------------------

    def _solicitar(self, tipo: str, seguir: bool = False, **datos) -> dict:
        # El mismo ref en cada reintento: si el coordinador ya había recibido
        # el pedido, responde con el mismo id en vez de crear un duplicado.
        ref = uuid.uuid4().hex
        ultimo_error: Exception | None = None
        for intento in range(self.reintentos + 1):
            if intento:
                time.sleep(min(0.25 * 2 ** (intento - 1), 2.0))
            try:
                canal = self._asegurar_conexion()
            except OSError as exc:
                ultimo_error = exc
                continue
            espera = _Espera(canal, seguir)
            with self._candado_pend:
                self._pendientes[ref] = espera
            try:
                canal.enviar(tipo, ref=ref, **datos)
                if not espera.evento.wait(self.timeout):
                    ultimo_error = TimeoutError(f"sin respuesta del coordinador en {self.timeout} s")
                    continue
            except OSError as exc:
                ultimo_error = exc
                self._descartar(canal)
                continue
            finally:
                with self._candado_pend:
                    self._pendientes.pop(ref, None)
            if espera.error is not None:
                ultimo_error = espera.error
                continue
            respuesta = espera.respuesta
            if respuesta["tipo"] == p.ERROR:
                raise ErrorCoordinador(respuesta.get("codigo"), respuesta.get("motivo"))
            return respuesta
        raise ConnectionError(f"no se pudo completar {tipo} con el coordinador "
                              f"{self.host}:{self.puerto} ({ultimo_error})")

    def _asegurar_conexion(self) -> p.Canal:
        with self._candado:
            if self._canal is not None:
                return self._canal
            if self._cerrado:
                raise ConnectionError("el proxy está cerrado")
            canal = p.Canal.conectar(self.host, self.puerto, timeout=self.timeout)
            canal.sock.settimeout(None)
            self._canal = canal
            es_reconexion, self._conectado_antes = self._conectado_antes, True
            threading.Thread(target=self._leer, args=(canal,), daemon=True,
                             name=f"lector-{self.host}:{self.puerto}").start()
        with self._candado_pend:
            seguidos = sorted(self._seguidos)
        if es_reconexion:
            self._log.info("Reconectado al coordinador; re-suscribiendo %d pedidos", len(seguidos))
            for pedido_id in seguidos:
                try:
                    canal.enviar(p.SUSCRIPCION, pedido_id=pedido_id)
                except OSError:
                    break
        self._avisar_conexion(True)
        return canal

    def _leer(self, canal: p.Canal) -> None:
        """Hilo lector: reparte respuestas a quien las espera y avisos al callback."""
        try:
            while True:
                mensaje = canal.recibir()
                if mensaje is None:
                    break
                ref = mensaje.get("ref")
                if ref is not None:
                    with self._candado_pend:
                        espera = self._pendientes.get(ref)
                        if espera and espera.seguir and mensaje["tipo"] == p.PEDIDO_ACEPTADO:
                            self._seguidos.add(mensaje["pedido_id"])
                    if espera is not None:
                        espera.respuesta = mensaje
                        espera.evento.set()
                    continue
                if mensaje["tipo"] == p.NOTIFICACION:
                    if mensaje.get("estado") == ENTREGADO:
                        with self._candado_pend:
                            self._seguidos.discard(mensaje.get("pedido_id"))
                    self._invocar(self.al_notificar, mensaje)
                elif mensaje["tipo"] == p.ERROR:
                    self._invocar(self.al_error, mensaje)
        except (OSError, p.ErrorProtocolo) as exc:
            self._log.debug("Lector del coordinador terminó: %s", exc)
        finally:
            perdida = self._descartar(canal)
            with self._candado_pend:
                afectadas = [e for e in self._pendientes.values() if e.canal is canal]
            for espera in afectadas:
                espera.error = ConnectionError("se perdió la conexión con el coordinador")
                espera.evento.set()
            if perdida and not self._cerrado:
                self._avisar_conexion(False)
                self._reconectar_en_segundo_plano()

    def _descartar(self, canal: p.Canal) -> bool:
        """Olvida la conexión si sigue siendo la actual. True si lo era."""
        with self._candado:
            actual = self._canal is canal
            if actual:
                self._canal = None
        canal.cerrar()
        return actual

    def _reconectar_en_segundo_plano(self) -> None:
        # Sin esto, un cliente que solo escucha avisos nunca se recuperaría
        # de una caída del coordinador.
        with self._candado:
            if self._reconectando:
                return
            self._reconectando = True

        def reconectar():
            espera = 0.5
            try:
                while not self._cerrado and self._canal is None:
                    try:
                        self._asegurar_conexion()
                        return
                    except OSError:
                        time.sleep(espera)
                        espera = min(espera * 2, 5.0)
            finally:
                self._reconectando = False

        threading.Thread(target=reconectar, name="reconexion", daemon=True).start()

    def _avisar_conexion(self, conectado: bool) -> None:
        self._invocar(self.al_conexion, conectado)

    def _invocar(self, callback, argumento) -> None:
        if callback is None:
            return
        try:
            callback(argumento)
        except Exception:  # un callback defectuoso no debe matar al lector
            self._log.exception("Error en callback del proxy")


class ProxyNodo:
    """Representante local de un nodo de preparación dentro del coordinador."""

    def __init__(self, nodo_id: str, host: str, puerto: int, timeout: float = 1.0):
        self.nodo_id = nodo_id
        self.host = host
        self.puerto = puerto
        self.timeout = timeout
        self._canal: p.Canal | None = None
        self._candado = threading.Lock()

    def asignar(self, datos_pedido: dict) -> dict:
        """Envía una ASIGNACION y devuelve la respuesta del nodo.

        La respuesta normal es CAMBIO_ESTADO con estado "recibido". Lanza
        OSError (conexión rechazada, reiniciada, timeout...) si el nodo no
        responde; eso es lo que cuenta el Circuit Breaker.
        """
        with self._candado:
            reutilizada = self._canal is not None
            try:
                return self._llamar(datos_pedido)
            except (OSError, p.ErrorProtocolo) as exc:
                self._descartar()
                # Una conexión reutilizada puede estar rota porque el nodo se
                # reinició: se reintenta una vez con una conexión nueva. Un
                # timeout no se reintenta: el nodo está colgado, no reiniciado.
                if not reutilizada or isinstance(exc, socket.timeout):
                    raise
            try:
                return self._llamar(datos_pedido)
            except (OSError, p.ErrorProtocolo):
                self._descartar()
                raise

    def cerrar(self) -> None:
        # Sin tomar el candado: si hay una llamada en curso, cerrar el socket
        # la despierta con un error en vez de esperar a que termine.
        self._descartar()

    def _llamar(self, datos_pedido: dict) -> dict:
        canal = self._canal
        if canal is None:
            canal = self._canal = p.Canal.conectar(self.host, self.puerto, timeout=self.timeout)
        canal.enviar(p.ASIGNACION, **datos_pedido)
        respuesta = canal.recibir()
        if respuesta is None:
            raise ConnectionError(f"{self.nodo_id} cerró la conexión")
        return respuesta

    def _descartar(self) -> None:
        canal, self._canal = self._canal, None
        if canal is not None:
            canal.cerrar()

"""Coordinador de CaféExpress Distribuido (patrón Mediator).

Es el único punto de coordinación: clientes y nodos solo hablan con él,
nunca entre sí. Con 3 nodos y N sucursales, la comunicación directa sería
una malla de todos contra todos; el mediador la reduce a radios desde un
centro. Internamente se separa en capas:

  API          ServidorTCP + manejadores de clientes (5000) y nodos (5001)
  Servicio     Despachador (Strategy + Circuit Breaker + Proxy) y vigilancia
               de latidos con reasignación de pedidos
  Repositorio  RegistroNodos y RepositorioPedidos, en memoria y seguros entre hilos
  Dominio      dominio.py

    python coordinador.py --estrategia menor_carga
"""
from __future__ import annotations

import argparse
import itertools
import queue
import socket
import threading
import time
from dataclasses import dataclass, field

import protocolo as p
from bitacora import configurar_bitacora
from dominio import ENTREGADO, ESTADOS, PENDIENTE, RECIBIDO, Pedido, validar_pedido
from patrones.balanceo import ESTRATEGIAS, EstrategiaBalanceo, crear_estrategia
from patrones.breaker import CircuitBreaker, CircuitoAbierto
from patrones.observer import ObservadorRemoto, SujetoPedidos
from patrones.proxy import ProxyNodo


@dataclass
class ConfigCoordinador:
    host: str = "127.0.0.1"
    puerto_clientes: int = p.PUERTO_CLIENTES
    puerto_nodos: int = p.PUERTO_NODOS
    estrategia: str = "menor_carga"
    reasignar: bool = True
    intervalo_latido: float = p.INTERVALO_LATIDO
    timeout_latido: float = p.TIMEOUT_LATIDO
    umbral_fallos: int = 3
    reintento_circuito: float = 10.0
    timeout_asignacion: float = 1.0
    resumen_cada: float = 10.0
    carpeta_logs: str | None = None
    consola: bool = True
    nivel_log: str = "INFO"


def describir_error(exc: BaseException) -> str:
    if isinstance(exc, socket.timeout):
        return "sin respuesta a tiempo"
    if isinstance(exc, ConnectionRefusedError):
        return "conexión rechazada"
    return str(exc) or type(exc).__name__


# =============================== Repositorio ================================

@dataclass(eq=False)
class InfoNodo:
    nodo_id: str
    host: str
    puerto: int
    hilos: int
    instancia: str
    proxy: ProxyNodo
    breaker: CircuitBreaker
    canal: p.Canal                 # conexión de control: registro, latidos y estados
    carga: int = 0                 # pedidos asignados aún no entregados
    completados: int = 0
    vivo: bool = True
    ultimo_latido: float = field(default_factory=time.monotonic)


class RegistroNodos:
    """Qué nodos existen, cuáles están vivos y cuánta carga tiene cada uno."""

    def __init__(self):
        self._nodos: dict[str, InfoNodo] = {}
        self._candado = threading.Lock()

    def registrar(self, info: InfoNodo) -> None:
        with self._candado:
            self._nodos[info.nodo_id] = info

    def obtener(self, nodo_id: str) -> InfoNodo | None:
        with self._candado:
            return self._nodos.get(nodo_id)

    def es_actual(self, nodo_id: str, canal: p.Canal) -> bool:
        with self._candado:
            info = self._nodos.get(nodo_id)
            return info is not None and info.canal is canal

    def latido(self, nodo_id: str) -> bool:
        """Anota un latido. Devuelve True si el nodo estaba dado por caído."""
        with self._candado:
            info = self._nodos.get(nodo_id)
            if info is None:
                return False
            info.ultimo_latido = time.monotonic()
            revivido = not info.vivo
            info.vivo = True
            return revivido

    def marcar_caido(self, nodo_id: str) -> InfoNodo | None:
        """Lo marca caído. Devuelve el nodo solo si estaba vivo (evita dobles avisos)."""
        with self._candado:
            info = self._nodos.get(nodo_id)
            if info is None or not info.vivo:
                return None
            info.vivo = False
            info.carga = 0
            return info

    def vencidos(self, timeout: float) -> list[str]:
        limite = time.monotonic() - timeout
        with self._candado:
            return [n.nodo_id for n in self._nodos.values() if n.vivo and n.ultimo_latido < limite]

    def disponibles(self) -> list[InfoNodo]:
        """Vivos y con el circuito dispuesto a recibir llamadas."""
        with self._candado:
            vivos = [n for n in self._nodos.values() if n.vivo]
        return [n for n in vivos if n.breaker.permite()]

    def vivos(self) -> list[InfoNodo]:
        with self._candado:
            return [n for n in self._nodos.values() if n.vivo]

    def todos(self) -> list[InfoNodo]:
        with self._candado:
            return sorted(self._nodos.values(), key=lambda n: n.nodo_id)

    def sumar_carga(self, nodo_id: str, delta: int) -> None:
        with self._candado:
            info = self._nodos.get(nodo_id)
            if info is not None:
                info.carga = max(0, info.carga + delta)

    def completar(self, nodo_id: str) -> None:
        with self._candado:
            info = self._nodos.get(nodo_id)
            if info is not None:
                info.carga = max(0, info.carga - 1)
                info.completados += 1


class RepositorioPedidos:
    """Todos los pedidos y su estado. Cada operación es atómica."""

    def __init__(self):
        self._pedidos: dict[int, Pedido] = {}
        self._por_ref: dict[str, int] = {}
        self._ids = itertools.count(1)
        self._candado = threading.Lock()

    def crear(self, producto: str, cantidad: int, sucursal: str,
              ref: str | None = None) -> tuple[Pedido, bool]:
        """Crea el pedido. Si el ref ya se vio (reintento del cliente), devuelve el existente."""
        with self._candado:
            if ref is not None and ref in self._por_ref:
                return self._pedidos[self._por_ref[ref]], False
            pedido = Pedido(next(self._ids), producto, cantidad, sucursal)
            self._pedidos[pedido.pedido_id] = pedido
            if ref is not None:
                self._por_ref[ref] = pedido.pedido_id
            return pedido, True

    def obtener(self, pedido_id: int) -> Pedido | None:
        with self._candado:
            return self._pedidos.get(pedido_id)

    def evento(self, pedido_id: int, **extra) -> dict | None:
        with self._candado:
            pedido = self._pedidos.get(pedido_id)
            return pedido.evento(**extra) if pedido else None

    def asignar(self, pedido_id: int, nodo_id: str) -> bool:
        with self._candado:
            pedido = self._pedidos.get(pedido_id)
            if pedido is None or pedido.estado != PENDIENTE or pedido.nodo_id is not None:
                return False
            pedido.asignar(nodo_id)
            return True

    def desasignar(self, pedido_id: int, nodo_id: str) -> None:
        """Deshace una asignación cuyo envío falló, si el nodo no llegó a reportar nada."""
        with self._candado:
            pedido = self._pedidos.get(pedido_id)
            if pedido and pedido.nodo_id == nodo_id and pedido.estado == PENDIENTE:
                pedido.nodo_id = None

    def avanzar(self, pedido_id: int, estado: str, nodo_id: str) -> list[dict]:
        """Aplica el cambio si es válido; devuelve un evento por estado recorrido."""
        with self._candado:
            pedido = self._pedidos.get(pedido_id)
            if pedido is None:
                return []
            return [pedido.evento(estado=e) for e in pedido.avanzar(estado, nodo_id)]

    def liberar_de_nodo(self, nodo_id: str, **extra) -> list[dict]:
        """Devuelve a pendiente los pedidos sin terminar de un nodo caído."""
        with self._candado:
            afectados = [x for x in self._pedidos.values()
                         if x.nodo_id == nodo_id and not x.terminado]
            for pedido in afectados:
                pedido.liberar()
            return [x.evento(**extra) for x in afectados]

    def sin_terminar_en(self, nodo_id: str) -> int:
        with self._candado:
            return sum(1 for x in self._pedidos.values() if x.nodo_id == nodo_id and not x.terminado)

    def contar(self) -> dict[str, int]:
        conteo = {PENDIENTE: 0, **{estado: 0 for estado in ESTADOS}}
        with self._candado:
            for pedido in self._pedidos.values():
                conteo[pedido.estado] += 1
        return conteo


# ================================= Servicio =================================

class Despachador:
    """Toma los pedidos pendientes y los asigna a un nodo.

    Un solo hilo, en orden de llegada; los pedidos reasignados pasan
    primero. Qué nodo recibe cada pedido lo decide la estrategia (Strategy);
    cada envío pasa por el Circuit Breaker del nodo y por su ProxyNodo.
    """

    def __init__(self, registro: RegistroNodos, pedidos: RepositorioPedidos,
                 sujeto: SujetoPedidos, estrategia: EstrategiaBalanceo, log):
        self.registro = registro
        self.pedidos = pedidos
        self.sujeto = sujeto
        self.estrategia = estrategia
        self.log = log
        self._cola: queue.PriorityQueue = queue.PriorityQueue()
        self._secuencia = itertools.count()
        self._detener = threading.Event()
        self._sin_nodos = False

    def iniciar(self) -> None:
        threading.Thread(target=self._ciclo, name="despachador", daemon=True).start()

    def detener(self) -> None:
        self._detener.set()

    def encolar(self, pedido_id: int, urgente: bool = False) -> None:
        self._cola.put((0 if urgente else 1, next(self._secuencia), pedido_id))

    def en_espera(self) -> int:
        return self._cola.qsize()

    def _ciclo(self) -> None:
        while not self._detener.is_set():
            try:
                prioridad, secuencia, pedido_id = self._cola.get(timeout=0.5)
            except queue.Empty:
                continue
            pedido = self.pedidos.obtener(pedido_id)
            if pedido is None or pedido.estado != PENDIENTE or pedido.nodo_id is not None:
                continue  # ya lo tomó un nodo (p. ej., se encoló dos veces)
            if self._asignar(pedido):
                if self._sin_nodos:
                    self._sin_nodos = False
                    self.log.info("Hay nodos disponibles de nuevo")
            else:
                if not self._sin_nodos:
                    self._sin_nodos = True
                    self.log.warning("Sin nodos disponibles: los pedidos esperan en el coordinador")
                self._cola.put((prioridad, secuencia, pedido_id))
                self._detener.wait(0.1)

    def _asignar(self, pedido: Pedido) -> bool:
        datos = pedido.datos_asignacion()
        descartados: set[str] = set()
        while True:
            candidatos = [n for n in self.registro.disponibles() if n.nodo_id not in descartados]
            nodo = self.estrategia.elegir(candidatos)
            if nodo is None:
                return False
            descartados.add(nodo.nodo_id)
            if not self.pedidos.asignar(pedido.pedido_id, nodo.nodo_id):
                return True
            self.registro.sumar_carga(nodo.nodo_id, +1)
            try:
                respuesta = nodo.breaker.llamar(nodo.proxy.asignar, datos)
                aceptado = respuesta.get("tipo") == p.CAMBIO_ESTADO
                if not aceptado:
                    self.log.warning("%s rechazó el pedido #%d: %s", nodo.nodo_id,
                                     pedido.pedido_id, respuesta.get("motivo"))
            except CircuitoAbierto:
                aceptado = False
            except (OSError, p.ErrorProtocolo) as exc:
                aceptado = False
                self.log.warning("No se pudo asignar el pedido #%d a %s: %s",
                                 pedido.pedido_id, nodo.nodo_id, describir_error(exc))
            if not aceptado:
                self.pedidos.desasignar(pedido.pedido_id, nodo.nodo_id)
                self.registro.sumar_carga(nodo.nodo_id, -1)
                continue
            self.log.debug("Pedido #%d (%s) -> %s [%s, carga %d/%d hilos]", pedido.pedido_id,
                          pedido.producto, nodo.nodo_id, self.estrategia.nombre,
                          nodo.carga, nodo.hilos)
            # Si el nodo ya reportó un estado posterior por su conexión de
            # control, esto no aplica nada: "recibido" ya se notificó.
            for evento in self.pedidos.avanzar(pedido.pedido_id, RECIBIDO, nodo.nodo_id):
                self.sujeto.notificar(pedido.pedido_id, evento)
            return True


class ServidorTCP:
    """Acepta conexiones y atiende cada una en su propio hilo."""

    def __init__(self, nombre: str, host: str, puerto: int, manejador):
        self.nombre = nombre
        self.host = host
        self.puerto = puerto
        self._manejador = manejador
        self._sock: socket.socket | None = None

    def iniciar(self) -> None:
        self._sock = socket.create_server((self.host, self.puerto), backlog=128)
        self.puerto = self._sock.getsockname()[1]
        threading.Thread(target=self._aceptar, name=f"escucha-{self.nombre}", daemon=True).start()

    def detener(self) -> None:
        if self._sock:
            p.cerrar_escucha(self._sock)

    def _aceptar(self) -> None:
        contador = itertools.count(1)
        while True:
            try:
                conexion, direccion = self._sock.accept()
            except OSError:
                return
            threading.Thread(target=self._manejador, args=(conexion, direccion),
                             name=f"conexion-{self.nombre}-{next(contador)}", daemon=True).start()


# ============================ Mediator + capa API ============================

class Coordinador:
    def __init__(self, config: ConfigCoordinador):
        self.config = config
        self.log = configurar_bitacora("coordinador", config.nivel_log, config.consola,
                                       config.carpeta_logs)
        self.registro = RegistroNodos()
        self.pedidos = RepositorioPedidos()
        self.sujeto = SujetoPedidos()
        self.estrategia = crear_estrategia(config.estrategia)
        self.despachador = Despachador(self.registro, self.pedidos, self.sujeto,
                                       self.estrategia, self.log)
        self._servidor_clientes = ServidorTCP("clientes", config.host, config.puerto_clientes,
                                              self._atender_cliente)
        self._servidor_nodos = ServidorTCP("nodos", config.host, config.puerto_nodos,
                                           self._atender_nodo)
        self._canales: set[p.Canal] = set()
        self._candado = threading.Lock()
        self._detener = threading.Event()

    @property
    def puerto_clientes(self) -> int:
        return self._servidor_clientes.puerto

    @property
    def puerto_nodos(self) -> int:
        return self._servidor_nodos.puerto

    def iniciar(self) -> "Coordinador":
        self._servidor_nodos.iniciar()
        self._servidor_clientes.iniciar()
        self.despachador.iniciar()
        threading.Thread(target=self._vigilar, name="vigilante", daemon=True).start()
        self.log.info("Coordinador listo | clientes %s:%d | nodos %s:%d | estrategia %s | "
                      "reasignación %s", self.config.host, self.puerto_clientes, self.config.host,
                      self.puerto_nodos, self.estrategia.nombre,
                      "activada" if self.config.reasignar else "DESACTIVADA")
        return self

    def detener(self) -> None:
        self._detener.set()
        self.despachador.detener()
        self._servidor_clientes.detener()
        self._servidor_nodos.detener()
        with self._candado:
            canales = list(self._canales)
        for canal in canales:
            canal.cerrar()
        for nodo in self.registro.todos():
            nodo.proxy.cerrar()
        self.log.info("Coordinador detenido | %s", self._texto_pedidos())

    # --- API de clientes (puerto 5000) ---------------------------------------------

    def _atender_cliente(self, conexion: socket.socket, direccion) -> None:
        canal = self._seguir_canal(p.Canal(conexion))
        origen = f"{direccion[0]}:{direccion[1]}"
        threading.current_thread().name = f"cliente-{direccion[1]}"
        cliente = ObservadorRemoto(canal, origen, self.log)
        self.log.info("Cliente conectado desde %s", origen)
        try:
            while True:
                try:
                    mensaje = canal.recibir()
                except p.ErrorProtocolo as exc:
                    cliente.enviar(p.ERROR, codigo=p.MENSAJE_INVALIDO, motivo=str(exc))
                    continue
                if mensaje is None:
                    break
                try:
                    if mensaje["tipo"] == p.PEDIDO_NUEVO:
                        self._pedido_nuevo(mensaje, cliente, origen)
                    elif mensaje["tipo"] == p.SUSCRIPCION:
                        self._suscripcion(mensaje, cliente)
                    else:
                        cliente.enviar(p.ERROR, ref=mensaje.get("ref"), codigo=p.MENSAJE_INVALIDO,
                                       motivo=f"un cliente no puede enviar {mensaje['tipo']}")
                except (KeyError, TypeError, ValueError) as exc:
                    cliente.enviar(p.ERROR, ref=mensaje.get("ref"), codigo=p.MENSAJE_INVALIDO,
                                   motivo=f"mensaje incompleto: {exc}")
        except OSError:
            pass
        finally:
            self.sujeto.retirar(cliente)
            cliente.cerrar()
            self._soltar_canal(canal)
            if not self._detener.is_set():
                self.log.info("Cliente %s desconectado", origen)

    def _pedido_nuevo(self, mensaje: dict, cliente: ObservadorRemoto, origen: str) -> None:
        ref = mensaje.get("ref")
        producto, cantidad = mensaje.get("producto"), mensaje.get("cantidad", 1)
        motivo = validar_pedido(producto, cantidad)
        if motivo:
            codigo = p.PRODUCTO_INVALIDO if "producto" in motivo else p.CANTIDAD_INVALIDA
            cliente.enviar(p.ERROR, ref=ref, codigo=codigo, motivo=motivo)
            return
        pedido, nuevo = self.pedidos.crear(producto, cantidad, mensaje.get("sucursal") or origen, ref)
        if mensaje.get("suscribir", True):
            self.sujeto.suscribir(pedido.pedido_id, cliente)
        # La respuesta sale antes que cualquier aviso del pedido: van por la misma cola.
        cliente.enviar(p.PEDIDO_ACEPTADO, ref=ref, pedido_id=pedido.pedido_id)
        if nuevo:
            self.log.debug("Pedido #%d aceptado: %s x%d (%s)", pedido.pedido_id, producto,
                          cantidad, pedido.sucursal)
            self.despachador.encolar(pedido.pedido_id)

    def _suscripcion(self, mensaje: dict, cliente: ObservadorRemoto) -> None:
        ref = mensaje.get("ref")
        pedido_id = mensaje["pedido_id"]
        extra = {"ref": ref} if ref is not None else {}
        if not mensaje.get("solo_consulta"):
            pedido = self.pedidos.obtener(pedido_id)
            if pedido is not None and not pedido.terminado:
                self.sujeto.suscribir(pedido_id, cliente)
        evento = self.pedidos.evento(pedido_id, **extra)
        if evento is None:
            cliente.enviar(p.ERROR, codigo=p.PEDIDO_DESCONOCIDO,
                           motivo=f"no existe el pedido #{pedido_id}", pedido_id=pedido_id, **extra)
            return
        cliente.notificar(evento)

    # --- API de nodos (puerto 5001) ----------------------------------------------

    def _atender_nodo(self, conexion: socket.socket, direccion) -> None:
        canal = self._seguir_canal(p.Canal(conexion))
        nodo_id = None
        try:
            mensaje = canal.recibir()
            if mensaje is None:
                return
            if mensaje["tipo"] != p.REGISTRO:
                canal.enviar(p.ERROR, codigo=p.NODO_NO_REGISTRADO,
                             motivo="el primer mensaje de un nodo debe ser REGISTRO")
                return
            nodo_id = self._registrar_nodo(mensaje, direccion[0], canal)
            threading.current_thread().name = f"control-{nodo_id}"
            canal.enviar(p.REGISTRO_OK, nodo_id=nodo_id, intervalo_latido=self.config.intervalo_latido)
            while (mensaje := canal.recibir()) is not None:
                if mensaje["tipo"] == p.LATIDO:
                    if self.registro.latido(nodo_id):
                        self.log.warning("Nodo %s volvió a enviar latidos; se reincorpora", nodo_id)
                elif mensaje["tipo"] == p.CAMBIO_ESTADO:
                    self._cambio_estado(mensaje, nodo_id)
                else:
                    canal.enviar(p.ERROR, codigo=p.MENSAJE_INVALIDO,
                                 motivo=f"un nodo no puede enviar {mensaje['tipo']}")
        except (OSError, p.ErrorProtocolo, KeyError, TypeError, ValueError) as exc:
            if not self._detener.is_set():
                self.log.debug("Conexión de control de %s terminó: %s", nodo_id, exc)
        finally:
            self._soltar_canal(canal)
            if nodo_id and self.registro.es_actual(nodo_id, canal):
                self.nodo_caido(nodo_id, "se cerró su conexión")

    def _registrar_nodo(self, mensaje: dict, ip: str, canal: p.Canal) -> str:
        nodo_id = str(mensaje["nodo_id"])
        instancia = str(mensaje.get("instancia", ""))
        previo = self.registro.obtener(nodo_id)
        if previo and previo.vivo and previo.instancia != instancia:
            self.nodo_caido(nodo_id, "se reinició")  # sus pedidos murieron con el proceso anterior
        info = InfoNodo(
            nodo_id=nodo_id, host=ip, puerto=int(mensaje["puerto"]),
            hilos=max(1, int(mensaje.get("hilos", 1))), instancia=instancia,
            proxy=ProxyNodo(nodo_id, ip, int(mensaje["puerto"]), self.config.timeout_asignacion),
            breaker=CircuitBreaker(nodo_id, self.config.umbral_fallos,
                                   self.config.reintento_circuito, al_cambiar=self._circuito_cambio),
            canal=canal,
        )
        if previo:
            info.completados = previo.completados
            if previo.vivo:  # el mismo proceso se reconectó: conserva su carga
                info.carga = previo.carga
            previo.proxy.cerrar()
        self.registro.registrar(info)
        self.log.info("Nodo %s registrado desde %s:%d con %d hilos | nodos vivos: %d",
                      nodo_id, ip, info.puerto, info.hilos, len(self.registro.vivos()))
        return nodo_id

    def _cambio_estado(self, mensaje: dict, nodo_id: str) -> None:
        pedido_id, estado = mensaje["pedido_id"], mensaje["estado"]
        if estado not in ESTADOS:
            raise ValueError(f"estado desconocido: {estado!r}")
        # Se usa el nodo de la conexión, no el que diga el mensaje.
        eventos = self.pedidos.avanzar(pedido_id, estado, nodo_id)
        if not eventos:
            self.log.debug("Descartado %s de #%s enviado por %s (tardío o de otro nodo)",
                           estado, pedido_id, nodo_id)
            return
        if estado == ENTREGADO:
            self.registro.completar(nodo_id)
            self.log.debug("Pedido #%d entregado por %s", pedido_id, nodo_id)
        for evento in eventos:
            self.sujeto.notificar(pedido_id, evento)

    # --- vigilancia y robustez -----------------------------------------------------

    def nodo_caido(self, nodo_id: str, motivo: str) -> None:
        if self._detener.is_set():
            return
        info = self.registro.marcar_caido(nodo_id)
        if info is None:
            return  # ya estaba marcado
        info.proxy.cerrar()
        vivos = len(self.registro.vivos())
        if not self.config.reasignar:
            perdidos = self.pedidos.sin_terminar_en(nodo_id)
            self.log.warning("NODO %s CAÍDO (%s) | reasignación desactivada: %d pedidos quedan "
                             "sin terminar | nodos vivos: %d", nodo_id, motivo, perdidos, vivos)
            return
        eventos = self.pedidos.liberar_de_nodo(nodo_id, reasignado_desde=nodo_id)
        for evento in eventos:
            self.despachador.encolar(evento["pedido_id"], urgente=True)
            self.sujeto.notificar(evento["pedido_id"], evento)
        self.log.warning("NODO %s CAÍDO (%s) | %d pedidos reasignados | nodos vivos: %d",
                         nodo_id, motivo, len(eventos), vivos)

    def _vigilar(self) -> None:
        ultimo_resumen, ultimo_texto = time.monotonic(), ""
        while not self._detener.wait(0.5):
            for nodo_id in self.registro.vencidos(self.config.timeout_latido):
                self.nodo_caido(nodo_id, f"sin latido en {self.config.timeout_latido:.0f} s")
            if self.config.resumen_cada and \
                    time.monotonic() - ultimo_resumen >= self.config.resumen_cada:
                ultimo_resumen = time.monotonic()
                texto = self._texto_resumen()
                if texto != ultimo_texto:
                    self.log.info("Resumen | %s", texto)
                    ultimo_texto = texto

    def _circuito_cambio(self, nodo_id, anterior, nuevo, motivo) -> None:
        self.log.warning("Circuito hacia %s: %s -> %s (%s)", nodo_id,
                         anterior.value.upper(), nuevo.value.upper(), motivo)

    def _texto_pedidos(self) -> str:
        conteo = self.pedidos.contar()
        en_curso = conteo["recibido"] + conteo["en_preparacion"] + conteo["listo"]
        return (f"pedidos: {conteo[PENDIENTE]} pendientes, {en_curso} en curso, "
                f"{conteo[ENTREGADO]} entregados")

    def _texto_resumen(self) -> str:
        nodos = ", ".join(
            f"{n.nodo_id} {n.carga}/{n.hilos} ({n.breaker.estado.value})" if n.vivo
            else f"{n.nodo_id} caído" for n in self.registro.todos()) or "ninguno"
        return f"nodos: {nodos} | {self._texto_pedidos()}"

    def _seguir_canal(self, canal: p.Canal) -> p.Canal:
        with self._candado:
            self._canales.add(canal)
        return canal

    def _soltar_canal(self, canal: p.Canal) -> None:
        with self._candado:
            self._canales.discard(canal)
        canal.cerrar()


def main() -> None:
    parser = argparse.ArgumentParser(description="Coordinador de CaféExpress Distribuido")
    parser.add_argument("--host", default="127.0.0.1",
                        help="interfaz donde escuchar; 0.0.0.0 para aceptar otras máquinas")
    parser.add_argument("--puerto-clientes", type=int, default=p.PUERTO_CLIENTES)
    parser.add_argument("--puerto-nodos", type=int, default=p.PUERTO_NODOS)
    parser.add_argument("--estrategia", choices=sorted(ESTRATEGIAS), default="menor_carga",
                        help="política de balanceo (patrón Strategy)")
    parser.add_argument("--sin-reasignacion", action="store_true",
                        help="no reasignar los pedidos de un nodo caído (línea base para comparar)")
    parser.add_argument("--timeout-latido", type=float, default=p.TIMEOUT_LATIDO)
    parser.add_argument("--umbral-fallos", type=int, default=3, help="fallos para abrir el circuito")
    parser.add_argument("--reintento-circuito", type=float, default=10.0,
                        help="segundos con el circuito abierto antes de probar de nuevo")
    parser.add_argument("--timeout-asignacion", type=float, default=1.0)
    parser.add_argument("--resumen-cada", type=float, default=10.0,
                        help="segundos entre resúmenes en el log (0 = nunca)")
    parser.add_argument("--logs", help="carpeta de logs (por defecto ./logs)")
    parser.add_argument("--nivel-log", default="INFO", choices=("DEBUG", "INFO", "WARNING"),
                        help="DEBUG muestra la traza de cada pedido (útil en la demo; "
                             "reduce la capacidad del coordinador ~30%%)")
    parser.add_argument("--silencioso", action="store_true", help="no escribir logs en consola")
    args = parser.parse_args()

    config = ConfigCoordinador(
        host=args.host, puerto_clientes=args.puerto_clientes, puerto_nodos=args.puerto_nodos,
        estrategia=args.estrategia, reasignar=not args.sin_reasignacion,
        timeout_latido=args.timeout_latido, umbral_fallos=args.umbral_fallos,
        reintento_circuito=args.reintento_circuito, timeout_asignacion=args.timeout_asignacion,
        resumen_cada=args.resumen_cada, carpeta_logs=args.logs, consola=not args.silencioso,
        nivel_log=args.nivel_log,
    )
    coordinador = Coordinador(config).iniciar()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        coordinador.detener()


if __name__ == "__main__":
    main()

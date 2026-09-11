"""Nodo de preparación de CaféExpress Distribuido.

Cada nodo es un proceso independiente que:
  1. escucha en su --puerto las ASIGNACIONES del coordinador,
  2. encola los pedidos y los prepara con un grupo de --hilos propio,
  3. se registra en el coordinador, le envía un LATIDO cada 2 s y le
     reporta cada CAMBIO_ESTADO de sus pedidos.

    python nodo.py --id nodo-1 --puerto 6001 --hilos 4

Modos de trabajo (--trabajo):
  espera  la preparación es tiempo de espera (como un barista esperando la
          máquina). Los hilos rinden en paralelo.
  cpu     la preparación es cálculo puro. Por el GIL de Python, más hilos en
          un mismo nodo NO aceleran; solo más nodos (procesos) lo hacen.
"""
from __future__ import annotations

import argparse
import itertools
import queue
import random
import socket
import threading
import time
import uuid
from dataclasses import dataclass

import protocolo as p
from bitacora import configurar_bitacora
from dominio import EN_PREPARACION, ENTREGADO, LISTO, RECIBIDO, TIEMPO_ENTREGA, tiempo_preparacion


@dataclass
class ConfigNodo:
    nodo_id: str = "nodo-1"
    host: str = "127.0.0.1"
    puerto: int = 6001
    hilos: int = 4
    coordinador: str = "127.0.0.1"
    puerto_coordinador: int = p.PUERTO_NODOS
    factor_tiempo: float = 1.0
    trabajo: str = "espera"
    calibracion: float = 0.0  # iteraciones/s para trabajo "cpu"; 0 = medir al arrancar
    carpeta_logs: str | None = None
    consola: bool = True
    nivel_log: str = "INFO"


class Nodo:
    def __init__(self, config: ConfigNodo):
        self.config = config
        self.nodo_id = config.nodo_id
        self.instancia = uuid.uuid4().hex[:8]  # distingue un reinicio de una reconexión
        self.log = configurar_bitacora(config.nodo_id, config.nivel_log,
                                       config.consola, config.carpeta_logs)
        self.puerto = config.puerto
        self.registrado = threading.Event()
        self.procesados = 0
        self.activos = 0
        self._cola: queue.Queue = queue.Queue()
        self._en_curso: set[int] = set()
        self._candado = threading.Lock()
        self._detener = threading.Event()
        self._servidor: socket.socket | None = None
        self._canal_coordinador: p.Canal | None = None
        self._conexiones: set[p.Canal] = set()
        self._iteraciones_por_segundo = 0.0

    # --- ciclo de vida ----------------------------------------------------------

    def iniciar(self) -> "Nodo":
        if self.config.trabajo == "cpu":
            self._calibrar_cpu()
        for i in range(1, self.config.hilos + 1):
            threading.Thread(target=self._trabajador, name=f"{self.nodo_id}-hilo-{i}",
                             daemon=True).start()

        self._servidor = socket.create_server((self.config.host, self.config.puerto), backlog=64)
        self.puerto = self._servidor.getsockname()[1]
        threading.Thread(target=self._aceptar_asignaciones, name=f"{self.nodo_id}-escucha",
                         daemon=True).start()
        threading.Thread(target=self._mantener_coordinador, name=f"{self.nodo_id}-latidos",
                         daemon=True).start()
        self.log.info("Nodo %s escuchando en %s:%d con %d hilos (trabajo: %s)",
                      self.nodo_id, self.config.host, self.puerto, self.config.hilos,
                      self.config.trabajo)
        return self

    def detener(self) -> None:
        """Apaga el nodo cerrando todas sus conexiones (como una caída)."""
        self._detener.set()
        if self._servidor:
            p.cerrar_escucha(self._servidor)
        with self._candado:
            canales = list(self._conexiones)
            if self._canal_coordinador:
                canales.append(self._canal_coordinador)
        for canal in canales:
            canal.cerrar()
        self.log.info("Nodo %s detenido (%d pedidos entregados)", self.nodo_id, self.procesados)

    # --- conexión con el coordinador: registro y latidos ------------------------

    def _mantener_coordinador(self) -> None:
        """Se registra y envía latidos; si pierde la conexión, se vuelve a registrar."""
        while not self._detener.is_set():
            canal = None
            try:
                canal = p.Canal.conectar(self.config.coordinador, self.config.puerto_coordinador,
                                         timeout=5.0)
                canal.enviar(p.REGISTRO, nodo_id=self.nodo_id, puerto=self.puerto,
                             hilos=self.config.hilos, instancia=self.instancia)
                respuesta = canal.recibir()
                if respuesta is None or respuesta["tipo"] != p.REGISTRO_OK:
                    raise ConnectionError(f"registro rechazado: {respuesta}")
                intervalo = float(respuesta.get("intervalo_latido", p.INTERVALO_LATIDO))
                with self._candado:
                    self._canal_coordinador = canal
                self.registrado.set()
                self.log.info("Registrado en el coordinador %s:%d (latido cada %.1f s)",
                              self.config.coordinador, self.config.puerto_coordinador, intervalo)
                while not self._detener.wait(intervalo):
                    canal.enviar(p.LATIDO, nodo_id=self.nodo_id, en_cola=self._cola.qsize(),
                                 activos=self.activos, procesados=self.procesados)
            except (OSError, p.ErrorProtocolo) as exc:
                if not self._detener.is_set():
                    self.log.warning("Sin conexión con el coordinador (%s); reintento en 2 s", exc)
            finally:
                self.registrado.clear()
                with self._candado:
                    if self._canal_coordinador is canal:
                        self._canal_coordinador = None
                if canal:
                    canal.cerrar()
            self._detener.wait(2.0)

    def _reportar(self, pedido_id: int, estado: str) -> None:
        with self._candado:
            canal = self._canal_coordinador
        if canal is None:
            self.log.warning("Pedido #%d -> %s no reportado: sin conexión con el coordinador",
                             pedido_id, estado)
            return
        try:
            canal.enviar(p.CAMBIO_ESTADO, pedido_id=pedido_id, estado=estado, nodo_id=self.nodo_id)
        except OSError as exc:
            self.log.warning("Pedido #%d -> %s no reportado: %s", pedido_id, estado, exc)

    # --- recepción de asignaciones --------------------------------------------

    def _aceptar_asignaciones(self) -> None:
        contador = itertools.count(1)
        while not self._detener.is_set():
            try:
                conexion, _ = self._servidor.accept()
            except OSError:
                return
            if self._detener.is_set():
                conexion.close()
                return
            threading.Thread(target=self._atender_asignaciones, args=(p.Canal(conexion),),
                             name=f"{self.nodo_id}-asignaciones-{next(contador)}",
                             daemon=True).start()

    def _atender_asignaciones(self, canal: p.Canal) -> None:
        with self._candado:
            self._conexiones.add(canal)
        try:
            while (mensaje := canal.recibir()) is not None:
                if mensaje["tipo"] != p.ASIGNACION:
                    canal.enviar(p.ERROR, codigo=p.MENSAJE_INVALIDO,
                                 motivo=f"un nodo solo acepta {p.ASIGNACION}")
                    continue
                pedido_id = mensaje["pedido_id"]
                with self._candado:
                    # Idempotente: si el coordinador reintenta, no se prepara dos veces.
                    nuevo = pedido_id not in self._en_curso
                    self._en_curso.add(pedido_id)
                canal.enviar(p.CAMBIO_ESTADO, pedido_id=pedido_id, estado=RECIBIDO,
                             nodo_id=self.nodo_id)
                if nuevo:
                    self._cola.put(mensaje)
                    self.log.info("Pedido #%d recibido (%s x%d). En cola: %d", pedido_id,
                                  mensaje["producto"], mensaje.get("cantidad", 1), self._cola.qsize())
        except (OSError, p.ErrorProtocolo) as exc:
            if not self._detener.is_set():
                self.log.debug("Conexión de asignaciones cerrada: %s", exc)
        finally:
            with self._candado:
                self._conexiones.discard(canal)
            canal.cerrar()

    # --- grupo de hilos -----------------------------------------------------------

    def _trabajador(self) -> None:
        while not self._detener.is_set():
            try:
                pedido = self._cola.get(timeout=0.5)
            except queue.Empty:
                continue
            pedido_id = pedido["pedido_id"]
            with self._candado:
                self.activos += 1
            entregado = False
            try:
                self._reportar(pedido_id, EN_PREPARACION)
                duracion = (tiempo_preparacion(pedido["producto"], pedido.get("cantidad", 1))
                            * self.config.factor_tiempo * random.uniform(0.85, 1.15))
                inicio = time.perf_counter()
                if not self._preparar(duracion):
                    return  # el nodo se está apagando
                self._reportar(pedido_id, LISTO)
                if self._detener.wait(TIEMPO_ENTREGA * self.config.factor_tiempo):
                    return
                self._reportar(pedido_id, ENTREGADO)
                entregado = True
                self.log.info("Pedido #%d entregado (%s, %.2f s en el hilo)",
                              pedido_id, pedido["producto"], time.perf_counter() - inicio)
            finally:
                with self._candado:
                    self.activos -= 1
                    # Un pedido abandonado a medias porque el nodo se apagó
                    # no cuenta: el coordinador lo reasigna a otro nodo.
                    self.procesados += entregado
                    self._en_curso.discard(pedido_id)

    def _preparar(self, segundos: float) -> bool:
        """Simula la preparación. Devuelve False si el nodo se detuvo a mitad."""
        if self.config.trabajo == "espera":
            return not self._detener.wait(segundos)
        # Trabajo de CPU: una cantidad FIJA de cálculo, no un tiempo fijo.
        # Así, si varios hilos compiten por el GIL, cada pedido tarda más.
        restantes = int(segundos * self._iteraciones_por_segundo)
        x = 1
        while restantes > 0:
            bloque = min(restantes, 20_000)
            for _ in range(bloque):
                x = (x * 1103515245 + 12345) & 0x7FFFFFFF
            restantes -= bloque
            if self._detener.is_set():
                return False
        return True

    def _calibrar_cpu(self) -> None:
        if self.config.calibracion > 0:
            self._iteraciones_por_segundo = self.config.calibracion
            origen = "recibida por parámetro"
        else:
            self._iteraciones_por_segundo = calibrar_cpu()
            origen = "medida al arrancar"
        self.log.info("Calibración CPU (%s): %.0f iteraciones/s por hilo",
                      origen, self._iteraciones_por_segundo)


def calibrar_cpu(intentos: int = 5, duracion: float = 0.1) -> float:
    """Iteraciones de cálculo por segundo de un hilo, sin competencia.

    Se toma el mejor de varios intentos cortos: un intento lento solo puede
    deberse a ruido (otro proceso, frecuencia baja del procesador), nunca a
    que la máquina sea más rápida de lo que es.
    """
    mejor = 0.0
    for _ in range(intentos):
        iteraciones, x = 0, 1
        inicio = time.perf_counter()
        while time.perf_counter() - inicio < duracion:
            for _ in range(20_000):
                x = (x * 1103515245 + 12345) & 0x7FFFFFFF
            iteraciones += 20_000
        mejor = max(mejor, iteraciones / (time.perf_counter() - inicio))
    return mejor


def main() -> None:
    parser = argparse.ArgumentParser(description="Nodo de preparación de CaféExpress Distribuido")
    parser.add_argument("--id", dest="nodo_id", help="identificador (por defecto nodo-<puerto>)")
    parser.add_argument("--puerto", type=int, default=6001, help="puerto de asignaciones (6001)")
    parser.add_argument("--hilos", type=int, default=4, help="tamaño del grupo de hilos (4)")
    parser.add_argument("--host", default="127.0.0.1",
                        help="interfaz donde escuchar; 0.0.0.0 para recibir desde otra máquina")
    parser.add_argument("--coordinador", default="127.0.0.1", help="IP del coordinador")
    parser.add_argument("--puerto-coordinador", type=int, default=p.PUERTO_NODOS)
    parser.add_argument("--factor-tiempo", type=float, default=1.0,
                        help="multiplica los tiempos de preparación (0.5 = el doble de rápido)")
    parser.add_argument("--trabajo", choices=("espera", "cpu"), default="espera")
    parser.add_argument("--calibracion", type=float, default=0.0,
                        help="iteraciones/s para --trabajo cpu (0 = medir al arrancar). Pasar el "
                             "mismo valor a todos los nodos garantiza el mismo trabajo por pedido")
    parser.add_argument("--logs", help="carpeta de logs (por defecto ./logs)")
    parser.add_argument("--nivel-log", default="INFO", choices=("DEBUG", "INFO", "WARNING"))
    parser.add_argument("--silencioso", action="store_true", help="no escribir logs en consola")
    args = parser.parse_args()

    if args.hilos < 1:
        parser.error("--hilos debe ser al menos 1")
    config = ConfigNodo(
        nodo_id=args.nodo_id or f"nodo-{args.puerto}", host=args.host, puerto=args.puerto,
        hilos=args.hilos, coordinador=args.coordinador, puerto_coordinador=args.puerto_coordinador,
        factor_tiempo=args.factor_tiempo, trabajo=args.trabajo, calibracion=args.calibracion,
        carpeta_logs=args.logs,
        consola=not args.silencioso, nivel_log=args.nivel_log,
    )
    nodo = Nodo(config).iniciar()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        nodo.detener()


if __name__ == "__main__":
    main()

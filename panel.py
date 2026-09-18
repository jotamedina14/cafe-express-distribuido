"""Panel de demostración: todo el sistema en una sola ventana.

Levanta el coordinador y tres nodos como procesos independientes (los mismos
coordinador.py y nodo.py de siempre, comunicándose por TCP) y muestra la
salida de cada uno en su propio recuadro, junto con un cliente de sucursal
integrado que recibe los avisos por suscripción.

    python3 panel.py                    # tiempos de preparación triplicados
    python3 panel.py --factor-tiempo 1  # tiempos reales

Teclas:
    i        iniciar el sistema: coordinador y luego los tres nodos
    p        un pedido (capuchino x2)
    v        diez pedidos al azar
    r        ráfaga de treinta pedidos
    1 2 3    tumbar ese nodo (como Ctrl+C) o, si está caído, levantarlo de nuevo
    q        salir y detener todo
"""
from __future__ import annotations

import argparse
import collections
import curses
import locale
import random
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import protocolo as p
from dominio import ENTREGADO, MENU
from patrones.proxy import ErrorCoordinador, ProxyCoordinador

RAIZ = Path(__file__).resolve().parent
EVENTOS_CLAVE = ("Coordinador listo", "registrado desde", "CAÍDO", "Circuito hacia",
                 "volvió a enviar latidos")
LEYENDA = (" [i] iniciar   [p] un pedido   [v] 10 pedidos   [r] ráfaga de 30   "
           "[1][2][3] tumbar o levantar nodo   [q] salir ")


def compactar(linea: str) -> str:
    """Quita la fecha y el nivel INFO para que quepa más en cada recuadro."""
    if len(linea) > 24 and linea[4] == "-" and linea[10] == " ":
        linea = linea[11:]
    return linea.replace(" INFO    ", " ", 1)


def color_de(linea: str) -> str | None:
    if "CAÍDO" in linea or "WARNING" in linea or "cayó" in linea:
        return "alerta"
    if "Traceback" in linea or "Error" in linea or "ERROR" in linea:
        return "error"
    if "registrado" in linea or "listo |" in linea or "Registrado" in linea:
        return "info"
    if "entregado" in linea or "Resumen:" in linea:
        return "ok"
    if "Resumen |" in linea:
        return "resumen"
    return None


class Recuadro:
    """Las últimas líneas de salida de un componente, seguras entre hilos."""

    def __init__(self, titulo: str):
        self.titulo = titulo
        self._lineas: collections.deque = collections.deque(maxlen=400)
        self._candado = threading.Lock()

    def agregar(self, texto: str, color: str | None = None) -> None:
        with self._candado:
            self._lineas.append((texto, color))

    def lineas(self) -> list:
        with self._candado:
            return list(self._lineas)


class Proceso:
    """Un componente del sistema corriendo como proceso aparte del sistema operativo."""

    def __init__(self, argumentos: list[str], recuadro: Recuadro, al_leer=None):
        self.argumentos = argumentos
        self.recuadro = recuadro
        self.al_leer = al_leer
        self.proc: subprocess.Popen | None = None

    @property
    def vivo(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def iniciar(self) -> None:
        # Sesión propia: un Ctrl+C en el panel no les llega a los procesos.
        self.proc = subprocess.Popen(
            [sys.executable, "-u", *self.argumentos], cwd=RAIZ, text=True,
            encoding="utf-8", errors="replace", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, start_new_session=True)
        threading.Thread(target=self._leer, args=(self.proc,), daemon=True).start()

    def _leer(self, proc: subprocess.Popen) -> None:
        for linea in proc.stdout:
            linea = compactar(linea.rstrip("\n"))
            self.recuadro.agregar(linea, color_de(linea))
            if self.al_leer:
                self.al_leer(linea)
        self.recuadro.agregar(f"--- proceso terminado (código {proc.wait()}) ---", "atenuado")

    def interrumpir(self) -> None:
        """Lo mismo que presionar Ctrl+C en su terminal."""
        if self.vivo:
            self.proc.send_signal(signal.SIGINT)

    def detener(self) -> None:
        self.interrumpir()
        if self.proc is not None:
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class ClienteIntegrado:
    """Una sucursal: envía pedidos y muestra los avisos que le llegan."""

    def __init__(self, recuadro: Recuadro, puerto: int):
        self.recuadro = recuadro
        self.puerto = puerto
        self.proxy: ProxyCoordinador | None = None
        self._lotes: list[dict] = []
        self._entregados: set[int] = set()
        self._candado = threading.Lock()

    def conectar(self) -> None:
        self.proxy = ProxyCoordinador("127.0.0.1", self.puerto, al_notificar=self._aviso,
                                      al_conexion=self._conexion).conectar()

    def cerrar(self) -> None:
        if self.proxy:
            self.proxy.cerrar()

    def enviar(self, pedidos: list[tuple[str, int]], intervalo: float) -> None:
        if not self.proxy:
            self.recuadro.agregar("Primero inicia el sistema con [i]", "alerta")
            return
        threading.Thread(target=self._enviar, args=(pedidos, intervalo), daemon=True).start()

    def _enviar(self, pedidos, intervalo) -> None:
        lote = {"pendientes": set(), "total": len(pedidos), "inicio": time.monotonic(),
                "enviado": False}
        with self._candado:
            self._lotes.append(lote)
        for producto, cantidad in pedidos:
            try:
                pid = self.proxy.hacer_pedido(producto, cantidad, sucursal="panel")
            except (ErrorCoordinador, ConnectionError) as exc:
                self.recuadro.agregar(f"No se pudo enviar: {exc}", "error")
                continue
            self.recuadro.agregar(f"{self._hora()} Pedido #{pid} enviado: {producto} x{cantidad}",
                                  "atenuado")
            with self._candado:
                if pid not in self._entregados:
                    lote["pendientes"].add(pid)
            time.sleep(intervalo)
        with self._candado:
            lote["enviado"] = True
            self._cerrar_lotes()

    def _aviso(self, evento: dict) -> None:
        pid, estado = evento["pedido_id"], evento["estado"]
        producto = f"{evento.get('producto', '?')} x{evento.get('cantidad', 1)}"
        if evento.get("reasignado_desde"):
            self.recuadro.agregar(f"{self._hora()} #{pid:<4} {producto:<14} "
                                  f"{evento['reasignado_desde']} cayó; se reasigna a otro nodo",
                                  "alerta")
            return
        self.recuadro.agregar(f"{self._hora()} #{pid:<4} {producto:<14} {estado.upper():<15} "
                              f"{evento.get('nodo_id') or '-'}", estado)
        if estado == ENTREGADO:
            with self._candado:
                self._entregados.add(pid)
                for lote in self._lotes:
                    lote["pendientes"].discard(pid)
                self._cerrar_lotes()

    def _cerrar_lotes(self) -> None:
        for lote in [l for l in self._lotes if l["enviado"] and not l["pendientes"]]:
            self._lotes.remove(lote)
            if lote["total"] > 1:
                self.recuadro.agregar(
                    f"{self._hora()} Resumen: {lote['total']}/{lote['total']} pedidos entregados "
                    f"en {time.monotonic() - lote['inicio']:.1f} s", "ok")

    def _conexion(self, conectado: bool) -> None:
        if conectado:
            self.recuadro.agregar(f"{self._hora()} Conectado al coordinador 127.0.0.1:{self.puerto}",
                                  "info")
        else:
            self.recuadro.agregar(f"{self._hora()} Se perdió la conexión con el coordinador",
                                  "error")

    @staticmethod
    def _hora() -> str:
        return time.strftime("%H:%M:%S")


class Panel:
    def __init__(self, factor_tiempo: float):
        self.recuadro_coord = Recuadro("coordinador")
        self.recuadro_cliente = Recuadro("cliente (sucursal norte)")
        self.recuadros_nodos = [Recuadro(f"nodo-{i}") for i in (1, 2, 3)]
        self.eventos: collections.deque = collections.deque(maxlen=3)
        self.coordinador = Proceso(["coordinador.py", "--nivel-log", "DEBUG"],
                                   self.recuadro_coord, al_leer=self._evento)
        self.nodos = [Proceso(["nodo.py", "--id", f"nodo-{i}", "--puerto", str(6000 + i),
                               "--hilos", "4", "--factor-tiempo", str(factor_tiempo)],
                              self.recuadros_nodos[i - 1]) for i in (1, 2, 3)]
        self.cliente = ClienteIntegrado(self.recuadro_cliente, p.PUERTO_CLIENTES)
        self.iniciado = False

    def _evento(self, linea: str) -> None:
        if any(clave in linea for clave in EVENTOS_CLAVE):
            self.eventos.append((linea, color_de(linea)))

    # --- acciones de las teclas ---------------------------------------------------

    def iniciar(self) -> None:
        if self.iniciado:
            return
        self.iniciado = True
        threading.Thread(target=self._secuencia_inicio, daemon=True).start()

    def _secuencia_inicio(self) -> None:
        self.coordinador.iniciar()
        limite = time.monotonic() + 5
        while time.monotonic() < limite and not any(
                "Coordinador listo" in l for l, _ in self.recuadro_coord.lineas()):
            time.sleep(0.1)
        for nodo in self.nodos:  # uno a uno, para que se vea cada registro
            time.sleep(0.8)
            nodo.iniciar()
        time.sleep(1.2)
        try:
            self.cliente.conectar()
        except OSError as exc:
            self.recuadro_cliente.agregar(f"No se pudo conectar: {exc}", "error")

    def un_pedido(self) -> None:
        self.cliente.enviar([("capuchino", 2)], 0)

    def varios(self, cantidad: int, intervalo: float) -> None:
        self.cliente.enviar([(random.choice(list(MENU)), 1) for _ in range(cantidad)], intervalo)

    def alternar_nodo(self, indice: int) -> None:
        if not self.iniciado:
            return
        nodo = self.nodos[indice]
        if nodo.vivo:
            nodo.recuadro.agregar("--- Ctrl+C enviado a este nodo ---", "alerta")
            nodo.interrumpir()
        else:
            nodo.recuadro.agregar("--- levantando el nodo de nuevo ---", "info")
            nodo.iniciar()

    def detener(self) -> None:
        self.cliente.cerrar()
        for nodo in self.nodos:
            nodo.detener()
        self.coordinador.detener()


# --- dibujo con curses -------------------------------------------------------------

COLORES = {"alerta": (curses.COLOR_YELLOW, curses.A_BOLD), "error": (curses.COLOR_RED, curses.A_BOLD),
           "info": (curses.COLOR_CYAN, 0), "ok": (curses.COLOR_GREEN, 0),
           "resumen": (curses.COLOR_MAGENTA, 0), "recibido": (curses.COLOR_CYAN, 0),
           "en_preparacion": (curses.COLOR_BLUE, curses.A_BOLD), "listo": (curses.COLOR_MAGENTA, 0),
           "entregado": (curses.COLOR_GREEN, curses.A_BOLD), "atenuado": (curses.COLOR_WHITE, curses.A_DIM)}


def preparar_colores() -> dict:
    atributos = {}
    if not curses.has_colors():
        return atributos
    curses.start_color()
    curses.use_default_colors()
    for n, (nombre, (color, extra)) in enumerate(COLORES.items(), start=1):
        curses.init_pair(n, color, -1)
        atributos[nombre] = curses.color_pair(n) | extra
    return atributos


def escribir(ventana, y: int, x: int, texto: str, ancho: int, atributo: int = 0) -> None:
    try:
        ventana.addnstr(y, x, texto.ljust(ancho), ancho, atributo)
    except curses.error:
        pass  # la esquina inferior derecha de la pantalla no admite escritura


def envolver(texto: str, ancho: int) -> list[str]:
    if ancho <= 0:
        return []
    return [texto[i:i + ancho] for i in range(0, len(texto), ancho)] or [""]


def dibujar_recuadro(pantalla, colores, y, x, alto, ancho, recuadro, estado, fijas=()):
    if alto < 2 or ancho < 10:
        return
    color_estado = colores.get("ok" if "activo" in estado else
                               "error" if "caído" in estado else "atenuado", 0)
    escribir(pantalla, y, x, f" {recuadro.titulo} ", ancho, curses.A_REVERSE | curses.A_BOLD)
    escribir(pantalla, y, x + len(recuadro.titulo) + 3, estado, max(0, ancho - len(recuadro.titulo) - 3),
             curses.A_REVERSE | color_estado)
    fila = y + 1
    for texto, color in fijas:  # eventos clave: no se desplazan con el resto
        for trozo in envolver(texto, ancho - 1)[:2]:
            if fila < y + alto:
                escribir(pantalla, fila, x, trozo, ancho - 1, colores.get(color, 0) | curses.A_BOLD)
                fila += 1
    if fijas and fila < y + alto:
        escribir(pantalla, fila, x, "─" * (ancho - 1), ancho - 1, colores.get("atenuado", 0))
        fila += 1
    disponibles = y + alto - fila
    trozos = []
    for texto, color in recuadro.lineas()[-disponibles * 2:]:
        trozos += [(t, color) for t in envolver(texto, ancho - 1)]
    trozos = trozos[-disponibles:] if disponibles > 0 else []
    for texto, color in trozos:
        escribir(pantalla, fila, x, texto, ancho - 1, colores.get(color, 0))
        fila += 1
    for f in range(fila, y + alto):
        escribir(pantalla, f, x, "", ancho - 1)


def estado_de(proceso: Proceso) -> str:
    if proceso.proc is None:
        return "○ sin iniciar"
    return "● activo" if proceso.vivo else "✖ caído"


def dibujar(pantalla, colores, panel: Panel) -> None:
    alto, ancho = pantalla.getmaxyx()
    area = alto - 1
    izquierda = ancho // 2
    derecha = ancho - izquierda - 1
    mitad = area // 2
    dibujar_recuadro(pantalla, colores, 0, 0, mitad, izquierda, panel.recuadro_coord,
                     estado_de(panel.coordinador), fijas=list(panel.eventos))
    conectado = panel.cliente.proxy is not None and panel.cliente.proxy.conectado
    dibujar_recuadro(pantalla, colores, mitad, 0, area - mitad, izquierda, panel.recuadro_cliente,
                     "● conectado" if conectado else "○ sin conectar")
    tercio = area // 3
    for i, nodo in enumerate(panel.nodos):
        y = i * tercio
        alto_nodo = tercio if i < 2 else area - 2 * tercio
        dibujar_recuadro(pantalla, colores, y, izquierda + 1, alto_nodo, derecha,
                         nodo.recuadro, estado_de(nodo))
    for fila in range(area):
        try:
            pantalla.addch(fila, izquierda, curses.ACS_VLINE, colores.get("atenuado", 0))
        except curses.error:
            pass
    escribir(pantalla, alto - 1, 0, LEYENDA, ancho - 1, curses.A_REVERSE)
    pantalla.refresh()


def ejecutar(pantalla, panel: Panel, guion_prueba: list) -> None:
    curses.curs_set(0)
    pantalla.timeout(100)
    colores = preparar_colores()
    inicio = time.monotonic()
    while True:
        dibujar(pantalla, colores, panel)
        tecla = pantalla.getch()
        if guion_prueba and time.monotonic() - inicio >= guion_prueba[0][0]:
            tecla = ord(guion_prueba.pop(0)[1])
        if tecla == curses.KEY_RESIZE:
            pantalla.erase()
        elif tecla in (ord("q"), ord("Q")):
            return
        elif tecla in (ord("i"), ord("I")):
            panel.iniciar()
        elif tecla in (ord("p"), ord("P")):
            panel.un_pedido()
        elif tecla in (ord("v"), ord("V")):
            panel.varios(10, 0.15)
        elif tecla in (ord("r"), ord("R")):
            panel.varios(30, 0.05)
        elif tecla in (ord("1"), ord("2"), ord("3")):
            panel.alternar_nodo(tecla - ord("1"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel de demostración de CaféExpress Distribuido")
    parser.add_argument("--factor-tiempo", type=float, default=3.0,
                        help="multiplica los tiempos de preparación (3 = más lento, para que se vea)")
    parser.add_argument("--autoprueba", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    locale.setlocale(locale.LC_ALL, "")
    panel = Panel(args.factor_tiempo)
    # Recorrido automático usado para probar el panel sin intervención.
    guion = [(0, "i"), (6, "p"), (13, "v"), (22, "r"), (24.5, "2"), (36, "2"), (40, "q")] \
        if args.autoprueba else []
    try:
        curses.wrapper(ejecutar, panel, guion)
    except KeyboardInterrupt:
        pass
    finally:
        panel.detener()
    if args.autoprueba:
        for recuadro in (panel.recuadro_coord, panel.recuadro_cliente):
            for texto, _ in recuadro.lineas():
                if any(c in texto for c in ("CAÍDO", "registrado", "Resumen:", "cayó", "Traceback")):
                    print(f"[{recuadro.titulo}] {texto}")


if __name__ == "__main__":
    main()

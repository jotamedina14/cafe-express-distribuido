"""Cliente de consola de una sucursal de CaféExpress.

No sabe qué nodo prepara sus pedidos ni le importa: solo habla con el
coordinador a través de ProxyCoordinador y recibe los cambios de estado por
suscripción (Observer), sin preguntar en bucle.

    python cliente.py --sucursal norte              # modo interactivo
    python cliente.py --sucursal norte --auto 10    # 10 pedidos al azar y espera la entrega
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
from datetime import datetime

import protocolo as p
from dominio import ENTREGADO, MENU
from patrones.proxy import ErrorCoordinador, ProxyCoordinador

COLORES = {
    "pendiente": "33", "recibido": "36", "en_preparacion": "34",
    "listo": "35", "entregado": "32", "error": "31", "info": "90",
}

AYUDA = """Comandos:
  pedir <producto> [cantidad]   hace un pedido (p latte 2)
  varios <n>                    hace n pedidos al azar
  consultar <id>                estado actual de un pedido (c 12)
  seguir <id>                   recibir los avisos de un pedido de otra sucursal
  menu                          productos disponibles
  ayuda | salir"""


def hora() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class ClienteSucursal:
    def __init__(self, host: str, puerto: int, sucursal: str, color: bool):
        self.sucursal = sucursal
        self.color = color
        self._candado = threading.Lock()
        self._sin_entregar: set[int] = set()
        self._entregados: set[int] = set()
        self._todos_entregados = threading.Event()
        self._todos_entregados.set()
        self.proxy = ProxyCoordinador(host, puerto, al_notificar=self._al_notificar,
                                      al_error=self._al_error, al_conexion=self._al_conexion)

    # --- salida -------------------------------------------------------------------

    def pintar(self, texto: str, clave: str) -> str:
        return f"\033[{COLORES[clave]}m{texto}\033[0m" if self.color else texto

    def decir(self, texto: str, clave: str = "info") -> None:
        with self._candado:
            print(f"[{hora()}] {self.pintar(texto, clave)}", flush=True)

    # --- avisos del coordinador (llegan por el hilo lector del proxy) -------------

    def _al_notificar(self, evento: dict) -> None:
        pid, estado = evento["pedido_id"], evento["estado"]
        producto = f"{evento.get('producto', '?')} x{evento.get('cantidad', 1)}"
        if evento.get("reasignado_desde"):
            self.decir(f"#{pid:<4} {producto:<14} {evento['reasignado_desde']} cayó; "
                       f"se reasigna a otro nodo", "pendiente")
        else:
            nodo = evento.get("nodo_id") or "-"
            self.decir(f"#{pid:<4} {producto:<14} {estado.upper():<15} {nodo}", estado)
        if estado == ENTREGADO:
            with self._candado:
                self._entregados.add(pid)
                self._sin_entregar.discard(pid)
                if not self._sin_entregar:
                    self._todos_entregados.set()

    def _al_error(self, mensaje: dict) -> None:
        self.decir(f"Error del coordinador: {mensaje.get('codigo')} - {mensaje.get('motivo')}", "error")

    def _al_conexion(self, conectado: bool) -> None:
        if conectado:
            self.decir(f"Conectado al coordinador {self.proxy.host}:{self.proxy.puerto}")
        else:
            self.decir("Se perdió la conexión con el coordinador; reintentando...", "error")

    # --- acciones -----------------------------------------------------------------

    def pedir(self, producto: str, cantidad: int = 1) -> int | None:
        try:
            pid = self.proxy.hacer_pedido(producto, cantidad, sucursal=self.sucursal)
        except ErrorCoordinador as exc:
            self.decir(f"Pedido rechazado: {exc.motivo}", "error")
            return None
        except ConnectionError as exc:
            self.decir(f"No se pudo enviar el pedido: {exc}", "error")
            return None
        with self._candado:
            # Con preparaciones muy cortas el aviso de entrega puede ganarle
            # a esta línea (llega por el hilo lector del proxy).
            if pid not in self._entregados:
                self._sin_entregar.add(pid)
                self._todos_entregados.clear()
        self.decir(f"Pedido #{pid} enviado: {producto} x{cantidad}")
        return pid

    def consultar(self, pid: int) -> None:
        try:
            evento = self.proxy.consultar(pid)
            self.decir(f"#{pid} está {evento['estado'].upper()} "
                       f"(nodo: {evento.get('nodo_id') or '-'}, reasignaciones: "
                       f"{evento.get('reasignaciones', 0)})", evento["estado"])
        except (ErrorCoordinador, ConnectionError) as exc:
            self.decir(str(exc), "error")

    def seguir(self, pid: int) -> None:
        try:
            evento = self.proxy.suscribir(pid)
            self.decir(f"Siguiendo el pedido #{pid} (ahora está {evento['estado']})")
        except (ErrorCoordinador, ConnectionError) as exc:
            self.decir(str(exc), "error")

    def esperar_entregas(self, timeout: float) -> bool:
        return self._todos_entregados.wait(timeout)

    def pendientes(self) -> int:
        with self._candado:
            return len(self._sin_entregar)


def modo_interactivo(cliente: ClienteSucursal) -> None:
    print(f"CaféExpress - sucursal {cliente.sucursal}. Escribe 'ayuda' para ver los comandos.")
    while True:
        try:
            linea = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not linea:
            continue
        comando, *args = linea.split()
        comando = comando.lower()
        try:
            if comando in ("salir", "exit", "q"):
                return
            if comando in ("ayuda", "help", "?"):
                print(AYUDA)
            elif comando == "menu":
                for producto, segundos in MENU.items():
                    print(f"  {producto:<12} {segundos:.1f} s por unidad")
            elif comando in ("pedir", "p"):
                if not args:
                    print("Uso: pedir <producto> [cantidad]")
                    continue
                cliente.pedir(args[0].lower(), int(args[1]) if len(args) > 1 else 1)
            elif comando in ("varios", "v"):
                for _ in range(int(args[0]) if args else 5):
                    cliente.pedir(random.choice(list(MENU)))
            elif comando in ("consultar", "c"):
                cliente.consultar(int(args[0]))
            elif comando in ("seguir", "s"):
                cliente.seguir(int(args[0]))
            else:
                print(f"Comando desconocido: {comando}. Escribe 'ayuda'.")
        except (ValueError, IndexError):
            print("Argumento inválido. Escribe 'ayuda'.")


def modo_automatico(cliente: ClienteSucursal, cantidad: int, intervalo: float, espera: float) -> bool:
    inicio = time.perf_counter()
    for _ in range(cantidad):
        cliente.pedir(random.choice(list(MENU)))
        time.sleep(intervalo)
    todos = cliente.esperar_entregas(espera)
    duracion = time.perf_counter() - inicio
    entregados = cantidad - cliente.pendientes()
    cliente.decir(f"Resumen: {entregados}/{cantidad} pedidos entregados en {duracion:.1f} s",
                  "entregado" if todos else "error")
    return todos


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente de sucursal de CaféExpress Distribuido")
    parser.add_argument("--coordinador", default="127.0.0.1", help="IP del coordinador")
    parser.add_argument("--puerto", type=int, default=p.PUERTO_CLIENTES)
    parser.add_argument("--sucursal", default="centro")
    parser.add_argument("--auto", type=int, metavar="N", help="envía N pedidos al azar y termina")
    parser.add_argument("--intervalo", type=float, default=0.3, help="segundos entre pedidos en --auto")
    parser.add_argument("--espera", type=float, default=120.0,
                        help="segundos máximos esperando las entregas en --auto")
    parser.add_argument("--sin-color", action="store_true")
    args = parser.parse_args()

    if os.name == "nt":
        os.system("")  # activa los colores ANSI en la consola de Windows
    color = sys.stdout.isatty() and not args.sin_color
    cliente = ClienteSucursal(args.coordinador, args.puerto, args.sucursal, color)
    try:
        cliente.proxy.conectar()
    except OSError as exc:
        print(f"No se pudo conectar al coordinador {args.coordinador}:{args.puerto} ({exc}). "
              f"¿Está corriendo coordinador.py?")
        sys.exit(1)
    try:
        if args.auto:
            ok = modo_automatico(cliente, args.auto, args.intervalo, args.espera)
            sys.exit(0 if ok else 1)
        modo_interactivo(cliente)
    finally:
        cliente.proxy.cerrar()


if __name__ == "__main__":
    main()

"""Generador de carga: N pedidos concurrentes desde C sucursales simultáneas.

Mide, desde el punto de vista del cliente:
  latencia       envío del pedido -> aviso de "entregado" (promedio, p50, p95, máx.)
  espera en cola envío -> "en_preparacion" (tiempo que el pedido esperó un hilo libre)
  servicio       "en_preparacion" -> "entregado" (tiempo de preparación real)
  throughput     pedidos entregados por segundo
  perdidos       pedidos que nunca llegaron a "entregado" dentro del tiempo límite

Separar espera y servicio es lo que permite ubicar el cuello de botella: si
la latencia se va en espera, faltan hilos o nodos; si se va en servicio, el
problema es la preparación en sí.

    python pruebas/carga.py --pedidos 120 --clientes 6
    python pruebas/carga.py --pedidos 90 --tasa 9      # 9 pedidos/s en vez de ráfaga
"""
from __future__ import annotations

import argparse
import csv
import math
import random
import sys
import threading
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

import protocolo as p  # noqa: E402
from dominio import EN_PREPARACION, ENTREGADO, MENU  # noqa: E402
from patrones.proxy import ErrorCoordinador, ProxyCoordinador  # noqa: E402

CARPETA_RESULTADOS = RAIZ / "resultados"


@dataclass
class ResultadoCarga:
    etiqueta: str
    pedidos: int
    clientes: int
    tasa: float
    entregados: int
    perdidos: int
    rechazados: int
    reasignados: int
    duracion_s: float
    throughput: float
    latencia_prom: float
    latencia_p50: float
    latencia_p95: float
    latencia_max: float
    espera_prom: float
    servicio_prom: float
    por_nodo: dict = field(default_factory=dict)
    inicio: float = 0.0
    primer_reasignado: float | None = None
    detalle: list = field(default_factory=list)

    def fila(self) -> dict:
        """Resumen plano para CSV."""
        fila = {k: v for k, v in asdict(self).items()
                if k not in ("detalle", "inicio", "primer_reasignado", "por_nodo")}
        fila["por_nodo"] = " ".join(f"{n}:{c}" for n, c in sorted(self.por_nodo.items()))
        for clave, valor in fila.items():
            if isinstance(valor, float):
                fila[clave] = round(valor, 4)
        return fila


def percentil(valores: list[float], porcentaje: float) -> float:
    """Percentil por rango más cercano (el valor por debajo del cual queda ese %)."""
    if not valores:
        return math.nan
    ordenados = sorted(valores)
    indice = max(0, math.ceil(porcentaje / 100 * len(ordenados)) - 1)
    return ordenados[indice]


def promedio(valores: list[float]) -> float:
    return sum(valores) / len(valores) if valores else math.nan


def ejecutar_carga(host: str = "127.0.0.1", puerto: int = p.PUERTO_CLIENTES, pedidos: int = 100,
                   clientes: int = 5, tasa: float = 0.0, timeout: float = 60.0,
                   semilla: int | None = 42, etiqueta: str = "") -> ResultadoCarga:
    """Lanza la carga y espera las entregas. tasa=0 envía todo en ráfaga."""
    azar = random.Random(semilla)
    productos = [azar.choice(list(MENU)) for _ in range(pedidos)]
    condicion = threading.Condition()
    marcas: dict[int, dict[str, float]] = defaultdict(dict)
    nodo_final: dict[int, str] = {}
    reasignaciones: Counter = Counter()
    entregados: set[int] = set()
    envios: dict[int, tuple[float, str]] = {}
    rechazos = [0]
    primer_reasignado: list[float | None] = [None]

    def al_notificar(evento: dict) -> None:
        ahora = time.perf_counter()
        pid = evento["pedido_id"]
        with condicion:
            if evento.get("reasignado_desde"):
                reasignaciones[pid] += 1
                primer_reasignado[0] = primer_reasignado[0] or ahora
                return
            # Se guarda la última vez que se vio cada estado: si el pedido se
            # reasignó, cuenta la preparación en el nodo que lo terminó.
            marcas[pid][evento["estado"]] = ahora
            if evento["estado"] == ENTREGADO and pid not in entregados:
                entregados.add(pid)
                nodo_final[pid] = evento.get("nodo_id")
                condicion.notify_all()

    proxies = [ProxyCoordinador(host, puerto, al_notificar=al_notificar, timeout=10).conectar()
               for _ in range(clientes)]
    inicio = time.perf_counter()

    def sucursal(k: int) -> None:
        for i in range(k, pedidos, clientes):
            if tasa > 0:
                pausa = inicio + i / tasa - time.perf_counter()
                if pausa > 0:
                    time.sleep(pausa)
            enviado = time.perf_counter()
            try:
                pid = proxies[k].hacer_pedido(productos[i], sucursal=f"sucursal-{k + 1}")
            except (ConnectionError, ErrorCoordinador):
                with condicion:
                    rechazos[0] += 1
                continue
            with condicion:
                envios[pid] = (enviado, productos[i])

    hilos = [threading.Thread(target=sucursal, args=(k,), name=f"sucursal-{k + 1}")
             for k in range(clientes)]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()
    with condicion:
        condicion.wait_for(lambda: len(entregados) >= len(envios), timeout=timeout)
    for proxy in proxies:
        proxy.cerrar()

    with condicion:
        return _resumir(etiqueta, pedidos, clientes, tasa, inicio, envios, marcas, nodo_final,
                        reasignaciones, entregados, rechazos[0], primer_reasignado[0])


def _resumir(etiqueta, pedidos, clientes, tasa, inicio, envios, marcas, nodo_final,
             reasignaciones, entregados, rechazados, primer_reasignado) -> ResultadoCarga:
    detalle, latencias, esperas, servicios = [], [], [], []
    for pid, (enviado, producto) in sorted(envios.items()):
        m = marcas.get(pid, {})
        fila = {"pedido_id": pid, "producto": producto, "nodo": nodo_final.get(pid, ""),
                "reasignaciones": reasignaciones[pid], "entregado": pid in entregados,
                "latencia_s": "", "espera_s": "", "servicio_s": ""}
        if pid in entregados:
            latencias.append(m[ENTREGADO] - enviado)
            fila["latencia_s"] = round(latencias[-1], 4)
            if EN_PREPARACION in m:
                esperas.append(m[EN_PREPARACION] - enviado)
                servicios.append(m[ENTREGADO] - m[EN_PREPARACION])
                fila["espera_s"] = round(esperas[-1], 4)
                fila["servicio_s"] = round(servicios[-1], 4)
        detalle.append(fila)

    ultima = max((marcas[pid][ENTREGADO] for pid in entregados), default=inicio)
    duracion = ultima - inicio
    return ResultadoCarga(
        etiqueta=etiqueta, pedidos=pedidos, clientes=clientes, tasa=tasa,
        entregados=len(entregados), perdidos=pedidos - len(entregados), rechazados=rechazados,
        reasignados=sum(1 for pid in envios if reasignaciones[pid]),
        duracion_s=duracion, throughput=len(entregados) / duracion if duracion > 0 else 0.0,
        latencia_prom=promedio(latencias), latencia_p50=percentil(latencias, 50),
        latencia_p95=percentil(latencias, 95), latencia_max=max(latencias, default=math.nan),
        espera_prom=promedio(esperas), servicio_prom=promedio(servicios),
        por_nodo=dict(Counter(nodo_final[pid] for pid in entregados)),
        inicio=inicio, primer_reasignado=primer_reasignado, detalle=detalle,
    )


def imprimir(r: ResultadoCarga) -> None:
    titulo = f" Resultado de carga{': ' + r.etiqueta if r.etiqueta else ''} "
    print(f"\n{titulo:=^60}")
    modo = f"{r.tasa:g} pedidos/s" if r.tasa else "ráfaga"
    filas = [
        ("Pedidos enviados", f"{r.pedidos}  ({r.clientes} sucursales, {modo})"),
        ("Entregados", f"{r.entregados}"),
        ("Perdidos", f"{r.perdidos}" + (f"  (rechazados al enviar: {r.rechazados})" if r.rechazados else "")),
        ("Reasignados", f"{r.reasignados}"),
        ("Latencia promedio", f"{r.latencia_prom:.3f} s"),
        ("Latencia p50 / p95", f"{r.latencia_p50:.3f} s / {r.latencia_p95:.3f} s"),
        ("Latencia máxima", f"{r.latencia_max:.3f} s"),
        ("  espera en cola (prom.)", f"{r.espera_prom:.3f} s"),
        ("  preparación (prom.)", f"{r.servicio_prom:.3f} s"),
        ("Throughput", f"{r.throughput:.2f} pedidos/s"),
        ("Duración", f"{r.duracion_s:.2f} s"),
        ("Pedidos por nodo", "  ".join(f"{n}: {c}" for n, c in sorted(r.por_nodo.items())) or "-"),
    ]
    for nombre, valor in filas:
        print(f"{nombre:<26}{valor}")
    print("=" * 60)


def exportar_csv(filas: list[dict], ruta: Path, anexar: bool = True) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not ruta.exists() or not anexar
    with open(ruta, "a" if anexar else "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=list(filas[0]))
        if nuevo:
            escritor.writeheader()
        escritor.writerows(filas)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prueba de carga de CaféExpress Distribuido")
    parser.add_argument("--coordinador", default="127.0.0.1")
    parser.add_argument("--puerto", type=int, default=p.PUERTO_CLIENTES)
    parser.add_argument("--pedidos", type=int, default=100)
    parser.add_argument("--clientes", type=int, default=5, help="sucursales enviando a la vez")
    parser.add_argument("--tasa", type=float, default=0.0,
                        help="pedidos por segundo en total (0 = todos de golpe)")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="segundos máximos esperando entregas tras el último envío")
    parser.add_argument("--semilla", type=int, default=42)
    parser.add_argument("--etiqueta", default="", help="nombre de la corrida en el CSV")
    parser.add_argument("--csv", default=str(CARPETA_RESULTADOS / "carga.csv"),
                        help="CSV de resumen (se anexa una fila por corrida)")
    parser.add_argument("--detalle", help="CSV opcional con una fila por pedido")
    args = parser.parse_args()

    try:
        resultado = ejecutar_carga(args.coordinador, args.puerto, args.pedidos, args.clientes,
                                   args.tasa, args.timeout, args.semilla, args.etiqueta)
    except OSError as exc:
        sys.exit(f"No se pudo conectar al coordinador {args.coordinador}:{args.puerto}: {exc}")
    imprimir(resultado)
    exportar_csv([resultado.fila()], Path(args.csv))
    print(f"Resumen anexado a {args.csv}")
    if args.detalle:
        exportar_csv(resultado.detalle, Path(args.detalle), anexar=False)
        print(f"Detalle por pedido en {args.detalle}")
    sys.exit(0 if resultado.perdidos == 0 else 1)


if __name__ == "__main__":
    main()

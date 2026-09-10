"""Matriz de experimentos de escalabilidad.

Para cada configuración levanta un sistema nuevo (coordinador + nodos como
procesos reales), lanza la misma carga y guarda las métricas. Genera:

  resultados/experimentos.csv   una fila por corrida
  resultados/experimentos.md    tablas listas para el informe

Escenarios:
  horizontal   1, 2 y 3 nodos de 4 hilos          (escalar agregando máquinas)
  vertical     1 nodo de 4, 8 y 12 hilos          (escalar agrandando una máquina)
  balanceo     nodos de 2, 4 y 4 hilos: round robin vs menor carga
  gil          trabajo de CPU: 1x1, 1x3 (vertical) y 3x1 (horizontal)
  saturacion   1 nodo vs 3 nodos a 4, 8, 12 y 16 pedidos/s
  coordinador  preparación casi instantánea: ¿cuántos pedidos/s aguanta el
               coordinador por sí solo, sin importar cuántos nodos haya?

    python pruebas/experimentos.py
    python pruebas/experimentos.py --escenarios horizontal,vertical --rapido
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from nodo import calibrar_cpu  # noqa: E402
from pruebas.carga import CARPETA_RESULTADOS, ejecutar_carga, exportar_csv  # noqa: E402
from pruebas.entorno import SistemaLocal  # noqa: E402


def corrida(etiqueta, hilos, estrategia="menor_carga", trabajo="espera", factor=1.0,
            pedidos=120, clientes=6, tasa=0.0, extra=()):
    return dict(etiqueta=etiqueta, hilos=tuple(hilos), estrategia=estrategia, trabajo=trabajo,
                factor=factor, pedidos=pedidos, clientes=clientes, tasa=tasa, extra=list(extra))


def nodos_x_hilos(hilos) -> str:
    if len(set(hilos)) == 1:
        return f"{len(hilos)} nodo{'s' if len(hilos) > 1 else ''} x {hilos[0]} hilo{'s' if hilos[0] > 1 else ''}"
    return f"{len(hilos)} nodos ({'+'.join(map(str, hilos))} hilos)"


ESCENARIOS = {
    "horizontal": dict(
        titulo="Escalabilidad horizontal: más nodos de 4 hilos",
        nota="Mismo total de pedidos en ráfaga; cada nodo agregado es un proceso aparte.",
        corridas=[corrida(nodos_x_hilos(h), h) for h in [(4,), (4, 4), (4, 4, 4)]]),
    "vertical": dict(
        titulo="Escalabilidad vertical: un solo nodo con más hilos",
        nota="Mismo total de pedidos en ráfaga; se agranda el grupo de hilos de un nodo.",
        corridas=[corrida(nodos_x_hilos(h), h) for h in [(4,), (8,), (12,)]]),
    "balanceo": dict(
        titulo="Estrategia de balanceo con nodos de distinta capacidad (2, 4 y 4 hilos)",
        nota="Round robin reparte por igual; menor carga reparte según hilos libres.",
        corridas=[corrida(f"{e}", (2, 4, 4), estrategia=e) for e in ("round_robin", "menor_carga")]),
    "gil": dict(
        titulo="Trabajo de CPU y el GIL de Python",
        nota="La preparación es cálculo puro (cantidad fija de operaciones). Más hilos en un "
             "proceso compiten por el GIL; más nodos son procesos distintos.",
        corridas=[corrida(f"{n} (vertical)" if len(h) == 1 and h[0] > 1 else
                          f"{n} (horizontal)" if len(h) > 1 else f"{n} (base)",
                          h, trabajo="cpu", factor=0.5, pedidos=30, clientes=3)
                  for h, n in [((1,), "1 nodo x 1 hilo"), ((3,), "1 nodo x 3 hilos"),
                               ((1, 1, 1), "3 nodos x 1 hilo")]]),
    "saturacion": dict(
        titulo="Saturación: latencia según la tasa de llegada",
        nota="Pedidos a ritmo constante durante ~6 s. Capacidad teórica: ~5.9 pedidos/s por "
             "nodo de 4 hilos (4 hilos / 0.68 s de preparación promedio).",
        corridas=[corrida(f"{nodos_x_hilos(h)} @ {t} ped/s", h, pedidos=6 * t, tasa=t)
                  for h in [(4,), (4, 4, 4)] for t in (4, 8, 12, 16)]),
    "coordinador": dict(
        titulo="Techo del coordinador (preparación casi instantánea)",
        nota="Con preparación de ~1 ms los nodos sobran: el límite lo pone el coordinador. "
             "La primera fila es la versión anterior al ajuste, que escribía en el log tres "
             "líneas por pedido; la aceleración de las demás se mide contra ella.",
        corridas=[corrida("2 nodos x 8 hilos, traza por pedido (antes)", (8, 8), factor=0.002,
                          pedidos=2000, clientes=8, extra=["--nivel-log", "DEBUG"])]
                 + [corrida(nodos_x_hilos(h), h, factor=0.002, pedidos=2000, clientes=8)
                    for h in [(8,), (8, 8), (8, 8, 8)]]),
}


def ejecutar(nombre: str, rapido: bool, puerto: list[int], calibracion: float) -> list[dict]:
    escenario = ESCENARIOS[nombre]
    print(f"\n### {escenario['titulo']}")
    filas = []
    for c in escenario["corridas"]:
        factor, pedidos = c["factor"], c["pedidos"]
        if rapido:
            factor, pedidos = factor / 2, max(10, pedidos // 2)
        puerto[0] += 10
        sistema = SistemaLocal(hilos=c["hilos"], estrategia=c["estrategia"], trabajo=c["trabajo"],
                               factor_tiempo=factor, puerto_base=puerto[0],
                               extra_coordinador=c["extra"], calibracion=calibracion,
                               carpeta_logs=RAIZ / "logs" / "experimentos" / nombre / c["etiqueta"]
                               .replace(" ", "_").replace("@", "a").replace("/", "-")
                               .replace(",", "").replace("(", "").replace(")", ""))
        inicio = time.monotonic()
        with sistema:
            r = ejecutar_carga(puerto=sistema.puerto_clientes, pedidos=pedidos,
                               clientes=c["clientes"], tasa=c["tasa"], timeout=120,
                               etiqueta=f"{nombre}: {c['etiqueta']}")
        fila = {"escenario": nombre, "configuracion": c["etiqueta"],
                "nodos": len(c["hilos"]), "hilos_totales": sum(c["hilos"]),
                "estrategia": c["estrategia"], "trabajo": c["trabajo"], "factor_tiempo": factor,
                **r.fila()}
        filas.append(fila)
        print(f"  {c['etiqueta']:<34} {r.entregados:>4}/{r.pedidos} entregados | "
              f"{r.throughput:7.2f} ped/s | latencia prom {r.latencia_prom:6.3f} s | "
              f"p95 {r.latencia_p95:6.3f} s | ({time.monotonic() - inicio:.0f} s)")
    return filas


def tabla(nombre: str, filas: list[dict]) -> str:
    escenario = ESCENARIOS[nombre]
    base = filas[0]["throughput"] or 1
    partes = [f"## {escenario['titulo']}", "", escenario["nota"], ""]
    if nombre == "saturacion":
        partes += ["| Configuración | Tasa ofrecida (ped/s) | Throughput (ped/s) | Latencia prom. (s) "
                   "| p95 (s) | Espera en cola (s) | Perdidos |", "|---|---|---|---|---|---|---|"]
        for f in filas:
            partes.append(f"| {f['configuracion'].split(' @ ')[0]} | {f['tasa']:g} | "
                          f"{f['throughput']:.2f} | {f['latencia_prom']:.3f} | "
                          f"{f['latencia_p95']:.3f} | {f['espera_prom']:.3f} | {f['perdidos']} |")
    else:
        partes += ["| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración "
                   "| Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) "
                   "| Pedidos por nodo |", "|---|---|---|---|---|---|---|---|---|---|"]
        for f in filas:
            partes.append(f"| {f['configuracion']} | {f['hilos_totales']} | "
                          f"{f['entregados']}/{f['pedidos']} | {f['throughput']:.2f} | "
                          f"x{f['throughput'] / base:.2f} | {f['latencia_prom']:.3f} | "
                          f"{f['latencia_p95']:.3f} | {f['espera_prom']:.3f} | "
                          f"{f['servicio_prom']:.3f} | {f['por_nodo']} |")
    return "\n".join(partes)


def main() -> None:
    parser = argparse.ArgumentParser(description="Experimentos de escalabilidad de CaféExpress")
    parser.add_argument("--escenarios", default=",".join(ESCENARIOS),
                        help=f"separados por coma: {', '.join(ESCENARIOS)}")
    parser.add_argument("--rapido", action="store_true",
                        help="mitad de pedidos y preparación el doble de rápida (para probar)")
    parser.add_argument("--puerto-base", type=int, default=5400)
    args = parser.parse_args()

    nombres = [n.strip() for n in args.escenarios.split(",")]
    desconocidos = [n for n in nombres if n not in ESCENARIOS]
    if desconocidos:
        parser.error(f"escenarios desconocidos: {', '.join(desconocidos)}")

    print(f"Máquina: {os.cpu_count()} núcleos lógicos | Python {sys.version.split()[0]} | "
          f"{sys.platform}")
    # Una sola calibración para todos los nodos de CPU: así cada configuración
    # hace exactamente la misma cantidad de cálculo por pedido.
    calibracion = calibrar_cpu() if "gil" in nombres else 0.0
    if calibracion:
        print(f"Calibración CPU compartida: {calibracion:.0f} iteraciones/s por hilo")
    inicio, puerto, todas, secciones = time.monotonic(), [args.puerto_base], [], []
    for nombre in nombres:
        filas = ejecutar(nombre, args.rapido, puerto, calibracion)
        todas += filas
        secciones.append(tabla(nombre, filas))

    CARPETA_RESULTADOS.mkdir(exist_ok=True)
    exportar_csv(todas, CARPETA_RESULTADOS / "experimentos.csv", anexar=False)
    encabezado = (f"# Resultados de los experimentos\n\nMáquina: {os.cpu_count()} núcleos lógicos, "
                  f"Python {sys.version.split()[0]} ({sys.platform}). Todos los componentes corren "
                  f"como procesos separados en la misma máquina y se comunican por TCP."
                  f"{' Modo rápido.' if args.rapido else ''}\n")
    (CARPETA_RESULTADOS / "experimentos.md").write_text(
        encabezado + "\n\n" + "\n\n".join(secciones) + "\n", encoding="utf-8")
    print(f"\nListo en {time.monotonic() - inicio:.0f} s. Resultados en resultados/experimentos.csv "
          f"y resultados/experimentos.md")


if __name__ == "__main__":
    main()

"""Prueba de robustez: se tumba un nodo en plena carga y se verifica que el
sistema siga respondiendo sin perder pedidos.

Escenarios:
  caida             SIGKILL a nodo-2 con reasignación activada. Debe perder 0.
  sin_reasignacion  lo mismo con --sin-reasignacion: la línea base, que sí
                    pierde los pedidos que tenía el nodo. Muestra por qué
                    la reasignación es necesaria.
  congelado         SIGSTOP a nodo-2 (Linux/macOS): el proceso sigue vivo y
                    con sus sockets abiertos, pero no responde. No hay cierre
                    de conexión que delate la falla: la detectan el Circuit
                    Breaker (timeouts de asignación) y el latido (6 s).
  congelado_ajustado  el mismo, con timeout de asignación de 0.25 s en vez
                    de 1 s: el ajuste que reduce el bloqueo del despachador.

    python pruebas/robustez.py
    python pruebas/robustez.py --escenarios caida --pedidos 150 --tasa 10
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from pruebas.carga import CARPETA_RESULTADOS, ejecutar_carga, exportar_csv, imprimir  # noqa: E402
from pruebas.entorno import SistemaLocal  # noqa: E402

VICTIMA = "nodo-2"

ESCENARIOS = {
    "caida": dict(reasignar=True, estrategia="menor_carga", falla="matar",
                  descripcion="SIGKILL a nodo-2, con reasignación"),
    "sin_reasignacion": dict(reasignar=False, estrategia="menor_carga", falla="matar",
                             descripcion="SIGKILL a nodo-2, SIN reasignación (línea base)"),
    # Con round robin el nodo congelado sigue siendo elegido en su turno,
    # que es justo la situación que el Circuit Breaker debe cortar.
    "congelado": dict(reasignar=True, estrategia="round_robin", falla="congelar",
                      descripcion="SIGSTOP a nodo-2, timeout de asignación 1 s"),
    # Ajuste: el despachador es un solo hilo y cada timeout lo frena entero.
    # En una red local la respuesta de un nodo tarda milisegundos; 0.25 s
    # sigue siendo holgado y reduce el tiempo que el despachador queda bloqueado.
    "congelado_ajustado": dict(reasignar=True, estrategia="round_robin", falla="congelar",
                               extra=["--timeout-asignacion", "0.25"],
                               descripcion="SIGSTOP a nodo-2, timeout de asignación 0.25 s (ajuste)"),
}


def correr(nombre: str, pedidos: int, tasa: float, momento: float, puerto_base: int) -> dict:
    esc = ESCENARIOS[nombre]
    print(f"\n>>> Escenario '{nombre}': {esc['descripcion']}, a los {momento:g} s")
    sistema = SistemaLocal(hilos=(4, 4, 4), estrategia=esc["estrategia"], reasignar=esc["reasignar"],
                           puerto_base=puerto_base, extra_coordinador=esc.get("extra", ()),
                           carpeta_logs=RAIZ / "logs" / "robustez" / nombre)
    with sistema:
        resultado = {}
        carga = threading.Thread(target=lambda: resultado.update(r=ejecutar_carga(
            puerto=sistema.puerto_clientes, pedidos=pedidos, clientes=4, tasa=tasa,
            timeout=20 if esc["reasignar"] else 12, etiqueta=f"robustez-{nombre}")))
        carga.start()
        time.sleep(momento)
        instante, instante_epoch = time.perf_counter(), time.time()
        if esc["falla"] == "matar":
            sistema.matar_nodo(VICTIMA)
        else:
            sistema.congelar_nodo(VICTIMA)
        print(f"    {VICTIMA} {'eliminado' if esc['falla'] == 'matar' else 'congelado'}; la carga sigue...")
        carga.join()
        r = resultado["r"]

        def segundos_hasta(patron: str) -> float | None:
            eventos = sistema.eventos_log("coordinador", patron)
            posteriores = [t for t, _ in eventos if t >= instante_epoch - 0.01]
            return round(posteriores[0] - instante_epoch, 3) if posteriores else None

        caida = segundos_hasta(f"NODO {VICTIMA} CAÍDO")
        circuito = segundos_hasta(f"Circuito hacia {VICTIMA}: CERRADO -> ABIERTO")
        timeouts = len([1 for t, _ in sistema.eventos_log("coordinador", f"a {VICTIMA}: sin respuesta")
                        if t >= instante_epoch])
    imprimir(r)
    reasignacion = (round(r.primer_reasignado - instante, 3)
                    if r.primer_reasignado and r.primer_reasignado >= instante else None)
    if esc["reasignar"]:
        aprobado = r.perdidos == 0
        veredicto = "CONTINUIDAD VERIFICADA: 0 pedidos perdidos" if aprobado else \
            f"FALLA: se perdieron {r.perdidos} pedidos"
    else:
        aprobado = r.perdidos > 0  # la línea base debe evidenciar la pérdida
        veredicto = f"Línea base: sin reasignación se pierden {r.perdidos} pedidos"
    print(f"    Detección de la caída: {caida if caida is not None else '-'} s | "
          f"primer aviso de reasignación: {reasignacion if reasignacion is not None else '-'} s | "
          f"circuito abierto: {circuito if circuito is not None else '-'} s | "
          f"timeouts de asignación: {timeouts}")
    print(f"    {veredicto}")
    return {
        "escenario": nombre, "descripcion": esc["descripcion"], "estrategia": esc["estrategia"],
        "pedidos": r.pedidos, "entregados": r.entregados, "perdidos": r.perdidos,
        "reasignados": r.reasignados, "deteccion_s": caida, "reasignacion_s": reasignacion,
        "circuito_abierto_s": circuito, "timeouts_asignacion": timeouts,
        "latencia_prom_s": round(r.latencia_prom, 3), "latencia_p95_s": round(r.latencia_p95, 3),
        "latencia_max_s": round(r.latencia_max, 3), "throughput": round(r.throughput, 2),
        "aprobado": aprobado,
    }


def tabla_markdown(filas: list[dict]) -> str:
    def v(x):
        return "-" if x is None else x
    lineas = [
        "| Escenario | Entregados | Perdidos | Reasignados | Detección (s) | Circuito abierto (s) "
        "| Latencia prom. (s) | p95 (s) | Máx. (s) | Resultado |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for f in filas:
        lineas.append(
            f"| {f['descripcion']} | {f['entregados']}/{f['pedidos']} | {f['perdidos']} | "
            f"{f['reasignados']} | {v(f['deteccion_s'])} | {v(f['circuito_abierto_s'])} | "
            f"{f['latencia_prom_s']} | {f['latencia_p95_s']} | {f['latencia_max_s']} | "
            f"{'OK' if f['aprobado'] else 'FALLA'} |")
    return "\n".join(lineas)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prueba de robustez de CaféExpress Distribuido")
    disponibles = [e for e in ESCENARIOS
                   if ESCENARIOS[e]["falla"] != "congelar" or sys.platform != "win32"]
    parser.add_argument("--escenarios", default=",".join(disponibles),
                        help=f"separados por coma: {', '.join(ESCENARIOS)}")
    parser.add_argument("--pedidos", type=int, default=120)
    parser.add_argument("--tasa", type=float, default=10.0, help="pedidos por segundo")
    parser.add_argument("--momento", type=float, default=4.0, help="segundo en que falla el nodo")
    parser.add_argument("--puerto-base", type=int, default=5300)
    args = parser.parse_args()

    filas = [correr(nombre.strip(), args.pedidos, args.tasa, args.momento, args.puerto_base)
             for nombre in args.escenarios.split(",")]
    exportar_csv(filas, CARPETA_RESULTADOS / "robustez.csv", anexar=False)
    markdown = tabla_markdown(filas)
    (CARPETA_RESULTADOS / "robustez.md").write_text(
        f"# Resultados de robustez\n\n{args.pedidos} pedidos a {args.tasa:g} pedidos/s, "
        f"3 nodos x 4 hilos; falla de {VICTIMA} a los {args.momento:g} s.\n\n{markdown}\n",
        encoding="utf-8")
    print(f"\n{markdown}\n\nResultados en resultados/robustez.csv y resultados/robustez.md")
    sys.exit(0 if all(f["aprobado"] for f in filas) else 1)


if __name__ == "__main__":
    main()

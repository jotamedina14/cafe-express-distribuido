"""Logs de cada componente: un archivo por componente, con milisegundos y el
nombre del hilo que escribió cada línea (evidencia directa del manejo de hilos).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

CARPETA_LOGS = Path(__file__).resolve().parent / "logs"
FORMATO = "%(asctime)s.%(msecs)03d %(levelname)-7s [%(threadName)s] %(message)s"
FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"


def configurar_bitacora(componente: str, nivel: int | str = logging.INFO,
                        consola: bool = True, carpeta: str | Path | None = None) -> logging.Logger:
    """Crea el logger de un componente; escribe en <carpeta>/<componente>.log."""
    carpeta = Path(carpeta) if carpeta else CARPETA_LOGS
    carpeta.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"cafe.{componente}")
    logger.setLevel(nivel)
    logger.propagate = False
    for manejador in list(logger.handlers):
        logger.removeHandler(manejador)
        manejador.close()

    formato = logging.Formatter(FORMATO, FORMATO_FECHA)
    # Modo anexar: si se lanza por error un segundo proceso con el mismo
    # nombre, no borra el log del que ya está corriendo.
    ruta = carpeta / f"{componente}.log"
    with open(ruta, "a", encoding="utf-8") as previo:
        previo.write(f"\n===== inicio de {componente} =====\n")
    archivo = logging.FileHandler(ruta, mode="a", encoding="utf-8")
    archivo.setFormatter(formato)
    logger.addHandler(archivo)
    if consola:
        pantalla = logging.StreamHandler(sys.stdout)
        pantalla.setFormatter(formato)
        logger.addHandler(pantalla)
    return logger

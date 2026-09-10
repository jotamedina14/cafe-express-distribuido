"""Levanta el sistema completo como procesos independientes del sistema
operativo: un coordinador y N nodos, cada uno con su propio intérprete de
Python, sus propios hilos y sus propios puertos TCP.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from bitacora import FORMATO_FECHA  # noqa: E402

MARCA_INICIO = "===== inicio de"


class SistemaLocal:
    def __init__(self, hilos=(4, 4, 4), estrategia="menor_carga", reasignar=True,
                 trabajo="espera", factor_tiempo=1.0, puerto_base=5100,
                 carpeta_logs: str | Path | None = None, extra_coordinador=()):
        self.hilos = tuple(hilos)
        self.estrategia = estrategia
        self.reasignar = reasignar
        self.trabajo = trabajo
        self.factor_tiempo = factor_tiempo
        self.puerto_clientes = puerto_base
        self.puerto_nodos = puerto_base + 1
        self.puerto_base = puerto_base
        self.carpeta_logs = Path(carpeta_logs) if carpeta_logs else RAIZ / "logs" / "pruebas"
        self.extra_coordinador = list(extra_coordinador)
        self.procesos: dict[str, subprocess.Popen] = {}
        self._errores = []

    @property
    def nodos(self) -> list[str]:
        return [f"nodo-{i}" for i in range(1, len(self.hilos) + 1)]

    def iniciar(self) -> "SistemaLocal":
        shutil.rmtree(self.carpeta_logs, ignore_errors=True)
        self.carpeta_logs.mkdir(parents=True)
        comando = [
            "coordinador.py", "--puerto-clientes", self.puerto_clientes,
            "--puerto-nodos", self.puerto_nodos, "--estrategia", self.estrategia,
            "--resumen-cada", 0, *self.extra_coordinador,
        ]
        if not self.reasignar:
            comando.append("--sin-reasignacion")
        self._lanzar("coordinador", comando)
        self._esperar_log("coordinador", "Coordinador listo", 1)
        for nodo_id, hilos in zip(self.nodos, self.hilos):
            self._lanzar(nodo_id, [
                "nodo.py", "--id", nodo_id, "--puerto", self.puerto_base + 1000 + int(nodo_id[5:]),
                "--hilos", hilos, "--puerto-coordinador", self.puerto_nodos,
                "--trabajo", self.trabajo, "--factor-tiempo", self.factor_tiempo,
            ])
        self._esperar_log("coordinador", "registrado desde", len(self.hilos),
                          timeout=10 + (2 * len(self.hilos) if self.trabajo == "cpu" else 0))
        return self

    def detener(self) -> None:
        for proceso in self.procesos.values():
            if proceso.poll() is None:
                proceso.kill()
        for proceso in self.procesos.values():
            proceso.wait(timeout=10)
        for archivo in self._errores:
            archivo.close()
        self.procesos.clear()

    def __enter__(self) -> "SistemaLocal":
        try:
            return self.iniciar()
        except Exception:
            self.detener()
            raise

    def __exit__(self, *_) -> None:
        self.detener()

    # --- fallas inyectadas -----------------------------------------------------

    def matar_nodo(self, nodo_id: str) -> None:
        """Caída abrupta: SIGKILL, sin cerrar nada ordenadamente."""
        self.procesos[nodo_id].kill()

    def congelar_nodo(self, nodo_id: str) -> None:
        """El proceso sigue existiendo y sus sockets abiertos, pero no responde."""
        if not hasattr(signal, "SIGSTOP"):
            raise RuntimeError("congelar un proceso requiere Linux o macOS")
        os.kill(self.procesos[nodo_id].pid, signal.SIGSTOP)

    # --- logs ---------------------------------------------------------------------

    def lineas_log(self, componente: str) -> list[str]:
        """Líneas del log del arranque actual del componente."""
        ruta = self.carpeta_logs / f"{componente}.log"
        if not ruta.exists():
            return []
        texto = ruta.read_text(encoding="utf-8", errors="replace")
        return texto.rsplit(MARCA_INICIO, 1)[-1].splitlines()[1:]

    def eventos_log(self, componente: str, patron: str) -> list[tuple[float, str]]:
        """(marca de tiempo epoch, línea) de cada línea del log que contiene `patron`."""
        eventos = []
        for linea in self.lineas_log(componente):
            if patron in linea:
                fecha = datetime.strptime(linea[:23], FORMATO_FECHA + ".%f")
                eventos.append((fecha.timestamp(), linea))
        return eventos

    # --- internos -------------------------------------------------------------------

    def _lanzar(self, nombre: str, argumentos: list) -> None:
        errores = open(self.carpeta_logs / f"{nombre}.err", "w", encoding="utf-8")
        self._errores.append(errores)
        comando = [sys.executable, str(RAIZ / argumentos[0]), *map(str, argumentos[1:]),
                   "--logs", str(self.carpeta_logs), "--silencioso"]
        self.procesos[nombre] = subprocess.Popen(comando, cwd=RAIZ, stdout=subprocess.DEVNULL,
                                                 stderr=errores)

    def _esperar_log(self, componente: str, texto: str, veces: int, timeout: float = 10) -> None:
        limite = time.monotonic() + timeout
        while time.monotonic() < limite:
            if sum(texto in linea for linea in self.lineas_log(componente)) >= veces:
                return
            for nombre, proceso in self.procesos.items():
                if proceso.poll() is not None:
                    error = (self.carpeta_logs / f"{nombre}.err").read_text(encoding="utf-8")
                    raise RuntimeError(f"{nombre} terminó al arrancar:\n{error}")
            time.sleep(0.05)
        raise TimeoutError(f"{componente} no registró '{texto}' {veces} veces en {timeout} s")

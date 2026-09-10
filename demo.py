"""Demo multiplataforma (Windows, Linux y macOS): coordinador + 3 nodos + cliente.

    python demo.py                    cliente automático con 12 pedidos
    python demo.py interactivo        cliente interactivo
    python demo.py auto round_robin   cambia la estrategia de balanceo

Los logs de cada componente quedan en logs/. Ctrl+C detiene todo.
"""
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent


def main() -> int:
    modo = sys.argv[1] if len(sys.argv) > 1 else "auto"
    estrategia = sys.argv[2] if len(sys.argv) > 2 else "menor_carga"
    procesos = []

    def lanzar(*argumentos):
        procesos.append(subprocess.Popen([sys.executable, *argumentos], cwd=RAIZ))

    try:
        print(f"Levantando el coordinador (estrategia: {estrategia})...", flush=True)
        lanzar("coordinador.py", "--estrategia", estrategia, "--nivel-log", "DEBUG", "--silencioso")
        time.sleep(1)
        for i in (1, 2, 3):
            print(f"Levantando nodo-{i} (puerto {6000 + i}, 4 hilos)...", flush=True)
            lanzar("nodo.py", "--id", f"nodo-{i}", "--puerto", str(6000 + i), "--hilos", "4",
                   "--silencioso")
        time.sleep(1.5)
        caidos = [p for p in procesos if p.poll() is not None]
        if caidos:
            print("Algún componente no arrancó. "
                  "¿Están ocupados los puertos 5000, 5001 o 6001-6003?", flush=True)
            return 1
        print("\nSistema arriba: coordinador en 5000/5001, nodos en 6001-6003. Logs en logs/.\n", flush=True)
        cliente = ["cliente.py", "--sucursal", "centro"]
        if modo != "interactivo":
            cliente += ["--auto", "12", "--intervalo", "0.25"]
        return subprocess.call([sys.executable, *cliente], cwd=RAIZ)
    except KeyboardInterrupt:
        return 130
    finally:
        print("\nDeteniendo el sistema...", flush=True)
        for proceso in procesos:
            proceso.terminate()
        for proceso in procesos:
            proceso.wait()


if __name__ == "__main__":
    sys.exit(main())

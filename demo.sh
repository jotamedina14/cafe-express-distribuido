#!/usr/bin/env bash
# Demo de CaféExpress Distribuido: coordinador + 3 nodos + cliente de sucursal.
#
#   ./demo.sh                      cliente automático con 12 pedidos
#   ./demo.sh interactivo          cliente interactivo (pedir, consultar, seguir...)
#   ./demo.sh auto round_robin     cambia la estrategia de balanceo
#
# Los logs de cada componente quedan en logs/. Ctrl+C detiene todo.
set -euo pipefail
cd "$(dirname "$0")"

MODO="${1:-auto}"
ESTRATEGIA="${2:-menor_carga}"
PY="${PYTHON:-python3}"
PIDS=()

detener() {
  trap - EXIT INT TERM
  echo
  echo "Deteniendo el sistema..."
  kill "${PIDS[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap detener EXIT INT TERM

echo "Levantando el coordinador (estrategia: $ESTRATEGIA)..."
"$PY" coordinador.py --estrategia "$ESTRATEGIA" --nivel-log DEBUG --silencioso &
PIDS+=($!)
sleep 1

for i in 1 2 3; do
  echo "Levantando nodo-$i (puerto 600$i, 4 hilos)..."
  "$PY" nodo.py --id "nodo-$i" --puerto "600$i" --hilos 4 --silencioso &
  PIDS+=($!)
done
sleep 1.5

echo
echo "Sistema arriba: coordinador en 5000 (clientes) y 5001 (nodos); nodos en 6001-6003."
echo "Para ver lo que pasa por dentro, en otra terminal:  tail -f logs/coordinador.log"
echo

if [ "$MODO" = "interactivo" ]; then
  "$PY" cliente.py --sucursal centro
else
  "$PY" cliente.py --sucursal centro --auto 12 --intervalo 0.25
fi

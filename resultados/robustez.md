# Resultados de robustez

120 pedidos a 10 pedidos/s, 3 nodos x 4 hilos; falla de nodo-2 a los 4 s.

| Escenario | Entregados | Perdidos | Reasignados | Detección (s) | Circuito abierto (s) | Latencia prom. (s) | p95 (s) | Máx. (s) | Resultado |
|---|---|---|---|---|---|---|---|---|---|
| SIGKILL a nodo-2, con reasignación | 120/120 | 0 | 3 | 0.002 | - | 0.778 | 1.186 | 1.6 | OK |
| SIGKILL a nodo-2, SIN reasignación (línea base) | 118/120 | 2 | 0 | 0.002 | - | 0.742 | 1.153 | 1.215 | OK |
| SIGSTOP a nodo-2, timeout de asignación 1 s | 120/120 | 0 | 1 | 6.364 | 3.008 | 2.05 | 3.004 | 9.675 | OK |
| SIGSTOP a nodo-2, timeout de asignación 0.25 s (ajuste) | 120/120 | 0 | 2 | 6.366 | 0.757 | 0.884 | 1.18 | 7.985 | OK |

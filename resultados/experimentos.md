# Resultados de los experimentos

Máquina: 12 núcleos lógicos, Python 3.12.3 (linux). Todos los componentes corren como procesos separados en la misma máquina y se comunican por TCP.


## Escalabilidad horizontal: más nodos de 4 hilos

Mismo total de pedidos en ráfaga; cada nodo agregado es un proceso aparte.

| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) | Pedidos por nodo |
|---|---|---|---|---|---|---|---|---|---|
| 1 nodo x 4 hilos | 4 | 120/120 | 5.36 | x1.00 | 11.096 | 20.682 | 10.372 | 0.724 | nodo-1:120 |
| 2 nodos x 4 hilos | 8 | 120/120 | 10.46 | x1.95 | 5.677 | 10.634 | 4.952 | 0.726 | nodo-1:60 nodo-2:60 |
| 3 nodos x 4 hilos | 12 | 120/120 | 14.22 | x2.65 | 3.910 | 7.252 | 3.191 | 0.719 | nodo-1:40 nodo-2:40 nodo-3:40 |

## Escalabilidad vertical: un solo nodo con más hilos

Mismo total de pedidos en ráfaga; se agranda el grupo de hilos de un nodo.

| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) | Pedidos por nodo |
|---|---|---|---|---|---|---|---|---|---|
| 1 nodo x 4 hilos | 4 | 120/120 | 5.43 | x1.00 | 11.276 | 20.982 | 10.552 | 0.725 | nodo-1:120 |
| 1 nodo x 8 hilos | 8 | 120/120 | 10.65 | x1.96 | 5.688 | 10.416 | 4.970 | 0.718 | nodo-1:120 |
| 1 nodo x 12 hilos | 12 | 120/120 | 15.28 | x2.81 | 3.966 | 7.083 | 3.234 | 0.731 | nodo-1:120 |

## Estrategia de balanceo con nodos de distinta capacidad (2, 4 y 4 hilos)

Round robin reparte por igual; menor carga reparte según hilos libres.

| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) | Pedidos por nodo |
|---|---|---|---|---|---|---|---|---|---|
| round_robin | 10 | 120/120 | 8.47 | x1.00 | 5.110 | 12.138 | 4.385 | 0.726 | nodo-1:40 nodo-2:40 nodo-3:40 |
| menor_carga | 10 | 120/120 | 12.16 | x1.44 | 4.646 | 8.704 | 3.916 | 0.730 | nodo-1:24 nodo-2:48 nodo-3:48 |

## Trabajo de CPU y el GIL de Python

La preparación es cálculo puro (cantidad fija de operaciones). Más hilos en un proceso compiten por el GIL; más nodos son procesos distintos.

| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) | Pedidos por nodo |
|---|---|---|---|---|---|---|---|---|---|
| 1 nodo x 1 hilo (base) | 1 | 30/30 | 2.84 | x1.00 | 5.320 | 10.058 | 4.968 | 0.352 | nodo-1:30 |
| 1 nodo x 3 hilos (vertical) | 3 | 30/30 | 2.76 | x0.97 | 5.706 | 10.782 | 4.655 | 1.051 | nodo-1:30 |
| 3 nodos x 1 hilo (horizontal) | 3 | 30/30 | 7.16 | x2.52 | 2.012 | 3.763 | 1.632 | 0.380 | nodo-1:10 nodo-2:10 nodo-3:10 |

## Saturación: latencia según la tasa de llegada

Pedidos a ritmo constante durante ~6 s. Capacidad teórica: ~5.9 pedidos/s por nodo de 4 hilos (4 hilos / 0.68 s de preparación promedio).

| Configuración | Tasa ofrecida (ped/s) | Throughput (ped/s) | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Perdidos |
|---|---|---|---|---|---|---|
| 1 nodo x 4 hilos | 4 | 3.77 | 0.691 | 1.122 | 0.003 | 0 |
| 1 nodo x 4 hilos | 8 | 5.19 | 1.979 | 3.255 | 1.255 | 0 |
| 1 nodo x 4 hilos | 12 | 5.53 | 3.896 | 6.790 | 3.190 | 0 |
| 1 nodo x 4 hilos | 16 | 5.44 | 6.085 | 11.221 | 5.361 | 0 |
| 3 nodos x 4 hilos | 4 | 3.77 | 0.688 | 1.175 | 0.001 | 0 |
| 3 nodos x 4 hilos | 8 | 7.19 | 0.708 | 1.050 | 0.001 | 0 |
| 3 nodos x 4 hilos | 12 | 10.68 | 0.711 | 1.088 | 0.001 | 0 |
| 3 nodos x 4 hilos | 16 | 13.58 | 0.756 | 1.145 | 0.034 | 0 |

## Techo del coordinador (preparación casi instantánea)

Con preparación de ~1 ms los nodos sobran: el límite lo pone el coordinador. La primera fila es la versión anterior al ajuste, que escribía en el log tres líneas por pedido; la aceleración de las demás se mide contra ella.

| Configuración | Hilos totales | Entregados | Throughput (ped/s) | Aceleración | Latencia prom. (s) | p95 (s) | Espera en cola (s) | Preparación (s) | Pedidos por nodo |
|---|---|---|---|---|---|---|---|---|---|
| 2 nodos x 8 hilos, traza por pedido (antes) | 16 | 2000/2000 | 1504.51 | x1.00 | 0.598 | 0.890 | 0.596 | 0.002 | nodo-1:1103 nodo-2:897 |
| 1 nodo x 8 hilos | 8 | 2000/2000 | 2278.83 | x1.51 | 0.338 | 0.538 | 0.336 | 0.002 | nodo-1:2000 |
| 2 nodos x 8 hilos | 16 | 2000/2000 | 2204.57 | x1.47 | 0.358 | 0.540 | 0.356 | 0.002 | nodo-1:1067 nodo-2:933 |
| 3 nodos x 8 hilos | 24 | 2000/2000 | 2184.63 | x1.45 | 0.357 | 0.562 | 0.356 | 0.002 | nodo-1:769 nodo-2:663 nodo-3:568 |

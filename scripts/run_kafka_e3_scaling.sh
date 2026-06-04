#!/usr/bin/env bash
# E3: Escalamiento horizontal -- 1, 2 y 4 consumers con alta carga.
#
# DISEÑO (corregido):
#   - LATENCY_FACTOR_MS=80 hace que cada miss tarde ~80-150ms en el backend
#     (proporcional al tamaño del dataset por zona, Q4 más lento por 2 zonas)
#   - Con 8.5% miss rate a 200 req/s → ~17 misses/s al backend
#   - 1 consumer (single-threaded async) puede procesar ~10-12 misses/s con
#     80ms de latencia backend → se satura → backlog crece
#   - 2 consumers procesan ~20-24 misses/s → backlog estable
#   - 4 consumers procesan ~40+ misses/s → backlog se vacía más rápido
#   - ARRIVAL_RATE=200 supera ampliamente la capacidad de 1 consumer
#   - NUM_REQUESTS=4000 da tiempo suficiente para ver diferencia entre N=1,2,4
#
# NOTA: response_generator corre con 1 worker (Dockerfile), así que el
#       cuello de botella real es el backend — exactamente lo que queremos.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

RATE_SCALING=200   # req/s publicados — supera capacidad de 1 consumer
NREQS_SCALING=4000 # suficientes para ver la diferencia entre 1, 2 y 4 consumers

echo "========================================"
echo "  ESCENARIO 3: Escalamiento horizontal"
echo "  arrival_rate=${RATE_SCALING} req/s"
echo "  latency_factor=80ms (backend lento, cuello de botella real)"
echo "========================================"

for N in 1 2 4; do
  echo ""
  echo "--- $N consumer(s) ---"

  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true

  # LATENCY_FACTOR_MS=80: cada miss tarda ~80ms * (registros/10k)
  # Con ~50k registros por zona → ~400ms por miss → consumer se satura fácilmente
  REDIS_EVICTION_POLICY="allkeys-lru" REDIS_MAXMEMORY="256mb" \
  LATENCY_FACTOR_MS=80 \
    $DC -f "$COMPOSE" up -d --build \
      redis metrics_store response_generator cache_service \
      zookeeper kafka

  wait_healthy
  wait_kafka_healthy

  $DC -f "$COMPOSE" up -d --build --scale kafka_consumer="$N" kafka_consumer

  wait_consumers_ready "$N"

  echo ""
  echo "  >> e3_kafka_${N}c"
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
  TRACK_PID=$(track_backlog_loop 2 "scaling_${N}c")

  run_traffic_async "zipf" "1.2" "$CONF_LOW" "$NREQS_SCALING" "$RATE_SCALING"
  wait_backlog_empty 300
  sleep 5
  kill "$TRACK_PID" 2>/dev/null || true
  save_and_show_kafka "e3_kafka_${N}c"
done

show_summary

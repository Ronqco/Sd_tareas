#!/usr/bin/env bash
# E4: Falla temporal del backend -- mide backlog, reintentos y recovery.
#
# DISENO:
#   - Falla de 15s
#   - MAX_RETRIES=5, RETRY_DELAY_MS=100 -> backoff: 100+200+400+800+1600ms = 3.1s total
#   - La falla dura 15s >> backoff total de 3.1s -> los primeros reintentos fallan
#     pero los ultimos alcanzan al backend restaurado -> recovery_rate visible
#   - LATENCY_FACTOR_MS=50 hace al backend mas lento -> backlog crece durante falla
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

echo "========================================"
echo "  ESCENARIO 4: Falla temporal del backend"
echo "========================================"

run_e4() {
  local label="$1"
  local dist="$2"
  local alpha="$3"

  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true

  # LATENCY_FACTOR_MS=50 hace que cada miss tarde ~50ms en el backend
  # -> el consumer se satura con misses -> backlog crece -> recovery visible
  REDIS_EVICTION_POLICY="allkeys-lru" REDIS_MAXMEMORY="256mb" \
  LATENCY_FACTOR_MS=50 \
    $DC -f "$COMPOSE" up -d --build \
      redis metrics_store response_generator cache_service \
      zookeeper kafka

  wait_healthy
  wait_kafka_healthy

  # Consumer con MAX_RETRIES=5 y RETRY_DELAY_MS=100 (backoff corto)
  # backoff total: 100+200+400+800+1600 = 3100ms << falla de 15s
  # -> reintentos 1-3 fallan durante la falla, reintentos 4-5 llegan
  #    cuando el backend ya volvio -> recovery_rate > 0
  MAX_RETRIES=5 RETRY_DELAY_MS=100 \
    $DC -f "$COMPOSE" up -d --build --scale kafka_consumer=2 kafka_consumer

  wait_consumers_ready 2
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null

  echo ""
  echo "  [1/5] Iniciando trafico $dist en background (800 req a 20 req/s)..."
  $DC -f "$COMPOSE" run --rm \
    -e MODE=async \
    -e DISTRIBUTION="$dist" \
    -e NUM_REQUESTS=800 \
    -e ARRIVAL_RATE=20 \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$CONF_LOW" \
    traffic_generator &
  TG_PID=$!

  TRACK_PID=$(track_backlog_loop 2 "pre_failure")

  sleep 10

  echo "  [2/5] Deteniendo response_generator (falla de 15s)..."
  report_backlog "failure_start" > /dev/null
  $DC -f "$COMPOSE" stop response_generator
  echo "       response_generator detenido"

  sleep 15

  echo "  [3/5] Restaurando response_generator..."
  $DC -f "$COMPOSE" start response_generator
  report_backlog "failure_end" > /dev/null
  echo "       response_generator restaurado"

  wait $TG_PID 2>/dev/null || true

  echo "  [4/5] Esperando vaciado del backlog + reintentos en vuelo..."
  wait_backlog_empty "queries" 180
  # Espera adicional para que reintentos en vuelo terminen de notificar
  sleep 20

  kill "$TRACK_PID" 2>/dev/null || true

  echo "  [5/5] Guardando resultados..."
  save_and_show_kafka "$label"

  echo ""
  echo "  Detalle Kafka:"
  curl -s "$METRICS_URL/kafka_summary" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('     retries         : {}'.format(d.get('total_retries', 0)))
print('     recoveries      : {}'.format(d.get('total_recoveries', 0)))
print('     dlq             : {}'.format(d.get('total_dlq', 0)))
print('     peak_backlog    : {}'.format(d.get('peak_backlog_size', 0)))
rt = d.get('recovery_time_s')
print('     recovery_time   : {}s'.format(rt if rt is not None else 'N/A'))
"
}

run_e4 "e4_falla_temporal" "zipf" "1.2"

show_summary

#!/usr/bin/env bash
# E5: Reintentos intermitentes -- mide retry_rate, recovery_rate y DLQ_rate.
#
# DISEÑO (corregido):
#   - 3 fallas de 12s separadas por 30s de operación normal
#   - MAX_RETRIES=5, RETRY_DELAY_MS=200 → backoff: 200+400+800+1600+3200ms = 6.2s
#   - Falla 12s > backoff parcial (6.2s) → reintentos 1-4 ocurren durante la falla,
#     el reintento 5 (a los ~6.2s) llega cuando el backend YA volvió → recovery > 0
#   - Con RETRY_DELAY_MS=100 anterior el backoff total era 3.1s << falla 8s,
#     todos los reintentos agotaban antes de que el backend volviera → recovery=0%
#   - LATENCY_FACTOR_MS=80 fuerza backlog real durante la falla
#   - cache_service tiene su propio retry interno (3 intentos, 0.2s*attempt).
#     Para que los errores lleguen al consumer hay que esperar que el cache_service
#     agote sus reintentos → timeout del consumer configurado en 15s (ya lo está).
#     Con la falla de 12s el cache_service SÍ agota sus reintentos y lanza 503
#     → el consumer recibe el error → lo manda a TOPIC_RETRY → recovery_rate > 0
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

echo "========================================"
echo "  ESCENARIO 5: Reintentos intermitentes"
echo "========================================"

run_e5() {
  local label="$1"
  local dist="$2"
  local alpha="$3"

  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true

  REDIS_EVICTION_POLICY="allkeys-lru" REDIS_MAXMEMORY="256mb" \
  LATENCY_FACTOR_MS=80 \
    $DC -f "$COMPOSE" up -d --build \
      redis metrics_store response_generator cache_service \
      zookeeper kafka

  wait_healthy
  wait_kafka_healthy

  # RETRY_DELAY_MS=200: backoff 200+400+800+1600+3200 = 6.2s total
  # Falla de 12s: reintentos 1-4 fallan (durante falla), reintento 5 (~6.2s)
  # alcanza al backend restaurado → recovery_rate > 0 y visible
  MAX_RETRIES=5 RETRY_DELAY_MS=200 \
    $DC -f "$COMPOSE" up -d --build --scale kafka_consumer=2 kafka_consumer

  wait_consumers_ready 2
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null

  echo ""
  echo "  Iniciando tráfico $dist en background..."
  $DC -f "$COMPOSE" run --rm \
    -e MODE=async \
    -e DISTRIBUTION="$dist" \
    -e NUM_REQUESTS="$NREQS" \
    -e ARRIVAL_RATE="$RATE" \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$CONF_LOW" \
    traffic_generator &
  TG_PID=$!

  TRACK_PID=$(track_backlog_loop 2 "${label}_tracking")

  # 3 fallas de 12s separadas por 30s
  # backoff total 6.2s < falla 12s → últimos reintentos llegan al backend restaurado
  for i in 1 2 3; do
    sleep 30
    echo "  [falla $i/3] Deteniendo response_generator 12s..."
    report_backlog "failure_${i}_start" > /dev/null
    $DC -f "$COMPOSE" stop response_generator
    sleep 12
    echo "  [falla $i/3] Restaurando..."
    $DC -f "$COMPOSE" start response_generator
    report_backlog "failure_${i}_end" > /dev/null
    sleep 3
  done

  wait $TG_PID 2>/dev/null || true
  wait_backlog_empty "queries" 180
  # Espera adicional para reintentos en vuelo (hasta 6.2s de backoff + procesamiento)
  sleep 25
  kill "$TRACK_PID" 2>/dev/null || true

  save_and_show_kafka "$label"

  echo ""
  curl -s "$METRICS_URL/kafka_summary" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('  Kafka detail:')
print('     retry_rate      : {:.1%}'.format(d.get('retry_rate', 0)))
print('     recovery_rate   : {:.1%}'.format(d.get('recovery_rate', 0)))
print('     dlq_rate        : {:.1%}'.format(d.get('dlq_rate', 0)))
print('     peak_backlog    : {}'.format(d.get('peak_backlog_size', 0)))
rt = d.get('recovery_time_s')
print('     recovery_time   : {}s'.format(rt if rt is not None else 'N/A'))
"
}

run_e5 "e5_reintentos" "zipf" "1.2"

show_summary

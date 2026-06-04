#!/usr/bin/env bash
# E7: Comparación directa Sync vs Async ante falla temporal.
# Corre en Zipf y Uniforme para mostrar que la ventaja de Kafka es
# independiente del patrón de acceso.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

echo "========================================"
echo "  ESCENARIO 7: Sync vs Async ante falla"
echo "========================================"

run_e7() {
  local dist="$1"
  local alpha="$2"
  local suffix="$3"   # "zipf" o "uniform"

  # ── Parte A: Sistema SÍNCRONO ───────────────────────────────────────────────
  echo ""
  echo "  [A-$suffix] Sistema síncrono con falla..."
  restart_stack "allkeys-lru" "256mb"
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null

  $DC -f "$COMPOSE" run --rm \
    -e MODE=sync \
    -e DISTRIBUTION="$dist" \
    -e NUM_REQUESTS=600 \
    -e ARRIVAL_RATE=20 \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$CONF_LOW" \
    traffic_generator &
  TG_PID=$!

  sleep 10
  echo "  Deteniendo response_generator 8s..."
  $DC -f "$COMPOSE" stop response_generator
  sleep 8
  $DC -f "$COMPOSE" start response_generator

  wait $TG_PID 2>/dev/null || true
  sleep 3
  curl -sf -X POST "$METRICS_URL/save?label=e7_sync_${suffix}" > /dev/null
  echo "  Sync-$suffix guardado."

  # ── Parte B: Sistema ASÍNCRONO (Kafka) ────────────────────────────────────
  echo ""
  echo "  [B-$suffix] Sistema Kafka con falla..."
  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true

  REDIS_EVICTION_POLICY="allkeys-lru" REDIS_MAXMEMORY="256mb" \
  MAX_RETRIES=5 RETRY_DELAY_MS=300 \
    $DC -f "$COMPOSE" up -d --build \
      redis metrics_store response_generator cache_service \
      zookeeper kafka

  wait_healthy
  wait_kafka_healthy

  MAX_RETRIES=5 RETRY_DELAY_MS=300 \
    $DC -f "$COMPOSE" up -d --scale kafka_consumer=2 kafka_consumer

  wait_consumers_ready 2
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null

  $DC -f "$COMPOSE" run --rm \
    -e MODE=async \
    -e DISTRIBUTION="$dist" \
    -e NUM_REQUESTS=600 \
    -e ARRIVAL_RATE=20 \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$CONF_LOW" \
    traffic_generator &
  TG_PID=$!

  TRACK_PID=$(track_backlog_loop 2 "e7_async_${suffix}")

  sleep 10
  echo "  Deteniendo response_generator 8s..."
  $DC -f "$COMPOSE" stop response_generator
  report_backlog "failure_start" > /dev/null
  sleep 8
  $DC -f "$COMPOSE" start response_generator
  report_backlog "failure_end" > /dev/null

  wait $TG_PID 2>/dev/null || true
  echo "  Esperando recuperación del backlog..."
  wait_backlog_empty "queries" 180
  sleep 5
  kill "$TRACK_PID" 2>/dev/null || true
  save_and_show_kafka "e7_async_${suffix}"

  # ── Tabla comparativa ──────────────────────────────────────────────────────
  echo ""
  echo "  ── SYNC vs ASYNC [$suffix] ──"
  curl -s "$METRICS_URL/results" | python3 -c "
import json, sys
suffix = '$suffix'
d    = json.load(sys.stdin)
rows = {r['label']: r for r in d.get('results', [])}
sync  = rows.get('e7_sync_' + suffix,  {})
async_ = rows.get('e7_async_' + suffix, {})
print('  {:<30} {:>12} {:>14}'.format('Métrica', 'SYNC', 'ASYNC (Kafka)'))
print('  ' + '-'*58)
for k, label, f in [
    ('total_requests',   'Requests procesadas', '{:.0f}'),
    ('throughput_rps',   'Throughput (req/s)',  '{:.1f}'),
    ('latency_p50_ms',   'Latencia p50 (ms)',   '{:.2f}'),
    ('latency_p95_ms',   'Latencia p95 (ms)',   '{:.2f}'),
    ('total_retries',    'Reintentos',           '{:.0f}'),
    ('total_recoveries', 'Recuperadas',          '{:.0f}'),
    ('total_dlq',        'DLQ',                  '{:.0f}'),
    ('peak_backlog_size','Peak backlog',          '{:.0f}'),
    ('recovery_time_s',  'Recovery time (s)',     '{:.1f}'),
]:
    sv = sync.get(k)
    av = async_.get(k)
    def fmt(v):
        if v is None: return 'N/A'
        try: return f.format(float(v))
        except: return str(v)
    print('  {:<30} {:>12} {:>14}'.format(label, fmt(sv), fmt(av)))
"
}

run_e7 "zipf"    "1.2" "zipf"
run_e7 "uniform" "1.0" "uniform"

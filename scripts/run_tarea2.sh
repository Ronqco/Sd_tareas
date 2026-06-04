#!/usr/bin/env bash
# scripts/run_tarea2.sh
# Ejecuta los escenarios Kafka/Sync de Tarea 2 y guarda resultados en results/.

set -euo pipefail
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPTS_DIR/common_kafka.sh"

SCENARIO_TOTAL=7
SCENARIO_CURRENT=0
START_TIME=$(date +%s)
BACKLOG_WAIT=${BACKLOG_WAIT:-300}
E3_NREQS=${E3_NREQS:-4000}
E3_RATE=${E3_RATE:-200}

header() {
  echo ""
  echo "=========================================================="
  printf "  %-56s\n" "$1"
  echo "=========================================================="
}

elapsed_total() {
  local now
  now=$(date +%s)
  local secs=$(( now - START_TIME ))
  printf "%dm%02ds" $(( secs/60 )) $(( secs%60 ))
}

finish_backlog_or_fail() {
  local tracker_pid="$1"
  local label="$2"
  if ! wait_backlog_empty "$BACKLOG_WAIT"; then
    stop_tracker "$tracker_pid"
    echo "  ERROR: no se guarda $label porque el backlog no termino."
    exit 1
  fi
  sleep 3
  stop_tracker "$tracker_pid"
  save_and_show_kafka "$label"
}

header "TAREA 2 - INICIO | $(date '+%H:%M:%S')"
echo "  Requests base por experimento : $NREQS"
echo "  Rate base                    : $RATE req/s"
echo "  Espera maxima backlog         : ${BACKLOG_WAIT}s"

# E1 - Sistema sincrono base
tick_scenario
header "E1/7 - Sistema sincrono base"
restart_stack "allkeys-lru" "256mb"
run_experiment "e1_sync_base" "zipf" "1.2" "$CONF_LOW"
echo "  Tiempo total: $(elapsed_total)"

# E2 - Kafka + 1 consumer
tick_scenario
header "E2/7 - Kafka + 1 consumer"
restart_kafka_stack 1 "allkeys-lru" "256mb"
run_experiment_kafka "e2_kafka_1consumer" "zipf" "1.2" "$CONF_LOW" "$NREQS" "$RATE" "$BACKLOG_WAIT"
echo "  Tiempo total: $(elapsed_total)"

# E3 - Escalamiento horizontal con carga suficiente
tick_scenario
header "E3/7 - Escalamiento horizontal"
echo "  Carga E3: n=$E3_NREQS rate=$E3_RATE latency_factor=80ms"
for N in 1 2 4; do
  echo ""
  echo "  $N consumer(s)"
  LATENCY_FACTOR_MS=80 restart_kafka_stack "$N" "allkeys-lru" "256mb"
  run_experiment_kafka "e3_kafka_${N}consumers" "zipf" "1.2" "$CONF_LOW" "$E3_NREQS" "$E3_RATE" "$BACKLOG_WAIT"
done
echo "  Tiempo total: $(elapsed_total)"

# E4 - Falla temporal del backend
tick_scenario
header "E4/7 - Falla temporal del response_generator"
MAX_RETRIES=5 RETRY_DELAY_MS=1000 restart_kafka_stack 2 "allkeys-lru" "256mb"
curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
TRACK_PID=$(track_backlog_loop 2 "e4_falla_temporal")

echo "  Iniciando trafico async en background..."
$DC -f "$COMPOSE" run --rm \
  -e MODE=async -e DISTRIBUTION=zipf \
  -e NUM_REQUESTS="$NREQS" -e ARRIVAL_RATE="$RATE" \
  -e ZIPF_ALPHA=1.2 -e CONF_VALUES="$CONF_LOW" \
  traffic_generator > /tmp/tg_out.txt 2>&1 &
TG_PID=$!

sleep 8
echo "  Deteniendo response_generator 10s..."
$DC -f "$COMPOSE" stop response_generator
echo "  Backlog al detener: $(get_backlog)"
sleep 10
echo "  Restaurando response_generator..."
$DC -f "$COMPOSE" start response_generator
wait "$TG_PID" 2>/dev/null || true
finish_backlog_or_fail "$TRACK_PID" "e4_falla_temporal"
echo "  Tiempo total: $(elapsed_total)"

# E5 - Reintentos intermitentes
tick_scenario
header "E5/7 - Reintentos intermitentes"
$DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
docker volume rm proyecto-sd_redis_data 2>/dev/null || true
REDIS_EVICTION_POLICY="allkeys-lru" REDIS_MAXMEMORY="256mb" \
  $DC -f "$COMPOSE" up -d --build \
    redis metrics_store response_generator cache_service \
    zookeeper kafka 2>&1 | grep -E "^(Creating|Starting|done)" || true
wait_healthy
wait_kafka_healthy
MAX_RETRIES=5 RETRY_DELAY_MS=1000 \
  $DC -f "$COMPOSE" up -d --build --scale kafka_consumer=2 kafka_consumer 2>&1 | grep -E "^(Creating|Starting|done)" || true
wait_consumers_ready 2
curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
TRACK_PID=$(track_backlog_loop 2 "e5_reintentos")

echo "  Iniciando trafico con fallas intermitentes..."
$DC -f "$COMPOSE" run --rm \
  -e MODE=async -e DISTRIBUTION=zipf \
  -e NUM_REQUESTS="$NREQS" -e ARRIVAL_RATE="$RATE" \
  -e ZIPF_ALPHA=1.2 -e CONF_VALUES="$CONF_LOW" \
  traffic_generator > /tmp/tg_out.txt 2>&1 &
TG_PID=$!

for i in 1 2; do
  sleep 8
  echo "  Falla $i: deteniendo response_generator 5s..."
  $DC -f "$COMPOSE" stop response_generator
  sleep 5
  echo "  Falla $i: restaurando response_generator..."
  $DC -f "$COMPOSE" start response_generator
done

wait "$TG_PID" 2>/dev/null || true
finish_backlog_or_fail "$TRACK_PID" "e5_reintentos"
echo "  Tiempo total: $(elapsed_total)"

# E6 - Spike de trafico
tick_scenario
header "E6/7 - Spike de trafico"
LATENCY_FACTOR_MS=80 MAX_RETRIES=5 RETRY_DELAY_MS=1000 restart_kafka_stack 2 "allkeys-lru" "256mb"
curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
TRACK_PID=$(track_backlog_loop 2 "e6_spike")

echo "  [1/3] Trafico normal..."
run_traffic_async "zipf" "1.2" "$CONF_LOW" $(( NREQS/3 )) 20

echo "  [2/3] Spike 5x..."
echo "        backlog antes: $(get_backlog)"
run_traffic_async "zipf" "1.2" "$CONF_LOW" $(( NREQS/2 )) $(( RATE*5 ))
echo "        backlog en pico: $(get_backlog)"

echo "  [3/3] Vuelta a normal..."
run_traffic_async "zipf" "1.2" "$CONF_LOW" $(( NREQS/3 )) 20

finish_backlog_or_fail "$TRACK_PID" "e6_spike"
echo "  Tiempo total: $(elapsed_total)"

# E7 - Sync vs Async ante falla
tick_scenario
header "E7/7 - Comparacion Sync vs Async ante falla"

echo ""
echo "  [A] Sistema sincrono con falla..."
restart_stack "allkeys-lru" "256mb"
curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
$DC -f "$COMPOSE" run --rm \
  -e MODE=sync -e DISTRIBUTION=zipf \
  -e NUM_REQUESTS="$NREQS" -e ARRIVAL_RATE="$RATE" \
  -e ZIPF_ALPHA=1.2 -e CONF_VALUES="$CONF_LOW" \
  traffic_generator > /tmp/tg_out.txt 2>&1 &
TG_PID=$!

sleep 8
echo "  Deteniendo response_generator 10s..."
$DC -f "$COMPOSE" stop response_generator
sleep 10
echo "  Restaurando response_generator..."
$DC -f "$COMPOSE" start response_generator
wait "$TG_PID" 2>/dev/null || true
sleep 3
curl -sf -X POST "$METRICS_URL/save?label=e7_sync_falla" > /dev/null
echo "  Sync guardado."

echo ""
echo "  [B] Sistema async Kafka con falla..."
MAX_RETRIES=5 RETRY_DELAY_MS=1000 restart_kafka_stack 2 "allkeys-lru" "256mb"
curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
TRACK_PID=$(track_backlog_loop 2 "e7_async_falla")
$DC -f "$COMPOSE" run --rm \
  -e MODE=async -e DISTRIBUTION=zipf \
  -e NUM_REQUESTS="$NREQS" -e ARRIVAL_RATE="$RATE" \
  -e ZIPF_ALPHA=1.2 -e CONF_VALUES="$CONF_LOW" \
  traffic_generator > /tmp/tg_out.txt 2>&1 &
TG_PID=$!

sleep 8
echo "  Deteniendo response_generator 10s..."
$DC -f "$COMPOSE" stop response_generator
sleep 10
echo "  Restaurando response_generator..."
$DC -f "$COMPOSE" start response_generator
wait "$TG_PID" 2>/dev/null || true
finish_backlog_or_fail "$TRACK_PID" "e7_async_falla"
echo "  Tiempo total: $(elapsed_total)"

header "COMPARACION FINAL - $(elapsed_total)"
show_summary

echo ""
curl -s "$METRICS_URL/results" | python3 -c "
import json, sys
d = json.load(sys.stdin)
rows = {r['label']: r for r in d.get('results', [])}
sync = rows.get('e7_sync_falla', {})
async_ = rows.get('e7_async_falla', {})
if sync and async_:
    print('  SYNC vs ASYNC ante falla:')
    print('  {:<30} {:>12} {:>14}'.format('Metrica','SYNC','ASYNC (Kafka)'))
    print('  ' + '-'*58)
    for k, label, f in [
        ('total_requests', 'Requests procesadas', '{:.0f}'),
        ('throughput_rps', 'Throughput (req/s)', '{:.1f}'),
        ('latency_p50_ms', 'Latencia p50 (ms)', '{:.1f}'),
        ('latency_p95_ms', 'Latencia p95 (ms)', '{:.1f}'),
        ('total_retries', 'Reintentos', '{:.0f}'),
        ('total_recoveries', 'Recuperadas', '{:.0f}'),
        ('total_dlq', 'DLQ', '{:.0f}'),
    ]:
        sv = sync.get(k); av = async_.get(k)
        try: sv_s = f.format(float(sv))
        except Exception: sv_s = 'N/A'
        try: av_s = f.format(float(av))
        except Exception: av_s = 'N/A'
        print('  {:<30} {:>12} {:>14}'.format(label, sv_s, av_s))
"

echo ""
echo "Tarea 2 completada. Resultados en: results/"

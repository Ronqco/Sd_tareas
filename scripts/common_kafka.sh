#!/usr/bin/env bash
# scripts/common_kafka.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

KAFKA_CONTAINER="sd_kafka"

wait_kafka_healthy() {
  echo -n "  esperando Kafka"
  local i=0
  while [ $i -lt 60 ]; do
    sleep 5
    if docker exec "$KAFKA_CONTAINER" \
         kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
      echo " ok"
      return 0
    fi
    echo -n "."
    i=$((i+1))
  done
  echo ""
  echo "  ERROR: Kafka no responde tras 300s"
  exit 1
}

wait_consumers_ready() {
  local n="${1:-1}"
  echo -n "  esperando consumers ($n replicas)"
  local i=0
  while [ $i -lt 20 ]; do
    sleep 3
    local assigned
    assigned=$(docker exec "$KAFKA_CONTAINER" \
      kafka-consumer-groups \
      --bootstrap-server localhost:9092 \
      --group sd-consumers \
      --describe 2>/dev/null \
      | awk 'NR>1 && $1=="sd-consumers" && $7!="-" {print $7}' \
      | sort -u \
      | wc -l || echo 0)
    if [ "$assigned" -ge "$n" ] 2>/dev/null; then
      echo " ok (consumers activos: $assigned)"
      return 0
    fi
    echo -n "."
    i=$((i+1))
  done
  echo " WARN: consumers pueden no tener particiones aun"
}

restart_kafka_stack() {
  local n_consumers="${1:-1}"
  local eviction="${2:-allkeys-lru}"
  local maxmem="${3:-256mb}"
  local max_retries="${MAX_RETRIES:-3}"
  local retry_delay_ms="${RETRY_DELAY_MS:-500}"
  local latency_factor_ms="${LATENCY_FACTOR_MS:-5}"

  echo "  reiniciando stack Kafka (consumers=$n_consumers eviction=$eviction mem=$maxmem retries=$max_retries delay=${retry_delay_ms}ms latency=${latency_factor_ms}ms)..."
  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true

  REDIS_EVICTION_POLICY="$eviction" REDIS_MAXMEMORY="$maxmem" LATENCY_FACTOR_MS="$latency_factor_ms" \
    $DC -f "$COMPOSE" up -d --build \
      redis metrics_store response_generator cache_service \
      zookeeper kafka \
      2>&1 | grep -E "^(Creating|Starting|done|error)" || true

  wait_healthy
  wait_kafka_healthy

  MAX_RETRIES="$max_retries" RETRY_DELAY_MS="$retry_delay_ms" \
    $DC -f "$COMPOSE" up -d --build --scale kafka_consumer="$n_consumers" kafka_consumer \
    2>&1 | grep -E "^(Creating|Starting|done|error)" || true

  wait_consumers_ready "$n_consumers"
}

run_traffic_async() {
  local distribution="$1"
  local alpha="$2"
  local conf="$3"
  local nreqs="${4:-$NREQS}"
  local rate="${5:-$RATE}"

  echo "  enviando $nreqs requests a $rate req/s (async -> Kafka)..."
  local est=$(( nreqs / rate ))
  echo "     estimado publicacion: ~${est}s + procesamiento"

  $DC -f "$COMPOSE" run --rm \
    -e MODE=async \
    -e DISTRIBUTION="$distribution" \
    -e NUM_REQUESTS="$nreqs" \
    -e ARRIVAL_RATE="$rate" \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$conf" \
    traffic_generator > /tmp/tg_out.txt 2>&1 &
  local TG_PID=$!

  local elapsed=0
  while kill -0 $TG_PID 2>/dev/null; do
    sleep 2
    elapsed=$((elapsed+2))
    local sent=0
    sent=$(grep -oP '[0-9]+(?=/[0-9]+)' /tmp/tg_out.txt 2>/dev/null | tail -1 || echo 0)
    sent=${sent:-0}
    progress_bar "$sent" "$nreqs" "publicados en Kafka | ${elapsed}s"
  done
  wait $TG_PID 2>/dev/null || true
  progress_bar "$nreqs" "$nreqs" "publicacion completa"
  echo ""
}

track_backlog_loop() {
  local interval="${1:-2}"
  local label="${2:-backlog}"
  (
    while true; do
      local lag
      lag=$(get_backlog)
      curl -sf -X POST "$METRICS_URL/backlog" \
        -H 'Content-Type: application/json' \
        -d "{\"size\": ${lag:-0}, \"label\": \"$label\"}" > /dev/null 2>&1 || true
      sleep "$interval"
    done
  ) > /dev/null 2>&1 &
  echo $!
}

stop_tracker() {
  local pid="${1:-}"
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

get_backlog() {
  docker exec "$KAFKA_CONTAINER" \
    kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group sd-consumers \
    --describe 2>/dev/null \
    | awk 'NR>1 && $6~/^[0-9]+$/ {sum+=$6} END {print sum+0}'
}

wait_backlog_empty() {
  local max_wait="${1:-180}"
  local initial_lag=""
  local i=0

  echo -n "  procesando backlog"
  while [ $i -lt $((max_wait/3)) ]; do
    sleep 3
    local lag
    lag=$(get_backlog)
    if [ -z "$initial_lag" ] || [ "$initial_lag" = "0" ]; then
      initial_lag=${lag:-1}
      [ "$initial_lag" = "0" ] && initial_lag=1
    fi
    if [ "$lag" = "0" ] || [ -z "$lag" ]; then
      progress_bar "$initial_lag" "$initial_lag" "backlog vacio"
      echo ""
      return 0
    fi
    local processed=$(( initial_lag - lag ))
    [ $processed -lt 0 ] && processed=0
    progress_bar "$processed" "$initial_lag" "lag=$lag mensajes restantes"
    i=$((i+1))
  done

  local final_lag
  final_lag=$(get_backlog)
  if [ "$final_lag" = "0" ] || [ -z "$final_lag" ]; then
    progress_bar "$initial_lag" "$initial_lag" "backlog vacio"
    echo ""
    return 0
  fi

  echo ""
  echo "  ERROR: backlog no se vacio tras ${max_wait}s (lag=$final_lag)"
  return 1
}

save_and_show_kafka() {
  local label="$1"
  curl -sf -X POST "$METRICS_URL/save?label=$label" > /dev/null
  echo "  guardado: results/$label.json"
  curl -s "$METRICS_URL/summary" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('     hit_rate      : {:.1%}'.format(d.get('hit_rate', 0)))
print('     throughput    : {:.1f} req/s'.format(d.get('throughput_rps', 0)))
print('     latencia p50  : {:.2f} ms'.format(d.get('latency_p50_ms', 0)))
print('     latencia p95  : {:.2f} ms'.format(d.get('latency_p95_ms', 0)))
print('     retry_rate    : {:.1%}'.format(d.get('retry_rate', 0)))
print('     recovery_rate : {:.1%}'.format(d.get('recovery_rate', 0)))
print('     dlq_rate      : {:.1%}'.format(d.get('dlq_rate', 0)))
print('     total_retries : {}'.format(d.get('total_retries', 0)))
print('     recoveries    : {}'.format(d.get('total_recoveries', 0)))
print('     total_dlq     : {}'.format(d.get('total_dlq', 0)))
print('     peak_backlog  : {}'.format(d.get('peak_backlog_size', 0)))
"
}

run_experiment_kafka() {
  local label="$1"
  local distribution="$2"
  local alpha="$3"
  local conf="$4"
  local nreqs="${5:-$NREQS}"
  local rate="${6:-$RATE}"
  local wait_seconds="${7:-180}"

  echo ""
  echo "  experimento $label"
  echo "     dist=$distribution alpha=$alpha conf=$conf n=$nreqs rate=$rate"
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
  local TRACK_PID
  TRACK_PID=$(track_backlog_loop 2 "$label")
  run_traffic_async "$distribution" "$alpha" "$conf" "$nreqs" "$rate"
  wait_backlog_empty "$wait_seconds"
  sleep 3
  stop_tracker "$TRACK_PID"
  save_and_show_kafka "$label"
}

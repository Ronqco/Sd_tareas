#!/usr/bin/env bash
# scripts/common.sh

METRICS_URL="http://localhost:8003"
CACHE_URL="http://localhost:8001"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$BASE_DIR/docker-compose.yml"

NREQS=${NUM_REQUESTS:-1000}
RATE=${ARRIVAL_RATE:-50}
CONF_LOW="0.0,0.7"
CONF_HIGH="0.0,0.3,0.5,0.7,0.9"

if docker compose version > /dev/null 2>&1; then
  DC="docker compose"
else
  DC="docker-compose"
fi

# ── Barra de progreso ─────────────────────────────────────────────────────────
# Uso: progress_bar <paso_actual> <total_pasos> <label>
progress_bar() {
  local current=$1
  local total=$2
  local label="$3"
  local pct=$(( current * 100 / total ))
  local filled=$(( current * 30 / total ))
  local bar=""
  for ((i=0; i<filled; i++));    do bar+="█"; done
  for ((i=filled; i<30; i++));   do bar+="░"; done
  printf "\r  [%s] %3d%% (%d/%d) %s" "$bar" "$pct" "$current" "$total" "$label"
}

# ── Contador global de escenarios ────────────────────────────────────────────
SCENARIO_CURRENT=0
SCENARIO_TOTAL=0

tick_scenario() {
  SCENARIO_CURRENT=$((SCENARIO_CURRENT+1))
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  progress_bar "$SCENARIO_CURRENT" "$SCENARIO_TOTAL" "Escenario $SCENARIO_CURRENT de $SCENARIO_TOTAL"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# ── wait_healthy ──────────────────────────────────────────────────────────────
wait_healthy() {
  echo -n "  ⏳ esperando servicios"
  local i=0
  while [ $i -lt 30 ]; do
    sleep 3
    local cache_ok="no" metrics_ok="no"
    curl -sf "$CACHE_URL/health"   > /dev/null 2>&1 && cache_ok="yes"   || true
    curl -sf "$METRICS_URL/health" > /dev/null 2>&1 && metrics_ok="yes" || true
    if [ "$cache_ok" = "yes" ] && [ "$metrics_ok" = "yes" ]; then
      echo " ✅"
      return 0
    fi
    echo -n "."
    i=$((i+1))
  done
  echo ""
  echo "  ❌ ERROR: servicios no responden tras 90s"
  exit 1
}

# ── restart_stack ─────────────────────────────────────────────────────────────
restart_stack() {
  local eviction="$1"
  local maxmem="$2"
  echo "  🔄 reiniciando stack (eviction=$eviction mem=$maxmem)..."
  $DC -f "$COMPOSE" down --remove-orphans -v 2>/dev/null || true
  docker volume rm proyecto-sd_redis_data 2>/dev/null || true
  REDIS_EVICTION_POLICY="$eviction" REDIS_MAXMEMORY="$maxmem" \
    $DC -f "$COMPOSE" up -d --build redis metrics_store response_generator cache_service \
    2>&1 | grep -E "^(Creating|Starting|done|error)" || true
  wait_healthy
}

# ── run_traffic con progreso ──────────────────────────────────────────────────
run_traffic() {
  local distribution="$1"
  local alpha="$2"
  local conf="$3"

  echo "  📤 enviando $NREQS requests a $RATE req/s..."
  local est=$(( NREQS / RATE ))
  echo "     estimado: ~${est}s"

  # Correr en background y mostrar progreso cada 3s
  $DC -f "$COMPOSE" run --rm \
    -e DISTRIBUTION="$distribution" \
    -e NUM_REQUESTS="$NREQS" \
    -e ARRIVAL_RATE="$RATE" \
    -e ZIPF_ALPHA="$alpha" \
    -e CONF_VALUES="$conf" \
    traffic_generator > /tmp/tg_out.txt 2>&1 &
  local TG_PID=$!

  local elapsed=0
  while kill -0 $TG_PID 2>/dev/null; do
    sleep 2
    elapsed=$((elapsed+2))
    # Leer cuántos enviados del log
    local sent=0
    sent=$(grep -oP '→ \K[0-9]+(?=/)' /tmp/tg_out.txt 2>/dev/null | tail -1 || echo 0)
    sent=${sent:-0}
    progress_bar "$sent" "$NREQS" "enviados | ${elapsed}s transcurridos"
  done
  wait $TG_PID 2>/dev/null || true
  progress_bar "$NREQS" "$NREQS" "completado"
  echo ""
}

# ── save_and_show ─────────────────────────────────────────────────────────────
save_and_show() {
  local label="$1"
  curl -sf -X POST "$METRICS_URL/save?label=$label" > /dev/null
  echo "  💾 guardado: results/$label.json"
  curl -s "$METRICS_URL/summary" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('     hit_rate        : {:.1%}'.format(d.get('hit_rate',0)))
print('     throughput      : {:.1f} req/s'.format(d.get('throughput_rps',0)))
print('     latencia p50    : {:.2f} ms'.format(d.get('latency_p50_ms',0)))
print('     latencia p95    : {:.2f} ms'.format(d.get('latency_p95_ms',0)))
print('     eviction_rate   : {} ev/min'.format(d.get('eviction_rate_per_min',0)))
print('     total_evictions : {}'.format(d.get('total_evictions',0)))
"
}

# ── run_experiment ────────────────────────────────────────────────────────────
run_experiment() {
  local label="$1"
  local distribution="$2"
  local alpha="$3"
  local conf="$4"
  echo ""
  echo "  🧪 $label"
  echo "     dist=$distribution alpha=$alpha conf=$conf n=$NREQS rate=$RATE"
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null
  run_traffic "$distribution" "$alpha" "$conf"
  save_and_show "$label"
}

# ── show_summary ──────────────────────────────────────────────────────────────
show_summary() {
  echo ""
  echo "╔══════════════════════════════════════════════════════════════════════════════════════╗"
  echo "║              RESUMEN COMPARATIVO DE TODOS LOS EXPERIMENTOS                          ║"
  echo "╚══════════════════════════════════════════════════════════════════════════════════════╝"
  curl -s "$METRICS_URL/results" | python3 -c "
import json, sys
d = json.load(sys.stdin)
rows = d.get('results', [])
if not rows:
    print('  (sin resultados guardados)')
    sys.exit(0)
print('  {:<32} {:>8} {:>7} {:>7} {:>7} {:>10} {:>8} {:>8} {:>9} {:>7}'.format(
    'Experimento','HitRate','RPS','p50ms','p95ms','Evict/min','Evict','Retries','Recovery','DLQ'))
print('  ' + '-'*106)
for r in rows:
    print('  {:<32} {:>8} {:>7} {:>7} {:>7} {:>10} {:>8} {:>8} {:>9} {:>7}'.format(
        r.get('label','?'),
        '{:.1%}'.format(r['hit_rate'])       if r.get('hit_rate')       is not None else 'N/A',
        '{:.1f}'.format(r['throughput_rps'])  if r.get('throughput_rps') is not None else 'N/A',
        '{:.1f}'.format(r['latency_p50_ms'])  if r.get('latency_p50_ms') is not None else 'N/A',
        '{:.1f}'.format(r['latency_p95_ms'])  if r.get('latency_p95_ms') is not None else 'N/A',
        str(r.get('eviction_rate_per_min','N/A')),
        str(r.get('total_evictions','N/A')),
        str(r.get('total_retries','N/A')),
        '{:.1%}'.format(r['recovery_rate'])   if r.get('recovery_rate')  is not None else 'N/A',
        str(r.get('total_dlq','N/A')),
    ))
"
  echo ""
  echo "  📁 Archivos JSON en: $BASE_DIR/results/"
}

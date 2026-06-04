#!/usr/bin/env bash
# E6: Spike de tráfico (normal → spike → normal) en Zipf y Uniforme.
# Mide crecimiento del backlog durante el spike y recovery_time para vaciarlo.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

echo "========================================"
echo "  ESCENARIO 6: Spike de tráfico"
echo "========================================"

run_e6() {
  local label="$1"
  local dist="$2"
  local alpha="$3"

  restart_kafka_stack 2 "allkeys-lru" "256mb"
  curl -sf -X DELETE "$METRICS_URL/reset" > /dev/null

  TRACK_PID=$(track_backlog_loop 2 "${label}_tracking")

  echo ""
  echo "  [1/3] Tráfico normal (10 req/s, 300 requests) [$dist]..."
  report_backlog "pre_spike" > /dev/null
  run_traffic_async "$dist" "$alpha" "$CONF_LOW" 300 10

  echo ""
  echo "  [2/3] SPIKE (100 req/s, 500 requests)..."
  BACKLOG_ANTES=$(report_backlog "spike_start")
  echo "       backlog antes del spike: $BACKLOG_ANTES"
  run_traffic_async "$dist" "$alpha" "$CONF_LOW" 500 100
  BACKLOG_PICO=$(report_backlog "spike_peak")
  echo "       backlog en el pico: $BACKLOG_PICO"

  echo ""
  echo "  [3/3] Vuelta a normal (10 req/s, 300 requests)..."
  run_traffic_async "$dist" "$alpha" "$CONF_LOW" 300 10
  report_backlog "post_spike" > /dev/null

  echo ""
  echo "  Esperando vaciado del backlog (recovery_time)..."
  wait_backlog_empty "queries" 240
  sleep 5

  kill "$TRACK_PID" 2>/dev/null || true
  save_and_show_kafka "$label"
}

run_e6 "e6_spike_zipf"    "zipf"    "1.2"
run_e6 "e6_spike_uniform" "uniform" "1.0"

show_summary

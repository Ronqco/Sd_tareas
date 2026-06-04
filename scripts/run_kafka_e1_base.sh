#!/usr/bin/env bash
# E1: Sistema base síncrono — línea de referencia en Zipf y Uniforme.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "========================================"
echo "  ESCENARIO 1: Sistema síncrono base"
echo "========================================"

restart_stack "allkeys-lru" "256mb"
run_experiment "e1_sync_zipf"    "zipf"    "1.2" "$CONF_LOW"

restart_stack "allkeys-lru" "256mb"
run_experiment "e1_sync_uniform" "uniform" "1.0" "$CONF_LOW"

show_summary

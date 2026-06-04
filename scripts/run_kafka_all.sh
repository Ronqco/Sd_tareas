#!/usr/bin/env bash
# run_kafka_all.sh — corre todos los escenarios de Tarea 2 en secuencia.
# Cada escenario limpia su propio stack antes de correr.
set -euo pipefail
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   TAREA 2 — Suite completa de experimentos Kafka     ║"
echo "║   Zipf (α=1.2) + Uniforme — Q1-Q5 — 5 zonas         ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

bash "$SCRIPTS_DIR/run_kafka_e1_base.sh"
bash "$SCRIPTS_DIR/run_kafka_e2_single.sh"
bash "$SCRIPTS_DIR/run_kafka_e3_scaling.sh"
bash "$SCRIPTS_DIR/run_kafka_e4_failure.sh"
bash "$SCRIPTS_DIR/run_kafka_e5_retries.sh"
bash "$SCRIPTS_DIR/run_kafka_e6_spike.sh"
bash "$SCRIPTS_DIR/run_kafka_e7_recovery.sh"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   ✅  Todos los escenarios completados               ║"
echo "╚══════════════════════════════════════════════════════╝"
bash "$SCRIPTS_DIR/show_kafka_comparison.sh"

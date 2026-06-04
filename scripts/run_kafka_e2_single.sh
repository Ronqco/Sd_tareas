#!/usr/bin/env bash
# E2: Kafka + 1 consumer — overhead de Kafka vs sync, en Zipf y Uniforme.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common_kafka.sh"

echo "========================================"
echo "  ESCENARIO 2: Kafka + 1 consumer"
echo "========================================"

restart_kafka_stack 1 "allkeys-lru" "256mb"
run_experiment_kafka "e2_kafka_zipf"    "zipf"    "1.2" "$CONF_LOW"

restart_kafka_stack 1 "allkeys-lru" "256mb"
run_experiment_kafka "e2_kafka_uniform" "uniform" "1.0" "$CONF_LOW"

show_summary

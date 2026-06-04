#!/usr/bin/env bash
# scripts/run_all.sh
# Script maestro — ejecuta todos los grupos de experimentos en orden.
#
# Uso:
#   bash scripts/run_all.sh           # corre los 4 grupos (25 experimentos)
#   bash scripts/run_all.sh 1         # solo grupo 1
#   bash scripts/run_all.sh 2 3       # grupos 2 y 3
#   bash scripts/run_all.sh 1 2 3 4   # todos (equivalente al default)
#
# Los resultados se guardan en results/<label>.json
# Para generar graficos: python3 scripts/plot_results.py

set -eo pipefail

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Normalizar line endings (por si los scripts vienen de Windows) ─────────────
sed -i 's/\r//' "$SCRIPTS_DIR"/*.sh

# Grupos a correr
if [ "$#" -gt 0 ]; then
  RUN_GROUPS="$@"
else
  RUN_GROUPS="1 2 3 4"
fi

should_run() {
  local target="$1"
  for g in $RUN_GROUPS; do
    [ "$g" = "$target" ] && return 0
  done
  return 1
}

source "$SCRIPTS_DIR/common.sh"

echo ""
echo "========================================================"
echo "  Sistemas Distribuidos — Entregable 1"
echo "  Grupos a correr: $RUN_GROUPS"
echo "  Requests por experimento: $NREQS  |  Rate: $RATE req/s"
echo "========================================================"

should_run 1 && bash "$SCRIPTS_DIR/run_g1.sh"
should_run 2 && bash "$SCRIPTS_DIR/run_g2.sh"
should_run 3 && bash "$SCRIPTS_DIR/run_g3.sh"
should_run 4 && bash "$SCRIPTS_DIR/run_g4.sh"

show_summary

#!/usr/bin/env bash
# scripts/run_g1.sh
# GRUPO 1 — Impacto de la distribucion de trafico
#
# Variable que cambia : DISTRIBUTION (zipf | uniform)
# Fijo                : LRU, 256mb, TTL default, CONF_LOW
#
# Objetivo: comparar hit rate y latencia entre distribucion
# concentrada (Zipf) y distribucion uniforme.
# Con 256mb nunca hay evictions — se aisla el efecto de la distribucion.
#
# Experimentos: 2
#   G1_zipf_lru_256mb
#   G1_uniform_lru_256mb

source "$(dirname "$0")/common.sh"

# ── Parametros del grupo ───────────────────────────────────────────────────────
EVICTION="allkeys-lru"
MAXMEM="256mb"
ALPHA="1.2"
CONF="$CONF_LOW"   # 2 valores de conf → experimento limpio

echo ""
echo "========================================================"
echo "  GRUPO 1 — Distribucion de trafico"
echo "  eviction=$EVICTION  mem=$MAXMEM  conf=$CONF"
echo "========================================================"

restart_stack "$EVICTION" "$MAXMEM"
set_ttls $TTL_DEFAULT_Q1 $TTL_DEFAULT_Q3 $TTL_DEFAULT_Q4 $TTL_DEFAULT_Q5

for DIST in zipf uniform; do
  run_experiment "G1_${DIST}_lru_256mb" "$DIST" "$ALPHA" "$CONF"
done

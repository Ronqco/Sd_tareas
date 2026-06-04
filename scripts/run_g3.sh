#!/usr/bin/env bash
# scripts/run_g3.sh
# GRUPO 3 — Impacto del tamano de cache
#
# Variable que cambia : REDIS_MAXMEMORY (50mb | 200mb | 500mb)
# Fijo                : LRU, TTL default, CONF_HIGH, ambas distribuciones
#
# Objetivo: ver como el tamano de la memoria disponible afecta
# el hit rate y la tasa de eviction.
#
# Tamaños definidos por el enunciado: 50mb, 200mb, 500mb.
# Con CONF_HIGH (25 claves posibles) y el reporte real de evictions
# via redis INFO stats, se puede observar si alguno de estos tamaños
# genera presion real sobre el cache con este workload.
# Si no hay evictions en ninguno, es un resultado valido: significa
# que el working set del sistema cabe holgadamente en 50mb, lo que
# tiene implicaciones de diseño para produccion.
#
# Experimentos: 6  (3 tamanos x 2 distribuciones)
#   G3_zipf_lru_50mb
#   G3_uniform_lru_50mb
#   G3_zipf_lru_200mb
#   G3_uniform_lru_200mb
#   G3_zipf_lru_500mb
#   G3_uniform_lru_500mb

source "$(dirname "$0")/common.sh"

# ── Parametros del grupo ───────────────────────────────────────────────────────
EVICTION="allkeys-lru"
ALPHA="1.2"
CONF="$CONF_HIGH"   # 5 valores de conf → mas claves → maxima presion posible

echo ""
echo "========================================================"
echo "  GRUPO 3 — Tamano de cache (50mb / 200mb / 500mb)"
echo "  eviction=$EVICTION  conf=$CONF"
echo "  Tamanos segun enunciado de la tarea"
echo "========================================================"

for MAXMEM in 50mb 200mb 500mb; do
  restart_stack "$EVICTION" "$MAXMEM"
  set_ttls $TTL_DEFAULT_Q1 $TTL_DEFAULT_Q3 $TTL_DEFAULT_Q4 $TTL_DEFAULT_Q5

  for DIST in zipf uniform; do
    run_experiment "G3_${DIST}_lru_${MAXMEM}" "$DIST" "$ALPHA" "$CONF"
  done
done

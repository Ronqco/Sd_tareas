#!/usr/bin/env bash
# scripts/run_g2.sh
# GRUPO 2 — Impacto de la politica de eviccion
#
# Variable que cambia : EVICTION_POLICY (lru | lfu | fifo)
# Fijo                : 10mb, TTL default, CONF_HIGH, ambas distribuciones
#
# Objetivo: comparar LRU vs LFU vs FIFO bajo presion real de memoria.
# Con 10mb y CONF_HIGH (25 claves posibles) Redis se llena y aplica
# la politica de eviccion — permitiendo ver diferencias reales.
#
# Nota sobre memoria: 1mb causa OutOfMemoryError porque Redis necesita
# ~1mb solo para su overhead interno, sin dejar espacio para claves.
# 10mb es el minimo practico que permite evictions reales con este workload.
#
# Nota sobre FIFO: Redis no implementa FIFO nativo. Se aproxima con
# allkeys-random + TTL uniforme, que produce un orden de eviccion
# sin sesgo de frecuencia ni recencia — equivalente funcional a FIFO.
#
# Experimentos: 6  (3 politicas x 2 distribuciones)
#   G2_zipf_lru_10mb
#   G2_uniform_lru_10mb
#   G2_zipf_lfu_10mb
#   G2_uniform_lfu_10mb
#   G2_zipf_fifo_10mb
#   G2_uniform_fifo_10mb

source "$(dirname "$0")/common.sh"

# ── Parametros del grupo ───────────────────────────────────────────────────────
MAXMEM="10mb"
ALPHA="1.2"
CONF="$CONF_HIGH"   # 5 valores de conf → mas claves → mas presion

echo ""
echo "========================================================"
echo "  GRUPO 2 — Politica de eviccion (LRU vs LFU vs FIFO)"
echo "  mem=$MAXMEM  conf=$CONF"
echo "  FIFO aproximado con allkeys-random + TTL uniforme"
echo "========================================================"

for EVICTION in allkeys-lru allkeys-lfu allkeys-random; do
  case "$EVICTION" in
    allkeys-lru)    POL="lru"  ;;
    allkeys-lfu)    POL="lfu"  ;;
    allkeys-random) POL="fifo" ;;
  esac

  restart_stack "$EVICTION" "$MAXMEM"
  set_ttls $TTL_DEFAULT_Q1 $TTL_DEFAULT_Q3 $TTL_DEFAULT_Q4 $TTL_DEFAULT_Q5

  for DIST in zipf uniform; do
    run_experiment "G2_${DIST}_${POL}_10mb" "$DIST" "$ALPHA" "$CONF"
  done
done

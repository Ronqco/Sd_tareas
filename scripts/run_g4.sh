#!/usr/bin/env bash
# scripts/run_g4.sh
# GRUPO 4 — Impacto del TTL por tipo de consulta
#
# El enunciado pide: "analizar como afecta el TTL al sistema cache
# para cada una de las consultas". Esto implica dos niveles de analisis:
#
# PARTE A — Efecto global del TTL (baseline)
#   Cambia todos los TTLs a la vez en 3 niveles (bajo/medio/alto).
#   Permite ver el efecto agregado del TTL sobre el sistema completo.
#   Variable: nivel de TTL global
#   Fijo: LRU, 256mb, CONF_LOW, ambas distribuciones
#   Experimentos: 6  (3 niveles x 2 distribuciones)
#
# PARTE B — Efecto del TTL aislado por query (Q1 a Q5)
#   Para cada query type, varia su TTL entre bajo/medio/alto
#   mientras el resto permanece en valores default.
#   Permite responder: "que pasa con el hit rate de Q1 cuando su
#   TTL es 10s vs 120s vs 600s, manteniendo Q2-Q5 constantes?"
#   Variable: TTL de una sola query
#   Fijo: LRU, 256mb, CONF_LOW, ambas distribuciones
#   Experimentos: 30  (5 queries x 3 niveles x 2 distribuciones)
#
# Total G4: 36 experimentos

source "$(dirname "$0")/common.sh"

# ── Parametros fijos del grupo ─────────────────────────────────────────────────
EVICTION="allkeys-lru"
MAXMEM="256mb"
ALPHA="1.2"
CONF="$CONF_LOW"

# ── Niveles de TTL (segundos) ──────────────────────────────────────────────────
# Bajo  : entradas expiran rapido → misses frecuentes
# Medio : valores default del sistema
# Alto  : entradas viven mucho → maximo hit rate posible
TTL_BAJO=10
TTL_MEDIO_Q1=120; TTL_MEDIO_Q3=90; TTL_MEDIO_Q4=60; TTL_MEDIO_Q5=30
TTL_ALTO=600

echo ""
echo "========================================================"
echo "  GRUPO 4 — Efecto del TTL por tipo de consulta"
echo "  eviction=$EVICTION  mem=$MAXMEM  conf=$CONF"
echo "========================================================"

restart_stack "$EVICTION" "$MAXMEM"

# ══════════════════════════════════════════════════════════════════════════════
# PARTE A — Efecto global del TTL (todos los TTLs cambian a la vez)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "  -- PARTE A: TTL global bajo/medio/alto --"

# TTL bajo global
set_ttls $TTL_BAJO $TTL_BAJO $TTL_BAJO $TTL_BAJO
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_global_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done

# TTL medio global (default)
set_ttls $TTL_MEDIO_Q1 $TTL_MEDIO_Q3 $TTL_MEDIO_Q4 $TTL_MEDIO_Q5
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_global_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done

# TTL alto global
set_ttls $TTL_ALTO $TTL_ALTO $TTL_ALTO $TTL_ALTO
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_global_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

# ══════════════════════════════════════════════════════════════════════════════
# PARTE B — TTL aislado por query (una query cambia, resto en default)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "  -- PARTE B: TTL aislado por query --"

# ── Q1 ────────────────────────────────────────────────────────────────────────
echo "  -- Q1 --"
# Q1 bajo, resto default
set_ttls $TTL_BAJO       $TTL_MEDIO_Q3 $TTL_MEDIO_Q4 $TTL_MEDIO_Q5
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q1_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done
# Q1 medio (default — sirve como referencia)
set_ttls $TTL_MEDIO_Q1   $TTL_MEDIO_Q3 $TTL_MEDIO_Q4 $TTL_MEDIO_Q5
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q1_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done
# Q1 alto, resto default
set_ttls $TTL_ALTO        $TTL_MEDIO_Q3 $TTL_MEDIO_Q4 $TTL_MEDIO_Q5
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q1_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

# ── Q2 ────────────────────────────────────────────────────────────────────────
# Nota: set_ttls pasa Q1 y Q2 juntos (mismo argumento q1).
# Para aislar Q2 usamos curl directo para setear Q2 independientemente.
echo "  -- Q2 --"
# Q2 bajo, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_BAJO,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q2_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done
# Q2 medio (default)
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q2_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done
# Q2 alto, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_ALTO,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q2_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

# ── Q3 ────────────────────────────────────────────────────────────────────────
echo "  -- Q3 --"
# Q3 bajo, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_BAJO,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q3_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done
# Q3 medio (default)
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q3_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done
# Q3 alto, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_ALTO,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q3_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

# ── Q4 ────────────────────────────────────────────────────────────────────────
echo "  -- Q4 --"
# Q4 bajo, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_BAJO,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q4_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done
# Q4 medio (default)
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q4_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done
# Q4 alto, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_ALTO,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q4_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

# ── Q5 ────────────────────────────────────────────────────────────────────────
echo "  -- Q5 --"
# Q5 bajo, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_BAJO}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q5_ttl_bajo" "$DIST" "$ALPHA" "$CONF"
done
# Q5 medio (default)
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_MEDIO_Q5}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q5_ttl_medio" "$DIST" "$ALPHA" "$CONF"
done
# Q5 alto, resto default
curl -sf -X POST "$CACHE_URL/config" -H "Content-Type: application/json" \
  -d "{\"ttl_by_query\":{\"Q1\":$TTL_MEDIO_Q1,\"Q2\":$TTL_MEDIO_Q1,\"Q3\":$TTL_MEDIO_Q3,\"Q4\":$TTL_MEDIO_Q4,\"Q5\":$TTL_ALTO}}" > /dev/null
for DIST in zipf uniform; do
  run_experiment "G4_${DIST}_Q5_ttl_alto" "$DIST" "$ALPHA" "$CONF"
done

#!/usr/bin/env bash
# show_kafka_comparison.sh — tabla comparativa final de todos los escenarios.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

curl -s "$METRICS_URL/results" | python3 -c "
import json, sys

d    = json.load(sys.stdin)
rows = {r['label']: r for r in d.get('results', [])}

# Orden canónico de escenarios con etiqueta, distribución y grupo
SCENARIOS = [
    # label                   display                          dist
    ('e1_sync_zipf',          'E1 Sync base',                  'Zipf'),
    ('e1_sync_uniform',       'E1 Sync base',                  'Unif'),
    ('e2_kafka_zipf',         'E2 Kafka 1c',                   'Zipf'),
    ('e2_kafka_uniform',      'E2 Kafka 1c',                   'Unif'),
    ('e3_kafka_1c',           'E3 Kafka 1c (80rps)',           'Zipf'),
    ('e3_kafka_2c',           'E3 Kafka 2c (80rps)',           'Zipf'),
    ('e3_kafka_4c',           'E3 Kafka 4c (80rps)',           'Zipf'),
    ('e4_falla_zipf',         'E4 Falla temporal',             'Zipf'),
    ('e4_falla_uniform',      'E4 Falla temporal',             'Unif'),
    ('e5_reintentos_zipf',    'E5 Reintentos',                 'Zipf'),
    ('e5_reintentos_uniform', 'E5 Reintentos',                 'Unif'),
    ('e6_spike_zipf',         'E6 Spike',                      'Zipf'),
    ('e6_spike_uniform',      'E6 Spike',                      'Unif'),
    ('e7_sync_zipf',          'E7 Sync+falla',                 'Zipf'),
    ('e7_async_zipf',         'E7 Async+falla',                'Zipf'),
    ('e7_sync_uniform',       'E7 Sync+falla',                 'Unif'),
    ('e7_async_uniform',      'E7 Async+falla',                'Unif'),
]

def fmt(v, f='{:.1f}'):
    if v is None: return '—'
    try: return f.format(float(v))
    except: return str(v)

def pct(v):
    if v is None: return '—'
    try: return '{:.1%}'.format(float(v))
    except: return str(v)

W = 26
print()
print('=' * 115)
print('  COMPARACIÓN EXPERIMENTAL — TAREA 2 — Sistemas Distribuidos 2026-1')
print('=' * 115)

# ── TABLA 1: Throughput, Latencia, Hit rate ──────────────────────────────────
print()
print('  [1] THROUGHPUT Y LATENCIA')
print('  {:<{w}} {:>5} {:>8} {:>8} {:>8} {:>8} {:>8}'.format(
    'Escenario', 'Dist', 'RPS', 'p50 ms', 'p95 ms', 'Hit%', 'Reqs', w=W))
print('  ' + '-'*72)
for key, label, dist in SCENARIOS:
    r = rows.get(key)
    if not r: continue
    print('  {:<{w}} {:>5} {:>8} {:>8} {:>8} {:>8} {:>8}'.format(
        label, dist,
        fmt(r.get('throughput_rps'),   '{:.1f}'),
        fmt(r.get('latency_p50_ms'),   '{:.2f}'),
        fmt(r.get('latency_p95_ms'),   '{:.1f}'),
        pct(r.get('hit_rate')),
        fmt(r.get('total_requests'),   '{:.0f}'),
        w=W))

# ── TABLA 2: Métricas Kafka ──────────────────────────────────────────────────
print()
print('  [2] MÉTRICAS KAFKA (retry / recovery / DLQ / backlog / recovery_time)')
print('  {:<{w}} {:>5} {:>8} {:>8} {:>6} {:>8} {:>8} {:>13}'.format(
    'Escenario', 'Dist', 'Retries', 'Recov.', 'DLQ',
    'Retry%', 'Recov%', 'PeakBacklog', w=W))
print('  ' + '-'*84)
for key, label, dist in SCENARIOS:
    r = rows.get(key)
    if not r: continue
    # Solo filas con datos Kafka relevantes
    has_kafka = any(r.get(k) for k in ['total_retries','total_dlq','total_recoveries'])
    is_e3     = 'e3' in key
    if not has_kafka and not is_e3: continue
    print('  {:<{w}} {:>5} {:>8} {:>8} {:>6} {:>8} {:>8} {:>13}'.format(
        label, dist,
        fmt(r.get('total_retries'),    '{:.0f}'),
        fmt(r.get('total_recoveries'), '{:.0f}'),
        fmt(r.get('total_dlq'),        '{:.0f}'),
        pct(r.get('retry_rate')),
        pct(r.get('recovery_rate')),
        fmt(r.get('peak_backlog_size'),'{:.0f}'),
        w=W))

# ── TABLA 3: Recovery time ───────────────────────────────────────────────────
print()
print('  [3] RECOVERY TIME (tiempo en vaciar el backlog tras falla)')
print('  {:<{w}} {:>5} {:>15}'.format('Escenario','Dist','Recovery time (s)', w=W))
print('  ' + '-'*50)
for key, label, dist in SCENARIOS:
    r = rows.get(key)
    if not r: continue
    rt = r.get('recovery_time_s')
    if rt is None: continue
    print('  {:<{w}} {:>5} {:>15}'.format(
        label, dist, fmt(rt, '{:.1f}'), w=W))

# ── TABLA 4: E3 Escalamiento horizontal ─────────────────────────────────────
e3_keys = [k for k,_,_ in SCENARIOS if 'e3' in k]
e3_rows = [(k, rows[k]) for k in e3_keys if k in rows]
if e3_rows:
    print()
    print('  [4] ESCALAMIENTO HORIZONTAL (E3) — arrival_rate=80 req/s')
    print('  {:<20} {:>8} {:>8} {:>8} {:>13}'.format(
        'Consumers','RPS','p50 ms','p95 ms','PeakBacklog'))
    print('  ' + '-'*56)
    for k, r in e3_rows:
        nc = k.replace('e3_kafka_','').replace('c','')
        print('  {:<20} {:>8} {:>8} {:>8} {:>13}'.format(
            nc + ' consumer(s)',
            fmt(r.get('throughput_rps'),   '{:.1f}'),
            fmt(r.get('latency_p50_ms'),   '{:.2f}'),
            fmt(r.get('latency_p95_ms'),   '{:.1f}'),
            fmt(r.get('peak_backlog_size'),'{:.0f}')))

# ── TABLA 5: Comparación sync vs async (E7) ──────────────────────────────────
for suffix, label_s in [('zipf','Zipf'), ('uniform','Uniforme')]:
    sync  = rows.get(f'e7_sync_{suffix}',  {})
    async_ = rows.get(f'e7_async_{suffix}', {})
    if not sync or not async_: continue
    print()
    print(f'  [5] SYNC vs ASYNC ANTE FALLA — distribución {label_s}')
    print('  {:<30} {:>12} {:>14}'.format('Métrica','SYNC','ASYNC (Kafka)'))
    print('  ' + '-'*58)
    for k, lbl, f in [
        ('total_requests',   'Requests procesadas', '{:.0f}'),
        ('throughput_rps',   'Throughput (req/s)',  '{:.1f}'),
        ('latency_p50_ms',   'Latencia p50 (ms)',   '{:.2f}'),
        ('latency_p95_ms',   'Latencia p95 (ms)',   '{:.1f}'),
        ('hit_rate',         'Hit rate',             '{:.1%}'),
        ('total_retries',    'Reintentos',           '{:.0f}'),
        ('total_recoveries', 'Recuperadas',          '{:.0f}'),
        ('total_dlq',        'DLQ',                  '{:.0f}'),
        ('peak_backlog_size','Peak backlog',          '{:.0f}'),
        ('recovery_time_s',  'Recovery time (s)',     '{:.1f}'),
    ]:
        sv = sync.get(k)
        av = async_.get(k)
        def fmt2(v, ff=f):
            if v is None: return '—'
            try: return ff.format(float(v))
            except: return str(v)
        print('  {:<30} {:>12} {:>14}'.format(lbl, fmt2(sv), fmt2(av)))

print()
print('=' * 115)
print()
"

"""
metrics_store/main.py
Tarea 1: hit/miss, throughput, latencia p50/p95, evictions, cache_efficiency.

"""

import json
import os
import statistics
import time
from pathlib import Path

from fastapi import FastAPI

app = FastAPI(title="Metrics Store")

RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "/app/results"))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

events:          list[dict]  = []
eviction_events: list[float] = []
kafka_events:    list[dict]  = []
backlog_snapshots: list[dict] = []   # {timestamp, size}
_start_time: float = time.time()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s   = sorted(data)
    idx = min(int(len(s) * p / 100), len(s) - 1)
    return s[idx]


def _compute_recovery_time() -> float | None:
    """
    Tiempo desde el primer reintento hasta que el backlog vuelve a 0.
    Si hay snapshots de backlog registramos el tiempo hasta que lag=0 por
    primera vez después del pico. Si no hay snapshots, usamos el tiempo entre
    primer retry y último recovery exitoso.
    """
    retries    = [e for e in kafka_events if e["type"] == "retry"]
    recoveries = [e for e in kafka_events if e["type"] == "recovery"]

    # Preferir backlog snapshots si están disponibles
    if backlog_snapshots and retries:
        t_first_retry = min(e["timestamp"] for e in retries)
        # Encontrar primer snapshot con backlog=0 después del primer retry
        zero_snaps = [
            s for s in backlog_snapshots
            if s["timestamp"] >= t_first_retry and s["size"] == 0
        ]
        if zero_snaps:
            return round(min(s["timestamp"] for s in zero_snaps) - t_first_retry, 2)

    # Fallback: tiempo entre primer retry y último recovery
    if retries and recoveries:
        return round(
            max(e["timestamp"] for e in recoveries) -
            min(e["timestamp"] for e in retries),
            2
        )
    return None


def _peak_backlog() -> int:
    if not backlog_snapshots:
        return 0
    return max(s["size"] for s in backlog_snapshots)


def _final_request_events() -> list[dict]:
    """
    Devuelve un evento por query_id. En escenarios con retries, el mismo request
    puede tener varios intentos; para metricas de requests usamos el ultimo.
    """
    by_id = {}
    anonymous = []
    for idx, event in enumerate(events):
        query_id = event.get("query_id")
        if not query_id:
            anonymous.append((idx, event))
            continue
        current = by_id.get(query_id)
        if current is None or event.get("timestamp", 0) >= current.get("timestamp", 0):
            by_id[query_id] = event

    return list(by_id.values()) + [event for _, event in anonymous]


def _compute_summary() -> dict:
    final_events = _final_request_events()
    hits   = [e for e in final_events if e["type"] == "hit"]
    misses = [e for e in final_events if e["type"] == "miss"]
    total  = len(hits) + len(misses)

    hit_lats  = [e["latency_ms"] for e in hits   if "latency_ms" in e]
    miss_lats = [e["latency_ms"] for e in misses if "latency_ms" in e]
    all_lats  = hit_lats + miss_lats

    elapsed    = max(time.time() - _start_time, 0.001)
    throughput = total / elapsed

    now                   = time.time()
    recent_ev             = [t for t in eviction_events if now - t <= 60]
    eviction_rate_per_min = len(recent_ev)

    avg_t_cache      = statistics.mean(hit_lats)  if hit_lats  else 0.0
    avg_t_db         = statistics.mean(miss_lats) if miss_lats else 0.0
    cache_efficiency = 0.0
    if total > 0:
        cache_efficiency = (len(hits) * avg_t_cache - len(misses) * avg_t_db) / total

    per_query = {}
    for qt in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
        qe    = [e for e in final_events if e.get("query_type") == qt]
        qhits = sum(1 for e in qe if e["type"] == "hit")
        qmiss = sum(1 for e in qe if e["type"] == "miss")
        qtot  = qhits + qmiss
        qlats = [e["latency_ms"] for e in qe if "latency_ms" in e]
        per_query[qt] = {
            "total":    qtot,
            "hits":     qhits,
            "misses":   qmiss,
            "hit_rate": round(qhits / qtot, 4) if qtot > 0 else 0,
            "p50_ms":   round(_percentile(qlats, 50), 3),
            "p95_ms":   round(_percentile(qlats, 95), 3),
        }

    # Métricas Kafka
    retries    = [e for e in kafka_events if e["type"] == "retry"]
    recoveries = [e for e in kafka_events if e["type"] == "recovery"]
    dlqs       = [e for e in kafka_events if e["type"] == "dlq"]

    recovery_time = _compute_recovery_time()
    peak_backlog  = _peak_backlog()

    return {
        "total_requests":        total,
        "total_attempts":         len(events),
        "hits":                  len(hits),
        "misses":                len(misses),
        "hit_rate":              round(len(hits) / total, 4) if total > 0 else 0,
        "miss_rate":             round(len(misses) / total, 4) if total > 0 else 0,
        "throughput_rps":        round(throughput, 3),
        "elapsed_seconds":       round(elapsed, 2),
        "latency_p50_ms":        round(_percentile(all_lats, 50), 3),
        "latency_p95_ms":        round(_percentile(all_lats, 95), 3),
        "avg_latency_hit_ms":    round(avg_t_cache, 3),
        "avg_latency_miss_ms":   round(avg_t_db, 3),
        "eviction_rate_per_min": eviction_rate_per_min,
        "total_evictions":       len(eviction_events),
        "cache_efficiency":      round(cache_efficiency, 4),
        "per_query":             per_query,
        # Kafka
        "total_retries":         len(retries),
        "total_recoveries":      len(recoveries),
        "total_dlq":             len(dlqs),
        "retry_rate":            round(len(retries)    / total, 4) if total > 0 else 0,
        "recovery_rate":         round(len(recoveries) / max(len(retries), 1), 4),
        "dlq_rate":              round(len(dlqs)       / total, 4) if total > 0 else 0,
        "peak_backlog_size":     peak_backlog,
        "recovery_time_s":       recovery_time,
        "backlog_snapshots":     backlog_snapshots[-60:],  # ultimos 60 puntos para graficos
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "events_stored": len(events)}


@app.post("/event")
def add_event(event: dict):
    event["timestamp"] = time.time()
    events.append(event)
    return {"status": "ok"}


@app.post("/eviction")
def add_eviction():
    eviction_events.append(time.time())
    return {"status": "ok"}


@app.post("/kafka_event")
def add_kafka_event(event: dict):
    """Registra retry, dlq o recovery desde el kafka_consumer."""
    event["timestamp"] = time.time()
    kafka_events.append(event)
    return {"status": "ok"}


@app.post("/backlog")
def add_backlog_snapshot(payload: dict):
    """
    Registra una medición puntual del backlog de Kafka.
    Payload: {"size": <int>, "label": "<str opcional>"}
    Los scripts llaman esto periódicamente con get_backlog() para trazar
    la curva de crecimiento/vaciado del backlog en el tiempo.
    """
    backlog_snapshots.append({
        "timestamp": time.time(),
        "t_rel":     round(time.time() - _start_time, 1),
        "size":      int(payload.get("size", 0)),
        "label":     payload.get("label", ""),
    })
    return {"status": "ok"}


@app.get("/kafka_summary")
def kafka_summary():
    retries    = [e for e in kafka_events if e["type"] == "retry"]
    recoveries = [e for e in kafka_events if e["type"] == "recovery"]
    dlqs       = [e for e in kafka_events if e["type"] == "dlq"]
    total      = len(_final_request_events())

    recovery_time = _compute_recovery_time()

    return {
        "total_retries":     len(retries),
        "total_recoveries":  len(recoveries),
        "total_dlq":         len(dlqs),
        "retry_rate":        round(len(retries)    / total, 4) if total > 0 else 0,
        "recovery_rate":     round(len(recoveries) / max(len(retries), 1), 4),
        "dlq_rate":          round(len(dlqs)       / total, 4) if total > 0 else 0,
        "recovery_time_s":   recovery_time,
        "peak_backlog_size": _peak_backlog(),
        "backlog_snapshots": backlog_snapshots[-60:],
        "recent_events":     kafka_events[-50:],
    }


@app.get("/summary")
def summary():
    return _compute_summary()


@app.get("/timeseries")
def timeseries(bucket_seconds: int = 5):
    if not events:
        return {"buckets": []}

    t_min = min(e["timestamp"] for e in events)
    t_max = max(e["timestamp"] for e in events)
    buckets = []
    t = t_min
    while t <= t_max:
        be     = [e for e in events       if t <= e["timestamp"]         < t + bucket_seconds]
        bk     = [e for e in kafka_events if t <= e.get("timestamp", 0) < t + bucket_seconds]
        hits   = sum(1 for e in be if e["type"] == "hit")
        misses = sum(1 for e in be if e["type"] == "miss")
        total  = hits + misses
        lats   = [e["latency_ms"] for e in be if "latency_ms" in e]
        buckets.append({
            "t":          round(t - t_min, 1),
            "hits":       hits,
            "misses":     misses,
            "total":      total,
            "hit_rate":   round(hits / total, 4) if total > 0 else 0,
            "throughput": round(total / bucket_seconds, 2),
            "p50_ms":     round(_percentile(lats, 50), 3),
            "p95_ms":     round(_percentile(lats, 95), 3),
            "retries":    sum(1 for e in bk if e["type"] == "retry"),
            "recoveries": sum(1 for e in bk if e["type"] == "recovery"),
            "dlqs":       sum(1 for e in bk if e["type"] == "dlq"),
        })
        t += bucket_seconds

    return {"bucket_seconds": bucket_seconds, "buckets": buckets}


@app.post("/save")
def save_results(label: str = "experiment"):
    data             = _compute_summary()
    data["label"]    = label
    data["saved_at"] = time.time()
    filepath = RESULTS_DIR / f"{label}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    return {"status": "saved", "path": str(filepath)}


@app.get("/results")
def list_results():
    files   = sorted(RESULTS_DIR.glob("*.json"))
    results = []
    for f in files:
        try:
            with open(f) as fp:
                data = json.load(fp)
            results.append({
                "label":                 data.get("label", f.stem),
                "hit_rate":              data.get("hit_rate"),
                "throughput_rps":        data.get("throughput_rps"),
                "total_requests":        data.get("total_requests"),
                "total_attempts":         data.get("total_attempts"),
                "latency_p50_ms":        data.get("latency_p50_ms"),
                "latency_p95_ms":        data.get("latency_p95_ms"),
                "eviction_rate_per_min": data.get("eviction_rate_per_min"),
                "total_evictions":       data.get("total_evictions"),
                "cache_efficiency":      data.get("cache_efficiency"),
                "total_retries":         data.get("total_retries"),
                "total_recoveries":      data.get("total_recoveries"),
                "total_dlq":             data.get("total_dlq"),
                "retry_rate":            data.get("retry_rate"),
                "recovery_rate":         data.get("recovery_rate"),
                "dlq_rate":              data.get("dlq_rate"),
                "peak_backlog_size":     data.get("peak_backlog_size"),
                "recovery_time_s":       data.get("recovery_time_s"),
                "file":                  f.name,
            })
        except Exception:
            pass
    return {"results": results}


@app.delete("/reset")
def reset():
    global _start_time
    events.clear()
    eviction_events.clear()
    kafka_events.clear()
    backlog_snapshots.clear()
    _start_time = time.time()
    return {"status": "reset"}

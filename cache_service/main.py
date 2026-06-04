"""
cache_service/main.py
Intercepta todas las queries, gestiona Redis y registra métricas.
  - Cache keys siguen el formato exacto del enunciado
  - TTL estático configurable por tipo de query (Q1-Q5)
  - Retry rápido hacia response_generator ante fallos transitorios
  - Endpoint /config para cambiar TTL en caliente
  - Reporte periodico de evictions a metrics_store via redis INFO

NOTA sobre reintentos internos:
  El cache_service hace 1 reintento rápido (0.1s) antes de lanzar 503.
  Esto permite distinguir errores transitorios de red de fallas reales del
  response_generator. Con MAX_RETRIES=1 y delay=0.1s el error llega al
  kafka_consumer en <200ms desde que el backend cae, dando tiempo suficiente
  para que el consumer aplique su propio backoff exponencial configurable.
"""

import asyncio
import json
import os
import time

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI

app = FastAPI(title="Cache Service")

REDIS_HOST  = os.getenv("REDIS_HOST",             "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT",         6379))
RG_URL      = os.getenv("RESPONSE_GENERATOR_URL", "http://response_generator:8002")
METRICS_URL = os.getenv("METRICS_URL",            "http://metrics_store:8003")

# Intervalo en segundos para consultar evictions a Redis y reportarlas
EVICTION_POLL_INTERVAL = int(os.getenv("EVICTION_POLL_INTERVAL", 5))

# TTL estatico por tipo de query (segundos) — configurable via POST /config
DEFAULT_TTL = {
    "Q1": int(os.getenv("TTL_Q1", 120)),
    "Q2": int(os.getenv("TTL_Q2", 120)),
    "Q3": int(os.getenv("TTL_Q3", 90)),
    "Q4": int(os.getenv("TTL_Q4", 60)),
    "Q5": int(os.getenv("TTL_Q5", 30)),
}

CONFIG = {"ttl_by_query": dict(DEFAULT_TTL)}

_redis: aioredis.Redis | None = None
_client: httpx.AsyncClient | None = None
_last_evicted_keys: int = 0   # para calcular delta de evictions

# 1 reintento rápido: distingue micro-cortes de red de fallas reales.
# Con solo 1 reintento + 0.1s de delay el error llega al kafka_consumer
# en <200ms, permitiendo que el consumer aplique su backoff exponencial.
MAX_RETRIES_INTERNAL = 1
RETRY_DELAY_INTERNAL = 0.1  # segundos


# ── Cache key — formato exacto del enunciado ──────────────────────────────────

def _make_key(payload: dict) -> str:
    """
    Q1 → count:{zona_id}:conf={confidence_min}
    Q2 → area:{zona_id}:conf={confidence_min}
    Q3 → density:{zona_id}:conf={confidence_min}
    Q4 → compare:density:{zona_a}:{zona_b}:conf={confidence_min}
    Q5 → confidence_dist:{zona_id}:bins={bins}
    """
    qt = payload["query_type"]
    p  = payload["params"]
    z  = p.get("zone_id", "")
    c  = round(float(p.get("confidence_min", 0.0)), 2)

    if qt == "Q1":
        return f"count:{z}:conf={c}"
    elif qt == "Q2":
        return f"area:{z}:conf={c}"
    elif qt == "Q3":
        return f"density:{z}:conf={c}"
    elif qt == "Q4":
        zb = p.get("zone_b", "")
        return f"compare:density:{z}:{zb}:conf={c}"
    elif qt == "Q5":
        bins = p.get("bins", 5)
        return f"confidence_dist:{z}:bins={bins}"
    else:
        return f"{qt}:{json.dumps(p, sort_keys=True)}"


# ── Tarea de fondo: reportar evictions periodicamente ────────────────────────

async def _eviction_reporter():
    """
    Consulta redis INFO stats cada EVICTION_POLL_INTERVAL segundos.
    Calcula el delta de evicted_keys desde la ultima consulta y envia
    un evento /eviction al metrics_store por cada eviction nueva.
    """
    global _last_evicted_keys
    await asyncio.sleep(5)  # esperar que Redis este listo
    while True:
        try:
            info = await _redis.info("stats")
            total_evicted = int(info.get("evicted_keys", 0))
            delta = total_evicted - _last_evicted_keys
            if delta > 0:
                _last_evicted_keys = total_evicted
                for _ in range(delta):
                    try:
                        await _client.post(
                            f"{METRICS_URL}/eviction",
                            timeout=1
                        )
                    except Exception:
                        pass
        except Exception:
            pass
        await asyncio.sleep(EVICTION_POLL_INTERVAL)


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global _redis, _client, _last_evicted_keys
    _redis  = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    _client = httpx.AsyncClient(timeout=30)

    # Leer baseline de evictions para no contar las de arranques anteriores
    try:
        info = await _redis.info("stats")
        _last_evicted_keys = int(info.get("evicted_keys", 0))
    except Exception:
        _last_evicted_keys = 0

    # Lanzar tarea de fondo para reportar evictions
    asyncio.create_task(_eviction_reporter())


@app.on_event("shutdown")
async def shutdown():
    await _redis.aclose()
    await _client.aclose()


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _notify(event_type: str, query_type: str, latency_ms: float, payload: dict):
    try:
        await _client.post(f"{METRICS_URL}/event", json={
            "type":       event_type,
            "query_type": query_type,
            "latency_ms": latency_ms,
            "query_id":   payload.get("query_id"),
            "retry_count": int(payload.get("retry_count", 0)),
        }, timeout=1)
    except Exception:
        pass


async def _fetch_from_rg(payload: dict) -> dict:
    """
    Llama al response_generator con 1 reintento rápido ante fallos transitorios.
    Con MAX_RETRIES_INTERNAL=1 y RETRY_DELAY_INTERNAL=0.1s el error llega al
    kafka_consumer en <200ms, dando tiempo para que el consumer aplique backoff.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES_INTERNAL + 2):  # +2 porque range excluye el último
        try:
            resp = await _client.post(f"{RG_URL}/query", json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exc = e
            if attempt <= MAX_RETRIES_INTERNAL:
                await asyncio.sleep(RETRY_DELAY_INTERNAL)
    raise last_exc


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    await _redis.ping()
    return {"status": "ok"}


@app.get("/config")
async def get_config():
    return CONFIG


@app.post("/config")
async def update_config(new_config: dict):
    """
    Actualiza TTLs en caliente sin reiniciar el servicio.
    Payload ejemplo: {"ttl_by_query": {"Q1": 180, "Q5": 10}}
    """
    if "ttl_by_query" in new_config:
        CONFIG["ttl_by_query"].update(new_config["ttl_by_query"])
    return {"status": "updated", "config": CONFIG}


@app.post("/query")
async def query(payload: dict):
    key        = _make_key(payload)
    query_type = payload.get("query_type", "")
    t0         = time.perf_counter()

    cached = await _redis.get(key)

    if cached:
        latency_ms = (time.perf_counter() - t0) * 1000
        await _notify("hit", query_type, latency_ms, payload)
        return {
            "source":     "cache",
            "key":        key,
            "latency_ms": round(latency_ms, 3),
            "result":     json.loads(cached),
        }

    # MISS — delegar al response_generator
    try:
        data = await _fetch_from_rg(payload)
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        await _notify("miss", query_type, latency_ms, payload)
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail=f"response_generator unavailable: {e}")

    ttl = CONFIG["ttl_by_query"].get(query_type, 60)
    await _redis.setex(key, ttl, json.dumps(data["result"]))

    latency_ms = (time.perf_counter() - t0) * 1000
    await _notify("miss", query_type, latency_ms, payload)

    return {
        "source":        "response_generator",
        "key":           key,
        "ttl_applied":   ttl,
        "latency_ms":    round(latency_ms, 3),
        "processing_ms": data.get("processing_ms"),
        "result":        data["result"],
    }

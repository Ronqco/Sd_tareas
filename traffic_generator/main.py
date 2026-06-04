"""
traffic_generator/main.py

MODE=sync  (Tarea 1, default): HTTP POST directo al cache_service — sin cambios.
MODE=async (Tarea 2):          publica mensajes en Kafka con query_id y retry_count.

Distribuciones: Zipf y Uniforme.
"""

import asyncio
import json
import os
import random
import time
import uuid

import httpx
import numpy as np

MODE         = os.getenv("MODE",              "sync")
CACHE_URL    = os.getenv("CACHE_URL",         "http://cache_service:8001")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC_MAIN   = os.getenv("TOPIC_MAIN",        "queries")
NUM_REQUESTS = int(os.getenv("NUM_REQUESTS",   1000))
ARRIVAL_RATE = float(os.getenv("ARRIVAL_RATE", 10))
DISTRIBUTION = os.getenv("DISTRIBUTION",       "zipf")
ZIPF_ALPHA   = float(os.getenv("ZIPF_ALPHA",   1.2))

_conf_str   = os.getenv("CONF_VALUES", "0.0,0.7")
CONF_VALUES = [round(float(x), 2) for x in _conf_str.split(",")]

ZONES       = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

_sent   = 0
_errors = 0


# ── Generación de consultas ───────────────────────────────────────────────────

def pick_zone(exclude=None):
    available = [z for z in ZONES if z != exclude] if exclude else ZONES
    if DISTRIBUTION == "zipf":
        for _ in range(20):
            val = int(np.random.zipf(ZIPF_ALPHA))
            idx = (val - 1) % len(available)
            return available[idx]
        return random.choice(available)
    return random.choice(available)


def build_payload():
    qt      = random.choice(QUERY_TYPES)
    zone_id = pick_zone()
    params  = {"zone_id": zone_id}

    if qt in ("Q1", "Q2", "Q3"):
        params["confidence_min"] = random.choice(CONF_VALUES)
    elif qt == "Q4":
        params["zone_b"]         = pick_zone(exclude=zone_id)
        params["confidence_min"] = random.choice(CONF_VALUES)
    elif qt == "Q5":
        params["bins"] = random.choice([5, 10, 20])

    return {
        "query_id":    str(uuid.uuid4()),
        "query_type":  qt,
        "params":      params,
        "retry_count": 0,
        "created_at":  time.time(),
    }


# ── Modo SYNC (Tarea 1, sin cambios) ─────────────────────────────────────────

async def wait_for_cache(client: httpx.AsyncClient, max_wait: int = 60):
    print(f"⏳ Esperando cache_service...")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = await client.get(f"{CACHE_URL}/health", timeout=3)
            if r.status_code == 200:
                print("✅ cache_service listo")
                return
        except Exception:
            pass
        await asyncio.sleep(2)
    raise RuntimeError("cache_service no disponible")


async def worker_sync(client: httpx.AsyncClient, payload: dict):
    global _sent, _errors
    try:
        await client.post(f"{CACHE_URL}/query", json=payload, timeout=15)
        _sent += 1
    except Exception:
        _errors += 1
    _log()


async def run_sync():
    async with httpx.AsyncClient() as client:
        await wait_for_cache(client)
        t0    = time.time()
        tasks = []
        for i in range(NUM_REQUESTS):
            tasks.append(asyncio.create_task(worker_sync(client, build_payload())))
            await asyncio.sleep(1.0 / ARRIVAL_RATE)
        await asyncio.gather(*tasks)
    return time.time() - t0


# ── Modo ASYNC (Tarea 2, publica en Kafka) ───────────────────────────────────

async def wait_for_kafka(max_wait: int = 120):
    from aiokafka import AIOKafkaProducer
    print(f"⏳ Esperando Kafka en {KAFKA_BOOTSTRAP}...")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            p = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            await p.start()
            await p.stop()
            print("✅ Kafka listo")
            return
        except Exception:
            await asyncio.sleep(3)
    raise RuntimeError("Kafka no disponible")


async def run_async():
    from aiokafka import AIOKafkaProducer
    global _sent, _errors

    await wait_for_kafka()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # linger_ms agrupa mensajes en batches cuando el rate es alto,
        # reduciendo round-trips al broker y permitiendo que el backlog
        # crezca realmente cuando el rate supera la capacidad del consumer.
        linger_ms=5,
        max_batch_size=65536,
    )
    await producer.start()
    t0 = time.time()
    try:
        for _ in range(NUM_REQUESTS):
            try:
                # fire-and-forget: no bloquea esperando ack del broker.
                # Permite publicar a la tasa real de ARRIVAL_RATE.
                await producer.send(TOPIC_MAIN, json.dumps(build_payload()).encode())
                _sent += 1
            except Exception:
                _errors += 1
            _log()
            await asyncio.sleep(1.0 / ARRIVAL_RATE)
        # flush al final para asegurar que todos los mensajes llegaron al broker
        await producer.flush()
    finally:
        await producer.stop()
    return time.time() - t0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log():
    done = _sent + _errors
    if done % 100 == 0:
        print(f"  → {done}/{NUM_REQUESTS} | mode={MODE} dist={DISTRIBUTION} "
              f"α={ZIPF_ALPHA} errores={_errors}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print(f"🚀 Generador de tráfico | mode={MODE}")
    print(f"   distribución : {DISTRIBUTION} (α={ZIPF_ALPHA})")
    print(f"   requests     : {NUM_REQUESTS}")
    print(f"   arrival rate : {ARRIVAL_RATE} req/s")
    print(f"   conf_values  : {CONF_VALUES}")

    elapsed = await run_async() if MODE == "async" else await run_sync()

    print(f"\n✅ Completado: {_sent} enviados, {_errors} errores — {elapsed:.1f}s")
    if elapsed > 0:
        print(f"   throughput publicado: {_sent/elapsed:.1f} req/s")


if __name__ == "__main__":
    asyncio.run(main())

"""
kafka_consumer/main.py

Consumidor Kafka para el modo asincrono.

Flujo real:
  1. Lee mensajes desde TOPIC_MAIN y TOPIC_RETRY.
  2. Llama a cache_service por HTTP.
  3. cache_service registra hit/miss; este consumer no duplica esas metricas.
  4. Si un mensaje reintentado tiene exito, publica evidencia en TOPIC_RECOVERY.
  5. Si falla y quedan intentos, publica en TOPIC_RETRY.
  6. Si agota intentos, publica en TOPIC_DLQ.
  7. Hace commit manual solo despues de publicar el resultado correspondiente.
"""

import asyncio
import json
import os
import time
import uuid

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "queries")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "queries.retry")
TOPIC_RECOVERY = os.getenv("TOPIC_RECOVERY", "queries.recovery")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "queries.dlq")
GROUP_ID = os.getenv("GROUP_ID", "sd-consumers")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
CACHE_URL = os.getenv("CACHE_URL", "http://cache_service:8001")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics_store:8003")
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", 500))


async def _notify_kafka(client: httpx.AsyncClient, event_type: str, extra: dict | None = None):
    try:
        await client.post(
            f"{METRICS_URL}/kafka_event",
            json={"type": event_type, **(extra or {})},
            timeout=1,
        )
    except Exception:
        pass


async def _publish(producer: AIOKafkaProducer, topic: str, msg: dict):
    await producer.send_and_wait(topic, json.dumps(msg).encode())


async def process_message(msg: dict, producer: AIOKafkaProducer, client: httpx.AsyncClient):
    query_type = msg.get("query_type", "")
    retry_count = int(msg.get("retry_count", 0))
    retry_available_at = float(msg.get("retry_available_at", 0))
    if retry_available_at > time.time():
        await asyncio.sleep(retry_available_at - time.time())

    t0 = time.perf_counter()

    try:
        resp = await client.post(f"{CACHE_URL}/query", json=msg, timeout=15)
        resp.raise_for_status()
        latency_ms = (time.perf_counter() - t0) * 1000

        if retry_count > 0:
            recovery_msg = {
                **msg,
                "recovered_at": time.time(),
                "final_retry": retry_count,
            }
            await _publish(producer, TOPIC_RECOVERY, recovery_msg)
            await _notify_kafka(client, "recovery", {
                "query_type": query_type,
                "retry_count": retry_count,
                "latency_ms": latency_ms,
            })
            print(f"[RECOVERY] {query_type} id={msg.get('query_id')} retry={retry_count}")

    except Exception as e:
        if retry_count >= MAX_RETRIES:
            msg["dlq_reason"] = str(e)
            msg["final_retry"] = retry_count
            await _publish(producer, TOPIC_DLQ, msg)
            await _notify_kafka(client, "dlq", {
                "query_type": query_type,
                "retry_count": retry_count,
            })
            print(f"[DLQ] {query_type} id={msg.get('query_id')} tras {retry_count} reintentos: {e}")
        else:
            msg["retry_count"] = retry_count + 1
            msg["retry_available_at"] = time.time() + ((RETRY_DELAY_MS / 1000) * (2 ** retry_count))
            await _publish(producer, TOPIC_RETRY, msg)
            await _notify_kafka(client, "retry", {
                "query_type": query_type,
                "retry_count": retry_count + 1,
            })
            print(f"[RETRY] {query_type} id={msg.get('query_id')} intento {retry_count + 1}/{MAX_RETRIES}: {e}")


async def wait_for_kafka(max_wait: int = 120):
    print(f"Esperando Kafka en {KAFKA_BOOTSTRAP}...")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            probe = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                request_timeout_ms=3000,
            )
            await probe.start()
            await probe.stop()
            print("Kafka listo")
            return
        except Exception:
            await asyncio.sleep(3)
    raise RuntimeError(f"Kafka no disponible tras {max_wait}s")


async def main():
    await wait_for_kafka()

    topics = [TOPIC_MAIN, TOPIC_RETRY]
    print(
        "Consumer iniciado | "
        f"group={GROUP_ID} | topics={topics} | recovery_topic={TOPIC_RECOVERY}"
    )

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    async with httpx.AsyncClient() as client:
        await consumer.start()
        await producer.start()
        try:
            async for record in consumer:
                msg = record.value
                if "query_id" not in msg:
                    msg["query_id"] = str(uuid.uuid4())
                await process_message(msg, producer, client)
                await consumer.commit()
        finally:
            await consumer.stop()
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())

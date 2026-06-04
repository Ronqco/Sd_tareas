# Sistemas Distribuidos — Tarea 2
**Procesamiento asíncrono y tolerancia a fallos con Apache Kafka**
Dataset: Google Open Buildings — Región Metropolitana de Santiago

> **Tarea 1** implementó la plataforma base con caché Redis y comunicación síncrona.
> **Tarea 2** incorpora Apache Kafka para desacoplar servicios, implementar reintentos automáticos, Dead Letter Queue y escalamiento horizontal.

---

## Requisitos previos

- Docker y docker-compose
- bash + curl (Linux/macOS o WSL en Windows)
- ~500 MB de espacio para el dataset

---

## Instalación y primer arranque

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd proyecto-sd
```

### 2. Descargar el dataset

El CSV de Google Open Buildings no está en el repositorio por su tamaño.

```bash
bash scripts/download_dataset.sh
```

Esto genera `data/buildings.csv` con las columnas `latitude, longitude, area_in_meters, confidence`.
Si el script falla, descarga manualmente desde https://sites.research.google/gr/open-buildings/ y coloca el archivo en `data/buildings.csv`.

### 3. Configurar variables de entorno

```bash
cp .env.example .env
```

Los valores por defecto son funcionales. Edita `.env` solo si quieres ajustar parámetros de experimentos.

---

## Arrancar el sistema

### Solo stack base (Tarea 1 — modo síncrono)

```bash
docker-compose up --build -d redis metrics_store response_generator cache_service
```

### Stack completo con Kafka (Tarea 2)

```bash
docker-compose up --build -d \
  redis metrics_store response_generator cache_service \
  zookeeper kafka

# Levantar 1 consumer (por defecto)
docker-compose up -d kafka_consumer

# Levantar N consumers en paralelo (escalamiento horizontal)
docker-compose up -d --scale kafka_consumer=4 kafka_consumer
```

Verificar que todos los servicios estén healthy:

```bash
docker-compose ps
```

### Health checks rápidos

```bash
curl http://localhost:8001/health   # cache_service
curl http://localhost:8002/health   # response_generator
curl http://localhost:8003/health   # metrics_store
```

---

## Arquitectura

### Tarea 1 — Flujo síncrono

```
traffic_generator (MODE=sync)
        │  POST /query
        ▼
  cache_service ──(hit)──► respuesta inmediata
        │ (miss)
        ▼
  response_generator ──► cómputo en memoria (Q1–Q5)
        │
        ▼
  metrics_store ◄── reportes de hit/miss/latencia
```

### Tarea 2 — Flujo asíncrono con Kafka

```
traffic_generator (MODE=async)
        │  produce → topic: queries
        ▼
     Kafka Broker (4 particiones)
        │  consume (grupo: sd-consumers)
        ▼
  kafka_consumer ──(hit)──► cache_service ──► respuesta
        │ (miss o falla)
        ▼
  response_generator

  Si falla:
    retry_count < MAX_RETRIES  →  topic: queries.retry  (backoff exponencial)
    retry_count >= MAX_RETRIES →  topic: queries.dlq
    reintento exitoso          →  topic: queries.recovery

  metrics_store ◄── /kafka_event (retry | dlq | recovery)
                ◄── /backlog     (snapshot del lag cada 2s)
```

---

## Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| `redis` | 6379 | Caché con TTL y política de evicción configurable |
| `cache_service` | 8001 | Intercepta queries, resuelve hits en Redis, delega misses |
| `response_generator` | 8002 | Procesa Q1–Q5 sobre el dataset en memoria |
| `metrics_store` | 8003 | Registra métricas T1 y T2; expone resultados |
| `traffic_generator` | — | Genera carga sintética en modo sync o async |
| `zookeeper` | 2181 | Coordinación de Kafka |
| `kafka` | 9092 | Broker de mensajes (4 particiones) |
| `kafka_consumer` | — | Consume topics, aplica reintentos y publica en DLQ |

---

## Tópicos de Kafka

| Tópico | Descripción |
|---|---|
| `queries` | Consultas nuevas publicadas por el traffic_generator |
| `queries.retry` | Consultas que fallaron y esperan reintento (con backoff) |
| `queries.recovery` | Reintentos que finalmente tuvieron éxito |
| `queries.dlq` | Consultas que agotaron todos los reintentos (Dead Letter Queue) |

---

## Consultas implementadas

| ID | Descripción | Cache key |
|---|---|---|
| Q1 | Conteo de edificios en una zona | `count:{zona}:conf={c}` |
| Q2 | Área promedio y total | `area:{zona}:conf={c}` |
| Q3 | Densidad por km² | `density:{zona}:conf={c}` |
| Q4 | Comparación de densidad entre dos zonas | `compare:density:{za}:{zb}:conf={c}` |
| Q5 | Distribución de confianza | `confidence_dist:{zona}:bins={n}` |

### Zonas geográficas

| ID | Sector |
|---|---|
| Z1 | Providencia |
| Z2 | Las Condes |
| Z3 | Maipú |
| Z4 | Santiago Centro |
| Z5 | Pudahuel |

---

## Experimentos

Los resultados se guardan automáticamente en `results/<label>.json`.

### Tarea 1 — Experimentos de caché

```bash
bash scripts/run_all.sh          # todos los grupos (~2 h)
bash scripts/run_all.sh 1        # G1: distribución de tráfico (Zipf vs Uniforme)
bash scripts/run_all.sh 2        # G2: política de evicción (LRU / LFU / RANDOM)
bash scripts/run_all.sh 3        # G3: tamaño de caché (1 / 5 / 10 mb)
bash scripts/run_all.sh 4        # G4: efecto del TTL (bajo / medio / alto)
```

| Grupo | Variable analizada | Experimentos |
|---|---|---|
| G1 | Distribución (Zipf vs Uniforme) | 2 |
| G2 | Política de evicción | 6 |
| G3 | Tamaño de caché | 6 |
| G4 | TTL | 6 |

### Tarea 2 — Experimentos Kafka

```bash
bash scripts/run_kafka_all.sh         # todos los escenarios T2
bash scripts/run_kafka_e1_base.sh     # E1: sistema síncrono de referencia
bash scripts/run_kafka_e2_single.sh   # E2: Kafka con 1 consumer
bash scripts/run_kafka_e3_scaling.sh  # E3: escalamiento N=1, 2, 4 consumers
bash scripts/run_kafka_e4_failure.sh  # E4: falla temporal del backend
bash scripts/run_kafka_e5_retries.sh  # E5: reintentos intermitentes (3 fallas)
bash scripts/run_kafka_e6_spike.sh    # E6: spike de tráfico (10→100→10 req/s)
bash scripts/run_kafka_e7_recovery.sh # E7: comparación directa Sync vs Kafka
```

| Escenario | Descripción |
|---|---|
| E1 | Línea de referencia síncrona (sin Kafka) |
| E2 | Kafka con 1 consumer — baseline async |
| E3 | Escalamiento horizontal: throughput y backlog con N=1, 2, 4 |
| E4 | Falla de 15 s del `response_generator` — mide retry/recovery/DLQ |
| E5 | Tres fallas intermitentes de 12 s — evalúa el backoff exponencial |
| E6 | Spike 10× de tráfico — crece backlog y mide recovery_time |
| E7 | Sync vs Async ante la misma falla — pérdida de consultas en T1 vs T2 |

---

## Métricas

### Tarea 1

| Métrica | Definición |
|---|---|
| `hit_rate` | hits / (hits + misses) |
| `throughput_rps` | consultas procesadas por segundo |
| `latency_p50/p95` | percentiles de tiempo de respuesta |
| `eviction_rate` | evictions por minuto |

### Tarea 2 (nuevas)

| Métrica | Definición |
|---|---|
| `retry_rate` | reintentos / total de consultas |
| `recovery_rate` | recuperaciones exitosas / total de reintentos |
| `dlq_rate` | mensajes en DLQ / total de consultas |
| `backlog_size` | mensajes pendientes en Kafka en un instante dado |
| `peak_backlog_size` | máximo backlog observado en el experimento |
| `recovery_time_s` | segundos hasta que la cola quedó vacía tras la falla |

Ver resultados en tiempo real:

```bash
curl http://localhost:8003/summary         # métricas T1
curl http://localhost:8003/kafka_summary   # métricas T2
curl http://localhost:8003/results         # todos los experimentos guardados
```

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `REDIS_MAXMEMORY` | `256mb` | Memoria máxima de Redis |
| `REDIS_EVICTION_POLICY` | `allkeys-lru` | Política: `allkeys-lru`, `allkeys-lfu`, `allkeys-random` |
| `TTL_Q1` / `TTL_Q2` | `120` | TTL en segundos para Q1/Q2 |
| `TTL_Q3` | `90` | TTL para Q3 |
| `TTL_Q4` | `60` | TTL para Q4 |
| `TTL_Q5` | `30` | TTL para Q5 |
| `LATENCY_FACTOR_MS` | `5` | ms de latencia simulada por cada 10k registros; sube a `80` para E3–E5 |
| `MODE` | `sync` | `sync` = T1 (HTTP directo) / `async` = T2 (publica en Kafka) |
| `TRAFFIC_DISTRIBUTION` | `zipf` | `zipf` o `uniform` |
| `NUM_REQUESTS` | `2000` | Total de requests a generar |
| `ARRIVAL_RATE` | `50` | Requests por segundo |
| `ZIPF_ALPHA` | `1.2` | Parámetro α de Zipf (mayor = más concentrado en Z1) |
| `CONF_VALUES` | `0.0,0.7` | Valores de `confidence_min`; más valores = más cardinalidad = menos hit rate |
| `MAX_RETRIES` | `3` | Reintentos antes de enviar a DLQ |
| `RETRY_DELAY_MS` | `500` | Base del backoff exponencial (ms); backoff total = `delay × (2^0 + … + 2^(N-1))` |

---

## Estructura del proyecto

```
.
├── cache_service/           # FastAPI + Redis — intercepta queries (T1, sin cambios en T2)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── response_generator/      # FastAPI + Pandas — computa Q1–Q5 en memoria (T1, sin cambios)
│   ├── main.py
│   ├── zones.py
│   ├── Dockerfile
│   └── requirements.txt
├── metrics_store/           # FastAPI — registra métricas T1 y T2 (extendido en T2)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── traffic_generator/       # Generador de carga — MODE=sync (T1) o MODE=async (T2)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── kafka_consumer/          # ★ NUEVO T2 — consume topics, reintentos, DLQ
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   ├── common.sh            # Funciones base T1
│   ├── common_kafka.sh      # ★ NUEVO T2 — wait_kafka, get_backlog, restart_kafka_stack
│   ├── run_all.sh           # Maestro de experimentos T1
│   ├── run_g1.sh … run_g4.sh
│   ├── run_kafka_all.sh     # ★ NUEVO T2 — maestro de experimentos T2
│   ├── run_kafka_e1_base.sh … run_kafka_e7_recovery.sh
│   ├── run_tarea2.sh        # Atajo: levanta stack Kafka + corre E1–E7
│   └── download_dataset.sh
├── data/                    # Dataset CSV (no incluido en el repo)
├── results/                 # JSONs con resultados de todos los experimentos
├── docker-compose.yml       # Stack completo T1 + T2
├── .env.example
└── README.md
```

---

## Notas de diseño

**¿Por qué `LATENCY_FACTOR_MS=5` y no `0.5`?**
Con 0.5 ms el `response_generator` era tan rápido que no había backlog observable. Con 5 ms los misses tardan ~25–150 ms según la zona, lo que hace visible el cuello de botella. Los experimentos E3–E5 lo suben a 80 ms para forzar saturación y estudiar el crecimiento del backlog.

**¿Por qué 4 particiones en Kafka?**
El límite de consumidores útiles en un grupo es igual al número de particiones. Con 4 particiones podemos comparar N=1, 2 y 4 consumers sin tener instancias ociosas.

**¿Por qué commit manual en el consumer?**
`enable_auto_commit=False` garantiza *at-least-once delivery*: el offset solo avanza después de que el mensaje fue procesado o publicado en `queries.retry`/`queries.dlq`. Si el consumer cae a mitad del proceso, el mensaje se retoma desde el último commit.

**Cálculo del backoff exponencial:**
```
retry_available_at = time.time() + (RETRY_DELAY_MS / 1000) × 2^retry_count
```
Con `RETRY_DELAY_MS=200` y `MAX_RETRIES=5` el backoff acumulado es 6.2 s, diseñado para ser menor que la duración típica de una falla (8–15 s) pero suficiente para dar tiempo al backend de recuperarse.


---

## Declaración de uso de IA

En el desarrollo de este proyecto se utilizaron herramientas de inteligencia artificial (Claude, Anthropic) como apoyo en las siguientes áreas:

- **Visualización de datos:** generación de gráficos a partir de los datos crudos de los experimentos (JSONs en `results/`) para facilitar el análisis comparativo entre escenarios.
- **Auditoría y corrección de código:** revisión del código de los servicios y scripts para detectar errores, inconsistencias y oportunidades de mejora; las correcciones fueron evaluadas y aplicadas por el equipo.
- **Investigación sobre Apache Kafka:** consultas sobre conceptos, patrones de diseño y mejores prácticas de Kafka (grupos de consumo, particiones, backoff exponencial, Dead Letter Queue, commit manual de offsets).

El diseño del sistema, la implementación, los experimentos y el análisis de resultados fueron realizados por el equipo. El uso de IA se limitó a apoyo puntual en las áreas indicadas.

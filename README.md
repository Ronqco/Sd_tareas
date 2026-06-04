# Sistemas Distribuidos — Entregable 1
**Plataforma de análisis de edificaciones con caché distribuido**
Dataset: Google Open Buildings — Región Metropolitana de Santiago

---

## Requisitos previos

- Docker y docker-compose instalados
- Python 3.11+ (solo para correr los scripts de experimentos)
- curl y bash (Linux/macOS) o WSL (Windows)
- ~500mb de espacio en disco para el dataset

---

## Instalación y primer arranque

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/nombre-repo.git
cd nombre-repo
```

### 2. Descargar el dataset

El dataset (Google Open Buildings v3) no está en el repositorio por su tamaño.
Descárgalo con el script incluido:

```bash
bash scripts/download_dataset.sh
```

Esto crea `data/buildings.csv` con los edificios de la RM de Santiago.
Si el script falla por los tiles, descarga manualmente desde:
https://sites.research.google/gr/open-buildings/
y coloca el CSV en `data/buildings.csv` con las columnas:
`latitude, longitude, area_in_meters, confidence`

### 3. Configurar variables de entorno

```bash
cp .env.example .env
```

El archivo `.env` ya tiene valores por defecto funcionales.
No es necesario modificarlo para el primer arranque.

### 4. Levantar el sistema

```bash
docker-compose up --build -d redis metrics_store response_generator cache_service
```

Verificar que todos los servicios estén healthy:

```bash
docker-compose ps
```

Deberías ver los 4 servicios con estado `Up (healthy)`.

---

## Verificar que funciona

```bash
# Health check de cada servicio
curl http://localhost:8001/health   # cache_service
curl http://localhost:8002/health   # response_generator
curl http://localhost:8003/health   # metrics_store

# Enviar una consulta manualmente
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type": "Q1", "params": {"zone_id": "Z1", "confidence_min": 0.7}}'

# Ver métricas
curl http://localhost:8003/summary
```

---

## Arquitectura

```
traffic_generator
      │  POST /query
      ▼
cache_service ──(hit)──► respuesta directa
      │ (miss)
      ▼
response_generator ──► cómputo en memoria (dataset precargado)

metrics_store ◄── cache_service reporta cada hit/miss aquí
```

### Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| `redis` | 6379 | Backend del caché con TTL y política de evicción |
| `cache_service` | 8001 | Intercepta queries, busca en Redis, delega misses |
| `response_generator` | 8002 | Procesa Q1–Q5 sobre el dataset en memoria |
| `metrics_store` | 8003 | Registra hits, misses, latencias y persiste resultados |
| `traffic_generator` | — | Genera carga sintética (Zipf o Uniforme) |

### Queries implementadas

| ID | Descripción | Cache key |
|---|---|---|
| Q1 | Conteo de edificios en una zona | `count:{zona}:conf={c}` |
| Q2 | Área promedio y total de edificios | `area:{zona}:conf={c}` |
| Q3 | Densidad de edificios por km² | `density:{zona}:conf={c}` |
| Q4 | Comparación de densidad entre dos zonas | `compare:density:{za}:{zb}:conf={c}` |
| Q5 | Distribución de confianza en una zona | `confidence_dist:{zona}:bins={n}` |

### Zonas geográficas

| ID | Sector |
|---|---|
| Z1 | Providencia |
| Z2 | Las Condes |
| Z3 | Maipú |
| Z4 | Santiago Centro |
| Z5 | Pudahuel |

---

## Correr los experimentos

Los experimentos se organizan en 4 grupos, cada uno analiza una variable distinta
del sistema de caché. Los resultados se guardan en `results/<label>.json`.

```bash
# Correr todos los grupos (25 experimentos, ~2 horas)
bash scripts/run_all.sh

# Correr solo un grupo específico
bash scripts/run_all.sh 1    # distribución de tráfico
bash scripts/run_all.sh 2    # política de evicción
bash scripts/run_all.sh 3    # tamaño de caché
bash scripts/run_all.sh 4    # efecto del TTL

# Correr varios grupos
bash scripts/run_all.sh 2 3
```

### Descripción de los grupos

| Grupo | Variable | Fijo | Experimentos |
|---|---|---|---|
| G1 | Distribución (Zipf vs Uniforme) | LRU, 256mb, TTL default | 2 |
| G2 | Política evicción (LRU/LFU/RANDOM) | 1mb, TTL default | 6 |
| G3 | Tamaño caché (1mb/5mb/10mb) | LRU, TTL default | 6 |
| G4 | TTL (bajo/medio/alto) | LRU, 256mb | 6 |

### Parámetros configurables

Todos los parámetros globales se modifican en `scripts/common.sh`:

```bash
NREQS=5000       # requests por experimento
RATE=20          # requests por segundo
CONF_LOW="0.0,0.7"                 # cardinalidad baja (G1, G4)
CONF_HIGH="0.0,0.3,0.5,0.7,0.9"   # cardinalidad alta (G2, G3)
TTL_DEFAULT_Q1=120
TTL_DEFAULT_Q3=90
TTL_DEFAULT_Q4=60
TTL_DEFAULT_Q5=30
```

### Ver resultados acumulados

```bash
curl http://localhost:8003/results
```

Los JSONs en `results/` contienen todas las métricas:
hit rate, throughput, latencia p50/p95, eviction rate, cache efficiency
y desglose por tipo de query (Q1–Q5).

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
| `TRAFFIC_DISTRIBUTION` | `zipf` | `zipf` o `uniform` |
| `NUM_REQUESTS` | `1000` | Total de requests a generar |
| `ARRIVAL_RATE` | `10` | Requests por segundo |
| `ZIPF_ALPHA` | `1.2` | Parámetro α de Zipf (mayor = más concentrado en Z1) |
| `CONF_VALUES` | `0.0,0.7` | Valores de confidence_min para Q1/Q2/Q3 |

---

## Estructura del proyecto

```
.
├── cache_service/           # Servicio de caché (FastAPI + Redis)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── response_generator/      # Procesador de queries (FastAPI + Pandas)
│   ├── main.py
│   ├── zones.py             # Definición de zonas geográficas
│   ├── Dockerfile
│   └── requirements.txt
├── metrics_store/           # Almacén de métricas (FastAPI)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── traffic_generator/       # Generador de carga (httpx + numpy)
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   ├── common.sh            # Funciones y variables compartidas
│   ├── run_all.sh           # Script maestro de experimentos
│   ├── run_g1.sh            # Grupo 1: distribución de tráfico
│   ├── run_g2.sh            # Grupo 2: política de evicción
│   ├── run_g3.sh            # Grupo 3: tamaño de caché
│   ├── run_g4.sh            # Grupo 4: efecto del TTL
│   └── download_dataset.sh  # Descarga el dataset
├── data/                    # Dataset CSV (no incluido en el repo)
├── results/                 # JSONs con resultados de experimentos
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Métricas registradas

| Métrica | Definición |
|---|---|
| Hit rate | hits / (hits + misses) |
| Throughput | consultas / segundo |
| Latencia p50/p95 | percentiles de tiempo de respuesta |
| Eviction rate | evictions por minuto |
| Cache efficiency | (hits × t_cache − misses × t_db) / total |

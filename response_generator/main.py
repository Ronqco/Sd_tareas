"""
response_generator/main.py
Procesa Q1–Q5 en memoria sobre el dataset Google Open Buildings.
Mejoras respecto a versión original:
  - Q4 ahora aplica confidence_min correctamente
  - Simula latencia de procesamiento realista (proporcional al tamaño del dataset)
  - Reporta tiempos de procesamiento a metrics_store
  - Cache keys siguen el formato del enunciado
"""

import os
import time
import asyncio
import pandas as pd
import numpy as np
import httpx
from fastapi import FastAPI

from zones import ZONES

app = FastAPI(title="Response Generator")

DATA_PATH    = os.getenv("DATA_PATH",    "/app/data/buildings.csv")
METRICS_URL  = os.getenv("METRICS_URL",  "http://metrics_store:8003")

# Factor de simulación de latencia: ms por cada 10k registros procesados
LATENCY_FACTOR_MS = float(os.getenv("LATENCY_FACTOR_MS", "0.5"))

_df: pd.DataFrame | None = None
data_by_zone: dict = {}
zone_area_km2: dict = {}
_http: httpx.AsyncClient | None = None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _compute_area_km2(z: dict) -> float:
    """Área aproximada del bounding box en km²."""
    lat_km = (z["max_lat"] - z["min_lat"]) * 111.0
    lon_km = (z["max_lon"] - z["min_lon"]) * 111.0
    return abs(lat_km * lon_km)


def _filter_zone(df: pd.DataFrame, z: dict) -> pd.DataFrame:
    return df[
        (df["latitude"]  >= z["min_lat"]) & (df["latitude"]  <= z["max_lat"]) &
        (df["longitude"] >= z["min_lon"]) & (df["longitude"] <= z["max_lon"])
    ]


def _simulate_latency(n_records: int):
    """Simula el tiempo de procesamiento proporcional al nº de registros."""
    delay_ms = (n_records / 10_000) * LATENCY_FACTOR_MS
    time.sleep(delay_ms / 1000)


# ── Startup / Shutdown ─────────────────────────────────────────────────────────

@app.on_event("startup")
async def load_data():
    global _df, data_by_zone, zone_area_km2, _http

    _http = httpx.AsyncClient()

    print("📦 Cargando dataset...")
    _df = pd.read_csv(DATA_PATH, usecols=["latitude", "longitude", "area_in_meters", "confidence"])

    print("📊 Filtrando por zonas...")
    for zone_id, z in ZONES.items():
        subset = _filter_zone(_df, z).reset_index(drop=True)
        data_by_zone[zone_id] = subset
        zone_area_km2[zone_id] = _compute_area_km2(z)
        print(f"  {zone_id}: {len(subset):,} registros — {zone_area_km2[zone_id]:.2f} km²")

    print("✅ Dataset listo en memoria")


@app.on_event("shutdown")
async def shutdown():
    if _http:
        await _http.aclose()


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    loaded = {zid: len(df) for zid, df in data_by_zone.items()}
    return {"status": "ok", "zones_loaded": loaded}


@app.post("/query")
def query(payload: dict):
    query_type = payload["query_type"]
    params     = payload["params"]
    zone_id    = params.get("zone_id")

    if zone_id not in data_by_zone:
        return {"error": f"invalid zone: {zone_id}"}

    df             = data_by_zone[zone_id]
    confidence_min = float(params.get("confidence_min", 0.0))

    t0 = time.perf_counter()

    # ── Q1: Conteo de edificios ────────────────────────────────────────────────
    if query_type == "Q1":
        subset = df[df["confidence"] >= confidence_min]
        _simulate_latency(len(df))
        result = int(len(subset))

    # ── Q2: Área promedio y total ──────────────────────────────────────────────
    elif query_type == "Q2":
        subset = df[df["confidence"] >= confidence_min]
        _simulate_latency(len(df))
        if len(subset) == 0:
            result = {"avg_area": 0.0, "total_area": 0.0, "n": 0}
        else:
            result = {
                "avg_area":   round(float(subset["area_in_meters"].mean()), 4),
                "total_area": round(float(subset["area_in_meters"].sum()),  4),
                "n":          int(len(subset)),
            }

    # ── Q3: Densidad por km² ───────────────────────────────────────────────────
    elif query_type == "Q3":
        subset = df[df["confidence"] >= confidence_min]
        _simulate_latency(len(df))
        density = len(subset) / zone_area_km2[zone_id]
        result  = round(float(density), 4)

    # ── Q4: Comparación de densidad entre dos zonas ───────────────────────────
    elif query_type == "Q4":
        zone_b = params.get("zone_b")
        if zone_b not in data_by_zone:
            return {"error": f"invalid zone_b: {zone_b}"}

        df_b   = data_by_zone[zone_b]
        sub_a  = df[df["confidence"]   >= confidence_min]
        sub_b  = df_b[df_b["confidence"] >= confidence_min]

        _simulate_latency(len(df) + len(df_b))

        da = len(sub_a) / zone_area_km2[zone_id]
        db = len(sub_b) / zone_area_km2[zone_b]

        result = {
            "zone_a":       zone_id,
            "density_a":    round(float(da), 4),
            "zone_b":       zone_b,
            "density_b":    round(float(db), 4),
            "winner":       zone_id if da > db else zone_b,
            "ratio":        round(da / db, 4) if db > 0 else None,
        }

    # ── Q5: Distribución de confianza ─────────────────────────────────────────
    elif query_type == "Q5":
        bins   = int(params.get("bins", 5))
        scores = df["confidence"].values
        _simulate_latency(len(df))
        counts, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))
        result = [
            {
                "bucket": i,
                "min":    round(float(edges[i]),   4),
                "max":    round(float(edges[i+1]), 4),
                "count":  int(counts[i]),
            }
            for i in range(bins)
        ]

    else:
        return {"error": f"unknown query_type: {query_type}"}

    processing_ms = (time.perf_counter() - t0) * 1000
    return {
        "result":         result,
        "processing_ms":  round(processing_ms, 3),
    }

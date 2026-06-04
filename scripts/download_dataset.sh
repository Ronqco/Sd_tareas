#!/usr/bin/env bash
# download_dataset.sh
# Descarga los tiles del dataset Google Open Buildings para la Región Metropolitana.
# Requiere: curl, gunzip
# Uso: bash scripts/download_dataset.sh

set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
mkdir -p "$DATA_DIR"

echo "=== Google Open Buildings – Región Metropolitana ==="
echo "Destino: $DATA_DIR"

# Tiles que cubren la RM de Santiago (aprox. -33.8 a -33.2 lat, -71.0 a -70.4 lon)
# Fuente: https://sites.research.google/gr/open-buildings/
# Los tiles se identifican por su código de celda S2.
# Listado actualizado para v3 del dataset.

TILES=(
  "https://openbuildings-public-dot-gweb-research.uw.r.appspot.com/public/tiles/v3/105/buildings_105_a9a3ffffff.csv.gz"
  "https://openbuildings-public-dot-gweb-research.uw.r.appspot.com/public/tiles/v3/105/buildings_105_a9b3ffffff.csv.gz"
  # Agregar más tiles según cobertura necesaria
  # Usar el mapa interactivo en la URL de arriba para identificar los tiles exactos.
)

OUTPUT_CSV="$DATA_DIR/buildings.csv"
HEADER_WRITTEN=false

for url in "${TILES[@]}"; do
  fname=$(basename "$url")
  echo "Descargando $fname ..."
  curl -sSL "$url" -o "/tmp/$fname"

  if [ "$HEADER_WRITTEN" = false ]; then
    gunzip -c "/tmp/$fname" > "$OUTPUT_CSV"
    HEADER_WRITTEN=true
  else
    # Omitir header (primera línea) en tiles adicionales
    gunzip -c "/tmp/$fname" | tail -n +2 >> "$OUTPUT_CSV"
  fi

  rm "/tmp/$fname"
done

ROWS=$(wc -l < "$OUTPUT_CSV")
echo "Dataset listo: $OUTPUT_CSV ($ROWS líneas)"
echo ""
echo "Columnas esperadas: latitude, longitude, area_in_meters, confidence, ..."
echo "Verifica con: head -n 2 $OUTPUT_CSV"

#!/usr/bin/env bash
set -euo pipefail

OUTDIR="data_raw/lookup"
mkdir -p "$OUTDIR"

URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OUT="${OUTDIR}/taxi_zone_lookup.csv"

echo "Downloading zone lookup -> ${OUT}"
curl -L --retry 10 --retry-all-errors --retry-delay 2 \
  --connect-timeout 20 --max-time 120 \
  "$URL" -o "$OUT"

ls -lh "$OUT"
head -n 3 "$OUT"

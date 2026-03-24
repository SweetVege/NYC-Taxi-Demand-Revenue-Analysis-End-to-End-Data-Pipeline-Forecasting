#!/usr/bin/env bash
set -u
set -o pipefail

OUTDIR="data_raw/taxi"
mkdir -p "$OUTDIR"

curl_get () {
  local url="$1"
  local out="$2"
  curl -L --retry 10 --retry-all-errors --retry-delay 2 \
    --connect-timeout 20 --max-time 600 \
    "$url" -o "$out"
}

for YEAR in 2023 2024 2025; do
  for MONTH in $(seq -w 1 12); do
    FILE="yellow_tripdata_${YEAR}-${MONTH}.parquet"
    OUT="${OUTDIR}/${FILE}"
    URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}"

    if [[ -s "$OUT" ]]; then
      echo "exists, skip: $OUT"
      continue
    fi

    echo "downloading: $FILE"
    TMP="${OUT}.tmp"
    rm -f "$TMP"
    if curl_get "$URL" "$TMP"; then
      mv "$TMP" "$OUT"
      ls -lh "$OUT"
    else
      echo "FAILED: $URL"
      rm -f "$TMP"
    fi
    sleep 0.2
  done
done

echo "Done downloading taxi 2023-2025."

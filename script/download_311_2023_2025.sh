#!/usr/bin/env bash
set -u
set -o pipefail

DATASET="erm2-nwe9"
OUTDIR="data_raw/311"
mkdir -p "$OUTDIR"

SELECT="unique_key,created_date,complaint_type,descriptor,borough,latitude,longitude"
LIMIT=20000
SLEEP_SEC=1.0

curl_get () {
  local url="$1"
  local out="$2"
  curl -L --retry 12 --retry-all-errors --retry-delay 3 \
    --connect-timeout 20 --max-time 300 \
    -H "Accept: text/csv" \
    "$url" -o "$out"
}

for YEAR in 2023 2024 2025; do
  for MONTH in $(seq -w 1 12); do
    START="${YEAR}-${MONTH}-01T00:00:00"
    if [[ "$MONTH" == "12" ]]; then
      NEXT_YEAR=$((YEAR+1)); NEXT_MONTH="01"
    else
      NEXT_YEAR=$YEAR; NEXT_MONTH=$(printf "%02d" $((10#$MONTH+1)))
    fi
    END="${NEXT_YEAR}-${NEXT_MONTH}-01T00:00:00"

    WHERE="created_date between '${START}' and '${END}'"
    WHERE_ENC=$(echo "$WHERE" | sed 's/ /%20/g')

    COUNT_URL="https://data.cityofnewyork.us/resource/${DATASET}.json?\$select=count(1)%20as%20n&\$where=${WHERE_ENC}"
    N=$(curl -sL --connect-timeout 20 --max-time 60 \
      --retry 8 --retry-all-errors --retry-delay 2 \
      "$COUNT_URL" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['n'])" 2>/dev/null || echo "ERR")

    if [[ "$N" == "ERR" ]]; then
      echo "==> ${YEAR}-${MONTH}: count failed, skip month"
      continue
    fi

    echo "==> ${YEAR}-${MONTH}: ${N} rows"
    if [[ "$N" == "0" ]]; then
      continue
    fi

    OFFSET=0
    PART=1
    while [[ $OFFSET -lt $N ]]; do
      OUT="${OUTDIR}/311_${YEAR}_${MONTH}_part${PART}.csv"
      if [[ -s "$OUT" ]]; then
        echo "    exists, skip: $OUT"
        OFFSET=$((OFFSET + LIMIT))
        PART=$((PART + 1))
        continue
      fi

      URL="https://data.cityofnewyork.us/resource/${DATASET}.csv?\$select=${SELECT}&\$where=${WHERE_ENC}&\$order=created_date&\$limit=${LIMIT}&\$offset=${OFFSET}"
      echo "    downloading part ${PART} offset=${OFFSET}"

      TMP="${OUT}.tmp"
      rm -f "$TMP"
      if curl_get "$URL" "$TMP"; then
        if [[ -s "$TMP" ]]; then
          mv "$TMP" "$OUT"
        else
          echo "    WARNING: empty response"
          rm -f "$TMP"
        fi
      else
        echo "    ERROR: curl failed; retry later"
        rm -f "$TMP"
        sleep 5
      fi

      OFFSET=$((OFFSET + LIMIT))
      PART=$((PART + 1))
      sleep "$SLEEP_SEC"
    done
  done
done

echo "Done downloading 311 2023-2025."

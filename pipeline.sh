#!/usr/bin/env bash
set -euo pipefail
mkdir -p logs

echo "[1/5] Download 311 (2023-2025)"
./scripts/
download_311_2023_2025.sh 2>&1 | tee logs/step1_download_311.log

echo "[2/5] Download Taxi (2023-2025)"
./scripts/download_taxi_2023_2025.sh 2>&1 | tee logs/step2_download_taxi.log

echo "[3/5] Download Taxi Zone Lookup"
./scripts/download_zone_lookup.sh 2>&1 | tee logs/step3_download_lookup.log
echo "[4/5] Download Weather (2023-2025)"
./scripts/download_weather_2023_2025.sh 2>&1 | tee logs/step4_download_weather.log

echo "[5/5] Spark ETL (TODO)"
echo "TODO: spark-submit spark_jobs/etl_*.py ..."

echo "DONE: ingest complete."

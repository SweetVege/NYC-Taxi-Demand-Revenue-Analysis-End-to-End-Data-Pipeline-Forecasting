#!/bin/bash

# Configuration
CLUSTER="msba405-cluster"
REGION="us-central1"
BUCKET="gs://msba405-nyc-project"
CODE_DIR="${BUCKET}/code"

echo "=========================================================="
echo "STARTING NYC DATA PIPELINE: WEATHER + TAXI + 311"
echo "FULL YEARS: 2023, 2024, 2025"
echo "=========================================================="

# 1. WEATHER PROCESSING (BigQuery -> GCS)
echo "[1/6] Processing Weather in BigQuery..."
bash reprocess_weather_v2_final.sql || exit 1

echo "[2/6] Extracting Weather to Parquet..."
bash extract_weather_v2_final.sh || exit 1

# 2. 311 PROCESSING
echo "[3/6] Submitting 311 ETL (Hourly/Borough)..."
gcloud dataproc jobs submit pyspark ${CODE_DIR}/311_hourly_borough_final.py \
    --cluster=${CLUSTER} --region=${REGION} || exit 1

# 3. TAXI PROCESSING (2023 - 2025)
echo "[4/6] Processing Taxi Data (Full 3-Year Range)..."
# Loop through 2023, 2024, and 2025 for all 12 months
for y in 2023 2024 2025; do
    for m in {1..12}; do
        echo "   -> Processing Taxi: $y-$m"
        gcloud dataproc jobs submit pyspark ${CODE_DIR}/etl_taxi_hourly_zone_final.py \
            --cluster=${CLUSTER} --region=${REGION} -- --year $y --month $m
    done
done

# 4. BRIDGE & MASTER JOIN
echo "[5/6] Bridging Taxi Zones to Boroughs..."
gcloud dataproc jobs submit pyspark ${CODE_DIR}/etl_taxi_hourly_borough_final.py \
    --cluster=${CLUSTER} --region=${REGION} || exit 1

echo "[6/6] Building Final Master Table..."
gcloud dataproc jobs submit pyspark ${CODE_DIR}/build_master_table_v2_final.py \
    --cluster=${CLUSTER} --region=${REGION} || exit 1

echo "=========================================================="
echo "PIPELINE SUCCESSFUL: FULL 2023-2025 MASTER TABLE REFRESHED"
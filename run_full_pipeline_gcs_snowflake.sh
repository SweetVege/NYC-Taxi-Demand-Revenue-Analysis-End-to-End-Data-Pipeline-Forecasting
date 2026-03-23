#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT_ID="tactile-flash-488804-p6"
REGION="us-central1"
BUCKET="gs://msba405-nyc-project"
CODE_DIR="${BUCKET}/code"
SNOWFLAKE_CONN="main"

echo "=========================================================="
echo "FULL PIPELINE START"
echo "GCP / GCS + MY TAXI ZONE + SNOWFLAKE + VALIDATION"
echo "=========================================================="

echo "[0/5] Downloading required local files..."
gcloud storage cp ${CODE_DIR}/run_nyc_pipeline_final.sh .
gcloud storage cp ${CODE_DIR}/reprocess_weather_v2_final.sql .
gcloud storage cp ${CODE_DIR}/extract_weather_v2_final.sh .

# Patch team pipeline locally so weather step uses the script wrapper directly
sed -i '' 's|bq query --use_legacy_sql=false < reprocess_weather_v2_final.sql|bash reprocess_weather_v2_final.sql|' run_nyc_pipeline_final.sh

chmod +x run_nyc_pipeline_final.sh
chmod +x extract_weather_v2_final.sh
chmod +x reprocess_weather_v2_final.sql

echo "[1/5] Running team pipeline..."
./run_nyc_pipeline_final.sh

echo "[2/5] Running my taxi zone PySpark batch..."
gcloud dataproc batches submit pyspark \
  ${CODE_DIR}/clean_taxi_zone_pyspark.py \
  --region=${REGION} \
  --version=2.3 \
  --properties=dataproc.tier=standard,spark.driver.cores=4,spark.executor.cores=4,spark.executor.instances=2,spark.dataproc.driver.disk.size=250g,spark.dataproc.executor.disk.size=250g,spark.sql.parquet.enableVectorizedReader=false,spark.sql.files.ignoreCorruptFiles=true

echo "[3/5] Running Snowflake import SQL..."
snow sql -c ${SNOWFLAKE_CONN} -f "$SCRIPT_DIR/sql/data import to snowflake.sql"

echo "[4/5] Refreshing integration for taxi zone path..."
snow sql -c ${SNOWFLAKE_CONN} -q "CREATE OR REPLACE STORAGE INTEGRATION GCS_INT TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = GCS ENABLED = TRUE STORAGE_ALLOWED_LOCATIONS = ('gcs://msba405-nyc-project/Processed/','gcs://msba405-nyc-project/processed/taxi_hourly_zone/','gcs://msba405-nyc-project/processed/taxi_hourly_zone/nyc_hourly_zone_final/');"

echo "[4.1/5] Running my taxi zone Snowflake SQL..."
snow sql -c ${SNOWFLAKE_CONN} -f "$SCRIPT_DIR/sql/(location) data connect to tableau.sql"

echo "[4.2/5] Running shared Tableau SQL..."
snow sql -c ${SNOWFLAKE_CONN} -f "$SCRIPT_DIR/sql/data connect to tableau.sql"

echo "[5/5] Running validation queries..."
snow sql -c ${SNOWFLAKE_CONN} -q "SELECT COUNT(*) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.NYC_HOURLY_ZONE_FINAL_GCS;"
snow sql -c ${SNOWFLAKE_CONN} -q "SELECT MIN(hour_ts), MAX(hour_ts) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.NYC_HOURLY_ZONE_FINAL_GCS;"
snow sql -c ${SNOWFLAKE_CONN} -q "SHOW VIEWS LIKE 'V_NYC_HOURLY_ZONE_FINAL' IN SCHEMA SNOWFLAKE_LEARNING_DB.PUBLIC;"

echo "=========================================================="
echo "FULL PIPELINE COMPLETED SUCCESSFULLY"
echo "=========================================================="

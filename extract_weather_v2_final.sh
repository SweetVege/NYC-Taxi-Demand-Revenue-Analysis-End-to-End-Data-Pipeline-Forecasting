bq extract \
  --destination_format=PARQUET \
  msba405_tmp.weather_reprocessed_v2 \
  gs://msba405-nyc-project/Processed_v2/weather_reprocessed_v2/*.parquet

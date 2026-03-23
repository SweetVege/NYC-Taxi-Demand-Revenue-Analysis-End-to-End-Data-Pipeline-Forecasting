bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE msba405_tmp.weather_reprocessed_v2 AS
SELECT
  time AS time_ns,
  DIV(time, 1000) AS time_us,

  TIMESTAMP_MICROS(DIV(time, 1000)) AS ts,
  TIMESTAMP_TRUNC(TIMESTAMP_MICROS(DIV(time, 1000)), HOUR) AS hour_ts,

  EXTRACT(YEAR  FROM TIMESTAMP_MICROS(DIV(time, 1000))) AS year,
  EXTRACT(MONTH FROM TIMESTAMP_MICROS(DIV(time, 1000))) AS month,
  EXTRACT(DAY   FROM TIMESTAMP_MICROS(DIV(time, 1000))) AS day,
  EXTRACT(HOUR  FROM TIMESTAMP_MICROS(DIV(time, 1000))) AS hour,

  temp,
  dwpt,
  rhum,
  prcp,
  snow,
  wdir,
  wspd,
  wpgt,
  pres,
  tsun,
  coco,

  CASE
    WHEN coco IN (7, 8, 9, 10, 11, 12, 13) THEN CAST(coco AS INT64)
    ELSE 0
  END AS rain_code,

  CASE
    WHEN coco = 7  THEN 'Light rain'
    WHEN coco = 8  THEN 'Rain'
    WHEN coco = 9  THEN 'Heavy rain'
    WHEN coco = 10 THEN 'Freezing rain'
    WHEN coco = 11 THEN 'Heavy freezing rain'
    WHEN coco = 12 THEN 'Sleet'
    WHEN coco = 13 THEN 'Heavy sleet'
    ELSE 'No rain'
  END AS rain_text,

  CASE
    WHEN coco IN (15, 16, 17, 18) THEN CAST(coco AS INT64)
    ELSE 0
  END AS snow_code,

  CASE
    WHEN coco = 15 THEN 'Light snow'
    WHEN coco = 16 THEN 'Snow'
    WHEN coco = 17 THEN 'Heavy snow'
    WHEN coco = 18 THEN 'Snowstorm'
    ELSE 'No snow'
  END AS snow_text,

  CASE
    WHEN coco IN (9, 11, 13, 17, 18) OR wpgt > 20 THEN 1
    ELSE 0
  END AS extreme_weather

FROM msba405_tmp.weather_ext
"


USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

CREATE FILE FORMAT IF NOT EXISTS ff_parquet
TYPE = PARQUET;

CREATE OR REPLACE STORAGE INTEGRATION gcs_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://msba405-nyc-project/Processed/');

  DESC INTEGRATION gcs_int;

CREATE OR REPLACE STAGE stg_master_hourly_borough
  URL='gcs://msba405-nyc-project/Processed/master_hourly_borough_v2/'
  STORAGE_INTEGRATION=gcs_int
  FILE_FORMAT=ff_parquet;


CREATE FILE FORMAT IF NOT EXISTS ff_parquet
TYPE = PARQUET;

CREATE OR REPLACE STAGE stg_master_hourly_borough
URL='gcs://msba405-nyc-project/Processed/master_hourly_borough_v2/'
STORAGE_INTEGRATION=gcs_int
FILE_FORMAT=ff_parquet;

LIST @stg_master_hourly_borough;


USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

-- 1) file format
CREATE FILE FORMAT IF NOT EXISTS ff_parquet
  TYPE = PARQUET;
-- 2) stage: （year/month）
CREATE OR REPLACE STAGE stg_master_hourly_borough
  URL='gcs://msba405-nyc-project/Processed/master_hourly_borough_v2/'
  STORAGE_INTEGRATION=gcs_int
  FILE_FORMAT=ff_parquet;

-- 3) external table
CREATE OR REPLACE EXTERNAL TABLE ext_master_hourly_borough
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@stg_master_hourly_borough',
      FILE_FORMAT => 'ff_parquet'
    )
  )
)
LOCATION=@stg_master_hourly_borough
FILE_FORMAT=ff_parquet
AUTO_REFRESH=FALSE;

-- 4) 
ALTER EXTERNAL TABLE ext_master_hourly_borough REFRESH;

-- 5) 
CREATE OR REPLACE VIEW v_master_hourly_borough AS
SELECT
  t.*,
  TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]+)', 1, 1, 'e', 1))  AS year,
  TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]+)', 1, 1, 'e', 1)) AS month,
  METADATA$FILENAME AS source_file
FROM ext_master_hourly_borough t;

SHOW TABLES LIKE 'EXT_MASTER_HOURLY_BOROUGH';
SHOW VIEWS LIKE 'V_MASTER_HOURLY_BOROUGH';







---- Stage1: we already connect the data from the Google cloud (datasets name: v_master_hourly_borough), but we have the format problem of Value column ---
SELECT *
FROM v_master_hourly_borough   
WHERE year = 2023 AND month = 1
LIMIT 5;





--- Stage2: we figure out the format problem of Value column （datasets name：v_master_hourly_borough_flat） ----
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

DECLARE cols STRING;

BEGIN
  WITH keys_raw AS (
    SELECT DISTINCT f.value::string AS k
    FROM v_master_hourly_borough,
         LATERAL FLATTEN(input => OBJECT_KEYS(VALUE)) f
    WHERE year = 2023 AND month = 1   
  ),
  keys_norm AS (
    SELECT k, LOWER(k) AS k_norm
    FROM keys_raw
  ),
  keys_dedup AS (
    SELECT
      k,
      k_norm,
      ROW_NUMBER() OVER (PARTITION BY k_norm ORDER BY k) AS rn
    FROM keys_norm
  )
  SELECT LISTAGG(
           'VALUE:"' || k || '" AS "v_' ||
           k_norm || CASE WHEN rn > 1 THEN '_' || rn ELSE '' END
           || '"'
         , ',\n'
         ) WITHIN GROUP (ORDER BY k_norm, rn)
    INTO :cols
  FROM keys_dedup;

  EXECUTE IMMEDIATE
    'CREATE OR REPLACE VIEW v_master_hourly_borough_flat AS
       SELECT * EXCLUDE (VALUE),
              ' || :cols || '
       FROM v_master_hourly_borough;';
END;


select *
from v_master_hourly_borough_flat
limit 20;


--- stage3: remove the duplicated columns (datasets name: nyc_hourly_borough ) ----

CREATE OR REPLACE VIEW nyc_hourly_borough AS
SELECT
    YEAR,
    MONTH,

    "v_day" AS day,
    "v_hour" AS hour,
    "v_hour_ts" AS hour_ts,

    "v_borough" AS borough,
    "v_trips" AS trips,

    "v_avg_fare_amount" AS avg_fare_amount,
    "v_avg_tip_amount" AS avg_tip_amount,
    "v_avg_total_amount" AS avg_total_amount,
    "v_avg_trip_distance" AS avg_trip_distance,
    "v_avg_trip_duration_min" AS avg_trip_duration_min,

    "v_temp_avg" AS temp_avg,
    "v_dewpoint_avg" AS dewpoint_avg,
    "v_humidity_avg" AS humidity_avg,
    "v_wind_avg" AS wind_avg,
    "wind_gust_avg" AS wind_gust_avg,
    "v_pressure_avg" AS pressure_avg,
    "v_precip_total" AS precip_total,
    "snow_total" AS snow_total,
    "v_snow_text" AS snow_text,
    "v_snow_code" AS snow_code, 
    "v_snow" AS snow,
    "v_rain" AS rain,
    "v_rain_code" AS rain_code,
    "v_rain_text" AS rain_text,
    "v_extreme_weather" AS extreme_weather,

    "v_complaints_311" AS complaints_311,
    "v_complaints_noise" AS complaints_noise,
    "v_complaints_sanitation" AS complaints_sanitation,
    "v_complaints_transportation" AS complaints_transportation,

    SOURCE_FILE

FROM v_master_hourly_borough_flat;


CREATE OR REPLACE TABLE nyc_hourly_borough_clean AS
SELECT *
FROM nyc_hourly_borough
WHERE borough NOT IN ('N/A', 'UNKNOWN');


--- final version : nyc_hourly_borough_final ----
CREATE OR REPLACE TABLE nyc_hourly_borough_final AS
SELECT *
FROM nyc_hourly_borough_clean;



--- sample ---
select distinct SNOW_TEXT
from nyc_hourly_borough_final;

SELECT SNOW_TEXT, COUNT(*)
FROM nyc_hourly_borough_final
GROUP BY SNOW_TEXT
ORDER BY COUNT(*) DESC;



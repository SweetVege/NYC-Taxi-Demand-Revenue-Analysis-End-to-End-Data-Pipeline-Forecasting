from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "gs://msba405-nyc-project/Processed_v2"
TAXI_PATH = f"{BASE}/taxi_hourly_borough/"
WEATHER_PATH = f"{BASE}/weather_reprocessed_v2/"
C311_PATH = f"{BASE}/311_hourly_borough/"
OUT_PATH = f"{BASE}/master_hourly_borough_v2/"

def std_borough(df):
    return df.withColumn("borough", F.upper(F.trim(F.col("borough")))) if "borough" in df.columns else df

def rename_time_cols(df, prefix):
    for c in ["year", "month", "day", "hour"]:
        if c in df.columns:
            df = df.withColumnRenamed(c, f"{prefix}_{c}")
    return df

def main():
    spark = SparkSession.builder.appName("build-master-hourly-borough-v2").getOrCreate()

    print("Loading datasets...")
    taxi = spark.read.parquet(TAXI_PATH)
    weather = spark.read.parquet(WEATHER_PATH)
    c311 = spark.read.parquet(C311_PATH)

    taxi = std_borough(taxi)
    c311 = std_borough(c311)

    # If 311 is not yet aggregated, aggregate it
    if "unique_key" in c311.columns:
        if "hour_ts" not in c311.columns and "created_ts" in c311.columns:
            c311 = c311.withColumn("hour_ts", F.date_trunc("hour", F.col("created_ts")))

        c311 = (
            c311
            .groupBy("hour_ts", "borough")
            .agg(
                F.count("*").alias("complaints_311"),
                F.sum(F.when(F.upper(F.col("complaint_type")).contains("NOISE"), 1).otherwise(0)).alias("complaints_noise"),
                F.sum(F.when(F.upper(F.col("complaint_type")).rlike("SANITATION|UNSANITARY|GARBAGE|RODENT|PEST"), 1).otherwise(0)).alias("complaints_sanitation"),
                F.sum(F.when(F.upper(F.col("complaint_type")).rlike("TRANSPORT|TRAFFIC|PARKING|STREET|VEHICLE"), 1).otherwise(0)).alias("complaints_transportation")
            )
        )
    else:
        # normalize common count column name
        if "complaints_311" not in c311.columns:
            for cand in ["complaints_total", "total_complaints", "complaints"]:
                if cand in c311.columns:
                    c311 = c311.withColumnRenamed(cand, "complaints_311")
                    break

    weather = rename_time_cols(weather, "weather")
    c311 = rename_time_cols(c311, "c311")

    master = (
        taxi
        .join(c311, ["hour_ts", "borough"], "left")
        .join(weather, ["hour_ts"], "left")
    )

    fill_map = {}
    for c in ["complaints_311", "complaints_noise", "complaints_sanitation", "complaints_transportation"]:
        if c in master.columns:
            fill_map[c] = 0
    if fill_map:
        master = master.fillna(fill_map)

    keep_cols = []

    # canonical time/location from taxi
    for c in ["hour_ts", "year", "month", "day", "hour", "borough"]:
        if c in master.columns:
            keep_cols.append(c)

    # taxi
    for c in ["trips", "avg_trip_duration_min", "avg_fare_amount", "avg_trip_distance",
              "avg_total_amount", "avg_tip_amount"]:
        if c in master.columns:
            keep_cols.append(c)

    # 311
    for c in ["complaints_311", "complaints_noise", "complaints_sanitation", "complaints_transportation"]:
        if c in master.columns:
            keep_cols.append(c)

    # weather v2
    for c in ["temp_avg", "dewpoint_avg", "humidity_avg", "wind_avg", "wind_gust_avg",
              "pressure_avg", "precip_total", "snow_total",
              "rain", "rain_code", "rain_text",
              "snow", "snow_code", "snow_text",
              "extreme_weather"]:
        if c in master.columns:
            keep_cols.append(c)

    master = master.select(*keep_cols)

    print("Writing:", OUT_PATH)
    (
        master
        .repartition("year", "month")
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUT_PATH)
    )

    print("\n===== MASTER V2 SCHEMA =====")
    master.printSchema()

    print("\n===== MASTER V2 SAMPLE ROWS =====")
    master.orderBy("hour_ts", "borough").show(20, truncate=False)

    print("\n===== QUICK COUNTS =====")
    print("Taxi rows   :", taxi.count())
    print("311 rows    :", c311.count())
    print("Weather rows:", weather.count())
    print("Master rows :", master.count())

    print("\n===== WEATHER FLAG COUNTS IN MASTER V2 =====")
    agg_exprs = []
    if "rain" in master.columns:
        agg_exprs.append(F.sum(F.when(F.col("rain") == 1, 1).otherwise(0)).alias("rain_rows"))
    if "snow" in master.columns:
        agg_exprs.append(F.sum(F.when(F.col("snow") == 1, 1).otherwise(0)).alias("snow_rows"))
    if "extreme_weather" in master.columns:
        agg_exprs.append(F.sum(F.when(F.col("extreme_weather") == 1, 1).otherwise(0)).alias("extreme_rows"))
    if agg_exprs:
        master.select(*agg_exprs).show()

    spark.stop()

if __name__ == "__main__":
    main()

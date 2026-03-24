import argparse
from pyspark.sql import SparkSession, functions as F

RAW_TAXI_DIR = "gs://msba405-nyc-project/raw/taxi"
OUT_PATH = "gs://msba405-nyc-project/Processed_v2/taxi_hourly_zone"

def main(year: int, month: int):
    spark = SparkSession.builder.appName(f"etl-taxi-hourly-zone-{year}-{month:02d}").getOrCreate()

    taxi_path = f"{RAW_TAXI_DIR}/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    df = spark.read.parquet(taxi_path)

    df = (df
          .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
          .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
          .withColumn("PULocationID", F.col("PULocationID").cast("int"))
          .withColumn("trip_distance", F.col("trip_distance").cast("double"))
          .withColumn("fare_amount", F.col("fare_amount").cast("double"))
          .withColumn("total_amount", F.col("total_amount").cast("double"))
          .withColumn("tip_amount", F.col("tip_amount").cast("double"))
          .withColumn("hour_ts", F.date_trunc("hour", F.col("pickup_ts")))
          .withColumn("trip_duration_min",
                      (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)
          )

    # Filter to correct month (removes weird/bad rows)
    start_ts = F.to_timestamp(F.lit(f"{year:04d}-{month:02d}-01 00:00:00"))
    if month == 12:
        end_ts = F.to_timestamp(F.lit(f"{year+1:04d}-01-01 00:00:00"))
    else:
        end_ts = F.to_timestamp(F.lit(f"{year:04d}-{month+1:02d}-01 00:00:00"))
    df = df.filter((F.col("pickup_ts") >= start_ts) & (F.col("pickup_ts") < end_ts))

    # Basic sanity filters
    df = df.filter(
        F.col("PULocationID").isNotNull() &
        (F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 240) &
        (F.col("trip_distance") >= 0) & (F.col("trip_distance") <= 200) &
        (F.col("fare_amount") >= 0) & (F.col("fare_amount") <= 1000) &
        (F.col("total_amount") >= 0) & (F.col("total_amount") <= 2000)
    )

    out = (df.groupBy("hour_ts", "PULocationID")
           .agg(
               F.count(F.lit(1)).alias("trips"),
               F.avg("trip_duration_min").alias("avg_trip_duration_min"),
               F.avg("fare_amount").alias("avg_fare_amount"),
               F.avg("trip_distance").alias("avg_trip_distance"),
               F.avg("total_amount").alias("avg_total_amount"),
               F.avg("tip_amount").alias("avg_tip_amount"),
           ))

    out = (out
           .withColumn("year",  F.year("hour_ts"))
           .withColumn("month", F.month("hour_ts"))
           .withColumn("day",   F.dayofmonth("hour_ts"))
           .withColumn("hour",  F.hour("hour_ts"))
           )

    out = out.select(
        "hour_ts", "year", "month", "day", "hour",
        "PULocationID",
        "trips",
        "avg_trip_duration_min",
        "avg_fare_amount",
        "avg_trip_distance",
        "avg_total_amount",
        "avg_tip_amount"
    )

    # IMPORTANT: use append (not overwrite) when doing many months
    (out.write
        .mode("append")
        .partitionBy("year", "month")
        .parquet(OUT_PATH))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    main(args.year, args.month)

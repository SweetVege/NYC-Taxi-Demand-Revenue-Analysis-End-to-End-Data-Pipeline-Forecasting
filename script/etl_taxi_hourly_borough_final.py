from pyspark.sql import SparkSession, functions as F

BUCKET = "gs://msba405-nyc-project"
IN_PATH = f"{BUCKET}/Processed_v2/taxi_hourly_zone/"
LOOKUP_PATH = f"{BUCKET}/raw/lookup/taxi_zone_lookup.csv"
OUT_PATH = f"{BUCKET}/Processed_v2/taxi_hourly_borough/"

def main():
    spark = (SparkSession.builder
             .appName("etl-taxi-hourly-borough")
             .getOrCreate())

    # -----------------------
    # Read inputs
    # -----------------------
    taxi = spark.read.parquet(IN_PATH)

    # Expected taxi schema:
    # hour_ts (timestamp), year, month, day, hour, PULocationID,
    # trips, avg_trip_duration_min, avg_fare_amount, avg_trip_distance, avg_total_amount, avg_tip_amount

    zones = (spark.read.option("header", True).csv(LOOKUP_PATH)
             .select(
                 F.col("LocationID").cast("int").alias("LocationID"),
                 F.upper(F.trim(F.col("Borough"))).alias("borough")
             ))

    taxi = taxi.withColumn("PULocationID", F.col("PULocationID").cast("int"))

    # -----------------------
    # Join PULocationID -> borough
    # -----------------------
    joined = (taxi.join(zones, taxi.PULocationID == zones.LocationID, "left")
                  .drop("LocationID"))

    # Optional cleanup: drop rows with unknown borough (you can change to .fillna("UNKNOWN"))
    joined = joined.filter(F.col("borough").isNotNull())

    # -----------------------
    # Correct aggregation to borough level
    # IMPORTANT: averages must be weighted by trips
    # -----------------------
    joined = joined.withColumn("trips_d", F.col("trips").cast("double"))

    # Convert existing per-zone averages back to totals, then aggregate, then divide
    joined = (joined
              .withColumn("dur_total", F.col("avg_trip_duration_min") * F.col("trips_d"))
              .withColumn("fare_total", F.col("avg_fare_amount") * F.col("trips_d"))
              .withColumn("dist_total", F.col("avg_trip_distance") * F.col("trips_d"))
              .withColumn("totalamt_total", F.col("avg_total_amount") * F.col("trips_d"))
              .withColumn("tip_total", F.col("avg_tip_amount") * F.col("trips_d"))
             )

    agg = (joined
           .groupBy("hour_ts", "year", "month", "day", "hour", "borough")
           .agg(
               F.sum("trips").alias("trips"),
               F.sum("dur_total").alias("dur_total"),
               F.sum("fare_total").alias("fare_total"),
               F.sum("dist_total").alias("dist_total"),
               F.sum("totalamt_total").alias("totalamt_total"),
               F.sum("tip_total").alias("tip_total")
           ))

    out = (agg
           .withColumn("avg_trip_duration_min", F.when(F.col("trips") > 0, F.col("dur_total") / F.col("trips")).otherwise(F.lit(None)))
           .withColumn("avg_fare_amount",       F.when(F.col("trips") > 0, F.col("fare_total") / F.col("trips")).otherwise(F.lit(None)))
           .withColumn("avg_trip_distance",     F.when(F.col("trips") > 0, F.col("dist_total") / F.col("trips")).otherwise(F.lit(None)))
           .withColumn("avg_total_amount",      F.when(F.col("trips") > 0, F.col("totalamt_total") / F.col("trips")).otherwise(F.lit(None)))
           .withColumn("avg_tip_amount",        F.when(F.col("trips") > 0, F.col("tip_total") / F.col("trips")).otherwise(F.lit(None)))
           .drop("dur_total", "fare_total", "dist_total", "totalamt_total", "tip_total")
          )

    # -----------------------
    # Write output (partitioned like your other processed tables)
    # -----------------------
    (out
     .repartition("year", "month")  # helps with partitioned writes
     .write.mode("overwrite")
     .partitionBy("year", "month")
     .parquet(OUT_PATH))

    # Quick sanity checks
    print("=== WROTE taxi_hourly_borough ===")
    out.orderBy("hour_ts", "borough").show(5, truncate=False)
    print("Distinct boroughs:", out.select("borough").distinct().count())
    spark.stop()

if __name__ == "__main__":
    main()

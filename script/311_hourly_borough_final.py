from pyspark.sql import SparkSession, functions as F

RAW_PATH = "gs://msba405-nyc-project/raw/311/*.csv"
OUT_PATH = "gs://msba405-nyc-project/Processed_v2/311_hourly_borough"

def main():
    spark = SparkSession.builder.appName("etl-311-hourly-borough").getOrCreate()

    # 1) Read raw 311 (schema inference is fine here since your inspect confirmed types)
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(RAW_PATH))

    # 2) Standardize borough, build hour bucket + time fields
    df = (df
          .withColumn("borough", F.upper(F.trim(F.col("borough"))))
          .filter(F.col("created_date").isNotNull())
          .withColumn("hour_ts", F.date_trunc("hour", F.col("created_date")))
          .withColumn("year",  F.year("hour_ts"))
          .withColumn("month", F.month("hour_ts"))
          .withColumn("day",   F.dayofmonth("hour_ts"))
          .withColumn("hour",  F.hour("hour_ts"))
          )

    # Optional: drop rows with missing/unknown borough
    # NYC often has blanks; decide your team policy.
    df = df.filter(F.col("borough").isNotNull() & (F.col("borough") != ""))

    # 3) Create broad complaint categories (edit keywords as you like)
    # Keep it simple and explainable in your write-up.
    complaint = F.coalesce(F.col("complaint_type"), F.lit(""))

    df = (df
          .withColumn("is_noise",
                      F.when(complaint.rlike("(?i)noise"), 1).otherwise(0))
          .withColumn("is_sanitation",
                      F.when(complaint.rlike("(?i)sanitation|unsanitary|garbage|rodent|pest"), 1).otherwise(0))
          .withColumn("is_transportation",
                      F.when(complaint.rlike("(?i)street|traffic|transport|vehicle|parking"), 1).otherwise(0))
          )

    # 4) Aggregate: hour_ts x borough
    out = (df
           .groupBy("hour_ts", "borough", "year", "month", "day", "hour")
           .agg(
               F.count("*").alias("complaints_total"),
               F.sum("is_noise").alias("complaints_noise"),
               F.sum("is_sanitation").alias("complaints_sanitation"),
               F.sum("is_transportation").alias("complaints_transportation"),
           ))

    # Optional: fill nulls (shouldn’t be needed after aggregation)
    out = out.fillna(0)

    # 5) Write partitioned parquet for fast downstream joins
    (out.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUT_PATH))

    print("WROTE:", OUT_PATH)
    out.orderBy("hour_ts", "borough").show(10, False)

    spark.stop()

if __name__ == "__main__":
    main()

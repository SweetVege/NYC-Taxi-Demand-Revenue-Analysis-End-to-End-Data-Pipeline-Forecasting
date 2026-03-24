from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    date_trunc,
    date_format,
    mean,
    count,
    min as spark_min,
    max as spark_max
)
from functools import reduce

spark = (
    SparkSession.builder
    .appName("clean_taxi_zone_debug_by_file")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

BUCKET = "msba405-nyc-project"
lookup_path = f"gs://{BUCKET}/raw/lookup/taxi_zone_lookup.csv"
out_path = f"gs://{BUCKET}/processed/taxi_hourly_zone/nyc_hourly_zone_final"

print("=== START JOB ===")
print("lookup_path:", lookup_path)
print("out_path:", out_path)

# Build the monthly file list explicitly
files = []

for y in [2023, 2024]:
    for m in range(1, 13):
        ym = f"{y}-{m:02d}"
        fp = f"gs://{BUCKET}/raw/taxi/yellow_tripdata_{ym}.parquet"
        files.append((ym, fp))

for m in range(1, 12):
    ym = f"2025-{m:02d}"
    fp = f"gs://{BUCKET}/raw/taxi/yellow_tripdata_{ym}.parquet"
    files.append((ym, fp))

monthly_aggs = []
debug_rows = []

for ym, fp in files:
    print(f"\n=== PROCESSING {ym} ===")
    print("file:", fp)

    try:
        df = spark.read.option("ignoreCorruptFiles", "true").parquet(fp)
    except Exception as e:
        print(f"SKIP {ym}: failed to read file -> {e}")
        continue

    # Keep required columns only
    required_cols = [
        "tpep_pickup_datetime",
        "PULocationID",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_distance"
    ]

    existing_cols = df.columns
    missing_cols = [c for c in required_cols if c not in existing_cols]

    if missing_cols:
        print(f"SKIP {ym}: missing columns -> {missing_cols}")
        continue

    df = df.select(
        col("tpep_pickup_datetime"),
        col("PULocationID").cast("int").alias("pulocationid"),
        col("fare_amount").cast("double"),
        col("tip_amount").cast("double"),
        col("total_amount").cast("double"),
        col("trip_distance").cast("double")
    )

    raw_count = df.count()
    print("raw rows:", raw_count)

    df = df.withColumn("pickup_ym", date_format(col("tpep_pickup_datetime"), "yyyy-MM"))

    month_df = df.filter(
        col("tpep_pickup_datetime").isNotNull() &
        (col("pickup_ym") == lit(ym))
    )

    month_count = month_df.count()
    print("rows after month filter:", month_count)

    null_ts_count = month_df.filter(col("tpep_pickup_datetime").isNull()).count()
    null_pu_count = month_df.filter(col("pulocationid").isNull()).count()

    print("null tpep_pickup_datetime rows:", null_ts_count)
    print("null pulocationid rows:", null_pu_count)

    month_df = month_df.withColumn("hour_ts", date_trunc("hour", col("tpep_pickup_datetime")))

    null_hour_count = month_df.filter(col("hour_ts").isNull()).count()
    print("null hour_ts rows:", null_hour_count)

    month_df = month_df.filter(
        col("hour_ts").isNotNull() &
        col("pulocationid").isNotNull()
    )

    after_null_count = month_df.count()
    print("rows after null filter:", after_null_count)

    if after_null_count == 0:
        print(f"SKIP {ym}: no valid rows after filtering")
        debug_rows.append((ym, raw_count, month_count, after_null_count, 0, None, None))
        continue

    time_range = month_df.select(
        spark_min("hour_ts").alias("min_hour_ts"),
        spark_max("hour_ts").alias("max_hour_ts")
    ).collect()[0]

    print("min hour_ts:", time_range["min_hour_ts"])
    print("max hour_ts:", time_range["max_hour_ts"])

    month_agg = (
        month_df.groupBy("hour_ts", "pulocationid")
        .agg(
            count("*").alias("trips"),
            mean("fare_amount").alias("avg_fare_amount"),
            mean("tip_amount").alias("avg_tip_amount"),
            mean("total_amount").alias("avg_total_amount"),
            mean("trip_distance").alias("avg_trip_distance")
        )
        .withColumn("ym", lit(ym))
    )

    agg_count = month_agg.count()
    print("hourly agg rows:", agg_count)

    debug_rows.append((
        ym,
        raw_count,
        month_count,
        after_null_count,
        agg_count,
        str(time_range["min_hour_ts"]),
        str(time_range["max_hour_ts"])
    ))

    monthly_aggs.append(month_agg)

print("\n=== MONTHLY DEBUG SUMMARY ===")
print("ym | raw_rows | after_month_filter | after_null_filter | hourly_agg_rows | min_hour_ts | max_hour_ts")
for r in debug_rows:
    print(" | ".join([str(x) for x in r]))

if not monthly_aggs:
    raise RuntimeError("No monthly aggregates were created.")

all_agg = reduce(lambda a, b: a.unionByName(b), monthly_aggs)

print("\n=== UNION DEBUG ===")
print("union rows:", all_agg.count())

union_time_range = all_agg.select(
    spark_min("hour_ts").alias("min_hour_ts"),
    spark_max("hour_ts").alias("max_hour_ts")
).collect()[0]

print("union min hour_ts:", union_time_range["min_hour_ts"])
print("union max hour_ts:", union_time_range["max_hour_ts"])

print("rows by ym after union:")
all_agg.groupBy("ym").count().orderBy("ym").show(100, False)

lookup = (
    spark.read.option("header", True).csv(lookup_path)
    .select(
        col("LocationID").cast("int").alias("pulocationid"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone").alias("service_zone")
    )
)

print("\n=== LOOKUP DEBUG ===")
print("lookup rows:", lookup.count())

final_df = (
    all_agg.join(lookup, on="pulocationid", how="left")
    .withColumnRenamed("pulocationid", "locationid")
    .select(
        "hour_ts",
        "locationid",
        "borough",
        "zone",
        "service_zone",
        "trips",
        "avg_fare_amount",
        "avg_tip_amount",
        "avg_total_amount",
        "avg_trip_distance",
        "ym"
    )
)

print("\n=== FINAL DF DEBUG ===")
print("final rows:", final_df.count())
print("final rows with null borough:", final_df.filter(col("borough").isNull()).count())
print("final rows with null zone:", final_df.filter(col("zone").isNull()).count())
print("final rows with null service_zone:", final_df.filter(col("service_zone").isNull()).count())

final_time_range = final_df.select(
    spark_min("hour_ts").alias("min_hour_ts"),
    spark_max("hour_ts").alias("max_hour_ts")
).collect()[0]

print("final min hour_ts:", final_time_range["min_hour_ts"])
print("final max hour_ts:", final_time_range["max_hour_ts"])

print("rows by ym in final_df:")
final_df.groupBy("ym").count().orderBy("ym").show(100, False)

print("sample final rows:")
final_df.show(10, False)

# Drop ym before writing if you do not want it in the final table
output_df = final_df.drop("ym")

output_df.write.mode("overwrite").parquet(out_path)

print("DONE:", out_path)
print("=== END JOB ===")
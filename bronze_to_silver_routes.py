from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_Routes")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------
# Read Bronze
# -------------------------

routes_df = spark.read.json(
    "data/bronze/routes.jsonl"
)

stops_df = spark.read.json(
    "data/bronze/stops.jsonl"
)


# -------------------------
# Silver Routes
# -------------------------

silver_routes = routes_df.select(
    col("route_id"),
    col("station_code"),
    to_date(
        col("date_YYYY_MM_DD"),
        "yyyy-MM-dd"
    ).alias("route_date"),
    col("departure_time_utc"),
    col("executor_capacity_cm3"),
    col("route_score")
)


# -------------------------
# Silver Stops
# -------------------------

silver_stops = stops_df.select(
    col("route_id"),
    col("stop_id"),
    col("lat"),
    col("lng"),
    col("stop_type"),
    col("zone_id")
)


# -------------------------
# Preview
# -------------------------

print("\n===== SILVER ROUTES =====")
silver_routes.show(5, truncate=False)

print("\n===== SILVER STOPS =====")
silver_stops.show(10, truncate=False)


# -------------------------
# Counts
# -------------------------

print("\n===== COUNTS =====")
print("Routes:", silver_routes.count())
print("Stops:", silver_stops.count())


# -------------------------
# Write Parquet
# -------------------------

silver_routes.write \
    .mode("overwrite") \
    .parquet("output/silver/routes")

silver_stops.write \
    .mode("overwrite") \
    .parquet("output/silver/stops")


print("\nSilver layer created successfully.")

spark.stop()

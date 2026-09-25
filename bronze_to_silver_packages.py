from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_Packages")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


INPUT_PATH = "data/bronze/packages.jsonl"
OUTPUT_PATH = "output/silver/packages"


packages_df = spark.read.json(INPUT_PATH)


silver_packages = packages_df.select(
    col("route_id"),
    col("stop_id"),
    col("package_id"),
    col("scan_status"),

    to_timestamp(
        col("time_window_start_utc")
    ).alias("time_window_start_utc"),

    to_timestamp(
        col("time_window_end_utc")
    ).alias("time_window_end_utc"),

    col("planned_service_time_seconds"),
    col("depth_cm"),
    col("height_cm"),
    col("width_cm"),
    col("package_volume_cm3")
)


print("\n===== SILVER PACKAGES =====")

silver_packages.show(
    10,
    truncate=False
)


print("\n===== PACKAGE COUNT =====")

package_count = silver_packages.count()

print("Packages:", package_count)


print("\nWriting packages...")

silver_packages.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("Packages written successfully.")
print("Silver packages saved to:", OUTPUT_PATH)


spark.stop()
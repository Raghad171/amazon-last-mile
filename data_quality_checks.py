from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_DataQuality")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 1. Load Silver Tables
# =========================================================

routes = spark.read.parquet("output/silver/routes")
stops = spark.read.parquet("output/silver/stops")
packages = spark.read.parquet("output/silver/packages")
sequences = spark.read.parquet("output/silver/actual_sequences")


print("\n==============================")
print("DATA QUALITY CHECKS")
print("==============================")


# =========================================================
# 2. Row Counts
# =========================================================

routes_count = routes.count()
stops_count = stops.count()
packages_count = packages.count()
sequences_count = sequences.count()

print("\n--- ROW COUNTS ---")
print("Routes:", routes_count)
print("Stops:", stops_count)
print("Packages:", packages_count)
print("Sequences:", sequences_count)


# =========================================================
# 3. Null Checks
# =========================================================

print("\n--- NULL CHECKS ---")

route_nulls = routes.filter(
    col("route_id").isNull()
).count()

stop_nulls = stops.filter(
    col("route_id").isNull() |
    col("stop_id").isNull()
).count()

package_nulls = packages.filter(
    col("route_id").isNull() |
    col("stop_id").isNull() |
    col("package_id").isNull()
).count()

sequence_nulls = sequences.filter(
    col("route_id").isNull() |
    col("stop_id").isNull() |
    col("actual_sequence").isNull()
).count()

print("Routes with null route_id:", route_nulls)
print("Stops with null keys:", stop_nulls)
print("Packages with null keys:", package_nulls)
print("Sequences with null keys:", sequence_nulls)


# =========================================================
# 4. Duplicate Checks
# =========================================================

print("\n--- DUPLICATE CHECKS ---")

duplicate_routes = (
    routes
    .groupBy("route_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_stops = (
    stops
    .groupBy("route_id", "stop_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_packages = (
    packages
    .groupBy("route_id", "stop_id", "package_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_sequences = (
    sequences
    .groupBy("route_id", "stop_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate routes:", duplicate_routes)
print("Duplicate stops:", duplicate_stops)
print("Duplicate packages:", duplicate_packages)
print("Duplicate sequences:", duplicate_sequences)


# =========================================================
# 5. Range / Business Rule Checks
# =========================================================

print("\n--- RANGE CHECKS ---")

invalid_lat = stops.filter(
    (col("lat") < -90) |
    (col("lat") > 90)
).count()

invalid_lng = stops.filter(
    (col("lng") < -180) |
    (col("lng") > 180)
).count()

invalid_sequence = sequences.filter(
    col("actual_sequence") < 0
).count()

invalid_service_time = packages.filter(
    col("planned_service_time_seconds") < 0
).count()

invalid_dimensions = packages.filter(
    (col("depth_cm") < 0) |
    (col("height_cm") < 0) |
    (col("width_cm") < 0)
).count()

print("Invalid latitude rows:", invalid_lat)
print("Invalid longitude rows:", invalid_lng)
print("Negative sequence values:", invalid_sequence)
print("Negative service times:", invalid_service_time)
print("Negative package dimensions:", invalid_dimensions)


# =========================================================
# 6. Referential Integrity Checks
# =========================================================

print("\n--- REFERENTIAL INTEGRITY ---")

# Stops without matching Route
orphan_stops = (
    stops.alias("s")
    .join(
        routes.select("route_id").alias("r"),
        col("s.route_id") == col("r.route_id"),
        "left_anti"
    )
    .count()
)

# Packages without matching Stop
orphan_packages = (
    packages.alias("p")
    .join(
        stops.select("route_id", "stop_id").alias("s"),
        (
            (col("p.route_id") == col("s.route_id")) &
            (col("p.stop_id") == col("s.stop_id"))
        ),
        "left_anti"
    )
    .count()
)

# Sequences without matching Stop
orphan_sequences = (
    sequences.alias("q")
    .join(
        stops.select("route_id", "stop_id").alias("s"),
        (
            (col("q.route_id") == col("s.route_id")) &
            (col("q.stop_id") == col("s.stop_id"))
        ),
        "left_anti"
    )
    .count()
)

print("Stops without matching route:", orphan_stops)
print("Packages without matching stop:", orphan_packages)
print("Sequences without matching stop:", orphan_sequences)


# =========================================================
# 7. Optional Content Checks
# =========================================================

print("\n--- CONTENT CHECKS ---")

missing_zone = stops.filter(
    col("zone_id").isNull()
).count()

station_rows = stops.filter(
    col("stop_type") == "Station"
).count()

dropoff_rows = stops.filter(
    col("stop_type") == "Dropoff"
).count()

packages_without_time_window = packages.filter(
    col("time_window_start_utc").isNull() &
    col("time_window_end_utc").isNull()
).count()

print("Stops with null zone_id:", missing_zone)
print("Station stops:", station_rows)
print("Dropoff stops:", dropoff_rows)
print("Packages without time window:", packages_without_time_window)


# =========================================================
# 8. Final Summary
# =========================================================

critical_issues = (
    route_nulls
    + stop_nulls
    + package_nulls
    + sequence_nulls
    + duplicate_routes
    + duplicate_stops
    + duplicate_packages
    + duplicate_sequences
    + invalid_lat
    + invalid_lng
    + invalid_sequence
    + invalid_service_time
    + invalid_dimensions
    + orphan_stops
    + orphan_packages
    + orphan_sequences
)

print("\n==============================")

if critical_issues == 0:
    print("DATA QUALITY STATUS: PASS")
else:
    print("DATA QUALITY STATUS: REVIEW REQUIRED")
    print("Total critical issues:", critical_issues)

print("==============================\n")


spark.stop()
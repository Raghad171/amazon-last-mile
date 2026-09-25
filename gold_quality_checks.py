from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_GoldQuality")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 1. Load Gold Tables
# =========================================================

route_gold = spark.read.parquet(
    "output/gold/route_performance"
)

station_gold = spark.read.parquet(
    "output/gold/station_performance"
)


print("\n==============================")
print("GOLD DATA QUALITY CHECKS")
print("==============================")


# =========================================================
# 2. Row Counts
# =========================================================

route_rows = route_gold.count()
station_rows = station_gold.count()

print("\n--- ROW COUNTS ---")
print("Route Gold Rows:", route_rows)
print("Station-Day Rows:", station_rows)


# =========================================================
# 3. Duplicate Checks
# =========================================================

print("\n--- DUPLICATE CHECKS ---")

duplicate_routes = (
    route_gold
    .groupBy("route_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_station_days = (
    station_gold
    .groupBy("route_date", "station_code")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate route_id:", duplicate_routes)
print(
    "Duplicate route_date + station_code:",
    duplicate_station_days
)


# =========================================================
# 4. Null Key Checks
# =========================================================

print("\n--- NULL KEY CHECKS ---")

route_key_nulls = route_gold.filter(
    col("route_id").isNull()
).count()

station_key_nulls = station_gold.filter(
    col("route_date").isNull() |
    col("station_code").isNull()
).count()

print("Route Gold null keys:", route_key_nulls)
print("Station Gold null keys:", station_key_nulls)


# =========================================================
# 5. KPI Range Checks
# =========================================================

print("\n--- KPI RANGE CHECKS ---")

invalid_route_success_rate = route_gold.filter(
    (col("delivery_success_rate") < 0) |
    (col("delivery_success_rate") > 100)
).count()

invalid_station_success_rate = station_gold.filter(
    (col("delivery_success_rate") < 0) |
    (col("delivery_success_rate") > 100)
).count()

invalid_capacity = route_gold.filter(
    col("capacity_utilization_pct") < 0
).count()

invalid_station_capacity = station_gold.filter(
    col("avg_capacity_utilization_pct") < 0
).count()

print(
    "Invalid route delivery success rates:",
    invalid_route_success_rate
)

print(
    "Invalid station delivery success rates:",
    invalid_station_success_rate
)

print(
    "Negative route capacity utilization:",
    invalid_capacity
)

print(
    "Negative station avg capacity utilization:",
    invalid_station_capacity
)


# =========================================================
# 6. Route Score Consistency
# =========================================================

print("\n--- ROUTE SCORE CONSISTENCY ---")

score_mismatch = station_gold.filter(
    (
        col("high_score_routes")
        + col("medium_score_routes")
        + col("low_score_routes")
    ) != col("total_routes")
).count()

print(
    "Rows where High + Medium + Low != Total Routes:",
    score_mismatch
)


# =========================================================
# 7. Aggregation Reconciliation
# =========================================================

print("\n--- AGGREGATION RECONCILIATION ---")

route_total_routes = route_gold.count()

station_total_routes = (
    station_gold
    .agg({"total_routes": "sum"})
    .collect()[0][0]
)

route_total_packages = (
    route_gold
    .agg({"total_packages": "sum"})
    .collect()[0][0]
)

station_total_packages = (
    station_gold
    .agg({"total_packages": "sum"})
    .collect()[0][0]
)

route_delivered = (
    route_gold
    .agg({"delivered_packages": "sum"})
    .collect()[0][0]
)

station_delivered = (
    station_gold
    .agg({"delivered_packages": "sum"})
    .collect()[0][0]
)

route_attempted = (
    route_gold
    .agg({"attempted_packages": "sum"})
    .collect()[0][0]
)

station_attempted = (
    station_gold
    .agg({"attempted_packages": "sum"})
    .collect()[0][0]
)


print(
    "Routes - Route Gold:",
    route_total_routes
)

print(
    "Routes - Station Gold Sum:",
    station_total_routes
)

print(
    "Packages - Route Gold:",
    route_total_packages
)

print(
    "Packages - Station Gold:",
    station_total_packages
)

print(
    "Delivered - Route Gold:",
    route_delivered
)

print(
    "Delivered - Station Gold:",
    station_delivered
)

print(
    "Attempted - Route Gold:",
    route_attempted
)

print(
    "Attempted - Station Gold:",
    station_attempted
)


# =========================================================
# 8. Reconciliation Differences
# =========================================================

route_diff = abs(
    route_total_routes - station_total_routes
)

package_diff = abs(
    route_total_packages - station_total_packages
)

delivered_diff = abs(
    route_delivered - station_delivered
)

attempted_diff = abs(
    route_attempted - station_attempted
)


print("\n--- RECONCILIATION DIFFERENCES ---")

print("Route Difference:", route_diff)
print("Package Difference:", package_diff)
print("Delivered Difference:", delivered_diff)
print("Attempted Difference:", attempted_diff)


# =========================================================
# 9. Final Status
# =========================================================

critical_issues = (
    duplicate_routes
    + duplicate_station_days
    + route_key_nulls
    + station_key_nulls
    + invalid_route_success_rate
    + invalid_station_success_rate
    + invalid_capacity
    + invalid_station_capacity
    + score_mismatch
    + route_diff
    + package_diff
    + delivered_diff
    + attempted_diff
)


print("\n==============================")

if critical_issues == 0:
    print("GOLD DATA QUALITY STATUS: PASS")
else:
    print("GOLD DATA QUALITY STATUS: REVIEW REQUIRED")
    print("Total Critical Issues:", critical_issues)

print("==============================\n")


spark.stop()
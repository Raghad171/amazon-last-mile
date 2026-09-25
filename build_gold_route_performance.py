from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as spark_sum,
    avg,
    when,
    round
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_Gold_RoutePerformance")
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


# =========================================================
# 2. Route-level Stop Metrics
# =========================================================

stop_metrics = (
    stops
    .groupBy("route_id")
    .agg(
        countDistinct("stop_id").alias("total_stops"),

        spark_sum(
            when(col("stop_type") == "Dropoff", 1).otherwise(0)
        ).alias("total_dropoff_stops"),

        countDistinct("zone_id").alias("total_zones")
    )
)


# =========================================================
# 3. Route-level Package Metrics
# =========================================================

package_metrics = (
    packages
    .groupBy("route_id")
    .agg(
        count("*").alias("total_packages"),

        spark_sum(
            when(col("scan_status") == "DELIVERED", 1).otherwise(0)
        ).alias("delivered_packages"),

        spark_sum(
            when(col("scan_status") == "DELIVERY_ATTEMPTED", 1).otherwise(0)
        ).alias("attempted_packages"),

        spark_sum(
            col("package_volume_cm3")
        ).alias("total_package_volume_cm3"),

        spark_sum(
            col("planned_service_time_seconds")
        ).alias("total_planned_service_time_seconds"),

        avg(
            col("planned_service_time_seconds")
        ).alias("avg_planned_service_time_seconds")
    )
)


# =========================================================
# 4. Route-level Sequence Metrics
# =========================================================

sequence_metrics = (
    sequences
    .groupBy("route_id")
    .agg(
        count("*").alias("sequence_stop_count")
    )
)


# =========================================================
# 5. Join Silver Routes with Aggregated Metrics
# =========================================================

gold_route_performance = (
    routes
    .join(stop_metrics, on="route_id", how="left")
    .join(package_metrics, on="route_id", how="left")
    .join(sequence_metrics, on="route_id", how="left")
)


# =========================================================
# 6. Derived KPIs
# =========================================================

gold_route_performance = (
    gold_route_performance

    .withColumn(
        "delivery_success_rate",
        when(
            col("total_packages") > 0,
            round(
                col("delivered_packages") / col("total_packages") * 100,
                2
            )
        ).otherwise(None)
    )

    .withColumn(
        "capacity_utilization_pct",
        when(
            col("executor_capacity_cm3") > 0,
            round(
                col("total_package_volume_cm3")
                / col("executor_capacity_cm3")
                * 100,
                2
            )
        ).otherwise(None)
    )

    .withColumn(
        "packages_per_dropoff",
        when(
            col("total_dropoff_stops") > 0,
            round(
                col("total_packages") / col("total_dropoff_stops"),
                2
            )
        ).otherwise(None)
    )

    .withColumn(
        "avg_planned_service_time_seconds",
        round(
            col("avg_planned_service_time_seconds"),
            2
        )
    )
)


# =========================================================
# 7. Select Final Gold Columns
# =========================================================

gold_route_performance = gold_route_performance.select(
    "route_id",
    "route_date",
    "station_code",
    "route_score",
    "executor_capacity_cm3",

    "total_stops",
    "total_dropoff_stops",
    "total_zones",

    "total_packages",
    "delivered_packages",
    "attempted_packages",

    "delivery_success_rate",

    "total_package_volume_cm3",
    "capacity_utilization_pct",

    "total_planned_service_time_seconds",
    "avg_planned_service_time_seconds",

    "packages_per_dropoff",

    "sequence_stop_count"
)


# =========================================================
# 8. Preview
# =========================================================

print("\n===== GOLD ROUTE PERFORMANCE =====")

gold_route_performance.show(
    10,
    truncate=False
)


print("\n===== GOLD COUNT =====")

print(
    "Gold Route Rows:",
    gold_route_performance.count()
)


# =========================================================
# 9. Write Gold Parquet
# =========================================================

OUTPUT_PATH = "output/gold/route_performance"

print("\nWriting Gold Route Performance...")

gold_route_performance.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("Gold Route Performance written successfully.")
print("Output:", OUTPUT_PATH)


spark.stop()
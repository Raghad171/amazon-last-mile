from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    when,
    round
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_Gold_StationPerformance")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 1. Load Gold Route Performance
# =========================================================

routes_gold = spark.read.parquet(
    "output/gold/route_performance"
)


# =========================================================
# 2. Aggregate to Station + Date Level
# =========================================================

station_performance = (
    routes_gold
    .groupBy(
        "station_code",
        "route_date"
    )
    .agg(

        count("*").alias("total_routes"),

        spark_sum(
            col("total_stops")
        ).alias("total_stops"),

        spark_sum(
            col("total_dropoff_stops")
        ).alias("total_dropoff_stops"),

        spark_sum(
            col("total_packages")
        ).alias("total_packages"),

        spark_sum(
            col("delivered_packages")
        ).alias("delivered_packages"),

        spark_sum(
            col("attempted_packages")
        ).alias("attempted_packages"),

        avg(
            col("capacity_utilization_pct")
        ).alias("avg_capacity_utilization_pct"),

        avg(
            col("packages_per_dropoff")
        ).alias("avg_packages_per_dropoff"),

        avg(
            col("total_packages")
        ).alias("avg_packages_per_route"),

        avg(
            col("total_stops")
        ).alias("avg_stops_per_route"),

        spark_sum(
            when(
                col("route_score") == "High",
                1
            ).otherwise(0)
        ).alias("high_score_routes"),

        spark_sum(
            when(
                col("route_score") == "Medium",
                1
            ).otherwise(0)
        ).alias("medium_score_routes"),

        spark_sum(
            when(
                col("route_score") == "Low",
                1
            ).otherwise(0)
        ).alias("low_score_routes")
    )
)


# =========================================================
# 3. Derived KPIs
# =========================================================

station_performance = (
    station_performance

    .withColumn(
        "delivery_success_rate",
        when(
            col("total_packages") > 0,
            round(
                col("delivered_packages")
                / col("total_packages")
                * 100,
                2
            )
        ).otherwise(None)
    )

    .withColumn(
        "avg_capacity_utilization_pct",
        round(
            col("avg_capacity_utilization_pct"),
            2
        )
    )

    .withColumn(
        "avg_packages_per_dropoff",
        round(
            col("avg_packages_per_dropoff"),
            2
        )
    )

    .withColumn(
        "avg_packages_per_route",
        round(
            col("avg_packages_per_route"),
            2
        )
    )

    .withColumn(
        "avg_stops_per_route",
        round(
            col("avg_stops_per_route"),
            2
        )
    )
)


# =========================================================
# 4. Final Column Order
# =========================================================

station_performance = station_performance.select(
    "route_date",
    "station_code",

    "total_routes",

    "total_stops",
    "total_dropoff_stops",

    "total_packages",
    "delivered_packages",
    "attempted_packages",

    "delivery_success_rate",

    "avg_capacity_utilization_pct",
    "avg_packages_per_dropoff",
    "avg_packages_per_route",
    "avg_stops_per_route",

    "high_score_routes",
    "medium_score_routes",
    "low_score_routes"
)


# =========================================================
# 5. Preview
# =========================================================

print("\n===== GOLD STATION PERFORMANCE =====")

station_performance.orderBy(
    "route_date",
    "station_code"
).show(
    20,
    truncate=False
)


print("\n===== GOLD STATION COUNT =====")

print(
    "Station-Day Rows:",
    station_performance.count()
)


# =========================================================
# 6. Write Gold Parquet
# =========================================================

OUTPUT_PATH = "output/gold/station_performance"

print("\nWriting Gold Station Performance...")

station_performance.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("Gold Station Performance written successfully.")
print("Output:", OUTPUT_PATH)


spark.stop()
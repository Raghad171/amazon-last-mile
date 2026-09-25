from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("AmazonLastMile_Sequences")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "data/bronze/actual_sequences.jsonl"
OUTPUT_PATH = "output/silver/actual_sequences"

df = spark.read.json(INPUT_PATH)

silver_sequences = df.select(
    col("route_id"),
    col("stop_id"),
    col("actual_sequence")
)

print("\n===== SILVER ACTUAL SEQUENCES =====")
silver_sequences.show(10, truncate=False)

print("\n===== COUNT =====")
print("Sequence rows:", silver_sequences.count())

silver_sequences.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("\nActual sequences written successfully.")

spark.stop()
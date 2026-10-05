"""
Quick inspection of the Bronze Delta table.
"""
import os

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/kraken_trades")

builder = (
    SparkSession.builder
    .appName("inspect-bronze")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total rows in Bronze: {df.count()}")
print("\n--- Schema ---")
df.printSchema()
print("\n--- Sample rows (3) ---")
df.select("kafka_ts", "ingested_at", "channel", "type").show(3, truncate=False)
spark.stop()

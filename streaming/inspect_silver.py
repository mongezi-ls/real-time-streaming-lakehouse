"""Inspect the Silver Delta table."""
import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
SILVER_PATH = os.getenv("SILVER_PATH", "data/silver/kraken_trades")

builder = (
    SparkSession.builder
    .appName("inspect-silver")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(SILVER_PATH)

print(f"Total rows in Silver: {df.count()}")
print(f"Unique trade_ids:     {df.select('trade_id').distinct().count()}")
print(f"Unique symbols:       {df.select('symbol').distinct().count()}")

print("\n--- Schema ---")
df.printSchema()

print("\n--- Sample rows (5) ---")
df.select(
    "trade_id", "symbol", "side", "price", "qty", "event_ts"
).orderBy("event_ts", ascending=False).show(5, truncate=False)

print("\n--- Trades per symbol ---")
df.groupBy("symbol").count().orderBy("symbol").show()

print("\n--- Delta history (last 5) ---")
DeltaTable.forPath(spark, SILVER_PATH).history(5).select(
    "version", "timestamp", "operation"
).show(truncate=False)

spark.stop()
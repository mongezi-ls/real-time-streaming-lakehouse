"""Show Delta's time travel + history using the Python API."""
import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/kraken_trades")

builder = (
    SparkSession.builder
    .appName("delta-features")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("--- Delta table history (last 10 versions) ---")
dt = DeltaTable.forPath(spark, BRONZE_PATH)
dt.history(10).select("version", "timestamp", "operation").show(truncate=False)

print("\n--- Latest row count ---")
df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total rows: {df.count()}")

spark.stop()
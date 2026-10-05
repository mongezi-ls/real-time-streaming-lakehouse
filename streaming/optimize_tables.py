"""One-time script to compact Delta tables locally before uploading to Databricks."""
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("optimize-tables")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tables = [
    "data/silver/kraken_trades",
    "data/gold/revenue_per_minute",
    "data/gold/symbol_metrics",
]

for path in tables:
    print(f"Optimizing {path} ...")
    dt = DeltaTable.forPath(spark, path)
    dt.optimize().executeCompaction()
    print(f"  done: {path}")

print("\nAll tables compacted.")
spark.stop()

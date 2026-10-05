"""Inspect the Gold Delta tables."""
import os
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
GOLD_BASE = "data/gold"

builder = (
    SparkSession.builder
    .appName("inspect-gold")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------- revenue_per_minute ----------
print("=" * 60)
print("GOLD: revenue_per_minute — latest 10 minutes")
print("=" * 60)
rpm = spark.read.format("delta").load(f"{GOLD_BASE}/revenue_per_minute")
rpm.orderBy(rpm["minute"].desc()).show(10, truncate=False)

# ---------- symbol_metrics ----------
print("=" * 60)
print("GOLD: symbol_metrics")
print("=" * 60)
sm = spark.read.format("delta").load(f"{GOLD_BASE}/symbol_metrics")
sm.show(truncate=False)

# ---------- top_trades ----------
print("=" * 60)
print("GOLD: top_trades — top 10 by notional value")
print("=" * 60)
tt = spark.read.format("delta").load(f"{GOLD_BASE}/top_trades")
tt.orderBy(tt["notional"].desc()).show(10, truncate=False)

spark.stop()
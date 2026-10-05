"""
Gold layer: Silver Delta -> business aggregates as Delta tables.

Runs as a batch job: reads the entire Silver table, computes business
metrics, overwrites the Gold tables atomically, and exits.


Usage:
    python streaming/gold_stream.py
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    date_trunc,
    lit,
    sum as spark_sum,
    when,
    window,
)
from pyspark.sql.types import DoubleType

# ---------- Config ----------

load_dotenv()

SILVER_PATH = os.getenv("SILVER_PATH", "data/silver/kraken_trades")
GOLD_BASE = "data/gold"

Path(GOLD_BASE).mkdir(parents=True, exist_ok=True)

# ---------- Spark session ----------

builder = (
    SparkSession.builder
    .appName("gold-kraken-trades")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark {spark.version} started")
print(f"Reading Silver from {SILVER_PATH}")

# ---------- Read Silver ----------

silver = spark.read.format("delta").load(SILVER_PATH)

row_count = silver.count()
print(f"Silver row count: {row_count}")

if row_count == 0:
    print("Silver is empty. No Gold tables produced. Exiting.")
    spark.stop()
    raise SystemExit(0)

# Add notional value (price × qty) — used by several aggregates
enriched = silver.withColumn("notional", col("price") * col("qty").cast(DoubleType()))

# ============================================================
# GOLD TABLE 1: revenue_per_minute
# ============================================================
# Per-symbol, per-minute: total notional volume, trade count, avg price
revenue_per_minute = (
    enriched
    .withColumn("minute", date_trunc("minute", col("event_ts")))
    .groupBy("symbol", "minute")
    .agg(
        spark_sum("notional").alias("notional_volume"),
        count("*").alias("trade_count"),
        spark_sum("qty").alias("total_qty"),
    )
    .withColumn("gold_processed_at", current_timestamp())
    .orderBy("minute", "symbol")
)

print(f"revenue_per_minute: {revenue_per_minute.count()} rows")
(
    revenue_per_minute.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_BASE}/revenue_per_minute")
)

# ============================================================
# GOLD TABLE 2: symbol_metrics
# ============================================================
# Per-symbol summary: VWAP, trade count, buy/sell ratio
symbol_metrics = (
    enriched
    .groupBy("symbol")
    .agg(
        count("*").alias("trade_count"),
        spark_sum("notional").alias("notional_volume"),
        spark_sum("qty").alias("total_qty"),
        # VWAP = sum(price * qty) / sum(qty)
        (spark_sum("notional") / spark_sum("qty")).alias("vwap"),
        # Buy vs sell counts
        spark_sum(when(col("side") == "buy", 1).otherwise(0)).alias("buy_count"),
        spark_sum(when(col("side") == "sell", 1).otherwise(0)).alias("sell_count"),
    )
    .withColumn(
        "buy_sell_ratio",
        when(col("sell_count") == 0, lit(None))
        .otherwise(col("buy_count") / col("sell_count")),
    )
    .withColumn("gold_processed_at", current_timestamp())
    .orderBy("symbol")
)

print(f"symbol_metrics: {symbol_metrics.count()} rows")
(
    symbol_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_BASE}/symbol_metrics")
)

# ============================================================
# GOLD TABLE 3: top_trades
# ============================================================
# The 100 largest trades by notional value (across all data)
top_trades = (
    enriched
    .orderBy(col("notional").desc())
    .limit(100)
    .select(
        "trade_id",
        "symbol",
        "side",
        "price",
        "qty",
        "notional",
        "event_ts",
    )
    .withColumn("gold_processed_at", current_timestamp())
)

print(f"top_trades: {top_trades.count()} rows")
(
    top_trades.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_BASE}/top_trades")
)

print("\nGold tables written to data/gold/")
print("  - revenue_per_minute")
print("  - symbol_metrics")
print("  - top_trades")

spark.stop()
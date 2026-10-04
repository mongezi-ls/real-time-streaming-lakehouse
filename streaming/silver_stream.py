"""
Silver layer: Bronze Delta -> cleaned, typed, deduped Silver Delta.

Reads the Bronze table (streaming), explodes nested trades, casts types,
deduplicates by trade_id using a watermark, and upserts into Silver.

Usage:
    python streaming/silver_stream.py
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------- Config ----------

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/kraken_trades")
SILVER_PATH = os.getenv("SILVER_PATH", "data/silver/kraken_trades")
CHECKPOINT_PATH = "data/_checkpoints/silver_kraken"
WATERMARK_DELAY = "2 minutes"

Path("data/silver").mkdir(parents=True, exist_ok=True)
Path("data/_checkpoints").mkdir(parents=True, exist_ok=True)

# ---------- Spark session ----------

builder = (
    SparkSession.builder
    .appName("silver-kraken-trades")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.ui.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark {spark.version} started")
print(f"Reading Bronze from {BRONZE_PATH}")
print(f"Writing Silver to  {SILVER_PATH}")
print(f"Checkpoint at     {CHECKPOINT_PATH}")
print(f"Watermark delay:  {WATERMARK_DELAY}")

# ---------- Read Bronze as a stream ----------

bronze = spark.readStream.format("delta").load(BRONZE_PATH)

# ---------- Flatten & conform ----------

silver_typed = (
    bronze
    # One row per trade in the nested data array
    .withColumn("trade", explode(col("data")))
    # Extract and cast fields
    .select(
        col("trade.trade_id").cast(LongType()).alias("trade_id"),
        col("trade.symbol").cast(StringType()).alias("symbol"),
        col("trade.price").cast(DoubleType()).alias("price"),
        col("trade.qty").cast(DoubleType()).alias("qty"),
        col("trade.ord_type").cast(StringType()).alias("ord_type"),
        col("trade.side").cast(StringType()).alias("side"),
        to_timestamp(col("trade.timestamp")).alias("event_ts"),
        col("kafka_ts").cast(TimestampType()).alias("kafka_ts"),
        col("ingested_at").cast(TimestampType()).alias("ingested_at"),
    )
    # Filter out malformed rows (null trade_id or price)
    .filter(col("trade_id").isNotNull() & col("price").isNotNull())
    # Watermark on event time: allow up to N minutes of late data
    .withWatermark("event_ts", WATERMARK_DELAY)
    # Partition column for efficient reads
    .withColumn("event_date", to_date(col("event_ts")))
    # Processing timestamp
    .withColumn("silver_processed_at", current_timestamp())
)

# ---------- Write to Silver (idempotent MERGE per micro-batch) ----------

def upsert_to_silver(batch_df, batch_id):
    """
    Called once per micro-batch. Uses Delta MERGE to upsert by trade_id so
    restarts and duplicate events are handled idempotently.
    """
    if batch_df.isEmpty():
        return

    # Dedupe within the batch by trade_id, keeping the latest ingested_at
    deduped = (
        batch_df
        .dropDuplicates(["trade_id"])
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(
                deduped.alias("s"),
                "t.trade_id = s.trade_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run: create the table
        deduped.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print(f"Batch {batch_id}: merged {deduped.count()} trades into Silver")


query = (
    silver_typed.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="20 seconds")
    .start()
)

print(f"Streaming query started: {query.id}")
print("Waiting for events... (Ctrl+C to stop)")

query.awaitTermination()
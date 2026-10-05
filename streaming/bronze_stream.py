"""
Bronze layer: Kafka (kraken.trades) -> Delta Lake (data/bronze/kraken_trades).
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------- Config ----------

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_KRAKEN", "kraken.trades")
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/kraken_trades")
CHECKPOINT_PATH = "data/_checkpoints/bronze_kraken"

Path("data/bronze").mkdir(parents=True, exist_ok=True)
Path("data/_checkpoints").mkdir(parents=True, exist_ok=True)

# ---------- Spark session ----------

builder = (
    SparkSession.builder
    .appName("bronze-kraken-trades")
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

# KEY: pass the Kafka connector via extra_packages so the Delta helper
# doesn't clobber it.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[KAFKA_PACKAGE],
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark {spark.version} started")
print(f"Reading from Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP}")
print(f"Writing to Delta table at {BRONZE_PATH}")
print(f"Checkpoint at {CHECKPOINT_PATH}")

# ---------- Kafka source ----------

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

raw_str = raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")

# ---------- Parse Kraken v2 trade schema ----------

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("qty", DoubleType()),
    StructField("ord_type", StringType()),
    StructField("side", StringType()),
    StructField("trade_id", LongType()),
    StructField("timestamp", StringType()),
])

event_schema = StructType([
    StructField("channel", StringType()),
    StructField("type", StringType()),
    StructField("data", ArrayType(trade_schema)),
])

parsed = (
    raw_str
    .withColumn("event", from_json(col("json_str"), event_schema))
    .withColumn("ingested_at", current_timestamp())
    .select(
        "json_str",
        "kafka_ts",
        "ingested_at",
        "event.channel",
        "event.type",
        "event.data",
    )
)

# ---------- Write to Delta Bronze ----------

query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .start(BRONZE_PATH)
)

print(f"Streaming query started: {query.id}")
print("Waiting for events... (Ctrl+C to stop)")

query.awaitTermination()

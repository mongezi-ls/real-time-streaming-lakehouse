"""
Quick Kafka consumer to verify the producer is publishing events.

Usage:
    python ingestion/consumer_check.py

Press Ctrl+C to stop.
"""

import json
import os
import sys
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_KRAKEN", "kraken.trades")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "kraken-consumer-check",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([KAFKA_TOPIC])
print(f"Subscribed to {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP}. Ctrl+C to stop.\n")

try:
    count = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}", file=sys.stderr)
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
            trades = event.get("data", [])
            for trade in trades:
                sym = trade.get("symbol", "?")
                price = trade.get("price", "?")
                qty = trade.get("qty", "?")
                print(f"[{count:5d}] {sym:8s}  price={price}  qty={qty}")
                count += 1
        except Exception as e:
            print(f"Parse error: {e}", file=sys.stderr)

except KeyboardInterrupt:
    print(f"\nStopped after {count} trades.")
finally:
    consumer.close()
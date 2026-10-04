"""
Kraken WebSocket → Kafka producer.

Subscribes to the public Kraken v2 trade channel and publishes every trade
event to the `kraken.trades` Kafka topic as JSON.

Usage:
    python ingestion/kraken_producer.py

Environment variables (from .env):
    KAFKA_BOOTSTRAP_SERVERS  e.g. localhost:29092
    KAFKA_TOPIC_KRAKEN       e.g. kraken.trades
    KRAKEN_WS_URL            e.g. wss://ws.kraken.com/v2
    KRAKEN_SYMBOLS           e.g. BTC/USD,ETH/USD,SOL/USD
"""

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------- Config ----------

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_KRAKEN", "kraken.trades")
KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")
KRAKEN_SYMBOLS = os.getenv("KRAKEN_SYMBOLS", "BTC/USD,ETH/USD,SOL/USD").split(",")

# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kraken-producer")

# ---------- Kafka ----------

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "kraken-producer",
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 20,
})


def delivery_report(err, msg) -> None:
    """Called once per message by confluent-kafka."""
    if err is not None:
        log.error(f"Delivery failed: {err}")
    # Uncomment for per-message debugging (very noisy):
    # else:
    #     log.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def on_shutdown(signum, frame) -> None:
    log.info("Shutdown signal received, flushing producer...")
    producer.flush(timeout=10)
    log.info("Producer flushed, exiting.")
    sys.exit(0)


signal.signal(signal.SIGINT, on_shutdown)
signal.signal(signal.SIGTERM, on_shutdown)


# ---------- Kraken ----------

def subscribe_message(symbols: list[str]) -> str:
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": symbols,
            "snapshot": False,
        },
    })


def is_trade_event(event: dict[str, Any]) -> bool:
    """Kraken v2 sends several message types; keep only trade updates."""
    return event.get("channel") == "trade" and event.get("type") in ("update", "snapshot")


async def stream() -> None:
    log.info(f"Connecting to {KRAKEN_WS_URL}")
    log.info(f"Symbols: {KRAKEN_SYMBOLS}")
    log.info(f"Publishing to Kafka topic: {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}")

    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(
                KRAKEN_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:
                sub = subscribe_message(KRAKEN_SYMBOLS)
                await ws.send(sub)
                log.info("Subscribed. Waiting for trade events...")

                count = 0
                async for raw in ws:
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning(f"Skipping non-JSON message: {raw[:120]}")
                        continue

                    if not is_trade_event(event):
                        # subscription confirmations, heartbeats, status messages
                        continue

                    # Publish the entire trade event (with channel metadata) to Kafka.
                    payload = json.dumps(event).encode("utf-8")
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=payload,
                        callback=delivery_report,
                    )
                    # Serve delivery callbacks without blocking
                    producer.poll(0)

                    count += 1
                    if count % 100 == 0:
                        log.info(f"Published {count} trade events")

        except websockets.ConnectionClosed as e:
            log.warning(f"WebSocket closed ({e}); reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Unexpected error: {e}; reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(stream())
    except KeyboardInterrupt:
        on_shutdown(None, None)
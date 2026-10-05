"""
Unit tests for ingestion/kraken_producer.py

Tests pure logic: JSON parsing, event filtering, subscription message format.
Does NOT connect to Kraken or Kafka — those are integration concerns.
"""
import json
import sys
from pathlib import Path

# Make ingestion/ importable
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from kraken_producer import is_trade_event, subscribe_message  # noqa: E402


class TestIsTradeEvent:
    def test_trade_update_is_recognized(self, sample_kraken_trade_event):
        assert is_trade_event(sample_kraken_trade_event) is True

    def test_trade_snapshot_is_recognized(self, sample_kraken_trade_event):
        sample_kraken_trade_event["type"] = "snapshot"
        assert is_trade_event(sample_kraken_trade_event) is True

    def test_subscription_confirmation_is_rejected(self):
        event = {"method": "subscribe", "result": {"channel": "trade"}}
        assert is_trade_event(event) is False

    def test_heartbeat_is_rejected(self):
        event = {"channel": "heartbeat"}
        assert is_trade_event(event) is False

    def test_unknown_channel_is_rejected(self):
        event = {"channel": "book", "type": "update", "data": []}
        assert is_trade_event(event) is False

    def test_empty_event_is_rejected(self):
        assert is_trade_event({}) is False


class TestSubscribeMessage:
    def test_message_is_valid_json(self):
        msg = subscribe_message(["BTC/USD"])
        parsed = json.loads(msg)
        assert isinstance(parsed, dict)

    def test_message_has_correct_structure(self):
        symbols = ["BTC/USD", "ETH/USD", "SOL/USD"]
        parsed = json.loads(subscribe_message(symbols))

        assert parsed["method"] == "subscribe"
        assert parsed["params"]["channel"] == "trade"
        assert parsed["params"]["symbol"] == symbols
        assert parsed["params"]["snapshot"] is False

    def test_message_preserves_symbol_order(self):
        symbols = ["SOL/USD", "BTC/USD", "ETH/USD"]
        parsed = json.loads(subscribe_message(symbols))
        assert parsed["params"]["symbol"] == symbols

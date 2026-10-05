"""
Shared pytest fixtures for all tests.
"""
import json

import pytest


@pytest.fixture
def sample_kraken_trade_event() -> dict:
    """A realistic single-event Kraken v2 trade message."""
    return {
        "channel": "trade",
        "type": "update",
        "data": [
            {
                "symbol": "BTC/USD",
                "price": 85312.50,
                "qty": 0.00123456,
                "ord_type": "limit",
                "side": "buy",
                "trade_id": 110137670,
                "timestamp": "2026-10-05T12:34:56.789012Z",
            }
        ],
    }


@pytest.fixture
def sample_kraken_trade_event_json(sample_kraken_trade_event) -> bytes:
    """The same event, as raw JSON bytes (what Kafka receives)."""
    return json.dumps(sample_kraken_trade_event).encode("utf-8")


@pytest.fixture
def sample_batch_of_trades() -> list:
    """A batch of trade events to test aggregation."""
    return [
        {"symbol": "BTC/USD", "price": 85000.00, "qty": 0.001, "side": "buy",  "trade_id": 1},
        {"symbol": "BTC/USD", "price": 85100.00, "qty": 0.002, "side": "sell", "trade_id": 2},
        {"symbol": "BTC/USD", "price": 85200.00, "qty": 0.0015, "side": "buy", "trade_id": 3},
        {"symbol": "ETH/USD", "price": 2700.00,  "qty": 0.5,   "side": "buy",  "trade_id": 4},
        {"symbol": "ETH/USD", "price": 2705.00,  "qty": 1.0,   "side": "sell", "trade_id": 5},
    ]

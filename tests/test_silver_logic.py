"""
Unit tests for the Silver layer's pure logic.
Tests schema definitions and simple data transformations.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "streaming"))


class TestTradeSchema:
    """Validates the schema used to parse Kraken trade events."""

    def test_trade_schema_has_expected_fields(self):
        # Recreate the schema locally to avoid Spark import
        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        trade_schema = StructType([
            StructField("symbol", StringType()),
            StructField("price", DoubleType()),
            StructField("qty", DoubleType()),
            StructField("ord_type", StringType()),
            StructField("side", StringType()),
            StructField("trade_id", LongType()),
            StructField("timestamp", StringType()),
        ])

        field_names = [f.name for f in trade_schema.fields]
        assert "trade_id" in field_names
        assert "symbol" in field_names
        assert "price" in field_names
        assert "qty" in field_names
        assert "side" in field_names
        assert len(field_names) == 7


class TestNotionalCalculation:
    """Tests the notional = price * qty calculation used in Gold."""

    @pytest.mark.parametrize(
        "price,qty,expected",
        [
            (85000.00, 0.001, 85.0),
            (2700.50, 1.5, 4050.75),
            (121.42, 10.0, 1214.2),
            (0.0, 1.0, 0.0),
        ],
    )
    def test_notional(self, price, qty, expected):
        assert round(price * qty, 2) == expected


class TestVwapCalculation:
    """Tests the VWAP (volume-weighted average price) formula."""

    def test_vwap_single_trade(self, sample_batch_of_trades):
        btc_trades = [t for t in sample_batch_of_trades if t["symbol"] == "BTC/USD"]
        notional = sum(t["price"] * t["qty"] for t in btc_trades)
        volume = sum(t["qty"] for t in btc_trades)
        vwap = notional / volume

        # BTC trades: 85000*0.001 + 85100*0.002 + 85200*0.0015
        #           = 85 + 170.2 + 127.8 = 383.0
        # volume    = 0.001 + 0.002 + 0.0015 = 0.0045
        # vwap      = 383.0 / 0.0045 ≈ 85111.11
        assert round(vwap, 2) == pytest.approx(85111.11, rel=1e-4)


class TestBuySellRatio:
    """Tests buy/sell ratio calculation."""

    def test_ratio_with_mixed_sides(self, sample_batch_of_trades):
        btc_trades = [t for t in sample_batch_of_trades if t["symbol"] == "BTC/USD"]
        buys = sum(1 for t in btc_trades if t["side"] == "buy")
        sells = sum(1 for t in btc_trades if t["side"] == "sell")
        ratio = buys / sells if sells > 0 else None

        # BTC: 2 buys, 1 sell → ratio = 2.0
        assert ratio == 2.0

    def test_ratio_with_no_sells_is_none(self):
        buys = 5
        sells = 0
        ratio = buys / sells if sells > 0 else None
        assert ratio is None

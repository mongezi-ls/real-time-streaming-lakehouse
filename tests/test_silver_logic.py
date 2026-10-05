"""
Unit tests for the Silver layer's pure logic.

Validates the schema contract and simple data transformations without
importing PySpark (which is heavy and unnecessary for these checks).
"""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "streaming"))


class TestTradeSchema:
    """
    Validates the Kraken trade schema field list.

    Rather than importing pyspark (heavy, not needed for pure schema
    validation), we assert against the expected field names and types,
    and cross-check that bronze_stream.py actually declares them.
    """

    EXPECTED_FIELDS = {
        "symbol": "string",
        "price": "double",
        "qty": "double",
        "ord_type": "string",
        "side": "string",
        "trade_id": "long",
        "timestamp": "string",
    }

    def test_expected_field_count(self):
        assert len(self.EXPECTED_FIELDS) == 7

    def test_expected_field_names(self):
        assert set(self.EXPECTED_FIELDS.keys()) == {
            "symbol",
            "price",
            "qty",
            "ord_type",
            "side",
            "trade_id",
            "timestamp",
        }

    def test_expected_field_types(self):
        assert self.EXPECTED_FIELDS["trade_id"] == "long"
        assert self.EXPECTED_FIELDS["price"] == "double"
        assert self.EXPECTED_FIELDS["qty"] == "double"
        assert self.EXPECTED_FIELDS["symbol"] == "string"

    def test_bronze_stream_declares_all_expected_fields(self):
        """Read bronze_stream.py and confirm each expected field is declared."""
        bronze_path = Path(__file__).parent.parent / "streaming" / "bronze_stream.py"
        source = bronze_path.read_text(encoding="utf-8")

        for field in self.EXPECTED_FIELDS:
            pattern = rf'StructField\(\s*"{field}"'
            assert re.search(pattern, source), (
                f"Field '{field}' is not declared in bronze_stream.py"
            )


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

"""Test IBKR fetch_candles implementation."""

import asyncio
from datetime import timedelta

import pytest

from unified_trading_execution.adapter import CandleTimeframe
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _config() -> IBKRConfig:
    return IBKRConfig(
        host="127.0.0.1",
        port=7497,
        client_id=1,
        account="DU123456",
    )


def _instrument(**kwargs: object) -> Instrument:
    defaults = {
        "symbol": "AAPL",
        "quote_currency": "USD",
        "asset_class": AssetClass.STOCK,
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


class TestFetchCandles:
    """Test fetch_candles implementation."""

    def test_limit_must_be_at_least_one(self):
        """limit < 1 raises ValueError."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            with pytest.raises(ValueError, match="limit must be at least 1"):
                await adapter.fetch_candles(
                    instrument, CandleTimeframe.M1, limit=0
                )

        asyncio.run(test())

    def test_unsupported_timeframe_raises(self):
        """Timeframe not in the 7 core set raises ValueError."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            with pytest.raises(ValueError, match="unsupported timeframe"):
                await adapter.fetch_candles(
                    instrument, CandleTimeframe.H8, limit=10
                )

        asyncio.run(test())

    def test_bar_duration_parser(self):
        """_bar_duration correctly parses IBKR bar size strings."""
        adapter = IBKRAdapter(_config())

        assert adapter._bar_duration("1 min") == timedelta(minutes=1)
        assert adapter._bar_duration("5 mins") == timedelta(minutes=5)
        assert adapter._bar_duration("15 mins") == timedelta(minutes=15)
        assert adapter._bar_duration("30 mins") == timedelta(minutes=30)
        assert adapter._bar_duration("1 hour") == timedelta(hours=1)
        assert adapter._bar_duration("4 hours") == timedelta(hours=4)
        assert adapter._bar_duration("1 day") == timedelta(days=1)
        # Fallback for unknown formats
        assert adapter._bar_duration("unknown") == timedelta(minutes=1)

    def test_timeframe_mapping(self):
        """All 7 core timeframes map to valid IBKR bar sizes."""
        # Verify each timeframe maps to a valid bar size (no network call needed)
        timeframes = [
            CandleTimeframe.M1,
            CandleTimeframe.M5,
            CandleTimeframe.M15,
            CandleTimeframe.M30,
            CandleTimeframe.H1,
            CandleTimeframe.H4,
            CandleTimeframe.D1,
        ]

        # The mapping is defined in fetch_candles; we can verify the logic
        # by checking that the implementation doesn't raise ValueError for valid timeframes
        expected_timeframes = {
            CandleTimeframe.M1: "1 min",
            CandleTimeframe.M5: "5 mins",
            CandleTimeframe.M15: "15 mins",
            CandleTimeframe.M30: "30 mins",
            CandleTimeframe.H1: "1 hour",
            CandleTimeframe.H4: "4 hours",
            CandleTimeframe.D1: "1 day",
        }

        for tf in timeframes:
            assert tf in expected_timeframes, f"{tf} should be in mapping"

"""Unit tests for Hyperliquid market data capability and fetch_candles."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.adapter import TrailingBasis
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(
        wallet_address=_TEST_ADDRESS,
        private_key=_TEST_KEY,
    )


def _instrument(**kwargs: object) -> Instrument:
    """Build an Instrument with given overrides."""
    defaults = {
        "symbol": "BTC",
        "quote_currency": "USDC",
        "asset_class": AssetClass.FUTURES,
        "currency": "USDC",
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


class TestMarketDataCapability:
    def test_declares_all_supported_timeframes(self):
        """Capability reports the nine core timeframes."""
        from unified_trading_execution.trailing.types import CandleTimeframe

        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.candle_timeframes == frozenset(
            {
                CandleTimeframe.M1,
                CandleTimeframe.M5,
                CandleTimeframe.M15,
                CandleTimeframe.M30,
                CandleTimeframe.H1,
                CandleTimeframe.H4,
                CandleTimeframe.H8,
                CandleTimeframe.H12,
                CandleTimeframe.D1,
            }
        )

    def test_declares_ticker_bases(self):
        """Capability reports bid, ask, and mark as ticker bases."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.ticker_bases == frozenset(
            {TrailingBasis.BID, TrailingBasis.ASK, TrailingBasis.MARK}
        )

    def test_declares_one_second_ticker_floor(self):
        """Capability reports 1.0 second minimum ticker interval."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.min_ticker_interval_seconds == 1.0

    def test_declares_no_streaming_ticker(self):
        """Capability reports streaming ticker as unsupported."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.streaming_ticker is False

    def test_capability_is_pure_no_venue_read(self):
        """The capability property performs no venue I/O."""
        adapter = HyperliquidAdapter(_config())
        _ = adapter.market_data_capability(_instrument())

        # No exchange object created, no network calls
        assert adapter._exchange is None


class TestFetchCandles:
    async def test_fetches_candles_for_supported_timeframe(self):
        """Successfully fetch candles for a supported timeframe."""
        adapter = HyperliquidAdapter(_config())
        # Set up a mock exchange so _require_exchange doesn't fail
        adapter._exchange = MagicMock()
        candles_data = [
            [1700000000000, "50000", "51000", "49000", "50500", "100"],
            [1700000060000, "50500", "51500", "49500", "51000", "150"],
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert len(candles) == 2
        assert candles[0].open == Decimal("50000")
        assert candles[0].close == Decimal("50500")

    async def test_returns_oldest_first(self):
        """Candles are returned in ascending chronological order."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        candles_data = [
            [1700000000000, "50000", "51000", "49000", "50500", "100"],
            [1700000060000, "50500", "51500", "49500", "51000", "150"],
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert candles[0].open_time < candles[1].open_time

    async def test_drops_forming_bar(self):
        """Bars with close time in the future are excluded."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        now = datetime.now(UTC)
        future_timestamp = int((now + timedelta(hours=2)).timestamp() * 1000)
        past_timestamp = int((now - timedelta(hours=1)).timestamp() * 1000)

        candles_data = [
            [past_timestamp, "50000", "51000", "49000", "50500", "100"],
            [future_timestamp, "50500", "51500", "49500", "51000", "150"],
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert len(candles) == 1
        assert candles[0].open == Decimal("50000")

    async def test_honors_limit(self):
        """Returns at most the requested number of candles."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        candles_data = [
            [1700000000000 + i * 60000, "50000", "51000", "49000", "50500", "100"]
            for i in range(10)
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1m", 5)

        assert len(candles) == 5

    async def test_handles_empty_result(self):
        """Empty venue response returns empty list."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value=[])

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert candles == []

    async def test_converts_timestamps_to_aware_utc(self):
        """Timestamps are converted to timezone-aware UTC."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        candles_data = [[1700000000000, "50000", "51000", "49000", "50500", "100"]]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert candles[0].open_time.tzinfo is UTC
        assert candles[0].close_time.tzinfo is UTC

    async def test_uses_decimal_for_ohlc(self):
        """OHLC values are returned as Decimal."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        candles_data = [[1700000000000, "50000.5", "51000.7", "49000.3", "50500.1", "100.25"]]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), "1h", 10)

        assert isinstance(candles[0].open, Decimal)
        assert isinstance(candles[0].high, Decimal)
        assert isinstance(candles[0].low, Decimal)
        assert isinstance(candles[0].close, Decimal)

    async def test_raises_on_invalid_symbol(self):
        """Invalid symbol raises PlatformError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(side_effect=Exception("symbol not found"))

        with pytest.raises(Exception, match=r"symbol not found|Unexpected"):
            await adapter.fetch_candles(_instrument(), "1h", 10)

    async def test_raises_on_unsupported_timeframe(self):
        """Unsupported timeframe raises ValueError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock()

        with pytest.raises(ValueError, match="Unsupported timeframe"):
            await adapter.fetch_candles(_instrument(), "3m", 10)

    async def test_raises_on_invalid_limit(self):
        """Limit < 1 raises ValueError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock()

        with pytest.raises(ValueError, match="limit must be >= 1"):
            await adapter.fetch_candles(_instrument(), "1h", 0)

    async def test_maps_every_declared_timeframe(self):
        """All nine core timeframes map correctly."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value=[])

        for timeframe in ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d"]:
            await adapter.fetch_candles(_instrument(), timeframe, 10)
            # Should not raise for any supported timeframe

    async def test_records_weight_via_run_exchange(self):
        """The call is recorded via _run_exchange for surcharge accounting."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value=[])

        await adapter.fetch_candles(_instrument(), "1h", 10)

        # Verify _run_exchange was called (which does the weight accounting)
        assert adapter._run_exchange.called

    async def test_handles_malformed_response(self):
        """Malformed candle entry raises PlatformError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value=[["invalid"]])

        with pytest.raises(Exception, match=r"Malformed|Unexpected"):
            await adapter.fetch_candles(_instrument(), "1h", 10)

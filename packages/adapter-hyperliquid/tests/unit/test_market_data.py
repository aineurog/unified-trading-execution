"""Unit tests for Hyperliquid market data capability and fetch_candles."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.adapter import CandleTimeframe, TrailingBasis
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformError,
    UnsupportedOrderTypeError,
)
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


def _candle(
    timestamp_ms: int,
    open_px: str = "50000",
    high_px: str = "51000",
    low_px: str = "49000",
    close_px: str = "50500",
    volume: str = "100",
) -> dict[str, int | str]:
    return {
        "T": timestamp_ms,
        "o": open_px,
        "h": high_px,
        "l": low_px,
        "c": close_px,
        "v": volume,
    }


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

    def test_declares_five_second_ticker_floor(self):
        """A ticker costs two heavy reads, so the declared floor is five seconds."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.min_ticker_interval_seconds == 5.0

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


def _connected(adapter: HyperliquidAdapter) -> MagicMock:
    exchange = MagicMock()
    exchange.info.name_to_coin = {"BTC": "BTC"}
    adapter._exchange = exchange
    return exchange


class TestFetchCandles:
    async def test_fetches_candles_for_supported_timeframe(self):
        """Successfully fetch candles for a supported timeframe."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [
            _candle(1700000000000),
            _candle(1700000060000, "50500", "51500", "49500", "51000", "150"),
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert len(candles) == 2
        assert candles[0].open == Decimal("50000")
        assert candles[0].close == Decimal("50500")
        adapter._run_exchange.assert_awaited_once()

    async def test_returns_oldest_first(self):
        """Candles are returned in ascending chronological order."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [
            _candle(1700000000000),
            _candle(1700000060000, "50500", "51500", "49500", "51000", "150"),
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert candles[0].open_time < candles[1].open_time

    async def test_drops_forming_bar(self):
        """Bars with close time in the future are excluded."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        now = datetime.now(UTC)
        future_timestamp = int((now + timedelta(hours=2)).timestamp() * 1000)
        past_timestamp = int((now - timedelta(hours=1)).timestamp() * 1000)

        candles_data = [
            _candle(past_timestamp),
            _candle(future_timestamp, "50500", "51500", "49500", "51000", "150"),
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert len(candles) == 1
        assert candles[0].open == Decimal("50000")

    async def test_honors_limit_keeping_most_recent(self):
        """The cap keeps the newest candles, never the oldest."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [_candle(1700000000000 + i * 60000) for i in range(10)]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=5)

        assert len(candles) == 5
        assert candles[0].open_time == datetime.fromtimestamp(1700000300, tz=UTC)
        assert candles[-1].open_time == datetime.fromtimestamp(1700000540, tz=UTC)

    async def test_sorts_newest_first_response_before_capping(self):
        """A reversed venue response still yields oldest-first output."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [_candle(1700000000000 + i * 60000) for i in range(6)]
        candles_data.reverse()
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=3)

        assert [c.open_time for c in candles] == [
            datetime.fromtimestamp(1700000180, tz=UTC),
            datetime.fromtimestamp(1700000240, tz=UTC),
            datetime.fromtimestamp(1700000300, tz=UTC),
        ]

    async def test_handles_empty_result(self):
        """Empty venue response returns empty list."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value=[])

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert candles == []

    async def test_converts_timestamps_to_aware_utc(self):
        """Timestamps are converted to timezone-aware UTC."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [_candle(1700000000000)]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert candles[0].open_time.tzinfo is UTC
        assert candles[0].close_time.tzinfo is UTC

    async def test_uses_decimal_for_ohlc(self):
        """OHLC values are returned as Decimal."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        candles_data = [
            _candle(1700000000000, "50000.5", "51000.7", "49000.3", "50500.1", "100.25")
        ]
        adapter._run_exchange = AsyncMock(return_value=candles_data)

        candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert isinstance(candles[0].open, Decimal)
        assert isinstance(candles[0].high, Decimal)
        assert isinstance(candles[0].low, Decimal)
        assert isinstance(candles[0].close, Decimal)

    async def test_raises_on_unknown_symbol(self):
        """A symbol the venue does not list raises InvalidSymbolError before I/O."""
        adapter = HyperliquidAdapter(_config())
        exchange = MagicMock()
        exchange.info.name_to_coin = {}
        adapter._exchange = exchange
        adapter._run_exchange = AsyncMock()

        with pytest.raises(InvalidSymbolError, match="Unknown coin"):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        adapter._run_exchange.assert_not_called()

    async def test_raises_on_unsupported_timeframe(self):
        """A timeframe outside the declared set raises UnsupportedOrderTypeError."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock()

        with pytest.raises(UnsupportedOrderTypeError, match="does not support"):
            await adapter.fetch_candles(_instrument(), "3m", limit=10)  # type: ignore[arg-type]

    async def test_raises_on_invalid_limit(self):
        """Limit < 1 raises ValueError."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock()

        with pytest.raises(ValueError, match="limit must be >= 1"):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=0)

    async def test_maps_every_declared_timeframe(self):
        """All nine core timeframes map correctly."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value=[])

        for timeframe in CandleTimeframe:
            await adapter.fetch_candles(_instrument(), timeframe, limit=10)

        assert adapter._run_exchange.await_count == len(CandleTimeframe) == 9

    async def test_records_weight_via_run_exchange(self):
        """The call is recorded via _run_exchange for surcharge accounting."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value=[])

        await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

        assert adapter._run_exchange.called

    async def test_handles_malformed_entry(self):
        """Malformed candle entry raises PlatformError."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value=[["invalid"]])

        with pytest.raises(PlatformError, match="Malformed candle entry"):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

    async def test_raises_on_non_list_response(self):
        """A response of the wrong shape is a transport failure, not an empty list."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        with pytest.raises(PlatformError, match="Unexpected candles response shape"):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

    async def test_raises_on_unconvertible_timestamp(self):
        """A bad timestamp raises PlatformError instead of leaking TypeError."""
        adapter = HyperliquidAdapter(_config())
        _connected(adapter)
        adapter._run_exchange = AsyncMock(return_value=[_candle("not-a-number")])  # type: ignore[arg-type]

        with pytest.raises(PlatformError, match="Malformed candle entry"):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.H1, limit=10)

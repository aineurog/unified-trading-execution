"""Unit tests for MT5 market-data capabilities and candle reads."""

from __future__ import annotations

from collections import namedtuple
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.trailing.types import CandleTimeframe, TrailingBasis
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_MT5_TIMEFRAMES: dict[CandleTimeframe, int] = {
    CandleTimeframe.M1: 1,
    CandleTimeframe.M5: 5,
    CandleTimeframe.M15: 15,
    CandleTimeframe.M30: 30,
    CandleTimeframe.H1: 16385,
    CandleTimeframe.H4: 16388,
    CandleTimeframe.H8: 16392,
    CandleTimeframe.H12: 16396,
    CandleTimeframe.D1: 16408,
}

_CANDLE_SECONDS: dict[CandleTimeframe, int] = {
    CandleTimeframe.M1: 60,
    CandleTimeframe.M5: 300,
    CandleTimeframe.M15: 900,
    CandleTimeframe.M30: 1800,
    CandleTimeframe.H1: 3600,
    CandleTimeframe.H4: 14400,
    CandleTimeframe.H8: 28800,
    CandleTimeframe.H12: 43200,
    CandleTimeframe.D1: 86400,
}

_OFFSET = 7200  # UTC+2 broker
_NOW = datetime(2024, 1, 10, 12, 0, 0, tzinfo=UTC)

Mt5Rate = namedtuple("Mt5Rate", ["time", "open", "high", "low", "close"])


def _instrument(platform_symbol: str | None = "EURUSD.m") -> Instrument:
    return Instrument(
        symbol="EUR",
        quote_currency="USD",
        asset_class=AssetClass.MARGIN_FX,
        platform_symbol=platform_symbol,
    )


def _rate(open_time: datetime, index: int = 0) -> Mt5Rate:
    return Mt5Rate(
        time=int(open_time.timestamp()) + _OFFSET,
        open=1.0 + index,
        high=2.0 + index,
        low=0.5 + index,
        close=1.0 + index,
    )


def _prepare(adapter: MT5Adapter, mock_mt5_module: MagicMock) -> None:
    mock_mt5_module.symbol_info_tick.return_value = None
    adapter._server_time_offset = _OFFSET
    adapter._server_time_offset_measured = True


class TestMarketDataCapability:
    def test_declares_ticker_and_every_timeframe(self, adapter: MT5Adapter) -> None:
        capability = adapter.market_data_capability(_instrument())

        assert capability.ticker_bases == frozenset(
            {TrailingBasis.LAST, TrailingBasis.BID, TrailingBasis.ASK}
        )
        assert capability.min_ticker_interval_seconds == 1.0
        assert capability.candle_timeframes == frozenset(_MT5_TIMEFRAMES)
        assert capability.streaming_ticker is False

    def test_missing_platform_symbol_falls_back_to_unsupported(self, adapter: MT5Adapter) -> None:
        capability = adapter.market_data_capability(_instrument(platform_symbol=None))

        assert capability.ticker_bases == frozenset()
        assert capability.candle_timeframes == frozenset()
        assert capability.min_ticker_interval_seconds == 0.0

    def test_blank_platform_symbol_falls_back_to_unsupported(self, adapter: MT5Adapter) -> None:
        capability = adapter.market_data_capability(_instrument(platform_symbol="   "))

        assert capability.candle_timeframes == frozenset()

    def test_construction_does_not_read_the_terminal(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        adapter.market_data_capability(_instrument())

        mock_mt5_module.symbol_info.assert_not_called()
        mock_mt5_module.symbol_info_tick.assert_not_called()
        mock_mt5_module.symbols_get.assert_not_called()
        mock_mt5_module.copy_rates_from_pos.assert_not_called()
        mock_mt5_module.initialize.assert_not_called()


class TestFetchCandles:
    @pytest.mark.parametrize("timeframe", list(CandleTimeframe))
    async def test_every_declared_timeframe_is_oldest_first(
        self,
        mock_mt5_module: MagicMock,
        adapter: MT5Adapter,
        timeframe: CandleTimeframe,
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        step = timedelta(seconds=_CANDLE_SECONDS[timeframe])
        closed_newest = _NOW - step
        closed_older = _NOW - 2 * step
        mock_mt5_module.copy_rates_from_pos.return_value = (
            _rate(_NOW, 2),
            _rate(closed_newest, 1),
            _rate(closed_older, 0),
        )

        with patch("unified_trading_execution.mt5.adapter._utcnow", return_value=_NOW):
            candles = await adapter.fetch_candles(_instrument(), timeframe, limit=2)

        mock_mt5_module.copy_rates_from_pos.assert_called_once_with(
            "EURUSD.m", _MT5_TIMEFRAMES[timeframe], 0, 3
        )
        assert [candle.open_time for candle in candles] == [closed_older, closed_newest]
        assert all(candle.open_time.tzinfo is not None for candle in candles)
        assert all(candle.close_time.tzinfo is not None for candle in candles)
        assert all(candle.close_time <= _NOW for candle in candles)
        assert all(isinstance(candle.open, Decimal) for candle in candles)
        assert candles[0].close_time == closed_newest
        assert candles[1].close_time == _NOW

    async def test_forming_bar_is_dropped_by_close_time(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        closed = _NOW - timedelta(seconds=60)
        mock_mt5_module.copy_rates_from_pos.return_value = (
            _rate(_NOW),
            _rate(closed),
        )

        with patch("unified_trading_execution.mt5.adapter._utcnow", return_value=_NOW):
            candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=5)

        assert len(candles) == 1
        assert candles[0].open_time == closed

    async def test_limit_keeps_the_most_recent_closed_bars(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        opens = [_NOW - timedelta(seconds=60 * index) for index in range(1, 7)]
        mock_mt5_module.copy_rates_from_pos.return_value = tuple(
            _rate(open_time, index) for index, open_time in enumerate(opens)
        )

        with patch("unified_trading_execution.mt5.adapter._utcnow", return_value=_NOW):
            candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=2)

        assert [candle.open_time for candle in candles] == [opens[1], opens[0]]
        mock_mt5_module.copy_rates_from_pos.assert_called_once_with("EURUSD.m", 1, 0, 3)

    async def test_empty_result_returns_empty_list(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        mock_mt5_module.copy_rates_from_pos.return_value = []

        with patch("unified_trading_execution.mt5.adapter._utcnow", return_value=_NOW):
            candles = await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=5)

        assert candles == []

    async def test_none_result_raises_transport_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        mock_mt5_module.copy_rates_from_pos.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "processing error")

        with pytest.raises(PlatformError):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=5)

    async def test_unknown_symbol_raises_before_fetch(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _prepare(adapter, mock_mt5_module)
        mock_mt5_module.symbol_select.return_value = False
        mock_mt5_module.symbols_get.return_value = None

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=5)

        mock_mt5_module.copy_rates_from_pos.assert_not_called()

    async def test_unsupported_timeframe_raises(self, adapter: MT5Adapter) -> None:
        unsupported = MagicMock()
        unsupported.value = "2h"

        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.fetch_candles(_instrument(), unsupported, limit=5)

    async def test_invalid_limit_raises(self, adapter: MT5Adapter) -> None:
        with pytest.raises(ValueError):
            await adapter.fetch_candles(_instrument(), CandleTimeframe.M1, limit=0)

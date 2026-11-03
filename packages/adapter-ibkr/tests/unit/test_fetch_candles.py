"""Test IBKR fetch_candles implementation."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.adapter import CandleTimeframe
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass, OptionRight
from unified_trading_execution.types.instrument import Instrument


def _config(*, timeout_seconds: float = 10.0) -> IBKRConfig:
    return IBKRConfig(
        host="127.0.0.1",
        port=7497,
        client_id=1,
        account="DU123456",
        timeout_seconds=timeout_seconds,
    )


def _instrument(**kwargs: object) -> Instrument:
    defaults: dict[str, object] = {
        "symbol": "AAPL",
        "quote_currency": "USD",
        "asset_class": AssetClass.STOCK,
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)  # type: ignore[arg-type]


def _bar(
    timestamp: datetime | date,
    *,
    open_: float = 100.0,
    high: float = 105.0,
    low: float = 95.0,
    close: float = 102.0,
) -> SimpleNamespace:
    return SimpleNamespace(
        date=timestamp, open=open_, high=high, low=low, close=close, volume=1, average=0.0
    )


def _adapter(
    *, bars: list[SimpleNamespace] | None = None, details: list[object] | None = None
) -> tuple[IBKRAdapter, MagicMock]:
    ib = MagicMock()
    ib.reqContractDetailsAsync = AsyncMock(
        return_value=[MagicMock(contract=MagicMock())] if details is None else details
    )
    ib.reqHistoricalDataAsync = AsyncMock(return_value=bars or [])
    adapter = IBKRAdapter(_config())
    adapter._ib = ib
    adapter._require_ib = lambda: ib  # type: ignore[method-assign]
    return adapter, ib


def _run(adapter: IBKRAdapter, instrument: Instrument, timeframe: CandleTimeframe, limit: int = 10):
    return asyncio.run(adapter.fetch_candles(instrument, timeframe, limit=limit))


class TestFetchCandlesArguments:
    def test_limit_must_be_at_least_one(self) -> None:
        adapter, ib = _adapter()
        with pytest.raises(ValueError, match="limit must be at least 1"):
            _run(adapter, _instrument(), CandleTimeframe.M1, limit=0)
        ib.reqHistoricalDataAsync.assert_not_awaited()

    @pytest.mark.parametrize("timeframe", [CandleTimeframe.H8, CandleTimeframe.H12])
    def test_unsupported_timeframe_raises(self, timeframe: CandleTimeframe) -> None:
        adapter, ib = _adapter()
        with pytest.raises(UnsupportedOrderTypeError, match="candle timeframe"):
            _run(adapter, _instrument(), timeframe)
        ib.reqHistoricalDataAsync.assert_not_awaited()

    def test_bar_duration_parser(self) -> None:
        adapter, _ = _adapter()
        assert adapter._bar_duration("1 min") == timedelta(minutes=1)
        assert adapter._bar_duration("5 mins") == timedelta(minutes=5)
        assert adapter._bar_duration("15 mins") == timedelta(minutes=15)
        assert adapter._bar_duration("30 mins") == timedelta(minutes=30)
        assert adapter._bar_duration("1 hour") == timedelta(hours=1)
        assert adapter._bar_duration("4 hours") == timedelta(hours=4)
        assert adapter._bar_duration("1 day") == timedelta(days=1)


class TestFetchCandlesParsing:
    def test_intraday_bar_is_open_then_close(self) -> None:
        """IB dates a bar by its start, so close is start plus the bar width."""
        start = datetime(2026, 1, 5, 10, 0, 0)
        adapter, _ = _adapter(bars=[_bar(start)])

        candles = _run(adapter, _instrument(), CandleTimeframe.H1)

        assert len(candles) == 1
        assert candles[0].open_time == datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
        assert candles[0].close_time == datetime(2026, 1, 5, 11, 0, tzinfo=UTC)
        assert candles[0].open == 100
        assert candles[0].close == 102

    def test_aware_bar_time_is_preserved(self) -> None:
        start = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
        adapter, _ = _adapter(bars=[_bar(start)])

        candles = _run(adapter, _instrument(), CandleTimeframe.M5)

        assert candles[0].open_time == start
        assert candles[0].close_time == start + timedelta(minutes=5)

    def test_daily_bar_date_becomes_midnight_utc(self) -> None:
        """Daily bars arrive as ``date`` even with formatDate=2."""
        adapter, _ = _adapter(bars=[_bar(date(2026, 1, 5))])

        candles = _run(adapter, _instrument(), CandleTimeframe.D1)

        assert len(candles) == 1
        assert candles[0].open_time == datetime(2026, 1, 5, tzinfo=UTC)
        assert candles[0].close_time == datetime(2026, 1, 6, tzinfo=UTC)

    def test_forming_bar_is_dropped(self) -> None:
        now = datetime.now(UTC)
        adapter, _ = _adapter(bars=[_bar(now.replace(microsecond=0))])

        candles = _run(adapter, _instrument(), CandleTimeframe.H1)

        assert candles == []

    def test_response_is_sorted_oldest_first(self) -> None:
        newer = datetime(2026, 1, 5, 11, 0, tzinfo=UTC)
        older = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
        adapter, _ = _adapter(bars=[_bar(newer), _bar(older)])

        candles = _run(adapter, _instrument(), CandleTimeframe.H1)

        assert [c.open_time for c in candles] == [older, newer]

    def test_limit_keeps_most_recent(self) -> None:
        base = datetime(2026, 1, 5, tzinfo=UTC)
        bars = [_bar(base + timedelta(hours=i)) for i in range(5)]
        adapter, _ = _adapter(bars=bars)

        candles = _run(adapter, _instrument(), CandleTimeframe.H1, limit=2)

        assert len(candles) == 2
        assert candles[-1].open_time == base + timedelta(hours=4)

    def test_empty_bars_returns_empty_list(self) -> None:
        adapter, _ = _adapter(bars=[])
        assert _run(adapter, _instrument(), CandleTimeframe.M1) == []

    @pytest.mark.parametrize(
        "broken",
        [
            SimpleNamespace(date=datetime(2026, 1, 5, tzinfo=UTC), open=100.0),
            _bar(datetime(2026, 1, 5, tzinfo=UTC), open_=None),  # type: ignore[arg-type]
        ],
        ids=["missing-field", "non-numeric-field"],
    )
    def test_malformed_bar_raises_platform_error(self, broken: SimpleNamespace) -> None:
        adapter, _ = _adapter(bars=[broken])
        with pytest.raises(PlatformError, match="malformed candle data"):
            _run(adapter, _instrument(), CandleTimeframe.H1)


class TestFetchCandlesSymbolAndSeries:
    def test_unknown_symbol_raises_and_skips_request(self) -> None:
        adapter, ib = _adapter(details=[])
        with pytest.raises(InvalidSymbolError, match="not available"):
            _run(adapter, _instrument(), CandleTimeframe.M1)
        ib.reqHistoricalDataAsync.assert_not_awaited()

    @pytest.mark.parametrize(
        ("instrument", "expected"),
        [
            (_instrument(), "TRADES"),
            (
                _instrument(
                    asset_class=AssetClass.FUTURES,
                    exchange="GLOBEX",
                    expiry=date(2026, 12, 18),
                    multiplier=50,
                ),
                "TRADES",
            ),
            (
                _instrument(
                    asset_class=AssetClass.OPTION,
                    expiry=date(2026, 12, 18),
                    strike=200,
                    option_right=OptionRight.CALL,
                    multiplier=100,
                ),
                "TRADES",
            ),
            (_instrument(asset_class=AssetClass.CFD), "TRADES"),
            (
                _instrument(
                    asset_class=AssetClass.MARGIN_FX,
                    symbol="EUR",
                    quote_currency="USD",
                    exchange="IDEALPRO",
                ),
                "MIDPOINT",
            ),
            (
                _instrument(asset_class=AssetClass.SPOT, symbol="BTC", quote_currency="USD"),
                "AGGTRADES",
            ),
        ],
    )
    def test_series_matches_asset_class(self, instrument: Instrument, expected: str) -> None:
        adapter, ib = _adapter(bars=[_bar(datetime(2026, 1, 5, tzinfo=UTC))])

        _run(adapter, instrument, CandleTimeframe.H1)

        assert ib.reqHistoricalDataAsync.call_args.kwargs["whatToShow"] == expected

    def test_gateway_timeout_raises_connection_error(self) -> None:
        adapter, ib = _adapter()
        adapter._config = _config(timeout_seconds=0.01)

        async def slow(*args: object, **kwargs: object) -> list[object]:
            await asyncio.sleep(1)
            return []

        ib.reqHistoricalDataAsync = slow
        with pytest.raises(PlatformConnectionError, match="timed out"):
            _run(adapter, _instrument(), CandleTimeframe.M1)

    def test_gateway_error_raises_platform_error(self) -> None:
        adapter, ib = _adapter()
        ib.reqHistoricalDataAsync = AsyncMock(side_effect=RuntimeError("socket closed"))
        with pytest.raises(PlatformError, match="failed to fetch historical data"):
            _run(adapter, _instrument(), CandleTimeframe.M1)

"""Bybit market data capability and closed-candle translation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError, InvalidRequestError

from unified_trading_execution.adapter import MarketDataCapability
from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.trailing.types import CandleTimeframe, TrailingBasis
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_DECLARED_TIMEFRAMES = frozenset(
    {
        CandleTimeframe.M1,
        CandleTimeframe.M5,
        CandleTimeframe.M15,
        CandleTimeframe.M30,
        CandleTimeframe.H1,
        CandleTimeframe.H4,
        CandleTimeframe.H12,
        CandleTimeframe.D1,
    }
)
_INTERVALS = {
    CandleTimeframe.M1: "1",
    CandleTimeframe.M5: "5",
    CandleTimeframe.M15: "15",
    CandleTimeframe.M30: "30",
    CandleTimeframe.H1: "60",
    CandleTimeframe.H4: "240",
    CandleTimeframe.H12: "720",
    CandleTimeframe.D1: "D",
}


def _linear_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        multiplier=1,
    )


def _inverse_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        multiplier=1,
    )


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
    )


def _unsupported_instrument() -> Instrument:
    return Instrument(
        symbol="EUR",
        quote_currency="USD",
        asset_class=AssetClass.MARGIN_FX,
    )


def _kline(
    start_ms: int,
    *,
    open_price: str = "100",
    high: str = "110",
    low: str = "90",
    close: str = "105",
) -> dict[str, str]:
    return {
        "startTime": str(start_ms),
        "openPrice": open_price,
        "highPrice": high,
        "lowPrice": low,
        "closePrice": close,
    }


def _response(
    entries: list[dict[str, str]] | None,
) -> tuple[dict[str, object], None, dict[str, str]]:
    return {"retCode": 0, "result": {"list": entries}}, None, {}


class TestMarketDataCapability:
    def test_linear_perpetual_declares_ticker_and_candles(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_derivative_capability(adapter.market_data_capability(_linear_instrument()))

    def test_inverse_perpetual_declares_ticker_and_candles(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_derivative_capability(adapter.market_data_capability(_inverse_instrument()))

    def test_spot_has_no_mark_basis(self, adapter: BybitAdapter) -> None:
        capability = adapter.market_data_capability(_spot_instrument())

        assert capability.ticker_bases == frozenset(
            {TrailingBasis.LAST, TrailingBasis.BID, TrailingBasis.ASK}
        )
        assert capability.min_ticker_interval_seconds == 2.0
        assert capability.candle_timeframes == _DECLARED_TIMEFRAMES
        assert capability.streaming_ticker is False

    def test_unsupported_asset_class_has_no_market_data(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_unsupported(adapter.market_data_capability(_unsupported_instrument()))

    def test_declared_timeframes_match_the_published_set(self, adapter: BybitAdapter) -> None:
        capability = adapter.market_data_capability(_linear_instrument())

        assert CandleTimeframe.H8 not in capability.candle_timeframes
        assert capability.candle_timeframes == _DECLARED_TIMEFRAMES

    async def test_ticker_bases_are_populated_by_fetch_ticker(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_tickers.return_value = (
            {
                "result": {
                    "list": [
                        {
                            "lastPrice": "100",
                            "markPrice": "101",
                            "bid1Price": "99",
                            "ask1Price": "100",
                        }
                    ]
                }
            },
            None,
            {},
        )
        capability = adapter.market_data_capability(_linear_instrument())
        ticker = await adapter.fetch_ticker(_linear_instrument())

        assert ticker is not None
        for basis in capability.ticker_bases:
            assert {
                TrailingBasis.LAST: ticker.last,
                TrailingBasis.BID: ticker.bid,
                TrailingBasis.ASK: ticker.ask,
                TrailingBasis.MARK: ticker.mark,
            }[basis] is not None

    async def test_spot_ticker_does_not_populate_mark(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_tickers.return_value = (
            {
                "result": {
                    "list": [
                        {
                            "lastPrice": "100",
                            "markPrice": "",
                            "bid1Price": "99",
                            "ask1Price": "100",
                        }
                    ]
                }
            },
            None,
            {},
        )
        capability = adapter.market_data_capability(_spot_instrument())
        ticker = await adapter.fetch_ticker(_spot_instrument())

        assert ticker is not None
        assert ticker.mark is None
        assert TrailingBasis.MARK not in capability.ticker_bases

    @staticmethod
    def _assert_derivative_capability(capability: MarketDataCapability) -> None:
        assert capability.ticker_bases == frozenset(
            {
                TrailingBasis.LAST,
                TrailingBasis.BID,
                TrailingBasis.ASK,
                TrailingBasis.MARK,
            }
        )
        assert capability.min_ticker_interval_seconds == 2.0
        assert capability.candle_timeframes == _DECLARED_TIMEFRAMES
        assert capability.streaming_ticker is False

    @staticmethod
    def _assert_unsupported(capability: MarketDataCapability) -> None:
        assert capability.ticker_bases == frozenset()
        assert capability.min_ticker_interval_seconds == 0.0
        assert capability.candle_timeframes == frozenset()
        assert capability.streaming_ticker is False


class TestFetchCandles:
    @pytest.mark.parametrize("timeframe", sorted(_DECLARED_TIMEFRAMES, key=lambda item: item.value))
    async def test_every_declared_timeframe_maps_to_a_venue_interval(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
        timeframe: CandleTimeframe,
    ) -> None:
        mock_pybit_http.get_kline.return_value = _response([_kline(1_600_000_000_000)])

        candles = await adapter.fetch_candles(_linear_instrument(), timeframe, limit=5)

        assert len(candles) == 1
        call = mock_pybit_http.get_kline.call_args
        assert call.kwargs["category"] == "linear"
        assert call.kwargs["symbol"] == "BTCUSDT"
        assert call.kwargs["interval"] == _INTERVALS[timeframe]
        assert call.kwargs["limit"] == 6

    async def test_inverse_uses_the_inverse_category(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_kline.return_value = _response([_kline(1_600_000_000_000)])

        await adapter.fetch_candles(_inverse_instrument(), CandleTimeframe.H1, limit=1)

        call = mock_pybit_http.get_kline.call_args
        assert call.kwargs["category"] == "inverse"
        assert call.kwargs["symbol"] == "BTCUSD"

    async def test_newest_first_entries_are_reversed(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        starts = [1_600_000_000_000 + index * 60_000 for index in range(3)]
        mock_pybit_http.get_kline.return_value = _response(
            [_kline(start) for start in reversed(starts)]
        )

        candles = await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=3)

        assert [candle.open_time for candle in candles] == [
            datetime.fromtimestamp(start / 1000, tz=UTC) for start in starts
        ]

    async def test_forming_bar_is_dropped(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        closed = _kline(1_600_000_000_000)
        forming_start = int((datetime.now(UTC) + timedelta(hours=1)).timestamp() * 1000)
        mock_pybit_http.get_kline.return_value = _response([_kline(forming_start), closed])

        candles = await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=2)

        assert len(candles) == 1
        assert candles[0].open_time == datetime.fromtimestamp(1_600_000_000, tz=UTC)

    async def test_limit_returns_the_newest_closed_candles(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        starts = [1_600_000_000_000 + index * 60_000 for index in range(5)]
        mock_pybit_http.get_kline.return_value = _response(
            [_kline(start) for start in reversed(starts)]
        )

        candles = await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=2)

        assert [candle.open_time for candle in candles] == [
            datetime.fromtimestamp(start / 1000, tz=UTC) for start in starts[-2:]
        ]

    async def test_ohlc_values_are_decimal(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_kline.return_value = _response(
            [_kline(1_600_000_000_000, open_price="100.1", high="110.2", low="90.3", close="105.4")]
        )

        candle = (await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.H1, limit=1))[0]

        assert candle.open == Decimal("100.1")
        assert candle.high == Decimal("110.2")
        assert candle.low == Decimal("90.3")
        assert candle.close == Decimal("105.4")
        assert candle.close_time == candle.open_time + timedelta(hours=1)

    @pytest.mark.parametrize("entries", [None, []])
    async def test_empty_result_returns_no_candles(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
        entries: list[dict[str, str]] | None,
    ) -> None:
        mock_pybit_http.get_kline.return_value = _response(entries)

        assert await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=5) == []

    async def test_malformed_entry_is_skipped(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_kline.return_value = _response(
            [
                {"startTime": "not-a-time"},
                _kline(1_600_000_000_000),
            ]
        )

        candles = await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=5)

        assert len(candles) == 1

    async def test_h8_is_rejected_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(UnsupportedOrderTypeError, match="8h"):
            await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.H8, limit=5)

        mock_pybit_http.get_kline.assert_not_called()

    async def test_unsupported_asset_class_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_candles(_unsupported_instrument(), CandleTimeframe.M1, limit=5)

        mock_pybit_http.get_kline.assert_not_called()

    async def test_limit_below_one_is_rejected(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=0)

        mock_pybit_http.get_kline.assert_not_called()

    async def test_invalid_symbol_is_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_kline.side_effect = InvalidRequestError(
            request="GET /v5/market/kline",
            message="The requested symbol is invalid",
            status_code=10029,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=5)

    async def test_transport_error_is_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_kline.side_effect = FailedRequestError(
            request="GET /v5/market/kline",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_candles(_linear_instrument(), CandleTimeframe.M1, limit=5)

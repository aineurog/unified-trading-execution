"""Binance market-data capability, ticker, and closed-candle translation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from binance.exceptions import BinanceAPIException  # type: ignore[import-untyped]

from unified_trading_execution.binance import BinanceAdapter, BinanceProduct
from unified_trading_execution.binance.market_data import (
    coin_m_contract_type,
    kline_interval,
    symbol_entry,
)
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    PlatformError,
)
from unified_trading_execution.trailing.types import CandleTimeframe, TrailingBasis
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_ALL_TIMEFRAMES = frozenset(CandleTimeframe)
_INTERVALS = {
    CandleTimeframe.M1: "1m",
    CandleTimeframe.M5: "5m",
    CandleTimeframe.M15: "15m",
    CandleTimeframe.M30: "30m",
    CandleTimeframe.H1: "1h",
    CandleTimeframe.H4: "4h",
    CandleTimeframe.H8: "8h",
    CandleTimeframe.H12: "12h",
    CandleTimeframe.D1: "1d",
}
_BASE_OPEN_MS = 1_600_000_000_000


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT",
    )


def _usd_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        currency="USDT",
        asset_class=AssetClass.FUTURES,
        multiplier=1,
        platform_symbol="BTCUSDT",
    )


def _coin_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        currency="BTC",
        asset_class=AssetClass.FUTURES,
        multiplier=100,
        platform_symbol="BTCUSD_PERP",
    )


def _unsupported_instrument() -> Instrument:
    return Instrument(
        symbol="EUR",
        quote_currency="USD",
        asset_class=AssetClass.BOND,
        currency="USD",
    )


def _exchange_info(product: BinanceProduct) -> dict[str, object]:
    if product == BinanceProduct.SPOT:
        return {
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "status": "TRADING",
        }
    if product == BinanceProduct.COIN_M:
        return {
            "symbol": "BTCUSD_PERP",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "contractType": "PERPETUAL",
            "contractSize": "100",
            "status": "TRADING",
        }
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
    }


def _kline_row(
    open_ms: int,
    *,
    open_price: str = "100",
    high: str = "110",
    low: str = "90",
    close: str = "105",
) -> list[object]:
    return [open_ms, open_price, high, low, close, "1", open_ms + 59_999, "0", 0, "0", "0", "0"]


def _api_error(code: int, status: int, message: str) -> BinanceAPIException:
    return BinanceAPIException(
        SimpleNamespace(text=message),
        status,
        f'{{"code": {code}, "msg": "{message}"}}',
    )


def _wire_market_data(
    adapter: BinanceAdapter,
    client: AsyncMock,
    product: BinanceProduct,
) -> None:
    """Attach a fake client and pre-populated exchange-info for one product."""
    adapter._client = client
    adapter._connected = True
    adapter._exchange_info_cache[product] = [_exchange_info(product)]


class TestMarketDataCapability:
    def test_spot_has_no_mark_basis(self, adapter: BinanceAdapter) -> None:
        capability = adapter.market_data_capability(_spot_instrument())

        assert capability.ticker_bases == frozenset(
            {TrailingBasis.LAST, TrailingBasis.BID, TrailingBasis.ASK}
        )
        assert capability.min_ticker_interval_seconds == 2.0
        assert capability.candle_timeframes == _ALL_TIMEFRAMES
        assert capability.streaming_ticker is False

    @pytest.mark.parametrize(
        "instrument",
        [_usd_m_instrument(), _coin_m_instrument()],
        ids=("usd-m", "coin-m"),
    )
    def test_futures_add_mark_basis(
        self,
        adapter: BinanceAdapter,
        instrument: Instrument,
    ) -> None:
        capability = adapter.market_data_capability(instrument)

        assert capability.ticker_bases == frozenset(
            {
                TrailingBasis.LAST,
                TrailingBasis.BID,
                TrailingBasis.ASK,
                TrailingBasis.MARK,
            }
        )
        assert capability.candle_timeframes == _ALL_TIMEFRAMES
        assert capability.streaming_ticker is False

    def test_unsupported_asset_class_has_no_market_data(
        self,
        adapter: BinanceAdapter,
    ) -> None:
        capability = adapter.market_data_capability(_unsupported_instrument())

        assert capability.ticker_bases == frozenset()
        assert capability.min_ticker_interval_seconds == 0.0
        assert capability.candle_timeframes == frozenset()
        assert capability.streaming_ticker is False

    def test_every_declared_timeframe_maps_to_a_venue_interval(
        self,
        adapter: BinanceAdapter,
    ) -> None:
        capability = adapter.market_data_capability(_usd_m_instrument())

        for timeframe in capability.candle_timeframes:
            assert kline_interval(timeframe) == _INTERVALS[timeframe]


class TestFetchTickerSpot:
    async def test_spot_ticker_populates_bid_ask_last(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.return_value = {
            "symbol": "BTCUSDT",
            "lastPrice": "67000.50",
            "bidPrice": "67000.00",
            "askPrice": "67001.00",
            "bidQty": "1.5",
            "askQty": "2.5",
        }

        ticker = await adapter.fetch_ticker(_spot_instrument())

        assert ticker is not None
        assert ticker.last == Decimal("67000.50")
        assert ticker.bid == Decimal("67000.00")
        assert ticker.ask == Decimal("67001.00")
        assert ticker.mark is None
        binance_client_mock.get_ticker.assert_awaited_once_with(symbol="BTCUSDT")

    async def test_spot_ticker_all_empty_returns_none(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.return_value = {"symbol": "BTCUSDT"}

        assert await adapter.fetch_ticker(_spot_instrument()) is None

    async def test_spot_ticker_missing_prices_are_none_fields(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.return_value = {
            "symbol": "BTCUSDT",
            "lastPrice": "",
            "bidPrice": "",
            "askPrice": "",
        }

        assert await adapter.fetch_ticker(_spot_instrument()) is None

    async def test_spot_ticker_symbol_mismatch_raises(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.return_value = {
            "symbol": "ETHUSDT",
            "lastPrice": "3000",
        }

        with pytest.raises(PlatformError, match="symbol mismatch"):
            await adapter.fetch_ticker(_spot_instrument())


class TestFetchTickerFutures:
    async def test_usd_m_ticker_combines_three_public_reads(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.USD_M)
        binance_client_mock.futures_symbol_ticker.return_value = {
            "symbol": "BTCUSDT",
            "price": "67000.50",
            "time": 1_700_000_000_000,
        }
        binance_client_mock.futures_orderbook_ticker.return_value = {
            "symbol": "BTCUSDT",
            "bidPrice": "67000.00",
            "askPrice": "67001.00",
        }
        binance_client_mock.futures_mark_price.return_value = {
            "symbol": "BTCUSDT",
            "markPrice": "67000.75",
            "indexPrice": "67000.80",
        }

        ticker = await adapter.fetch_ticker(_usd_m_instrument())

        assert ticker is not None
        assert ticker.last == Decimal("67000.50")
        assert ticker.bid == Decimal("67000.00")
        assert ticker.ask == Decimal("67001.00")
        assert ticker.mark == Decimal("67000.75")
        binance_client_mock.futures_symbol_ticker.assert_awaited_once_with(symbol="BTCUSDT")
        binance_client_mock.futures_orderbook_ticker.assert_awaited_once_with(symbol="BTCUSDT")
        binance_client_mock.futures_mark_price.assert_awaited_once_with(symbol="BTCUSDT")

    async def test_coin_m_ticker_accepts_list_payloads(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.COIN_M)
        binance_client_mock.futures_coin_symbol_ticker.return_value = [
            {"symbol": "BTCUSD_PERP", "price": "67000.5"}
        ]
        binance_client_mock.futures_coin_orderbook_ticker.return_value = [
            {"symbol": "BTCUSD_PERP", "bidPrice": "67000.0", "askPrice": "67001.0"}
        ]
        binance_client_mock.futures_coin_mark_price.return_value = [
            {"symbol": "BTCUSD_PERP", "markPrice": "67000.8"}
        ]

        ticker = await adapter.fetch_ticker(_coin_m_instrument())

        assert ticker is not None
        assert ticker.last == Decimal("67000.5")
        assert ticker.bid == Decimal("67000.0")
        assert ticker.ask == Decimal("67001.0")
        assert ticker.mark == Decimal("67000.8")

    async def test_futures_ticker_empty_rows_return_none(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.COIN_M)
        binance_client_mock.futures_coin_symbol_ticker.return_value = []
        binance_client_mock.futures_coin_orderbook_ticker.return_value = []
        binance_client_mock.futures_coin_mark_price.return_value = []

        assert await adapter.fetch_ticker(_coin_m_instrument()) is None

    async def test_futures_ticker_book_only_still_returns_ticker(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        """A live book without trades is still a quote: last stays ``None``."""
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.USD_M)
        binance_client_mock.futures_symbol_ticker.return_value = []
        binance_client_mock.futures_orderbook_ticker.return_value = {
            "symbol": "BTCUSDT",
            "bidPrice": "67000.00",
            "askPrice": "67001.00",
        }
        binance_client_mock.futures_mark_price.return_value = {
            "symbol": "BTCUSDT",
            "markPrice": "67000.75",
        }

        ticker = await adapter.fetch_ticker(_usd_m_instrument())

        assert ticker is not None
        assert ticker.last is None
        assert ticker.bid == Decimal("67000.00")
        assert ticker.ask == Decimal("67001.00")
        assert ticker.mark == Decimal("67000.75")


class TestFetchTickerErrors:
    async def test_unsupported_asset_class_raises_before_network(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_ticker(_unsupported_instrument())

        binance_client_mock.get_ticker.assert_not_called()

    async def test_unknown_symbol_raises_invalid_symbol(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        adapter._exchange_info_cache[BinanceProduct.SPOT] = []

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_ticker(_spot_instrument())

        binance_client_mock.get_ticker.assert_not_called()

    async def test_venue_invalid_symbol_is_translated(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.side_effect = _api_error(-1121, 400, "Invalid symbol.")

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_ticker(_spot_instrument())

    async def test_transport_error_is_translated(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_ticker.side_effect = OSError("connection reset")

        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_ticker(_spot_instrument())

    async def test_market_data_works_with_client_but_not_connected(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        """Public reads need a client, not the private-stream lifecycle."""
        adapter._client = binance_client_mock
        adapter._connected = False
        adapter._exchange_info_cache[BinanceProduct.SPOT] = [_exchange_info(BinanceProduct.SPOT)]
        binance_client_mock.get_ticker.return_value = {
            "symbol": "BTCUSDT",
            "lastPrice": "67000.50",
            "bidPrice": "67000.00",
            "askPrice": "67001.00",
        }

        ticker = await adapter.fetch_ticker(_spot_instrument())

        assert ticker is not None and ticker.last == Decimal("67000.50")

    async def test_no_client_raises_connection_error(
        self,
        adapter: BinanceAdapter,
    ) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_ticker(_spot_instrument())


class TestFetchCandles:
    @pytest.mark.parametrize("timeframe", sorted(_ALL_TIMEFRAMES, key=lambda item: item.value))
    async def test_every_declared_timeframe_maps_to_a_venue_interval(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
        timeframe: CandleTimeframe,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = [_kline_row(_BASE_OPEN_MS)]

        candles = await adapter.fetch_candles(_spot_instrument(), timeframe, limit=5)

        assert len(candles) == 1
        binance_client_mock.get_klines.assert_awaited_once_with(
            symbol="BTCUSDT",
            interval=_INTERVALS[timeframe],
            limit=6,
        )

    async def test_usd_m_uses_futures_klines(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.USD_M)
        binance_client_mock.futures_klines.return_value = [_kline_row(_BASE_OPEN_MS)]

        await adapter.fetch_candles(_usd_m_instrument(), CandleTimeframe.H1, limit=3)

        binance_client_mock.futures_klines.assert_awaited_once_with(
            symbol="BTCUSDT",
            interval="1h",
            limit=4,
        )
        binance_client_mock.get_klines.assert_not_called()

    async def test_coin_m_uses_futures_coin_klines_with_contract_type(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.COIN_M)
        binance_client_mock.futures_coin_klines.return_value = [_kline_row(_BASE_OPEN_MS)]

        await adapter.fetch_candles(_coin_m_instrument(), CandleTimeframe.M5, limit=2)

        binance_client_mock.futures_coin_klines.assert_awaited_once_with(
            symbol="BTCUSD_PERP",
            interval="5m",
            limit=3,
            contractType="PERPETUAL",
        )

    async def test_candles_stay_oldest_first(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        starts = [_BASE_OPEN_MS + index * 60_000 for index in range(3)]
        binance_client_mock.get_klines.return_value = [_kline_row(start) for start in starts]

        candles = await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=3)

        assert [candle.open_time for candle in candles] == [
            datetime.fromtimestamp(start / 1000, tz=UTC) for start in starts
        ]

    async def test_forming_bar_is_dropped(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        forming_open_ms = int(datetime.now(UTC).timestamp() * 1000)
        binance_client_mock.get_klines.return_value = [
            _kline_row(_BASE_OPEN_MS),
            _kline_row(forming_open_ms),
        ]

        candles = await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=5)

        assert len(candles) == 1
        assert candles[0].open_time == datetime.fromtimestamp(_BASE_OPEN_MS / 1000, tz=UTC)

    async def test_limit_returns_the_newest_closed_candles(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        starts = [_BASE_OPEN_MS + index * 60_000 for index in range(5)]
        binance_client_mock.get_klines.return_value = [_kline_row(start) for start in starts]

        candles = await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=2)

        assert [candle.open_time for candle in candles] == [
            datetime.fromtimestamp(start / 1000, tz=UTC) for start in starts[-2:]
        ]

    async def test_ohlc_values_and_close_time(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = [
            _kline_row(
                _BASE_OPEN_MS,
                open_price="100.1",
                high="110.2",
                low="90.3",
                close="105.4",
            )
        ]

        candle = (await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.H1, limit=1))[0]

        assert candle.open == Decimal("100.1")
        assert candle.high == Decimal("110.2")
        assert candle.low == Decimal("90.3")
        assert candle.close == Decimal("105.4")
        assert candle.close_time == candle.open_time + timedelta(hours=1)

    async def test_malformed_row_is_skipped(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = [
            "not-a-row",
            ["bad-time", "1", "2", "0.5", "1.5"],
            _kline_row(_BASE_OPEN_MS),
        ]

        candles = await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=5)

        assert len(candles) == 1

    async def test_empty_result_returns_no_candles(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = []

        assert await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=5) == []

    async def test_non_list_response_raises(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = {"code": -1121}

        with pytest.raises(PlatformError, match="not a list"):
            await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=5)

    async def test_limit_below_one_is_rejected(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=0)

        binance_client_mock.get_klines.assert_not_called()

    async def test_unsupported_asset_class_raises_before_network(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_candles(_unsupported_instrument(), CandleTimeframe.M1, limit=5)

        binance_client_mock.get_klines.assert_not_called()

    async def test_limit_is_capped_at_venue_maximum(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = []

        await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=5000)

        binance_client_mock.get_klines.assert_awaited_once_with(
            symbol="BTCUSDT",
            interval="1m",
            limit=1000,
        )

    async def test_market_data_response_feeds_rate_limit_tracker(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_market_data(adapter, binance_client_mock, BinanceProduct.SPOT)
        binance_client_mock.get_klines.return_value = [_kline_row(_BASE_OPEN_MS)]
        binance_client_mock.response.headers = {"x-mbx-used-weight-1m": "123"}

        await adapter.fetch_candles(_spot_instrument(), CandleTimeframe.M1, limit=1)

        usage = adapter._rate_limit_tracker._usage[BinanceProduct.SPOT]
        assert usage[("REQUEST_WEIGHT", 1, "M")].used == 123


class TestCoinMContractType:
    def test_perpetual_maps_to_perpetual(self) -> None:
        assert (
            coin_m_contract_type(_exchange_info(BinanceProduct.COIN_M), symbol="BTCUSD_PERP")
            == "PERPETUAL"
        )

    def test_delivering_contract_strips_suffix(self) -> None:
        entry = dict(_exchange_info(BinanceProduct.COIN_M))
        entry["contractType"] = "CURRENT_QUARTER DELIVERING"

        assert coin_m_contract_type(entry, symbol="BTCUSD_261225") == "CURRENT_QUARTER"

    def test_missing_contract_type_raises(self) -> None:
        entry = dict(_exchange_info(BinanceProduct.COIN_M))
        entry.pop("contractType")

        with pytest.raises(PlatformError, match="contractType"):
            coin_m_contract_type(entry, symbol="BTCUSD_PERP")

    def test_unknown_contract_type_raises(self) -> None:
        entry = dict(_exchange_info(BinanceProduct.COIN_M))
        entry["contractType"] = "TRADIFI_PERPETUAL"

        with pytest.raises(PlatformError, match="klines mapping"):
            coin_m_contract_type(entry, symbol="BTCUSD_PERP")


class TestSymbolEntry:
    def test_dict_response_is_returned(self) -> None:
        response = {"symbol": "BTCUSDT", "lastPrice": "1"}

        assert symbol_entry(response, symbol="BTCUSDT", product=BinanceProduct.SPOT) == response

    def test_dict_without_symbol_is_accepted(self) -> None:
        response = {"lastPrice": "1"}

        assert symbol_entry(response, symbol="BTCUSDT", product=BinanceProduct.SPOT) == response

    def test_dict_symbol_mismatch_raises(self) -> None:
        with pytest.raises(PlatformError, match="symbol mismatch"):
            symbol_entry(
                {"symbol": "ETHUSDT"},
                symbol="BTCUSDT",
                product=BinanceProduct.SPOT,
            )

    def test_list_response_filters_by_symbol(self) -> None:
        response = [
            {"symbol": "ETHUSDT", "lastPrice": "1"},
            {"symbol": "BTCUSDT", "lastPrice": "2"},
        ]

        entry = symbol_entry(response, symbol="BTCUSDT", product=BinanceProduct.COIN_M)

        assert entry == {"symbol": "BTCUSDT", "lastPrice": "2"}

    def test_list_without_match_returns_none(self) -> None:
        assert (
            symbol_entry(
                [{"symbol": "ETHUSDT"}],
                symbol="BTCUSDT",
                product=BinanceProduct.COIN_M,
            )
            is None
        )

    def test_non_object_response_raises(self) -> None:
        with pytest.raises(PlatformError, match="not an object or list"):
            symbol_entry("oops", symbol="BTCUSDT", product=BinanceProduct.SPOT)

    def test_non_object_list_entry_raises(self) -> None:
        with pytest.raises(PlatformError, match="non-object entry"):
            symbol_entry(["oops"], symbol="BTCUSDT", product=BinanceProduct.COIN_M)

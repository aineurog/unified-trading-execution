"""Read-only Binance Demo integration coverage for ticker and candle reads.

These tests exercise the public market-data endpoints through the adapter on
the Demo Trading environment. They require ``BINANCE_DEMO=true`` and no
credentials: ``demo_metadata_adapter`` injects a verified Demo SDK client.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceProduct
from unified_trading_execution.binance.symbols import (
    from_binance_symbol,
    parse_instrument_spec,
)
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.trailing.types import CandleTimeframe, TrailingBasis
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_PRODUCTS = [
    pytest.param(BinanceProduct.SPOT, id="spot"),
    pytest.param(BinanceProduct.USD_M, id="usd-m"),
    pytest.param(BinanceProduct.COIN_M, id="coin-m"),
]

_LAST_PRICE_METHODS = {
    BinanceProduct.SPOT: "get_ticker",
    BinanceProduct.USD_M: "futures_symbol_ticker",
    BinanceProduct.COIN_M: "futures_coin_symbol_ticker",
}
_LAST_PRICE_FIELDS = {
    BinanceProduct.SPOT: "lastPrice",
    BinanceProduct.USD_M: "price",
    BinanceProduct.COIN_M: "price",
}
_BOOK_TICKER_METHODS = {
    BinanceProduct.SPOT: "get_orderbook_ticker",
    BinanceProduct.USD_M: "futures_orderbook_ticker",
    BinanceProduct.COIN_M: "futures_coin_orderbook_ticker",
}
_MARK_METHODS = {
    BinanceProduct.USD_M: "futures_mark_price",
    BinanceProduct.COIN_M: "futures_coin_mark_price",
}
_KLINES_METHODS = {
    BinanceProduct.SPOT: "get_klines",
    BinanceProduct.USD_M: "futures_klines",
    BinanceProduct.COIN_M: "futures_coin_klines",
}


def _tradable_instrument(
    entries: list[object],
    product: BinanceProduct,
) -> tuple[Instrument, str]:
    """Pick a TRADING catalog row, preferring BTC for liquid Demo quotes."""
    fallback: tuple[Instrument, str] | None = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        status = entry.get("status")
        if status is None:
            status = entry.get("contractStatus")
        if status != "TRADING":
            continue
        try:
            instrument = from_binance_symbol(entry, product)
            parse_instrument_spec(entry)
        except (InvalidSymbolError, PlatformError):
            continue
        symbol = str(entry["symbol"])
        if fallback is None:
            fallback = (instrument, symbol)
        if "BTC" in symbol:
            return instrument, symbol
    if fallback is None:
        raise AssertionError(f"No tradable {product.value} symbol in Demo exchange info")
    return fallback


async def _load_entries(adapter: BinanceAdapter, product: BinanceProduct) -> list[object]:
    return await adapter._load_exchange_info(product)


def _symbol_row(response: object, symbol: str) -> dict[str, Any]:
    """Normalize dict-or-list venue payloads to the symbol's row."""
    if isinstance(response, dict):
        return response
    assert isinstance(response, list)
    for entry in response:
        if isinstance(entry, dict) and entry.get("symbol") == symbol:
            return entry
    raise AssertionError(f"No venue row for {symbol}")


def _assert_close(actual: Decimal, expected: Decimal, field: str) -> None:
    """Assert two sequential live quotes agree within a small tolerance."""
    assert actual > 0 and expected > 0, f"{field}: non-positive quote"
    drift = abs(actual - expected) / expected
    assert drift <= Decimal("0.01"), f"{field} drifted {drift}: {actual} vs {expected}"


@pytest.mark.parametrize("product", _PRODUCTS)
async def test_ticker_matches_live_quote(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """fetch_ticker agrees with the raw public endpoints for every product."""
    entries = await _load_entries(demo_metadata_adapter, product)
    instrument, symbol = _tradable_instrument(entries, product)
    client = demo_metadata_adapter._client
    assert client is not None

    capability = demo_metadata_adapter.market_data_capability(instrument)
    ticker = await demo_metadata_adapter.fetch_ticker(instrument)
    assert ticker is not None, f"{product.value} {symbol} must have a live quote"

    raw_last = _symbol_row(
        await getattr(client, _LAST_PRICE_METHODS[product])(symbol=symbol), symbol
    )
    _assert_close(
        ticker.last or Decimal(0),
        Decimal(str(raw_last[_LAST_PRICE_FIELDS[product]])),
        "last",
    )

    raw_book = _symbol_row(
        await getattr(client, _BOOK_TICKER_METHODS[product])(symbol=symbol), symbol
    )
    if ticker.bid is not None:
        _assert_close(ticker.bid, Decimal(str(raw_book["bidPrice"])), "bid")
    if ticker.ask is not None:
        _assert_close(ticker.ask, Decimal(str(raw_book["askPrice"])), "ask")
    if ticker.bid is not None and ticker.ask is not None:
        assert ticker.ask >= ticker.bid

    if product == BinanceProduct.SPOT:
        assert TrailingBasis.MARK not in capability.ticker_bases
        assert ticker.mark is None
    else:
        assert TrailingBasis.MARK in capability.ticker_bases
        raw_mark = _symbol_row(await getattr(client, _MARK_METHODS[product])(symbol=symbol), symbol)
        assert ticker.mark is not None
        _assert_close(ticker.mark, Decimal(str(raw_mark["markPrice"])), "mark")


@pytest.mark.parametrize("product", _PRODUCTS)
async def test_ticker_capability_bases_are_populated(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """Every declared ticker basis is populated by a live fetch."""
    entries = await _load_entries(demo_metadata_adapter, product)
    instrument, _symbol = _tradable_instrument(entries, product)
    capability = demo_metadata_adapter.market_data_capability(instrument)
    ticker = await demo_metadata_adapter.fetch_ticker(instrument)

    assert ticker is not None
    populated = {
        TrailingBasis.LAST: ticker.last,
        TrailingBasis.BID: ticker.bid,
        TrailingBasis.ASK: ticker.ask,
        TrailingBasis.MARK: ticker.mark,
    }
    for basis in capability.ticker_bases:
        assert populated[basis] is not None, f"{basis} declared but not populated"


@pytest.mark.parametrize("product", _PRODUCTS)
async def test_candles_are_closed_oldest_first(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """fetch_candles returns only closed bars, oldest first, within limit."""
    entries = await _load_entries(demo_metadata_adapter, product)
    instrument, symbol = _tradable_instrument(entries, product)
    client = demo_metadata_adapter._client
    assert client is not None

    timeframe = CandleTimeframe.M5
    limit = 10
    candles = await demo_metadata_adapter.fetch_candles(instrument, timeframe, limit=limit)

    assert 0 < len(candles) <= limit, f"{product.value} {symbol} returned {len(candles)} candles"
    now = datetime.now(UTC)
    previous_close: datetime | None = None
    for candle in candles:
        assert candle.close_time <= now, "fetch_candles must never return the forming bar"
        assert candle.close_time > candle.open_time
        assert candle.high >= max(candle.open, candle.close)
        assert candle.low <= min(candle.open, candle.close)
        if previous_close is not None:
            assert candle.open_time >= previous_close, "candles must be oldest first"
        previous_close = candle.close_time

    # Cross-check the newest closed bar against the raw venue klines payload.
    interval = {CandleTimeframe.M5: "5m"}[timeframe]
    raw = await getattr(client, _KLINES_METHODS[product])(
        symbol=symbol, interval=interval, limit=limit + 1
    )
    assert isinstance(raw, list) and raw
    newest_raw_open_ms = int(raw[-2][0])  # last row is the forming bar
    assert candles[-1].open_time == datetime.fromtimestamp(newest_raw_open_ms / 1000, tz=UTC)


@pytest.mark.parametrize("product", _PRODUCTS)
async def test_candles_every_declared_timeframe(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """All nine core timeframes are served by every Binance product."""
    entries = await _load_entries(demo_metadata_adapter, product)
    instrument, _symbol = _tradable_instrument(entries, product)
    capability = demo_metadata_adapter.market_data_capability(instrument)

    assert capability.candle_timeframes == frozenset(CandleTimeframe)
    for timeframe in sorted(capability.candle_timeframes, key=lambda item: item.value):
        candles = await demo_metadata_adapter.fetch_candles(instrument, timeframe, limit=2)
        assert isinstance(candles, list)
        for candle in candles:
            assert candle.close_time <= datetime.now(UTC)


@pytest.mark.parametrize("product", _PRODUCTS)
async def test_candles_limit_and_validation(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    entries = await _load_entries(demo_metadata_adapter, product)
    instrument, _symbol = _tradable_instrument(entries, product)

    with pytest.raises(ValueError, match="at least 1"):
        await demo_metadata_adapter.fetch_candles(instrument, CandleTimeframe.M1, limit=0)

    one = await demo_metadata_adapter.fetch_candles(instrument, CandleTimeframe.M1, limit=1)
    assert len(one) <= 1


async def test_ticker_invalid_instrument_raises(
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """An asset class Binance does not support fails before any network call."""
    bad = Instrument(symbol="X", asset_class=AssetClass.BOND, currency="USD")
    with pytest.raises(InvalidSymbolError):
        await demo_metadata_adapter.fetch_ticker(bad)


async def test_candles_unknown_platform_symbol_raises(
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """A symbol absent from exchange info raises InvalidSymbolError."""
    instrument = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT_NOTREAL",
    )
    with pytest.raises(InvalidSymbolError):
        await demo_metadata_adapter.fetch_candles(instrument, CandleTimeframe.M1, limit=5)


async def test_rate_limits_snapshot_after_market_data(
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """Market-data responses feed the shared rate-limit tracker."""
    entries = await _load_entries(demo_metadata_adapter, BinanceProduct.SPOT)
    instrument, _symbol = _tradable_instrument(entries, BinanceProduct.SPOT)

    await demo_metadata_adapter.fetch_ticker(instrument)
    limits = await demo_metadata_adapter.get_rate_limits()

    assert limits.requests_per_interval >= 1
    assert limits.interval_seconds > 0

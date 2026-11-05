"""Tests for Binance exchange-info filter parsing and adapter lookup."""

from __future__ import annotations

from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from unified_trading_execution.binance.symbols import parse_instrument_spec
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    PlatformError,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec

_FILTERS: list[dict[str, str]] = [
    {
        "filterType": "PRICE_FILTER",
        "minPrice": "0.01000000",
        "maxPrice": "1000000.00000000",
        "tickSize": "0.01000000",
    },
    {
        "filterType": "LOT_SIZE",
        "minQty": "0.00100000",
        "maxQty": "9000.00000000",
        "stepSize": "0.00100000",
    },
    {"filterType": "MIN_NOTIONAL", "minNotional": "5.00"},
]
_SPOT_ENTRY: dict[str, object] = {
    "symbol": "BTCUSDT",
    "status": "TRADING",
    "baseAsset": "BTC",
    "quoteAsset": "USDT",
    "filters": _FILTERS,
}
_USD_M_ENTRY: dict[str, object] = {
    "symbol": "ETHUSDT",
    "status": "TRADING",
    "baseAsset": "ETH",
    "quoteAsset": "USDT",
    "marginAsset": "USDT",
    "contractType": "PERPETUAL",
    "filters": [
        _FILTERS[0],
        _FILTERS[1],
        {"filterType": "MIN_NOTIONAL", "notional": "10"},
    ],
}
_COIN_M_ENTRY: dict[str, object] = {
    "symbol": "BTCUSD_PERP",
    "status": "TRADING",
    "baseAsset": "BTC",
    "quoteAsset": "USD",
    "marginAsset": "BTC",
    "contractType": "PERPETUAL",
    "contractSize": 100,
    "filters": _FILTERS,
}


def _spot_instrument() -> Instrument:
    return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)


def _eth_spot_instrument() -> Instrument:
    return Instrument(symbol="ETH", quote_currency="USDT", asset_class=AssetClass.SPOT)


def _usd_m_instrument() -> Instrument:
    return Instrument(
        symbol="ETH",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        multiplier=1,
    )


def _coin_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        multiplier=1,
    )


@pytest.mark.parametrize(
    ("entry", "expected_notional"),
    [(_SPOT_ENTRY, Decimal("5.00")), (_USD_M_ENTRY, Decimal("10"))],
    ids=("spot-minNotional", "futures-notional"),
)
def test_parses_exact_filters(
    entry: dict[str, object],
    expected_notional: Decimal,
) -> None:
    spec = parse_instrument_spec(entry)
    assert spec == InstrumentSpec(
        tick_size=Decimal("0.01000000"),
        lot_size=Decimal("0.00100000"),
        min_qty=Decimal("0.00100000"),
        max_qty=Decimal("9000.00000000"),
        min_notional=expected_notional,
        price_precision=2,
        qty_precision=3,
        max_leverage=None,
        min_stop_distance=None,
    )


def test_parses_larger_minimum_across_notional_filters() -> None:
    entry = {
        **_SPOT_ENTRY,
        "filters": [
            *_FILTERS,
            {"filterType": "NOTIONAL", "minNotional": "7"},
        ],
    }
    assert parse_instrument_spec(entry).min_notional == Decimal("7")


@pytest.mark.parametrize(
    "filters",
    [
        [],
        [_FILTERS[1]],
        [_FILTERS[0], {**_FILTERS[1], "stepSize": "0"}],
        [_FILTERS[0], {**_FILTERS[1], "maxQty": "NaN"}],
        [_FILTERS[0], {**_FILTERS[1], "minQty": "2", "maxQty": "1"}],
        [*_FILTERS, _FILTERS[0]],
        [_FILTERS[0], _FILTERS[1], {"filterType": "MIN_NOTIONAL"}],
    ],
    ids=(
        "empty",
        "missing-price-filter",
        "zero-step",
        "non-finite-max-qty",
        "reversed-quantity-bounds",
        "duplicate-filter",
        "missing-notional-value",
    ),
)
def test_rejects_invalid_filter_metadata(filters: list[dict[str, str]]) -> None:
    with pytest.raises(PlatformError):
        parse_instrument_spec({"symbol": "BTCUSDT", "filters": filters})


@pytest.mark.parametrize(
    ("instrument", "entry", "method_name"),
    [
        (
            _spot_instrument(),
            _SPOT_ENTRY,
            "get_exchange_info",
        ),
        (
            _usd_m_instrument(),
            _USD_M_ENTRY,
            "futures_exchange_info",
        ),
        (
            _coin_m_instrument(),
            _COIN_M_ENTRY,
            "futures_coin_exchange_info",
        ),
    ],
    ids=("spot", "usd-m", "coin-m"),
)
async def test_fetch_spec_routes_by_product_and_caches(
    adapter,
    binance_client_mock: AsyncMock,
    instrument: Instrument,
    entry: dict[str, object],
    method_name: str,
) -> None:
    method = getattr(binance_client_mock, method_name)
    method.return_value = {"symbols": [entry]}
    adapter._client = cast(AsyncClient, binance_client_mock)

    first = await adapter.fetch_instrument_spec(instrument)
    second = await adapter.fetch_instrument_spec(instrument)

    assert first == second
    assert first.max_leverage is None
    method.assert_awaited_once_with()
    assert adapter._instrument_spec_cache[(instrument, instrument.platform_symbol)][0] == first
    for other_method_name in (
        "get_exchange_info",
        "futures_exchange_info",
        "futures_coin_exchange_info",
    ):
        if other_method_name != method_name:
            getattr(binance_client_mock, other_method_name).assert_not_awaited()


async def test_fetch_spec_requires_connected_client(adapter) -> None:
    with pytest.raises(PlatformConnectionError, match="must be connected"):
        await adapter.fetch_instrument_spec(_spot_instrument())


async def test_exchange_info_catalog_is_cached_per_product(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    eth_entry = {
        **_SPOT_ENTRY,
        "symbol": "ETHUSDT",
        "baseAsset": "ETH",
    }
    binance_client_mock.get_exchange_info.return_value = {"symbols": [_SPOT_ENTRY, eth_entry]}
    adapter._client = cast(AsyncClient, binance_client_mock)

    await adapter.fetch_instrument_spec(_spot_instrument())
    await adapter.fetch_instrument_spec(_eth_spot_instrument())

    binance_client_mock.get_exchange_info.assert_awaited_once_with()
    assert len(adapter._exchange_info_cache) == 1


async def test_fetch_spec_rejects_non_trading_symbol(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    entry = {**_SPOT_ENTRY, "status": "BREAK"}
    binance_client_mock.get_exchange_info.return_value = {"symbols": [entry]}
    adapter._client = cast(AsyncClient, binance_client_mock)

    with pytest.raises(InvalidSymbolError, match="not trading"):
        await adapter.fetch_instrument_spec(_spot_instrument())


async def test_fetch_spec_uses_coin_m_contract_status(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    entry = {
        **_COIN_M_ENTRY,
        "status": None,
        "contractStatus": "TRADING",
    }
    binance_client_mock.futures_coin_exchange_info.return_value = {"symbols": [entry]}
    adapter._client = cast(AsyncClient, binance_client_mock)

    spec = await adapter.fetch_instrument_spec(_coin_m_instrument())
    assert spec.tick_size == Decimal("0.01000000")


@pytest.mark.parametrize(
    "response",
    [None, {}, {"symbols": None}],
    ids=("not-object", "missing-symbols", "invalid-symbols"),
)
async def test_fetch_spec_rejects_malformed_exchange_info(
    adapter,
    binance_client_mock: AsyncMock,
    response: object,
) -> None:
    binance_client_mock.get_exchange_info.return_value = response
    adapter._client = cast(AsyncClient, binance_client_mock)

    with pytest.raises(PlatformError, match="exchange-info"):
        await adapter.fetch_instrument_spec(_spot_instrument())


async def test_fetch_spec_translates_binance_api_errors(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    api_error = BinanceAPIException(MagicMock(), 400, '{"code": -1121, "msg": "Invalid symbol."}')
    binance_client_mock.get_exchange_info.side_effect = api_error
    adapter._client = cast(AsyncClient, binance_client_mock)

    with pytest.raises(PlatformError, match="exchange-info request failed") as info:
        await adapter.fetch_instrument_spec(_spot_instrument())

    assert info.value.platform_error is api_error


async def test_fetch_spec_translates_transport_errors(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    binance_client_mock.get_exchange_info.side_effect = ConnectionResetError("reset")
    adapter._client = cast(AsyncClient, binance_client_mock)

    with pytest.raises(PlatformConnectionError, match="exchange-info request failed"):
        await adapter.fetch_instrument_spec(_spot_instrument())

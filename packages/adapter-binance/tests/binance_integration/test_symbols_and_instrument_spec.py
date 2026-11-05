"""Read-only Binance Demo integration coverage for symbols and specs."""

from __future__ import annotations

from typing import Any

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceProduct
from unified_trading_execution.binance.symbols import (
    from_binance_symbol,
    parse_instrument_spec,
    resolve_binance_symbol,
    to_binance_symbol,
)
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.types.instrument import Instrument

_EXCHANGE_INFO_METHODS = {
    BinanceProduct.SPOT: "get_exchange_info",
    BinanceProduct.USD_M: "futures_exchange_info",
    BinanceProduct.COIN_M: "futures_coin_exchange_info",
}


def _find_tradable_instrument(
    response: dict[str, Any],
    product: BinanceProduct,
) -> tuple[Instrument, str]:
    symbols = response.get("symbols")
    if not isinstance(symbols, list):
        raise AssertionError(f"Binance {product.value} response has no symbols list")

    for entry in symbols:
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
        return instrument, str(entry["symbol"])

    raise AssertionError(f"No tradable {product.value} symbol has supported instrument filters")


@pytest.mark.parametrize(
    "product",
    [BinanceProduct.SPOT, BinanceProduct.USD_M, BinanceProduct.COIN_M],
    ids=("spot", "usd-m", "coin-m"),
)
async def test_symbol_round_trip_and_instrument_spec(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """Resolve a live Demo catalog row and fetch its normalized spec."""
    client = demo_metadata_adapter._client
    response = await getattr(client, _EXCHANGE_INFO_METHODS[product])()
    instrument, expected_symbol = _find_tradable_instrument(response, product)

    assert to_binance_symbol(instrument, product) == expected_symbol
    resolved_symbol, resolved_entry = resolve_binance_symbol(
        instrument,
        product,
        response["symbols"],
    )
    assert resolved_symbol == expected_symbol
    assert from_binance_symbol(resolved_entry, product) == instrument

    spec = await demo_metadata_adapter.fetch_instrument_spec(instrument)

    assert spec.tick_size > 0
    assert spec.lot_size > 0
    assert spec.min_qty >= 0
    assert spec.max_qty >= spec.min_qty
    assert spec.min_notional >= 0
    assert demo_metadata_adapter._exchange_info_cache[product]

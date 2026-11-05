"""Read-only Binance Demo integration coverage for REST rate-limit headers."""

from __future__ import annotations

import re
from typing import Any

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceProduct
from unified_trading_execution.binance.symbols import from_binance_symbol, parse_instrument_spec
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.types.instrument import Instrument

_EXCHANGE_INFO_METHODS = {
    BinanceProduct.SPOT: "get_exchange_info",
    BinanceProduct.USD_M: "futures_exchange_info",
    BinanceProduct.COIN_M: "futures_coin_exchange_info",
}
_WEIGHT_HEADER = re.compile(r"^x-mbx-used-weight-\d+[smhd]$", re.IGNORECASE)


def _find_tradable_instrument(response: dict[str, Any], product: BinanceProduct) -> Instrument:
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
        return instrument

    raise AssertionError(f"No tradable {product.value} symbol has supported instrument filters")


@pytest.mark.parametrize(
    "product",
    [BinanceProduct.SPOT, BinanceProduct.USD_M, BinanceProduct.COIN_M],
    ids=("spot", "usd-m", "coin-m"),
)
async def test_demo_exchange_info_updates_rate_limit_observation(
    product: BinanceProduct,
    demo_metadata_adapter: BinanceAdapter,
) -> None:
    """Observe headers from a real, read-only exchange-info response."""
    client = demo_metadata_adapter._client
    response = await getattr(client, _EXCHANGE_INFO_METHODS[product])()
    if not isinstance(response, dict):
        raise AssertionError(f"Binance {product.value} exchange-info response is not an object")
    instrument = _find_tradable_instrument(response, product)

    await demo_metadata_adapter.fetch_instrument_spec(instrument)
    limits = await demo_metadata_adapter.get_rate_limits()

    sdk_response = client.response
    headers = sdk_response.headers
    assert any(_WEIGHT_HEADER.fullmatch(name) for name in headers)
    assert product in demo_metadata_adapter._rate_limit_tracker._usage
    assert any(
        key[0] == "REQUEST_WEIGHT"
        for key in demo_metadata_adapter._rate_limit_tracker._usage[product]
    )
    assert limits.requests_per_interval >= 1
    assert limits.remaining >= 0
    # Order count headers are absent on metadata calls, so the observed
    # one minute weight window must drive the snapshot, not an unseen
    # daily order bucket.
    assert limits.interval_seconds <= 60.0
    assert limits.reset_at.tzinfo is not None

"""Integration: instrument specs from live meta (perps + spot).

Gate: specs carry tick/lot/min/max/maxLeverage, cache with TTL, unknown coins raise.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.instrument import Instrument

from .helpers import assert_is_decimal, make_perp, make_spot


async def test_btc_perp_spec_shape(
    connected_adapter: HyperliquidAdapter, btc_perp: Instrument
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    assert spec.tick_size > 0
    assert spec.lot_size > 0
    assert spec.max_leverage is not None and spec.max_leverage >= 1
    assert spec.min_notional == Decimal("10")  # venue $10 floor
    assert_is_decimal(spec.tick_size, "tick_size")
    assert_is_decimal(spec.lot_size, "lot_size")


async def test_eth_perp_spec_shape(
    connected_adapter: HyperliquidAdapter, eth_perp: Instrument
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(eth_perp)
    assert spec.tick_size > 0
    assert spec.lot_size > 0
    assert spec.max_leverage is not None and spec.max_leverage >= 1


async def test_spot_spec_shape(connected_adapter: HyperliquidAdapter) -> None:
    spec = await connected_adapter.fetch_instrument_spec(make_spot("PURR"))
    assert spec.tick_size > 0
    assert spec.lot_size > 0


async def test_spec_cache_returns_same_object(
    connected_adapter: HyperliquidAdapter, btc_perp: Instrument
) -> None:
    first = await connected_adapter.fetch_instrument_spec(btc_perp)
    second = await connected_adapter.fetch_instrument_spec(btc_perp)
    assert first == second


async def test_unknown_coin_raises(
    connected_adapter: HyperliquidAdapter,
) -> None:
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_instrument_spec(make_perp("NOPECOINZZZ"))


async def test_perp_and_spot_btc_differ(
    connected_adapter: HyperliquidAdapter, btc_perp: Instrument
) -> None:
    perp_spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    spot_spec = await connected_adapter.fetch_instrument_spec(make_spot("BTC"))
    assert (perp_spec.tick_size, perp_spec.lot_size) != (
        spot_spec.tick_size,
        spot_spec.lot_size,
    )

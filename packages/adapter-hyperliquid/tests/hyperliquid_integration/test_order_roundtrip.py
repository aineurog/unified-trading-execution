"""Integration: order write roundtrips with real fills.

Gate: LIMIT place/get/cancel, MARKET fill with attribution, modify-by-cloid,
cancel idempotency. Leaves the book flat (flattened_book fixture).
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest

from unified_trading_execution.errors import OrderNotFoundError
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument

from .helpers import (
    build_unified_order,
    modify_price,
    valid_price_from_spec,
    valid_qty_from_spec,
    venue_price,
    wait_for_fill,
)


async def test_limit_place_get_cancel(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    flattened_book: None,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    # Rest far below the touch so it never fills: half of mid, tick-floored.
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    # Size from the ORDER price, not mid — notional is qty times resting price.
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("lim")
    placed = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.LIMIT,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            price=px,
        )
    )
    assert placed.status == OrderStatus.OPEN
    assert placed.platform_order_id

    seen = await connected_adapter.get_order_by_client_id(cid)
    assert seen is not None
    assert seen.status == OrderStatus.OPEN

    cancelled = await connected_adapter.cancel_order(cid)
    assert cancelled.status == OrderStatus.CANCELLED


async def test_market_fill_with_attribution(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    cid = unique_cid("mkt")
    result = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            time_in_force=TimeInForce.IOC,
        )
    )
    assert result.status == OrderStatus.FILLED
    assert result.filled_quantity == qty

    filled = await wait_for_fill(connected_adapter, cid)
    assert filled.status == OrderStatus.FILLED

    fills = await connected_adapter.fetch_fills()
    mine = fills.get(cid, [])
    assert mine, "market fill must attribute back to our client id"
    # The venue may split one IOC across book levels (partial fills are
    # legitimate) — attribution is per fill, completeness is the sum.
    assert sum((f.fill_quantity for f in mine), Decimal("0")) == qty


async def test_modify_resting_limit_price(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    flattened_book: None,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    # Size from the ORDER price, not mid — notional is qty times resting price.
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("mod")
    placed = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.LIMIT,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            price=px,
        )
    )
    assert placed.status == OrderStatus.OPEN

    new_px = valid_price_from_spec(spec, px * Decimal("0.9"))
    modified = await connected_adapter.modify_order(modify_price(cid, new_px))
    assert modified.status == OrderStatus.OPEN

    await connected_adapter.cancel_order(cid)


async def test_cancel_unknown_cloid_raises(
    connected_adapter: HyperliquidAdapter,
    unique_cid: Callable[[str], str],
) -> None:
    with pytest.raises(OrderNotFoundError):
        await connected_adapter.cancel_order(unique_cid("ghost"))

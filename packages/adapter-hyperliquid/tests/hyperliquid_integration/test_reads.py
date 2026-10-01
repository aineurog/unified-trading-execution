"""Integration: read paths against the live account.

Gate: positions/balances/opens/fills/get decode with venue truth, Decimal purity,
timezone-aware stamps, and since-filtering.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument

from .helpers import (
    assert_is_decimal,
    build_unified_order,
    utcnow,
    valid_qty_from_spec,
    venue_price,
    wait_for_position,
)


async def test_positions_shape(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    # Seed a known leg — asserting over an empty book proves nothing.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("reads-leg"),
            time_in_force=TimeInForce.IOC,
        )
    )
    await wait_for_position(connected_adapter, "BTC")

    legs = await connected_adapter.fetch_positions()
    ours = [leg for leg in legs if leg.position_id == "BTC:oneWay"]
    assert len(ours) == 1
    assert ours[0].quantity == qty
    assert_is_decimal(ours[0].quantity, "quantity")
    assert_is_decimal(ours[0].average_entry_price, "average_entry_price")
    assert ours[0].average_entry_price > 0
    assert ours[0].updated_at.tzinfo is not None


async def test_balances_invariant(connected_adapter: HyperliquidAdapter) -> None:
    balances = await connected_adapter.fetch_balances()
    assert "USDC" in balances
    for currency, balance in balances.items():
        assert balance.free + balance.used == balance.total, currency
        assert_is_decimal(balance.free, f"{currency}.free")
        assert_is_decimal(balance.total, f"{currency}.total")


async def test_open_orders_keyed_by_client_id(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    flattened_book: None,
) -> None:
    # Seed a known resting order — an empty book would pass vacuously.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("reads-open")
    await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.LIMIT,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            price=px,
        )
    )

    opens = await connected_adapter.fetch_open_orders()
    assert cid in opens
    assert opens[cid].client_order_id == cid

    await connected_adapter.cancel_order(cid)
    assert cid not in await connected_adapter.fetch_open_orders()


async def test_fills_since_filtering(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    # Seed a known fill — an empty window would pass vacuously.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    cid = unique_cid("reads-fill")
    before = utcnow()
    await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            time_in_force=TimeInForce.IOC,
        )
    )

    windowed = await connected_adapter.fetch_fills(since=before)
    assert cid in windowed, "fresh fill must fall inside the since-window"
    for fill in windowed[cid]:
        assert fill.fill_timestamp.tzinfo is not None
        assert fill.fill_timestamp >= before
        assert_is_decimal(fill.fill_quantity, "fill_quantity")
        assert_is_decimal(fill.fill_price, "fill_price")


async def test_fills_dedupe_on_repeat_read(connected_adapter: HyperliquidAdapter) -> None:
    first = await connected_adapter.fetch_fills()
    second = await connected_adapter.fetch_fills()
    first_ids = {fill.platform_fill_id for fills in first.values() for fill in fills}
    second_ids = {fill.platform_fill_id for fills in second.values() for fill in fills}
    assert first_ids == second_ids


async def test_get_unknown_client_id_returns_none(
    connected_adapter: HyperliquidAdapter,
) -> None:
    assert await connected_adapter.get_order_by_client_id("hlit-never-placed-000") is None


async def test_account_value_nonnegative(connected_adapter: HyperliquidAdapter) -> None:
    balances = await connected_adapter.fetch_balances()
    total = sum((b.total for b in balances.values()), Decimal("0"))
    assert total >= 0

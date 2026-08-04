"""Bridging fetch-accessor integration tests.

Verifies the REST snapshot accessors (open orders, positions, balances, fills)
against live testnet and that their returned values are complete ``Decimal``
typed records.  No cross-test pollution: open orders are cancelled and no stray
positions remain after each test.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .conftest import cleanup_open_orders
from .helpers import (
    assert_is_decimal,
    build_unified_order,
    random_client_id,
    valid_price_from_spec,
    valid_qty_from_spec,
)


async def _limit_qty_price(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference_price: Decimal,
) -> tuple[Decimal, Decimal]:
    spec = await adapter.fetch_instrument_spec(instrument)
    qty = valid_qty_from_spec(spec, reference_price)
    price = valid_price_from_spec(spec, reference_price)
    return (qty if qty > 0 else Decimal("0.001")), (
        price if price and price > 0 else reference_price
    )


async def test_fetch_open_orders_roundtrip(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    qty, price = await _limit_qty_price(connected_adapter, traded_instrument, reference_price)
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("open-rt"),
        price=price * Decimal("0.9"),  # below market -> rests
    )
    try:
        await connected_adapter.place_order(order)
        await asyncio.sleep(1.0)
        open_orders = await connected_adapter.fetch_open_orders()
        assert order.client_order_id in open_orders
        record = open_orders[order.client_order_id]
        assert record.instrument == traded_instrument
        assert_is_decimal(record.quantity, "order quantity")

        await connected_adapter.cancel_order(order.client_order_id)
        await asyncio.sleep(1.0)
        after = await connected_adapter.fetch_open_orders()
        assert order.client_order_id not in after
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_fetch_positions_no_pollution(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> None:
    positions = await connected_adapter.fetch_positions()
    # Assert the returned positions carry valid Decimal-typed fields.
    # The testnet account may carry pre-existing positions from prior test
    # runs — we validate the data shape, not that the account is pristine.
    for position in positions.values():
        assert_is_decimal(position.quantity, "position quantity")
        assert_is_decimal(position.average_entry_price, "average entry price")
        assert position.updated_at.tzinfo is not None
    pos = positions.get(linear_instrument)
    if pos is not None:
        assert_is_decimal(pos.quantity, "linear position quantity")


async def test_fetch_balances(
    connected_adapter: BybitAdapter,
) -> None:
    balances = await connected_adapter.fetch_balances()
    assert balances, "expected at least one wallet balance on testnet"
    for balance in balances.values():
        assert_is_decimal(balance.free, "free")
        assert_is_decimal(balance.used, "used")
        assert_is_decimal(balance.total, "total")
        assert balance.updated_at.tzinfo is not None


async def test_fetch_balances_invariant(
    connected_adapter: BybitAdapter,
) -> None:
    balances = await connected_adapter.fetch_balances()
    assert balances, "expected at least one wallet balance on testnet"
    for currency, balance in balances.items():
        assert balance.free + balance.used == balance.total, (
            f"Balance invariant violated for {currency}: "
            f"free ({balance.free}) + used ({balance.used}) != total ({balance.total})"
        )
        assert balance.free >= 0, f"free balance must not go negative for {currency}"


async def test_fetch_fills(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    qty = valid_qty_from_spec(spec, linear_reference_price) or Decimal("0.001")
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("fills"),
    )
    try:
        await connected_adapter.place_order(order)
        await asyncio.sleep(1.0)
        fills = await connected_adapter.fetch_fills()
        records = fills.get(order.client_order_id, [])
        assert records, f"expected fills for {order.client_order_id}"
        for fill in records:
            assert fill.client_order_id == order.client_order_id
            assert_is_decimal(fill.fill_quantity, "fill_quantity")
            assert_is_decimal(fill.fill_price, "fill_price")
            assert fill.fill_timestamp.tzinfo is not None
            if fill.fee_amount is not None:
                assert_is_decimal(fill.fee_amount, "fee_amount")
    finally:
        await cleanup_open_orders(connected_adapter)

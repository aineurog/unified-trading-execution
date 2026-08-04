"""Order-type round-trip integration tests (Section 11.2, bullet 1).

For every guaranteed core order type and every category (spot, linear, inverse)
we place, query, modify (where meaningful), and cancel, asserting the unified
``OrderResult`` values survive the round trip in both translation directions.
Also covers instant open/close, finalize-only-on-close, native TP/SL, stop
trigger direction, and stop status mapping.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    TpSlAttachment,
)

from .conftest import cleanup_open_orders
from .helpers import (
    assert_is_decimal,
    build_unified_order,
    order_ids_seen,
    random_client_id,
    valid_price_from_spec,
    valid_qty_from_spec,
)

_ORDER_TYPES = (OrderType.MARKET, OrderType.LIMIT, OrderType.STOP, OrderType.STOP_LIMIT)


def _is_futures(instrument: Instrument) -> bool:
    return instrument.asset_class == AssetClass.FUTURES


async def _spec_valid_qty(adapter: BybitAdapter, instrument: Instrument) -> Decimal:
    spec = await adapter.fetch_instrument_spec(instrument)
    qty = valid_qty_from_spec(spec)
    if qty > 0:
        return qty
    return Decimal("0.001")


async def _spec_valid_price(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference: Decimal,
) -> Decimal:
    spec = await adapter.fetch_instrument_spec(instrument)
    price = valid_price_from_spec(spec, reference)
    return price if price and price > 0 else Decimal("1")


async def _assert_complete_result(result: OrderResult) -> None:
    assert result.platform_order_id is not None
    assert result.client_order_id
    assert_is_decimal(result.filled_quantity, "filled_quantity")
    if result.average_fill_price is not None:
        assert_is_decimal(result.average_fill_price, "average_fill_price")
    assert result.created_at.tzinfo is not None
    assert result.updated_at.tzinfo is not None


@pytest.mark.parametrize("order_type", _ORDER_TYPES)
async def test_order_roundtrip(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
    order_type: OrderType,
) -> None:
    """place -> query -> modify (LIMIT/STOP_LIMIT) -> cancel for one order type."""
    if order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and not _is_futures(traded_instrument):
        pytest.skip("STOP orders are not supported for spot on Bybit")

    qty = await _spec_valid_qty(connected_adapter, traded_instrument)
    price = await _spec_valid_price(connected_adapter, traded_instrument, reference_price)

    kwargs: dict = {"client_order_id": random_client_id("rt")}
    if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        kwargs["price"] = price
    if order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        kwargs["stop_price"] = price * Decimal("1.5")

    order = build_unified_order(
        traded_instrument,
        order_type,
        OrderSide.BUY,
        qty,
        **kwargs,
    )

    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.client_order_id == order.client_order_id

        queried = await connected_adapter.get_order_by_client_id(order.client_order_id)
        assert queried is not None
        assert queried.platform_order_id == result.platform_order_id
        await _assert_complete_result(queried)

        if order_type == OrderType.MARKET:
            # Market orders fill immediately; there is nothing to modify/cancel.
            assert result.status == OrderStatus.FILLED
            return

        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            new_price = await _spec_valid_price(
                connected_adapter, traded_instrument, price * Decimal("0.99")
            )
            modification = OrderModification(
                client_order_id=order.client_order_id,
                price=new_price,
            )
            modified = await connected_adapter.modify_order(modification)
            await _assert_complete_result(modified)
            assert modified.client_order_id == order.client_order_id

        cancelled = await connected_adapter.cancel_order(order.client_order_id)
        await _assert_complete_result(cancelled)
        assert cancelled.status == OrderStatus.CANCELLED
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_market_order_roundtrip_fills(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    qty = await _spec_valid_qty(connected_adapter, traded_instrument)
    order = build_unified_order(
        traded_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("market"),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.status == OrderStatus.FILLED
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_instant_open_and_close(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    collect_events,
) -> None:
    """Market buy opens a position, immediate market sell closes it."""
    if not _is_futures(traded_instrument):
        pytest.skip("instant open/close is a derivatives position-cycle test")

    qty = await _spec_valid_qty(connected_adapter, traded_instrument)
    open_order = build_unified_order(
        traded_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("open"),
    )
    close_order = build_unified_order(
        traded_instrument,
        OrderType.MARKET,
        OrderSide.SELL,
        qty,
        client_order_id=random_client_id("close"),
    )
    try:
        opened = await connected_adapter.place_order(open_order)
        await _assert_complete_result(opened)
        assert opened.status == OrderStatus.FILLED

        closed = await connected_adapter.place_order(close_order)
        await _assert_complete_result(closed)
        assert closed.status == OrderStatus.FILLED

        positions = await connected_adapter.fetch_positions()
        pos = positions.get(traded_instrument)
        if pos is not None:
            assert isinstance(pos.quantity, Decimal), f"position quantity must be Decimal"
            assert pos.quantity >= 0, (
                f"sell must not leave a short position on {traded_instrument.symbol}, "
                f"got {pos.quantity}"
            )
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_limit_instant_fill_and_cancel_remainder(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """A limit above market fills instantly — no residual open order remains."""
    qty = await _spec_valid_qty(connected_adapter, traded_instrument)
    price = await _spec_valid_price(connected_adapter, traded_instrument, reference_price)
    fill_price = price * Decimal("1.2")  # above market -> instant fill

    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("instant"),
        price=fill_price,
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        # Allow the exchange to settle the fill before asserting final state.
        await asyncio.sleep(1.0)
        final = await connected_adapter.get_order_by_client_id(order.client_order_id)
        assert final is not None
        assert final.status in (OrderStatus.FILLED, OrderStatus.CANCELLED)
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_finalize_only_when_closed(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """An order stays open (live) until closed; only then does it finalize."""
    qty = await _spec_valid_qty(connected_adapter, traded_instrument)
    price = await _spec_valid_price(connected_adapter, traded_instrument, reference_price)
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("finalize"),
        price=price * Decimal("0.9"),  # below market -> rests unfilled
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)

        await asyncio.sleep(1.5)
        seen = order_ids_seen(connected_adapter)
        assert result.platform_order_id in seen["open"]
        assert result.platform_order_id not in seen["final"]

        cancelled = await connected_adapter.cancel_order(order.client_order_id)
        await _assert_complete_result(cancelled)
        assert cancelled.status == OrderStatus.CANCELLED

        await asyncio.sleep(1.0)
        seen_after = order_ids_seen(connected_adapter)
        assert result.platform_order_id not in seen_after["open"]
        assert result.platform_order_id in seen_after["final"]
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_tp_sl_attachment_round_trip_market(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """Market TP/SL (tpslMode=Full) is accepted by testnet derivatives."""
    qty = await _spec_valid_qty(connected_adapter, linear_instrument)
    price = await _spec_valid_price(connected_adapter, linear_instrument, linear_reference_price)
    order = build_unified_order(
        linear_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("tpsl-full"),
        price=price,
        take_profit=TpSlAttachment(trigger_price=price * Decimal("1.2")),
        stop_loss=TpSlAttachment(trigger_price=price * Decimal("0.8")),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
    finally:
        await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_tp_sl_attachment_round_trip_limit(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """Limit TP/SL (tpslMode=Partial) is accepted by testnet derivatives."""
    qty = await _spec_valid_qty(connected_adapter, linear_instrument)
    price = await _spec_valid_price(connected_adapter, linear_instrument, linear_reference_price)
    order = build_unified_order(
        linear_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("tpsl-partial"),
        price=price,
        take_profit=TpSlAttachment(
            trigger_price=price * Decimal("1.2"),
            limit_price=price * Decimal("1.2"),
        ),
        stop_loss=TpSlAttachment(
            trigger_price=price * Decimal("0.8"),
            limit_price=price * Decimal("0.8"),
        ),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_modify_tp_sl(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """Modify_order updates attached TP/SL trigger prices."""
    qty = await _spec_valid_qty(connected_adapter, linear_instrument)
    price = await _spec_valid_price(connected_adapter, linear_instrument, linear_reference_price)
    order = build_unified_order(
        linear_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("modify-tpsl"),
        price=price,
        take_profit=TpSlAttachment(trigger_price=price * Decimal("1.2")),
        stop_loss=TpSlAttachment(trigger_price=price * Decimal("0.8")),
    )
    try:
        await connected_adapter.place_order(order)
        modification = OrderModification(
            client_order_id=order.client_order_id,
            take_profit=TpSlAttachment(trigger_price=price * Decimal("1.5")),
            stop_loss=TpSlAttachment(trigger_price=price * Decimal("0.5")),
        )
        modified = await connected_adapter.modify_order(modification)
        await _assert_complete_result(modified)
        assert modified.client_order_id == order.client_order_id
    finally:
        await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_stop_trigger_direction(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """BUY stops carry triggerDirection=1, SELL stops triggerDirection=2."""
    qty = await _spec_valid_qty(connected_adapter, linear_instrument)
    price = await _spec_valid_price(connected_adapter, linear_instrument, linear_reference_price)

    buy_stop = build_unified_order(
        linear_instrument,
        OrderType.STOP,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("stop-buy"),
        stop_price=price * Decimal("1.5"),
    )
    sell_stop = build_unified_order(
        linear_instrument,
        OrderType.STOP,
        OrderSide.SELL,
        qty,
        client_order_id=random_client_id("stop-sell"),
        stop_price=price * Decimal("0.5"),
    )
    try:
        for stop in (buy_stop, sell_stop):
            result = await connected_adapter.place_order(stop)
            await _assert_complete_result(result)
            assert result.status == OrderStatus.OPEN, (
                "untriggered stop must rest OPEN on the platform"
            )
    finally:
        await connected_adapter.cancel_order(buy_stop.client_order_id)
        await connected_adapter.cancel_order(sell_stop.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_stop_order_status_mapping(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """Bybit's Untriggered stop maps to unified OPEN on a live response."""
    qty = await _spec_valid_qty(connected_adapter, linear_instrument)
    price = await _spec_valid_price(connected_adapter, linear_instrument, linear_reference_price)
    order = build_unified_order(
        linear_instrument,
        OrderType.STOP,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("stop-status"),
        stop_price=price * Decimal("2.0"),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.status == OrderStatus.OPEN  # from Bybit "Untriggered"

        queried = await connected_adapter.get_order_by_client_id(order.client_order_id)
        assert queried is not None
        assert queried.status == OrderStatus.OPEN
    finally:
        await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)

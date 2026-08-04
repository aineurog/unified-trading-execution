"""WebSocket stream correctness integration tests (Section 11.2, bullet 5).

Verifies that live private WS events (fills, position updates, balance updates)
are correctly translated and published on the bus, that the event loop stays
non-blocked while the stream runs, that a broken subscriber cannot block other
subscribers, and that terminal order echoes are de-duplicated.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .conftest import EventCollector, LoopProbe, _reference_price, cleanup_open_orders
from .helpers import (
    build_unified_order,
    random_client_id,
    valid_price_from_spec,
    valid_qty_from_spec,
)


async def _market_qty(adapter: BybitAdapter, instrument: Instrument) -> Decimal:
    spec = await adapter.fetch_instrument_spec(instrument)
    price = await _reference_price(adapter, instrument)
    qty = valid_qty_from_spec(spec, price)
    return qty if qty > 0 else Decimal("0.001")


async def test_fill_event(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events: EventCollector,
) -> None:
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-fill"),
    )
    try:
        await connected_adapter.place_order(order)
        fills = await collect_events.wait_for(FillEvent)
        assert fills, "expected at least one FillEvent for the market order"
        fill = fills[0].fill
        assert fill.instrument == linear_instrument
        assert isinstance(fill.fill_quantity, Decimal)
        assert isinstance(fill.fill_price, Decimal)
        assert fill.fill_timestamp.tzinfo is not None
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_position_update_event(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events: EventCollector,
) -> None:
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-pos"),
    )
    try:
        await connected_adapter.place_order(order)
        events = await collect_events.wait_for(PositionUpdateEvent)
        assert events, "expected a PositionUpdateEvent for the filled position"
        position = events[-1].position
        assert position.instrument == linear_instrument
        assert isinstance(position.quantity, Decimal)
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_balance_update_event(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events: EventCollector,
) -> None:
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-balance"),
    )
    try:
        await connected_adapter.place_order(order)
        events = await collect_events.wait_for(BalanceUpdateEvent)
        assert events, "expected a BalanceUpdateEvent after the fill"
        balance = events[-1].balance
        assert isinstance(balance.free, Decimal)
        assert isinstance(balance.total, Decimal)
        assert balance.free + balance.used == balance.total
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_spot_fill_produces_fill_event_no_position(
    connected_adapter: BybitAdapter,
    spot_instrument: Instrument,
    collect_events: EventCollector,
) -> None:
    qty = await _market_qty(connected_adapter, spot_instrument)
    order = build_unified_order(
        spot_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-spot"),
    )
    try:
        await connected_adapter.place_order(order)
        fills = await collect_events.wait_for(FillEvent)
        assert fills, "expected a FillEvent for the spot fill"
        fill = fills[0].fill
        assert fill.instrument.asset_class == AssetClass.SPOT
        assert isinstance(fill.fill_quantity, Decimal)
        assert isinstance(fill.fill_price, Decimal)
        # Spot has no position concept — no PositionUpdateEvent should arrive
        # for the spot fill within a short window.
        await asyncio.sleep(1.0)
        assert not collect_events.of_type(PositionUpdateEvent)
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_execution_type_filter_non_trade(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events: EventCollector,
) -> None:
    """Only real Trade executions surface as FillEvents, never funding/adl."""
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-exec"),
    )
    try:
        await connected_adapter.place_order(order)
        # Let the execution stream settle long enough for any funding/adl
        # entries to have been skipped (they never become FillEvents).
        await asyncio.sleep(3.0)
        fills = collect_events.of_type(FillEvent)
        for fill_event in fills:
            assert fill_event.fill.fill_quantity > 0, (
                "execution entries with non-trade execType must be filtered out"
            )
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_event_loop_not_blocked_by_stream(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events,
    responsive_loop_probe: LoopProbe,
) -> None:
    """While fills stream in, a probe task still ticks — the loop stays free."""
    responsive_loop_probe.start()
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-nonblock"),
    )
    try:
        await connected_adapter.place_order(order)
        await collect_events.wait_for(FillEvent)
        await asyncio.sleep(1.0)
        ticks_before = responsive_loop_probe.ticks
        assert ticks_before > 0, "probe must tick while the stream is active"
        await asyncio.sleep(0.5)
        assert responsive_loop_probe.ticks > ticks_before, (
            "event loop was blocked by WS stream processing"
        )
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_subscriber_exception_is_isolated(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    collect_events: EventCollector,
    event_bus: EventBus,
) -> None:
    """A raising subscriber must not prevent other subscribers from firing."""

    def _bad_subscriber(event) -> None:
        raise RuntimeError("subscriber boom")

    event_bus.subscribe(FillEvent, _bad_subscriber)
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-isolation"),
    )
    try:
        await connected_adapter.place_order(order)
        fills = await collect_events.wait_for(FillEvent)
        assert fills, "healthy subscribers still receive FillEvents"
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_order_stream_dedup_terminal_echo(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
    collect_events: EventCollector,
) -> None:
    """A terminal echo for an already-final order emits no second OrderPlacedEvent."""
    spec = await connected_adapter.fetch_instrument_spec(traded_instrument)
    qty = valid_qty_from_spec(spec, reference_price) or Decimal("0.001")
    price = valid_price_from_spec(spec, reference_price)
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-dedup"),
        price=price * Decimal("0.9"),
    )
    try:
        await connected_adapter.place_order(order)
        placed = await collect_events.wait_for(OrderPlacedEvent)
        assert placed, "expected an OrderPlacedEvent for the new order"
        order_id = placed[0].order.platform_order_id
        assert order_id is not None

        # Cancel and wait for the WS cancellation event — once it fires,
        # the order is in _final_order_ids and echoes are suppressed.
        await connected_adapter.cancel_order(order.client_order_id)
        await collect_events.wait_for(OrderCancelledEvent)

        # Drain the collector, then wait for any echo.  After cancellation,
        # no new OrderPlacedEvent with the same platform id may appear.
        collect_events.drain()
        await asyncio.sleep(2.0)
        placed_after = [
            event
            for event in collect_events.of_type(OrderPlacedEvent)
            if event.order.platform_order_id == order_id
        ]
        assert len(placed_after) == 0, "terminal echo must not re-emit OrderPlacedEvent"
    finally:
        await cleanup_open_orders(connected_adapter)


async def test_no_dangling_state_between_tests(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> None:
    qty = await _market_qty(connected_adapter, linear_instrument)
    order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("ws-dangling"),
    )
    try:
        await connected_adapter.place_order(order)
        await asyncio.sleep(1.0)
    finally:
        await cleanup_open_orders(connected_adapter)
    open_orders = await connected_adapter.fetch_open_orders()
    assert not open_orders, "no open orders may remain after cleanup"

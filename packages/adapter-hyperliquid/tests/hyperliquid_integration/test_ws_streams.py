"""Integration: push-channel receipt against live testnet.

Gate: order/fill lifecycle plus account-state (positions, balances) arrives
on the bus via WS, with heartbeat snapshots swallowed by the adapter diff.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from unified_trading_execution.events import (
    BalanceUpdateEvent,
    FillEvent,
    OrderCancelledEvent,
    OrderStatusEvent,
    PositionUpdateEvent,
)
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
    flatten_all,
    valid_qty_from_spec,
    venue_price,
    wait_for_position,
)

if TYPE_CHECKING:
    from .conftest import EventCollector


def _summarize_raw(channel: object, data: object) -> str:
    """One-line slice of a raw push message for the debug tap."""
    if channel == "orderUpdates":
        rows = data if isinstance(data, list) else [data]
        bits = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            inner = row.get("order")
            inner = inner if isinstance(inner, dict) else {}
            bits.append(
                f"{inner.get('coin')}:{row.get('status')}:"
                f"{str(inner.get('cloid'))[:18]}:oid={inner.get('oid')}"
            )
        return "updates=[" + " | ".join(bits) + "]"
    if channel in ("userFills", "user"):
        fills: list[object] = []
        if isinstance(data, dict):
            fills = data.get("fills") or []
        bits = [
            f"{f.get('coin')}:{f.get('dir')}:oid={f.get('oid')}"
            for f in fills
            if isinstance(f, dict)
        ]
        snap = " snapshot" if isinstance(data, dict) and data.get("isSnapshot") else ""
        return f"fills=[{' | '.join(bits)}]{snap}"
    keys = sorted(data.keys()) if isinstance(data, dict) else type(data).__name__
    return f"keys={keys}"


def _describe_event(event: object) -> str:
    if isinstance(event, OrderStatusEvent):
        return f"cid={event.order.client_order_id} status={event.order.status.value}"
    if isinstance(event, OrderCancelledEvent):
        return f"cid={event.client_order_id}"
    if isinstance(event, FillEvent):
        return (
            f"cid={event.fill.client_order_id} "
            f"qty={event.fill.fill_quantity}@{event.fill.fill_price}"
        )
    return repr(event)[:160]


def _install_debug_tap(adapter: HyperliquidAdapter, collect_events: EventCollector) -> None:
    """Log every raw push plus the events it produces (compact; full JSON opt-in).

    Full raw bodies print only with ``HLIT_WS_DEBUG=1`` — the one-liners stay
    on so CI output still shows the push→event chain per test.
    """
    orig = adapter._dispatch_ws_message

    async def spy(message: object) -> None:
        if not isinstance(message, dict):
            await orig(message)
            return
        channel: object = message.get("channel")
        data: object = message.get("data")
        print(f"\n[ws-raw] channel={channel} {_summarize_raw(channel, data)}", flush=True)
        if os.getenv("HLIT_WS_DEBUG") == "1":
            print(f"[ws-full] {json.dumps(message)[:3000]}", flush=True)
        before = len(collect_events)
        await orig(message)
        for event in collect_events.events_since(before):
            print(f"[ws-event] {type(event).__name__} {_describe_event(event)}", flush=True)

    adapter._dispatch_ws_message = spy


async def _ensure_streams(adapter: HyperliquidAdapter, collect_events: EventCollector) -> None:
    if not adapter.streams_connected:
        await adapter.start_streams()
    assert adapter.streams_connected
    _install_debug_tap(adapter, collect_events)
    # Drop any pre-test bus traffic (pre-flatten cancels/closes publish too)
    # so the waits below only match pushes caused by this test's actions.
    collect_events.drain()


async def test_order_open_push(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    collect_events: EventCollector,
    flattened_book: None,
) -> None:
    await _ensure_streams(connected_adapter, collect_events)
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    # Size from the ORDER price, not mid — notional is qty times resting price.
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("ws-open")
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
    events = await collect_events.wait_for(OrderStatusEvent, timeout=30.0)
    assert any(
        e.order.client_order_id == cid and e.order.status == OrderStatus.OPEN for e in events
    )
    await connected_adapter.cancel_order(cid)


async def test_cancel_push(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    collect_events: EventCollector,
    flattened_book: None,
) -> None:
    await _ensure_streams(connected_adapter, collect_events)
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    # Size from the ORDER price, not mid — notional is qty times resting price.
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("ws-cancel")
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
    await connected_adapter.cancel_order(cid)
    events = await collect_events.wait_for(OrderCancelledEvent, timeout=30.0)
    assert any(e.client_order_id == cid for e in events)


async def test_fill_push(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    collect_events: EventCollector,
    flattened_book: None,
) -> None:
    await _ensure_streams(connected_adapter, collect_events)
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    cid = unique_cid("ws-fill")
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
    events = await collect_events.wait_for(FillEvent, timeout=30.0)
    assert any(e.fill.client_order_id == cid for e in events)


async def test_position_update_push(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    collect_events: EventCollector,
    flattened_book: None,
) -> None:
    await _ensure_streams(connected_adapter, collect_events)
    # Let the seed land first: a leg opened before the first push would be
    # swallowed into the baseline. Heartbeat is ~5s, so 8s is ample.
    await asyncio.sleep(8)
    collect_events.drain()

    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("ws-pos"),
            time_in_force=TimeInForce.IOC,
        )
    )
    await wait_for_position(connected_adapter, "BTC")
    events = await collect_events.wait_for(PositionUpdateEvent, timeout=30.0)
    opened = [
        e for e in events if e.position.position_id == "BTC:oneWay" and e.position.quantity != 0
    ]
    assert opened, "leg appearance must publish exactly one PositionUpdateEvent"
    assert opened[0].position.quantity == qty

    await flatten_all(connected_adapter)
    deleted = await collect_events.wait_for(PositionUpdateEvent, timeout=30.0)
    gone = [
        e for e in deleted if e.position.position_id == "BTC:oneWay" and e.position.quantity == 0
    ]
    assert gone, "leg disappearance must publish the zero-quantity close signal"


async def test_balance_update_push(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    collect_events: EventCollector,
    flattened_book: None,
) -> None:
    await _ensure_streams(connected_adapter, collect_events)
    await asyncio.sleep(8)
    collect_events.drain()

    # A taker fill pays a fee, moving the USDC total — the one deterministic
    # balance touch available without touching other currencies.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("ws-bal"),
            time_in_force=TimeInForce.IOC,
        )
    )
    events = await collect_events.wait_for(BalanceUpdateEvent, timeout=30.0)
    assert any(e.balance.currency == "USDC" for e in events)

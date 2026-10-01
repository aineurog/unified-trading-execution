"""Integration: push-channel receipt against live testnet.

Gate: order/fill lifecycle arrives on the bus via WS; position/balance push
assertions are pre-written but skipped until the P5 account-state push lands
(see not_push/hyperliquid/WS_ACCOUNT_PUSH_PLAN.md).
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from unified_trading_execution.events import (
    FillEvent,
    OrderCancelledEvent,
    OrderStatusEvent,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import build_unified_order, valid_qty_from_spec, venue_price

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
        channel: object = None
        data: object = None
        if isinstance(message, dict):
            channel, data = message.get("channel"), message.get("data")
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
    from unified_trading_execution.types.enums import TimeInForce

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


@pytest.mark.skip(reason="P5 account-state push not implemented — see WS_ACCOUNT_PUSH_PLAN.md")
async def test_position_update_push() -> None:
    raise NotImplementedError("push-branch gate: dust leg open → one PositionUpdateEvent")


@pytest.mark.skip(reason="P5 account-state push not implemented — see WS_ACCOUNT_PUSH_PLAN.md")
async def test_balance_update_push() -> None:
    raise NotImplementedError("push-branch gate: balance touch → BalanceUpdateEvent")

"""Unit tests for push-channel handlers (no sockets for dispatch, mocked for lifecycle)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.events import (
    Event,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderStatusEvent,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import OrderStatus

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _fill(oid: int = 5, tid: int = 9, coin: str = "BTC", **over: Any) -> dict[str, Any]:
    entry = {
        "coin": coin,
        "px": "100",
        "sz": "0.01",
        "side": "B",
        "time": 2000,
        "dir": "Close Long",
        "hash": "0xh",
        "oid": oid,
        "fee": "0.01",
        "feeToken": "USDC",
        "tid": tid,
    }
    entry.update(over)
    return entry


def _order_update(*, oid: int = 7, status: str = "open", cloid: str = "abc") -> dict[str, Any]:
    return {
        "order": {
            "coin": "BTC",
            "side": "B",
            "limitPx": "50000",
            "sz": "0.001",
            "oid": oid,
            "timestamp": 1700000000000,
            "origSz": "0.001",
            "cloid": cloid,
        },
        "status": status,
        "statusTimestamp": 1700000001000,
    }


def _adapter() -> tuple[HyperliquidAdapter, EventBus, list[Event]]:
    bus = EventBus()
    seen: list[Event] = []
    for event_type in (FillEvent, OrderStatusEvent, OrderCancelledEvent):
        bus.subscribe(event_type, seen.append)
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=bus)
    exchange = MagicMock()
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = []
    adapter._exchange = exchange
    adapter._connected = True
    adapter._loop = asyncio.get_event_loop()
    return adapter, bus, seen


def _of(seen: list[Event], kind: type[Event]) -> list[Event]:
    return [e for e in seen if isinstance(e, kind)]


# ---- userFills channel ----


async def test_user_fills_snapshot_seeds_without_publishing() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {
            "channel": "userFills",
            "data": {"user": _TEST_ADDRESS, "isSnapshot": True, "fills": [_fill()]},
        }
    )
    assert seen == []
    # Same fill arriving live afterwards is a duplicate — still nothing.
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    assert seen == []


async def test_user_fills_streaming_publishes() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    (event,) = _of(seen, FillEvent)
    assert isinstance(event, FillEvent)
    assert (event.fill.fill_quantity, event.fill.fill_price) == (Decimal("0.01"), Decimal("100"))
    assert event.fill.platform_fill_id == "0xh:9"
    assert event.adapter_name == "hyperliquid"
    assert event.account_id == _TEST_ADDRESS


async def test_user_fills_wrong_user_dropped() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {
            "channel": "userFills",
            "data": {"user": "0x0000000000000000000000000000000000000002", "fills": [_fill()]},
        }
    )
    assert seen == []


async def test_user_fills_malformed_entry_skips_rest_processes() -> None:
    adapter, _, seen = _adapter()
    bad = {"coin": "BTC"}  # no px/sz/hash — untranslatable
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [bad, _fill(tid=10)]}}
    )
    assert len(_of(seen, FillEvent)) == 1


async def test_user_fills_unknown_coin_skips() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill(coin="dex:XYZ")]}}
    )
    assert seen == []


async def test_fill_attribution_uses_oid_index() -> None:
    adapter, _, seen = _adapter()
    adapter._oid_clients["5"] = ("parent-1", None, None)
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    (event,) = _of(seen, FillEvent)
    assert isinstance(event, FillEvent)
    assert event.fill.client_order_id == "parent-1"


async def test_fill_without_oid_falls_back_to_hash() -> None:
    """An unattributed, oid-less fill keys by its L1 hash — as ``fetch_fills`` does."""
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill(oid="")]}}
    )
    (event,) = _of(seen, FillEvent)
    assert isinstance(event, FillEvent)
    assert event.fill.client_order_id == "0xh"


# ---- user channel ----


async def test_user_channel_fills_share_seen_set() -> None:
    """Dual-subscribed fills (user + userFills) publish exactly once."""
    adapter, _, seen = _adapter()
    message = {"channel": "user", "data": {"fills": [_fill()]}}
    await adapter._dispatch_ws_message(message)
    await adapter._dispatch_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    assert len(_of(seen, FillEvent)) == 1


async def test_user_channel_non_fill_variants_logged_only() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message({"channel": "user", "data": {"funding": {"coin": "BTC"}}})
    await adapter._dispatch_ws_message({"channel": "user", "data": {"liquidation": {"lid": 1}}})
    await adapter._dispatch_ws_message({"channel": "user", "data": {"nonUserCancel": []}})
    await adapter._dispatch_ws_message({"channel": "user", "data": {"somethingNew": 1}})
    assert seen == []


async def test_unknown_channel_ignored() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message({"channel": "candle", "data": {}})
    await adapter._dispatch_ws_message("not-a-dict")  # type: ignore[arg-type]
    assert seen == []


# ---- orderUpdates channel ----


async def test_order_updates_publish_status() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [_order_update()]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.platform_order_id == "7"
    assert event.order.status is OrderStatus.OPEN
    assert _of(seen, OrderCancelledEvent) == []


async def test_order_updates_cancelled_publishes_both() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(status="canceled")]}
    )
    (status_event,) = _of(seen, OrderStatusEvent)
    (cancelled,) = _of(seen, OrderCancelledEvent)
    assert isinstance(status_event, OrderStatusEvent)
    assert status_event.order.status is OrderStatus.CANCELLED
    assert isinstance(cancelled, OrderCancelledEvent)
    assert cancelled.client_order_id == "abc"


async def test_order_updates_malformed_entry_skips_rest() -> None:
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [{"no": "order"}, _order_update(oid=8)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.platform_order_id == "8"


async def test_order_updates_unknown_coin_skips() -> None:
    adapter, _, seen = _adapter()
    update = _order_update()
    assert isinstance(update["order"], dict)
    update["order"]["coin"] = "dex:XYZ"
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [update]})
    assert seen == []


async def test_order_updates_carry_status_timestamp() -> None:
    """``updated_at`` tracks the sibling ``statusTimestamp``, not creation.

    ``statusTimestamp`` sits on the update next to ``order`` (not inside it);
    dropping it would leave ``updated_at`` pinned to the order's ``timestamp``.
    """
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(status="filled")]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.created_at == datetime.fromtimestamp(1700000000, tz=UTC)
    assert event.order.updated_at == datetime.fromtimestamp(1700000001, tz=UTC)


# ---- thread marshalling ----


async def test_on_ws_message_schedules() -> None:
    adapter, _, seen = _adapter()
    adapter._on_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    await asyncio.sleep(0.05)
    assert len(_of(seen, FillEvent)) == 1
    assert adapter._ws_pending == set()


# ---- lifecycle ----


def _socket_mock() -> MagicMock:
    socket = MagicMock()
    socket.is_connected.return_value = True
    return socket


async def test_start_stop_lifecycle() -> None:
    adapter, _, _ = _adapter()
    with patch(
        "unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"
    ) as socket_class:
        socket_class.return_value = _socket_mock()
        await adapter.start_streams()
        socket = adapter._ws
        assert socket is not None and adapter._ws_task is not None
        assert socket.subscribe_user_events.call_count == 1
        assert socket.subscribe_order_updates.call_count == 1
        assert socket.subscribe_user_fills.call_count == 1
        await adapter.start_streams()  # idempotent while alive
        assert socket_class.call_count == 1
        await adapter.stop_streams()
        assert adapter._ws is None and adapter._ws_task is None
        socket.disconnect.assert_called_once()
        await adapter.stop_streams()  # idempotent no-op


async def test_start_subscribe_failure_tears_down() -> None:
    adapter, _, _ = _adapter()
    with patch(
        "unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"
    ) as socket_class:
        socket = _socket_mock()
        socket.subscribe_user_events.side_effect = RuntimeError("denied")
        socket_class.return_value = socket
        try:
            await adapter.start_streams()
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected subscribe failure to propagate")
        assert adapter._ws is None
        socket.disconnect.assert_called_once()


async def test_rebuild_resubscribes_and_gap_fills() -> None:
    adapter, _, seen = _adapter()
    dead = _socket_mock()
    dead.is_connected.return_value = False
    adapter._ws = dead
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    exchange.info.user_fills.return_value = [_fill(), _fill(tid=10)]
    with patch(
        "unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"
    ) as socket_class:
        fresh = _socket_mock()
        socket_class.return_value = fresh
        await adapter._rebuild_streams(dead)
        assert adapter._ws is fresh
        assert fresh.subscribe_user_events.call_count == 1
        dead.disconnect.assert_called_once()
        # Both gap fills were unseen: both publish.
        assert len(_of(seen, FillEvent)) == 2
        assert adapter._streams_up is True


async def test_rebuild_loses_race_to_stop() -> None:
    adapter, _, seen = _adapter()
    dead = _socket_mock()
    adapter._ws = MagicMock()  # stop already swapped it
    with patch(
        "unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"
    ) as socket_class:
        fresh = _socket_mock()
        socket_class.return_value = fresh
        await adapter._rebuild_streams(dead)
        assert fresh.subscribe_user_events.call_count == 0
        assert seen == []


async def test_rebuild_resubscribe_failure_tears_down_replacement() -> None:
    """A failed resubscribe must not leave a live-looking socket behind.

    A connected-but-unsubscribed replacement satisfies ``is_connected``, so
    the monitor would announce the streams up and never rebuild again.
    """

    class _FakeSocket:
        def __init__(self) -> None:
            self.connected = True
            self.disconnects = 0

        def is_connected(self) -> bool:
            return self.connected

        def connect(self) -> None:
            self.connected = True

        def subscribe_user_events(self, callback: object) -> None:
            raise RuntimeError("denied")

        def subscribe_order_updates(self, callback: object) -> None:
            pass

        def subscribe_user_fills(self, callback: object) -> None:
            pass

        def disconnect(self) -> None:
            self.connected = False
            self.disconnects += 1

    adapter, _, seen = _adapter()
    dead = _socket_mock()
    adapter._ws = dead
    with patch(
        "unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"
    ) as socket_class:
        fresh = _FakeSocket()
        socket_class.return_value = fresh
        await adapter._rebuild_streams(dead)
    assert adapter._streams_up is False
    assert fresh.disconnects == 1
    assert adapter._ws is fresh and not adapter._ws.is_connected()
    assert seen == []


async def test_monitor_rebuilds_dead_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead socket triggers rebuild attempts and a state announcement."""
    import unified_trading_execution.hyperliquid.adapter as adapter_module

    monkeypatch.setattr(adapter_module, "_WS_MONITOR_INTERVAL_SECONDS", 0.05)
    adapter, bus, seen = _adapter()
    from unified_trading_execution.events import ConnectionStateEvent

    bus.subscribe(ConnectionStateEvent, seen.append)
    dead = _socket_mock()
    dead.is_connected.return_value = False
    adapter._ws = dead
    adapter._streams_up = True
    calls: list[bool] = []
    real_rebuild = adapter._rebuild_streams

    async def _spy(socket: object) -> None:
        calls.append(True)

    adapter._rebuild_streams = _spy  # type: ignore[method-assign]
    task = asyncio.ensure_future(adapter._monitor_streams())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        adapter._rebuild_streams = real_rebuild
    assert calls, "monitor must attempt rebuild while the socket is dead"
    states = [e.connected for e in seen if type(e).__name__ == "ConnectionStateEvent"]
    assert states == [False]


async def test_gap_fill_skips_seen() -> None:
    adapter, _, seen = _adapter()
    adapter._seen_fill_ids.append("0xh:9")
    dead = _socket_mock()
    adapter._ws = dead
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    exchange.info.user_fills.return_value = [_fill(), _fill(tid=10)]
    with patch("unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"):
        await adapter._rebuild_streams(dead)
        assert len(_of(seen, FillEvent)) == 1

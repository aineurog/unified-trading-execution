"""Unit tests for push-channel handlers (no sockets for dispatch, mocked for lifecycle)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderStatusEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderRecord, TpSlAttachment

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _fill(oid: int | str = 5, tid: int = 9, coin: str = "BTC", **over: Any) -> dict[str, Any]:
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
    adapter._flush_all_parked()  # unattributed fills park until the hold expires
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
    adapter._flush_all_parked()
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
    adapter._flush_all_parked()
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
    adapter._flush_all_parked()
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


async def test_order_updates_restore_hashed_client_id() -> None:
    """A push update for a hashed-cloid order carries the caller id, not venue hex."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter, _, seen = _adapter()
    client_id = "my-order-123"
    adapter._client_coins[client_id] = ("BTC", False)
    hashed = client_order_id_to_cloid(client_id)
    assert hashed != client_id
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(cloid=hashed)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.client_order_id == client_id


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


def _stored_market(
    cid: str = "my-order-123", *, filled: str = "0", status: OrderStatus = OrderStatus.OPEN
) -> OrderRecord:
    return OrderRecord(
        instrument=Instrument(
            symbol="BTC",
            quote_currency="USDC",
            asset_class=AssetClass.FUTURES,
            currency="USDC",
            multiplier=1,
        ),
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.IOC,
        client_order_id=cid,
        price=None,
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=TpSlAttachment(trigger_price=Decimal("86113")),
        stop_loss=TpSlAttachment(trigger_price=Decimal("86806")),
        platform_order_id="7",
        status=status,
        filled_quantity=Decimal(filled),
        average_fill_price=None,
        correlation_id="corr-1",
        created_at=datetime(2026, 10, 5, tzinfo=UTC),
        updated_at=datetime(2026, 10, 5, tzinfo=UTC),
    )


def _store_with(record: OrderRecord | None) -> MagicMock:
    from unittest.mock import AsyncMock

    store = MagicMock()
    store.get_order = AsyncMock(return_value=record)
    return store


async def test_order_updates_skip_position_legs() -> None:
    """Position TP/SL legs are attachments: no status, no cancel, no history."""
    from unified_trading_execution.hyperliquid.orders import (
        TP_CLOID_SUFFIX,
        position_tpsl_cloid,
    )

    adapter, _, seen = _adapter()
    leg = position_tpsl_cloid("BTC:oneWay", TP_CLOID_SUFFIX)
    adapter._position_leg_cloids.add(leg)
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(cloid=leg), _order_update(oid=8)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.platform_order_id == "8"
    assert _of(seen, OrderCancelledEvent) == []
    # A terminal leg update is swallowed too — never mirrored, never cancelled.
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(cloid=leg, status="canceled")]}
    )
    assert len(_of(seen, OrderStatusEvent)) == 1
    assert _of(seen, OrderCancelledEvent) == []


async def test_order_updates_skip_bracket_children() -> None:
    """Placement-time TP/SL legs (same-batch children) never mirror either."""
    from unified_trading_execution.hyperliquid.orders import (
        TP_CLOID_SUFFIX,
        client_order_id_to_cloid,
    )

    adapter, _, seen = _adapter()
    adapter._client_coins["my-order-123"] = ("BTC", False)
    child = client_order_id_to_cloid(f"my-order-123:{TP_CLOID_SUFFIX}")
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(cloid=child), _order_update(oid=8)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.platform_order_id == "8"
    assert _of(seen, OrderCancelledEvent) == []


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


async def test_thin_update_merges_onto_stored_record() -> None:
    """Issue 11: a thin MARKET update keeps type/TIF/attachments from the mirror."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(_stored_market()))
    adapter._client_coins["my-order-123"] = ("BTC", False)
    hashed = client_order_id_to_cloid("my-order-123")
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(status="filled", cloid=hashed)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.client_order_id == "my-order-123"
    assert event.order.order_type is OrderType.MARKET
    assert event.order.time_in_force is TimeInForce.IOC
    assert event.order.take_profit is not None and event.order.stop_loss is not None
    assert event.order.status is OrderStatus.FILLED
    assert event.order.correlation_id == "corr-1"


async def test_thin_update_filled_never_regresses() -> None:
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(_stored_market(filled="0.0008")))
    adapter._client_coins["my-order-123"] = ("BTC", False)
    update = _order_update(
        status="open", cloid=client_order_id_to_cloid("my-order-123")
    )
    assert isinstance(update["order"], dict)
    update["order"]["origSz"] = "0.001"
    update["order"]["sz"] = "0.0009"  # thin math says 0.0001 < stored 0.0008
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [update]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.filled_quantity == Decimal("0.0008")


async def test_thin_update_without_store_keeps_legacy() -> None:
    """Bare adapter (no store): thin translates standalone, as before."""
    adapter, _, seen = _adapter()
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [_order_update()]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.order_type is OrderType.LIMIT


async def test_thin_update_unknown_with_store_requeries() -> None:
    """Orphan with a store: one bounded orderStatus read supplies the rich type."""
    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(None))
    rich = dict(_order_update()["order"])
    assert isinstance(rich, dict)
    rich["orderType"] = "Limit"
    # True envelope: top-level status is the response kind ("order"), the
    # order's own status rides one level down — folding the top level
    # mistranslates (regression: "Unknown order status 'order'").
    adapter._exchange.info.query_order_by_cloid.return_value = {
        "status": "order",
        "order": {"order": rich, "status": "filled", "statusTimestamp": 1700000001000},
    }
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [_order_update()]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.order_type is OrderType.LIMIT
    assert event.order.status is OrderStatus.FILLED
    assert adapter._exchange.info.query_order_by_cloid.call_count == 1


async def test_requeried_orphan_keeps_caller_client_id() -> None:
    """The rich fallback must not drop the resolved caller id for raw hex."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(None))
    adapter._client_coins["my-order-123"] = ("BTC", False)
    hashed = client_order_id_to_cloid("my-order-123")
    rich = dict(_order_update()["order"])
    assert isinstance(rich, dict)
    rich["orderType"] = "Limit"
    rich["cloid"] = hashed
    adapter._exchange.info.query_order_by_cloid.return_value = {
        "status": "order",
        "order": {"order": rich, "status": "open", "statusTimestamp": 1700000001000},
    }
    await adapter._dispatch_ws_message(
        {"channel": "orderUpdates", "data": [_order_update(cloid=hashed)]}
    )
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.client_order_id == "my-order-123"


async def test_thin_update_requery_failure_falls_back() -> None:
    """Re-query miss/failure never silences a status — thin publishes."""
    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(None))
    adapter._exchange.info.query_order_by_cloid.return_value = {"status": "unknownOid"}
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [_order_update()]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.order_type is OrderType.LIMIT


async def test_thin_update_store_failure_falls_back() -> None:
    from unittest.mock import AsyncMock

    adapter, _, seen = _adapter()
    store = _store_with(None)
    store.get_order = AsyncMock(side_effect=RuntimeError("db down"))
    adapter.attach_state_store(store)
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [_order_update()]})
    assert len(_of(seen, OrderStatusEvent)) == 1


async def test_rich_update_ignores_store() -> None:
    """Merge is thin-only: a rich update translates fully even against a stored row."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter, _, seen = _adapter()
    adapter.attach_state_store(_store_with(_stored_market()))
    adapter._client_coins["my-order-123"] = ("BTC", False)
    update = _order_update(cloid=client_order_id_to_cloid("my-order-123"))
    assert isinstance(update["order"], dict)
    update["order"]["orderType"] = "Limit"
    await adapter._dispatch_ws_message({"channel": "orderUpdates", "data": [update]})
    (event,) = _of(seen, OrderStatusEvent)
    assert isinstance(event, OrderStatusEvent)
    assert event.order.order_type is OrderType.LIMIT


# ---- thread marshalling ----


async def test_on_ws_message_schedules() -> None:
    adapter, _, seen = _adapter()
    adapter._on_ws_message(
        {"channel": "userFills", "data": {"user": _TEST_ADDRESS, "fills": [_fill()]}}
    )
    await asyncio.sleep(0.05)
    adapter._flush_all_parked()
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
        socket = cast(MagicMock, adapter._ws)
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


async def test_stop_streams_drops_wedged_socket_within_timeout() -> None:
    """Teardown is time-bounded: a socket wedged in disconnect must not hang stop."""
    import threading
    import time

    from unified_trading_execution.hyperliquid import HyperliquidConfig

    bus = EventBus()
    config = HyperliquidConfig(
        wallet_address=_TEST_ADDRESS,
        private_key=_TEST_KEY,
        testnet=True,
        request_timeout_seconds=0.2,
    )
    adapter = HyperliquidAdapter(config, event_bus=bus)
    release = threading.Event()

    def _wedged() -> None:
        assert not release.wait(timeout=30)

    socket = MagicMock()
    socket.disconnect.side_effect = _wedged
    adapter._ws = socket
    started = time.monotonic()
    await adapter.stop_streams()
    assert time.monotonic() - started < 5
    assert adapter._ws is None
    release.set()


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
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    exchange.info.user_fills_by_time.return_value = [_fill(time=now_ms), _fill(tid=10, time=now_ms)]
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
    states = [e.connected for e in seen if isinstance(e, ConnectionStateEvent)]
    assert states == [False]


async def test_gap_fill_skips_seen() -> None:
    adapter, _, seen = _adapter()
    adapter._seen_fill_ids.append("0xh:9")
    dead = _socket_mock()
    adapter._ws = dead
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    exchange.info.user_fills_by_time.return_value = [_fill(time=now_ms), _fill(tid=10, time=now_ms)]
    with patch("unified_trading_execution.hyperliquid.adapter.HyperliquidWebSocket"):
        await adapter._rebuild_streams(dead)
        assert len(_of(seen, FillEvent)) == 1


# ---- account-state channels (clearinghouseState / spotState) ----


def _clearinghouse(legs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "channel": "clearinghouseState",
        "data": {
            "dex": "",
            "user": _TEST_ADDRESS,
            "clearinghouseState": {
                "marginSummary": {
                    "accountValue": "85.0",
                    "totalNtlPos": "85.0",
                    "totalRawUsd": "0.0",
                    "totalMarginUsed": "85.0",
                },
                "crossMarginSummary": {
                    "accountValue": "85.0",
                    "totalNtlPos": "85.0",
                    "totalRawUsd": "0.0",
                    "totalMarginUsed": "85.0",
                },
                "crossMaintenanceMarginUsed": "1.0",
                "withdrawable": "0.0",
                "assetPositions": legs,
                "time": 1700000000000,
            },
        },
    }


def _leg(
    coin: str = "BTC", szi: str = "0.001", entry_px: str = "85454.0", upl: str = "-0.02"
) -> dict[str, Any]:
    return {
        "type": "oneWay",
        "position": {
            "coin": coin,
            "szi": szi,
            "leverage": {"type": "cross", "value": 1},
            "entryPx": entry_px,
            "positionValue": "85.4",
            "unrealizedPnl": upl,
            "returnOnEquity": "0.0",
            "liquidationPx": None,
            "marginUsed": "85.4",
            "maxLeverage": 40,
            "cumFunding": {"allTime": "0.0", "sinceOpen": "0.0", "sinceChange": "0.0"},
        },
    }


def _spot(balances: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "channel": "spotState",
        "data": {"user": _TEST_ADDRESS, "spotState": {"balances": balances}},
    }


def _row(coin: str, total: str, hold: str = "0.0") -> dict[str, Any]:
    return {"coin": coin, "token": 0, "total": total, "hold": hold, "entryNtl": "0.0"}


def _account_adapter() -> tuple[HyperliquidAdapter, EventBus, list[Event]]:
    adapter, bus, seen = _adapter()
    bus.subscribe(PositionUpdateEvent, seen.append)
    bus.subscribe(BalanceUpdateEvent, seen.append)
    return adapter, bus, seen


async def test_clearinghouse_first_push_seeds_silently() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    assert [e for e in seen if type(e).__name__ == "PositionUpdateEvent"] == []


async def test_clearinghouse_appearance_publishes_once() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    (event,) = [e for e in seen if type(e).__name__ == "PositionUpdateEvent"]
    assert isinstance(event, PositionUpdateEvent)
    assert event.position.position_id == "BTC:oneWay"
    assert event.position.quantity == Decimal("0.001")
    assert event.position.average_entry_price == Decimal("85454.0")


async def test_clearinghouse_heartbeat_swallowed() -> None:
    """Same qty/entry, moved upl — heartbeat, never news."""
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg(upl="-0.09")]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg(upl="-0.01")]))
    assert len([e for e in seen if type(e).__name__ == "PositionUpdateEvent"]) == 1


async def test_clearinghouse_qty_change_publishes() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg(szi="0.002")]))
    updates = [e for e in seen if type(e).__name__ == "PositionUpdateEvent"]
    assert len(updates) == 2
    assert isinstance(updates[1], PositionUpdateEvent)
    assert updates[1].position.quantity == Decimal("0.002")


async def test_clearinghouse_disappearance_deletes() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([]))
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_clearinghouse([]))
    updates = [e for e in seen if type(e).__name__ == "PositionUpdateEvent"]
    assert len(updates) == 2
    assert isinstance(updates[1], PositionUpdateEvent)
    assert updates[1].position.quantity == 0
    assert updates[1].position.position_id == "BTC:oneWay"
    # Baseline dropped: another flat push stays silent.
    await adapter._dispatch_ws_message(_clearinghouse([]))
    assert len([e for e in seen if type(e).__name__ == "PositionUpdateEvent"]) == 2


async def test_clearinghouse_malformed_safe() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message({"channel": "clearinghouseState", "data": {"nope": True}})
    await adapter._dispatch_ws_message({"channel": "clearinghouseState", "data": None})
    assert [e for e in seen if type(e).__name__ == "PositionUpdateEvent"] == []


async def test_spot_seed_change_and_new_coin() -> None:
    adapter, _, seen = _account_adapter()
    rows = [_row("USDC", "895.05"), _row("HYPE", "0.0")]
    await adapter._dispatch_ws_message(_spot(rows))
    await adapter._dispatch_ws_message(_spot(rows))
    assert [e for e in seen if type(e).__name__ == "BalanceUpdateEvent"] == []
    await adapter._dispatch_ws_message(_spot([_row("USDC", "894.00"), _row("HYPE", "0.0")]))
    balances = [e for e in seen if type(e).__name__ == "BalanceUpdateEvent"]
    assert len(balances) == 1
    assert isinstance(balances[0], BalanceUpdateEvent)
    assert balances[0].balance.currency == "USDC"
    assert balances[0].balance.total == Decimal("894.00")
    await adapter._dispatch_ws_message(
        _spot([_row("USDC", "894.00"), _row("HYPE", "0.0"), _row("PURR", "3.9")])
    )
    balances = [e for e in seen if type(e).__name__ == "BalanceUpdateEvent"]
    assert len(balances) == 2
    assert isinstance(balances[1], BalanceUpdateEvent)
    assert balances[1].balance.currency == "PURR"


async def test_stop_clears_account_baselines() -> None:
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_spot([_row("USDC", "1.0")]))
    assert adapter._position_baseline and adapter._balance_baseline
    await adapter.stop_streams()
    assert adapter._position_baseline == {}
    assert adapter._balance_baseline == {}
    assert adapter._account_seeded == set()
    # Next pushes re-seed instead of bursting against stale truth.
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))
    await adapter._dispatch_ws_message(_spot([_row("USDC", "1.0")]))
    assert seen == []


async def test_clearinghouse_missing_legs_does_not_falsely_close() -> None:
    """A dict push lacking ``assetPositions`` must not read as an empty book."""
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_clearinghouse([_leg()]))  # seed a live leg
    seen.clear()

    malformed = {
        "channel": "clearinghouseState",
        "data": {"user": _TEST_ADDRESS, "clearinghouseState": {"marginSummary": {}}},
    }
    await adapter._dispatch_ws_message(malformed)
    assert [e for e in seen if isinstance(e, PositionUpdateEvent)] == []

    # Baseline survives: a genuine flat push still emits the close signal.
    await adapter._dispatch_ws_message(_clearinghouse([]))
    closed = [e for e in seen if isinstance(e, PositionUpdateEvent)]
    assert len(closed) == 1
    assert closed[0].position.position_id == "BTC:oneWay"
    assert closed[0].position.quantity == 0


async def test_spot_non_list_balances_does_not_falsely_zero() -> None:
    """A present-but-non-list ``balances`` must not read as an empty book."""
    adapter, _, seen = _account_adapter()
    await adapter._dispatch_ws_message(_spot([_row("USDC", "895.05")]))
    seen.clear()

    malformed = {
        "channel": "spotState",
        "data": {"user": _TEST_ADDRESS, "spotState": {"balances": {"coin": "USDC"}}},
    }
    await adapter._dispatch_ws_message(malformed)
    assert [e for e in seen if isinstance(e, BalanceUpdateEvent)] == []


async def test_poison_fill_does_not_burn_seen_id() -> None:
    """Issue 12: a poison entry must not block its own redelivery."""
    adapter, _, seen = _adapter()
    poison = _fill(coin="")
    await adapter._publish_fill_entry(poison)
    assert _of(seen, FillEvent) == []
    await adapter._publish_fill_entry(_fill())
    adapter._flush_all_parked()  # hold expiry: now valid, still unattributed → raw
    (event,) = _of(seen, FillEvent)
    assert isinstance(event, FillEvent)
    assert event.fill.platform_fill_id == "0xh:9"
    # Genuine duplicates are still swallowed.
    await adapter._publish_fill_entry(_fill())
    adapter._flush_all_parked()
    assert len(_of(seen, FillEvent)) == 1


async def test_parked_fill_resolves_on_ack() -> None:
    """Issue 16: a fill parked pre-ack publishes with the uuid once indexed."""
    adapter, _, seen = _adapter()
    await adapter._publish_fill_entry(_fill())
    assert _of(seen, FillEvent) == []  # parked, not published
    adapter._oid_clients["5"] = ("parent-1", None, None)  # the ack lands
    adapter._flush_all_parked()
    (event,) = _of(seen, FillEvent)
    assert isinstance(event, FillEvent)
    assert event.fill.client_order_id == "parent-1"


async def test_parked_duplicate_dropped_and_seed_wins() -> None:
    adapter, _, seen = _adapter()
    await adapter._publish_fill_entry(_fill())
    await adapter._publish_fill_entry(_fill())  # duplicate while parked
    adapter._seen_fill_ids.append("0xh:9")  # snapshot seeds meanwhile
    adapter._flush_all_parked()
    assert _of(seen, FillEvent) == []


async def test_stop_streams_flushes_parked() -> None:
    adapter, _, seen = _adapter()
    await adapter._publish_fill_entry(_fill())
    assert _of(seen, FillEvent) == []
    await adapter.stop_streams()
    assert len(_of(seen, FillEvent)) == 1
    assert adapter._pending_fills == {}


async def test_park_cap_overflow_publishes_oldest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import unified_trading_execution.hyperliquid.adapter as adapter_module

    monkeypatch.setattr(adapter_module, "_PENDING_FILL_CAP", 1)
    adapter, _, seen = _adapter()
    await adapter._publish_fill_entry(_fill())
    await adapter._publish_fill_entry(_fill(tid=10))  # evicts the first, publishes it
    assert len(_of(seen, FillEvent)) == 1
    adapter._flush_all_parked()
    assert len(_of(seen, FillEvent)) == 2


async def test_park_deadline_fires_with_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timer path itself: expiry publishes without an explicit flush."""
    import unified_trading_execution.hyperliquid.adapter as adapter_module

    monkeypatch.setattr(adapter_module, "_FILL_ATTRIBUTION_HOLD_SECONDS", 0.01)
    adapter, _, seen = _adapter()
    await adapter._publish_fill_entry(_fill())
    await asyncio.sleep(0.05)
    assert len(_of(seen, FillEvent)) == 1

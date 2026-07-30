"""Unit tests for event types and EventBus — Section 17.12."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    HaltClearedEvent,
    HaltEnteredEvent,
    HaltEvent,
    OrderCancelledEvent,
    OrderModifiedEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
    ReconciliationCompleteEvent,
    ReconciliationEvent,
    ReconciliationMismatch,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_instrument(symbol="BTC"):
    return Instrument(
        symbol=symbol,
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_position():
    return Position(
        instrument=make_instrument(),
        quantity=Decimal("0.5"),
        average_entry_price=Decimal("50000"),
        updated_at=NOW,
    )


def make_balance():
    return Balance(
        currency="USDT",
        free=Decimal("9000"),
        used=Decimal("1000"),
        total=Decimal("10000"),
        updated_at=NOW,
    )


def make_fill_record():
    return FillRecord(
        client_order_id="abc",
        platform_fill_id="fill-1",
        instrument=make_instrument(),
        fill_quantity=Decimal("0.001"),
        fill_price=Decimal("50000"),
        fill_timestamp=NOW,
        fee_currency="USDT",
        fee_amount=Decimal("0.05"),
        correlation_id="corr-1",
    )


def make_order_record():
    return OrderRecord(
        instrument=make_instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.GTC,
        client_order_id="abc",
        price=Decimal("50000"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="plat-123",
        status=OrderStatus.OPEN,
        filled_quantity=Decimal("0"),
        average_fill_price=None,
        correlation_id="corr-1",
        created_at=NOW,
        updated_at=NOW,
    )


# ============================================================
# Event base type
# ============================================================


def test_event_base_constructs():
    e = Event(
        event_id="evt-001",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
    )
    assert e.event_id == "evt-001"
    assert e.adapter_name == "bybit"


def test_event_correlation_id_optional():
    e = Event(
        event_id="evt-001",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
    )
    assert e.correlation_id is None


# ============================================================
# Concrete event types — construction
# ============================================================


def test_fill_event_constructs():
    fill = make_fill_record()
    e = FillEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        fill=fill,
    )
    assert e.fill == fill
    assert e.fill.fill_quantity == Decimal("0.001")


def test_position_update_event_constructs():
    pos = make_position()
    e = PositionUpdateEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        position=pos,
    )
    assert e.position == pos
    assert e.position.quantity == Decimal("0.5")


def test_balance_update_event_constructs():
    bal = make_balance()
    e = BalanceUpdateEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        balance=bal,
    )
    assert e.balance == bal
    assert e.balance.free == Decimal("9000")


def test_connection_state_event_constructs():
    e = ConnectionStateEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        connected=True,
    )
    assert e.connected is True


def test_order_placed_event_constructs():
    order = make_order_record()
    e = OrderPlacedEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        order=order,
    )
    assert e.order == order
    assert e.order.status == OrderStatus.OPEN


def test_order_modified_event_constructs():
    prev = make_order_record()
    updated = make_order_record()
    e = OrderModifiedEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        order=updated,
        previous=prev,
    )
    assert e.order == updated
    assert e.previous == prev


def test_order_cancelled_event_constructs():
    inst = make_instrument()
    e = OrderCancelledEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id="corr-1",
        client_order_id="abc",
        instrument=inst,
    )
    assert e.client_order_id == "abc"
    assert e.instrument == inst


def test_reconciliation_complete_event_clean():
    e = ReconciliationCompleteEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        mismatches=(),
    )
    assert e.mismatches == ()


def test_reconciliation_complete_event_with_mismatches():
    m = ReconciliationMismatch(
        mismatch_type="position_quantity",
        instrument=make_instrument(),
        local_value='{"qty": 0.5}',
        platform_value='{"qty": 1.0}',
    )
    e = ReconciliationCompleteEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        mismatches=(m,),
    )
    assert len(e.mismatches) == 1
    assert e.mismatches[0].mismatch_type == "position_quantity"


def test_halt_entered_event_instrument_scope():
    inst = make_instrument()
    e = HaltEnteredEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        scope="instrument",
        instrument=inst,
        reason="position_quantity_mismatch",
        detail="Local qty 0.5 vs platform qty 1.0",
    )
    assert e.scope == "instrument"
    assert e.instrument == inst
    assert e.reason == "position_quantity_mismatch"


def test_halt_entered_event_account_scope():
    e = HaltEnteredEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        scope="account",
        instrument=None,
        reason="balance_mismatch",
        detail="USDT free 9000 vs platform 8000",
    )
    assert e.scope == "account"
    assert e.instrument is None


def test_halt_cleared_event_automatic():
    e = HaltClearedEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        scope="instrument",
        instrument=make_instrument(),
        cleared_by="automatic",
    )
    assert e.cleared_by == "automatic"


def test_halt_cleared_event_manual():
    e = HaltClearedEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        scope="account",
        instrument=None,
        cleared_by="manual",
    )
    assert e.cleared_by == "manual"


# ============================================================
# ReconciliationMismatch
# ============================================================


def test_reconciliation_mismatch_all_types():
    for mtype in (
        "position_quantity",
        "balance",
        "orphan_on_platform",
        "orphan_in_local",
        "partial_fill",
    ):
        m = ReconciliationMismatch(
            mismatch_type=mtype,
            instrument=make_instrument() if mtype != "balance" else None,
            local_value="{}",
            platform_value="{}",
        )
        assert m.mismatch_type == mtype


# ============================================================
# ReconciliationEvent — audit record
# ============================================================


def test_reconciliation_event_constructs():
    m = ReconciliationMismatch(
        mismatch_type="balance",
        instrument=None,
        local_value='{"free": 9000}',
        platform_value='{"free": 8000}',
    )
    e = ReconciliationEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        mismatches=(m,),
        duration_ms=42.5,
    )
    assert len(e.mismatches) == 1
    assert e.duration_ms == 42.5


# ============================================================
# HaltEvent — audit record
# ============================================================


def test_halt_event_entered():
    e = HaltEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        action="entered",
        scope="instrument",
        instrument=make_instrument(),
        reason="position_quantity_mismatch",
        detail="qty drift",
        cleared_by=None,
    )
    assert e.action == "entered"
    assert e.cleared_by is None


def test_halt_event_cleared():
    e = HaltEvent(
        event_id="evt-1",
        timestamp=NOW,
        adapter_name="bybit",
        account_id="acct-1",
        correlation_id=None,
        action="cleared",
        scope="instrument",
        instrument=make_instrument(),
        reason="",
        detail="manual clear by operator",
        cleared_by="manual",
    )
    assert e.action == "cleared"
    assert e.cleared_by == "manual"


# ============================================================
# EventBus — subscribe / publish / unsubscribe
# ============================================================


class TestEventBus:
    def test_subscribe_and_receive(self):
        received = []
        bus = EventBus()
        bus.subscribe(Event, lambda e: received.append(e))
        evt = Event(
            event_id="e1", timestamp=NOW, adapter_name="a", account_id="acct", correlation_id=None
        )
        bus.publish(evt)
        assert len(received) == 1
        assert received[0] is evt

    def test_publish_to_multiple_subscribers(self):
        received = []
        bus = EventBus()
        bus.subscribe(Event, lambda e: received.append("a"))
        bus.subscribe(Event, lambda e: received.append("b"))
        bus.publish(
            Event(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
            )
        )
        assert received == ["a", "b"]

    def test_subscriber_order_is_respected(self):
        received = []
        bus = EventBus()
        bus.subscribe(Event, lambda e: received.append(1))
        bus.subscribe(Event, lambda e: received.append(2))
        bus.subscribe(Event, lambda e: received.append(3))
        bus.publish(
            Event(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
            )
        )
        assert received == [1, 2, 3]

    def test_unsubscribe_stops_delivery(self):
        received = []
        bus = EventBus()

        def cb(e):
            received.append(e)

        bus.subscribe(Event, cb)
        bus.unsubscribe(Event, cb)
        bus.publish(
            Event(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
            )
        )
        assert received == []

    def test_unsubscribe_nonexistent_is_noop(self):
        bus = EventBus()
        bus.unsubscribe(Event, lambda e: None)  # does not raise

    def test_subscriber_exception_does_not_block_others(self):
        received = []
        bus = EventBus()

        def bad(e):
            raise RuntimeError("boom")

        def good(e):
            received.append(e)

        bus.subscribe(Event, bad)
        bus.subscribe(Event, good)
        bus.publish(
            Event(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
            )
        )
        assert len(received) == 1

    def test_publish_with_no_subscribers_does_not_crash(self):
        bus = EventBus()
        bus.publish(
            Event(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
            )
        )

    def test_supertype_subscription_receives_subtypes(self):
        received = []
        bus = EventBus()
        bus.subscribe(Event, lambda e: received.append(type(e).__name__))
        bus.publish(
            FillEvent(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
                fill=make_fill_record(),
            )
        )
        bus.publish(
            ConnectionStateEvent(
                event_id="e2",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
                connected=True,
            )
        )
        assert received == ["FillEvent", "ConnectionStateEvent"]

    def test_specific_type_subscription_receives_only_that_type(self):
        received = []
        bus = EventBus()
        bus.subscribe(FillEvent, lambda e: received.append(e))
        bus.publish(
            ConnectionStateEvent(
                event_id="e1",
                timestamp=NOW,
                adapter_name="a",
                account_id="acct",
                correlation_id=None,
                connected=True,
            )
        )
        assert received == []

    def test_eventbus_does_not_import_state_store(self):
        """The EventBus must not know about audit writes — that's the Engine's job."""
        import inspect

        source = inspect.getsource(EventBus.publish)
        assert "write_audit" not in source
        assert "state_store" not in source

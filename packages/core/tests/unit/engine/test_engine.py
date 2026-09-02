"""Engine and dispatch integration tests.

Uses MockAdapter + in-memory SQLiteStateStore for full-stack testing
without hitting a real platform.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    EngineShutdownError,
    InstrumentHaltedError,
    InvalidSymbolError,
    OrderNotFoundError,
    RateLimitError,
    ReconciliationError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
    AuditEvent,
    EventBus,
    OrderCancelledEvent,
    OrderModifiedEvent,
    OrderPlacedEvent,
    ReconciliationCompleteEvent,
)
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import (
    HaltConfig,
    HaltStateMachine,
    StateStore,
)
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.testing import MockAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

# ── helpers ──────────────────────────────────────────────────────────


def _instrument(symbol: str = "BTCUSDT") -> Instrument:
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


def _spec(**overrides: object) -> InstrumentSpec:
    d: dict[str, object] = dict(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("100"),
        min_notional=Decimal("10"),
        price_precision=2,
        qty_precision=3,
    )
    d.update(overrides)
    return InstrumentSpec(**d)  # type: ignore[arg-type]


def _order(**kwargs: object) -> UnifiedOrder:
    defaults: dict[str, object] = dict(
        instrument=_instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        price=Decimal("50000"),
    )
    defaults.update(kwargs)
    return UnifiedOrder(**defaults)  # type: ignore[arg-type]


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _ref_price(instrument: Instrument) -> Decimal | None:
    return Decimal("50000")


# ── fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def mock_adapter(event_bus: EventBus) -> MockAdapter:
    adapter = MockAdapter(event_bus=event_bus)
    adapter.add_instrument_spec(_instrument(), _spec())
    return adapter


@pytest.fixture
async def engine(mock_adapter: MockAdapter, event_bus: EventBus) -> Engine:
    store = SQLiteStateStore(":memory:")
    eng = Engine(
        adapter=mock_adapter,
        state_store=store,
        event_bus=event_bus,
        get_reference_price=_ref_price,
    )
    await eng.connect()
    yield eng
    try:
        await eng.ashutdown()
    except Exception:
        pass


# ── lifecycle ────────────────────────────────────────────────────────


class TestEngineLifecycle:
    async def test_connect_initializes_and_connects(self, engine, mock_adapter):
        assert mock_adapter.is_connected

    async def test_disconnect(self, engine, mock_adapter):
        await engine.disconnect()
        assert not mock_adapter.is_connected

    async def test_shutdown_marks_dead(self, engine):
        await engine.ashutdown()
        with pytest.raises(EngineShutdownError):
            await engine.place_order(_order())

    async def test_shutdown_is_idempotent(self, engine):
        await engine.ashutdown()
        await engine.ashutdown()  # should not raise

    async def test_connect_populates_known_order_ids(self, engine, mock_adapter):
        # Place an order, then create a new engine against the same state store
        # to verify IDs are seeded on connect.
        result = await engine.place_order(_order())
        known = engine._known_order_ids
        assert result.client_order_id in known

    async def test_get_audit_events_returns_written_records(self, engine):
        await engine.state_store.write_audit_event(
            AuditEvent(
                event_id="audit-1",
                timestamp=_utcnow(),
                adapter_name="mock",
                account_id="acc",
                correlation_id="corr-1",
                event_type="bybit.leverage.applied",
                payload={"symbol": "BTCUSDT", "leverage": 10},
            )
        )
        results = await engine.get_audit_events()
        assert len(results) == 1
        assert results[0].event_type == "bybit.leverage.applied"


class TestEngineConstruction:
    def test_auto_creates_event_bus_when_none_provided(self, mock_adapter):
        store = SQLiteStateStore(":memory:")
        eng = Engine(adapter=mock_adapter, state_store=store)
        assert eng.event_bus is not None

    def test_auto_creates_state_store_when_none_provided(self, mock_adapter):
        eng = Engine(adapter=mock_adapter)
        assert isinstance(eng.state_store, SQLiteStateStore)
        assert eng.state_store.path != ":memory:"

    def test_default_state_store_path_is_user_visible(self, mock_adapter, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        eng = Engine(adapter=mock_adapter)
        path = eng.state_store.path
        assert path.endswith(".db")
        assert "mock" in path
        assert "mock_account" in path

    def test_default_state_store_initializes_to_file(self, mock_adapter, monkeypatch, tmp_path):
        import asyncio

        monkeypatch.chdir(tmp_path)
        eng = Engine(adapter=mock_adapter)
        asyncio.run(eng.connect())
        assert eng.state_store.path != ":memory:"
        assert (tmp_path / eng.state_store.path[2:]).exists()

    def test_accepts_risk_config(self, mock_adapter):
        store = SQLiteStateStore(":memory:")
        cfg = RiskConfig(max_order_size=Decimal("50"))
        eng = Engine(adapter=mock_adapter, state_store=store, risk_config=cfg)
        assert eng.risk_config.max_order_size == Decimal("50")

    def test_accepts_halt_config(self, mock_adapter):
        store = SQLiteStateStore(":memory:")
        cfg = HaltConfig(auto_halt_enabled=False)
        eng = Engine(adapter=mock_adapter, state_store=store, halt_config=cfg)
        assert eng.halt_machine.config.auto_halt_enabled is False

    def test_exposes_halt_machine(self, engine):
        assert isinstance(engine.halt_machine, HaltStateMachine)

    def test_exposes_state_store(self, engine):
        assert isinstance(engine.state_store, StateStore)


# ── adapter method auto-proxy via __getattr__ ─────────────────────────


class TestAdapterAutoProxy:
    """Async Engine proxies unknown attributes to the adapter coroutine."""

    async def test_proxies_adapter_coroutine(self, mock_adapter, event_bus):
        """Adapter-specific methods are returned as coroutines (caller awaits)."""
        eng = Engine(adapter=mock_adapter, event_bus=event_bus)

        async def _fake() -> dict:
            return {"BTC": "fake"}

        mock_adapter.fetch_account_leverage = _fake  # type: ignore[attr-defined]

        method = eng.fetch_account_leverage
        assert asyncio.iscoroutinefunction(method)
        assert await method() == {"BTC": "fake"}

    async def test_unknown_attribute_raises(self, mock_adapter, event_bus):
        """An attribute absent from both Engine and adapter raises AttributeError."""
        eng = Engine(adapter=mock_adapter, event_bus=event_bus)

        with pytest.raises(AttributeError, match="Engine"):
            eng.nonexistent_method()


# ── place_order ──────────────────────────────────────────────────────


class TestPlaceOrder:
    async def test_returns_order_result(self, engine):
        result = await engine.place_order(_order())
        assert isinstance(result, OrderResult)
        assert result.status == OrderStatus.OPEN

    async def test_generates_client_order_id_when_none(self, engine):
        order = _order(client_order_id=None)
        result = await engine.place_order(order)
        assert result.client_order_id is not None
        assert len(result.client_order_id) > 0

    async def test_respects_user_supplied_client_order_id(self, engine):
        result = await engine.place_order(_order(client_order_id="my-id"))
        assert result.client_order_id == "my-id"

    async def test_rejects_duplicate_client_order_id(self, engine):
        await engine.place_order(_order(client_order_id="dup-id"))
        with pytest.raises(DuplicateOrderIdError, match="already in use"):
            await engine.place_order(_order(client_order_id="dup-id"))

    async def test_rejects_unsupported_order_type(self, engine, mock_adapter):
        mock_adapter.set_supported_order_types(frozenset({OrderType.MARKET}))
        with pytest.raises(UnsupportedOrderTypeError):
            await engine.place_order(_order(order_type=OrderType.LIMIT))

    async def test_rejects_unknown_instrument(self, engine, mock_adapter):
        # MockAdapter has no spec for this instrument
        order = _order(instrument=_instrument("UNKNOWN"))
        with pytest.raises(InvalidSymbolError):
            await engine.place_order(order)

    async def test_persists_to_state_store(self, engine):
        result = await engine.place_order(_order(client_order_id="persist-test"))
        stored = await engine.state_store.get_order(result.client_order_id)
        assert stored is not None
        assert stored.client_order_id == "persist-test"

    async def test_publishes_order_placed_event(self, engine, event_bus):
        events: list = []
        event_bus.subscribe(OrderPlacedEvent, lambda e: events.append(e))
        await engine.place_order(_order())
        assert len(events) == 1
        assert isinstance(events[0], OrderPlacedEvent)
        assert events[0].order is not None

    async def test_decrements_rate_limit_budget(self, engine):
        initial = engine._rate_limit_budget
        await engine.place_order(_order())
        assert engine._rate_limit_budget == initial - 1

    async def test_market_order_works(self, engine):
        result = await engine.place_order(
            _order(
                order_type=OrderType.MARKET,
                price=None,
            )
        )
        assert result.status == OrderStatus.OPEN

    async def test_audit_event_persisted_to_db(self, engine):
        """After place_order, an audit row must exist in the state store DB."""
        result = await engine.place_order(_order(client_order_id="audit-verify"))
        # Query the state store's internal SQLite connection directly
        conn = engine.state_store.conn  # type: ignore[attr-defined]
        cursor = await conn.execute(
            "SELECT event_type, correlation_id, payload_json FROM audit_events "
            "WHERE json_extract(payload_json, '$.client_order_id') = ?",
            (result.client_order_id,),
        )
        row = await cursor.fetchone()
        assert row is not None, "audit event must be written to the DB"
        assert row["event_type"] == "order.placed"
        # correlation_id must match what was used during dispatch
        assert row["correlation_id"] is not None


# ── modify_order ─────────────────────────────────────────────────────


class TestModifyOrder:
    async def test_modifies_price(self, engine, mock_adapter):
        await engine.place_order(_order(client_order_id="mod-test", price=Decimal("50000")))
        modification = OrderModification(client_order_id="mod-test", price=Decimal("51000"))
        result = await engine.modify_order(modification)
        assert result.status == OrderStatus.OPEN

    async def test_persists_updated_record(self, engine, mock_adapter):
        await engine.place_order(_order(client_order_id="mod-persist", price=Decimal("50000")))
        mod = OrderModification(client_order_id="mod-persist", price=Decimal("51000"))
        await engine.modify_order(mod)
        stored = await engine.state_store.get_order("mod-persist")
        assert stored is not None
        assert stored.price == Decimal("51000")

    async def test_publishes_order_modified_event(self, engine, event_bus):
        await engine.place_order(_order(client_order_id="mod-event", price=Decimal("50000")))
        events: list = []
        event_bus.subscribe(OrderModifiedEvent, lambda e: events.append(e))
        mod = OrderModification(client_order_id="mod-event", price=Decimal("51000"))
        await engine.modify_order(mod)
        assert len(events) == 1
        assert isinstance(events[0], OrderModifiedEvent)
        assert events[0].previous is not None

    async def test_rejects_unknown_order(self, engine):
        mod = OrderModification(client_order_id="nonexistent", price=Decimal("100"))
        with pytest.raises(OrderNotFoundError):
            await engine.modify_order(mod)

    async def test_risk_checks_run_on_would_be_order(self, engine):
        await engine.place_order(_order(client_order_id="risk-mod", price=Decimal("50000")))
        # Modify to a wildly deviant price
        mod = OrderModification(client_order_id="risk-mod", price=Decimal("999999"))
        with pytest.raises(InvalidSymbolError, match="deviates"):
            await engine.modify_order(mod)

    async def test_modify_rejected_when_halted(self, engine, mock_adapter):
        await engine.place_order(_order(client_order_id="halted-mod", price=Decimal("50000")))
        engine.halt_machine.enter_halt("instrument", _instrument(), "test", "testing")
        mod = OrderModification(client_order_id="halted-mod", price=Decimal("51000"))
        with pytest.raises(InstrumentHaltedError):
            await engine.modify_order(mod)


# ── cancel_order ─────────────────────────────────────────────────────


class TestCancelOrder:
    async def test_cancels_order(self, engine, mock_adapter):
        result = await engine.place_order(_order(client_order_id="cancel-test"))
        cancel_result = await engine.cancel_order(result.client_order_id)
        assert cancel_result.status == OrderStatus.CANCELLED

    async def test_persists_cancelled_status(self, engine):
        result = await engine.place_order(_order(client_order_id="cancel-persist"))
        await engine.cancel_order(result.client_order_id)
        stored = await engine.state_store.get_order(result.client_order_id)
        assert stored is not None
        assert stored.status == OrderStatus.CANCELLED

    async def test_publishes_order_cancelled_event(self, engine, event_bus):
        result = await engine.place_order(_order(client_order_id="cancel-event"))
        events: list = []
        event_bus.subscribe(OrderCancelledEvent, lambda e: events.append(e))
        await engine.cancel_order(result.client_order_id)
        assert len(events) == 1
        assert isinstance(events[0], OrderCancelledEvent)
        assert events[0].client_order_id == "cancel-event"

    async def test_rejects_unknown_order(self, engine):
        with pytest.raises(OrderNotFoundError):
            await engine.cancel_order("nonexistent")

    async def test_cancelled_id_cannot_be_reused(self, engine):
        """Permanent uniqueness: cancelled IDs stay in the known set forever."""
        await engine.place_order(_order(client_order_id="cancel-perm"))
        await engine.cancel_order("cancel-perm")
        with pytest.raises(DuplicateOrderIdError, match="already in use"):
            await engine.place_order(_order(client_order_id="cancel-perm"))


# ── risk integration ─────────────────────────────────────────────────


class TestRiskIntegration:
    async def test_place_order_runs_risk_checks(self, engine, mock_adapter):
        # Quantity below minimum should be caught by risk checks
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            await engine.place_order(_order(quantity=Decimal("0.0001")))

    async def test_risk_config_controls_thresholds(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        cfg = RiskConfig(max_order_size=Decimal("0.5"))
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
            risk_config=cfg,
        )
        await eng.connect()
        try:
            with pytest.raises(InvalidSymbolError, match="exceeds global max"):
                await eng.place_order(_order(quantity=Decimal("1")))
        finally:
            await eng.ashutdown()


# ── halt enforcement ─────────────────────────────────────────────────


class TestHaltEnforcement:
    async def test_instrument_halt_blocks_place(self, engine):
        engine.halt_machine.enter_halt("instrument", _instrument(), "test", "halted")
        with pytest.raises(InstrumentHaltedError):
            await engine.place_order(_order())

    async def test_account_halt_blocks_place(self, engine):
        engine.halt_machine.enter_halt("account", None, "test", "account halted")
        with pytest.raises(InstrumentHaltedError):
            await engine.place_order(_order())

    async def test_reduce_only_allowed_when_halted_if_closing_permitted(self, engine):
        engine.halt_machine.enter_halt("instrument", _instrument(), "test", "halted")
        result = await engine.place_order(_order(reduce_only=True))
        assert result.status == OrderStatus.OPEN

    async def test_reduce_only_blocked_when_closing_not_permitted(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        cfg = HaltConfig(closing_orders_permitted=False)
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
            halt_config=cfg,
        )
        await eng.connect()
        try:
            eng.halt_machine.enter_halt("instrument", _instrument(), "test", "halted")
            with pytest.raises(InstrumentHaltedError):
                await eng.place_order(_order(reduce_only=True))
        finally:
            await eng.ashutdown()

    async def test_halt_does_not_block_cancel(self, engine, mock_adapter):
        result = await engine.place_order(_order(client_order_id="halt-cancel"))
        engine.halt_machine.enter_halt("instrument", _instrument(), "test", "halted")
        # Cancel should still work
        cancel = await engine.cancel_order(result.client_order_id)
        assert cancel.status == OrderStatus.CANCELLED


# ── rate-limit tracking ──────────────────────────────────────────────


class TestRateLimitTracking:
    async def test_rate_limit_exhausted_blocks_dispatch(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        # reset_at in the distant future so the budget doesn't auto-refresh
        far_future = datetime(2099, 1, 1, tzinfo=UTC)
        mock_adapter.set_rate_limits(
            RateLimits(
                requests_per_interval=1,
                interval_seconds=60.0,
                remaining=1,
                reset_at=far_future,
            )
        )
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
        )
        await eng.connect()
        try:
            # First order consumes the last budget unit
            await eng.place_order(_order(client_order_id="rl-1"))
            # Second should be rejected
            with pytest.raises(RateLimitError, match="budget exhausted"):
                await eng.place_order(_order(client_order_id="rl-2"))
        finally:
            await eng.ashutdown()

    async def test_budget_override_takes_precedence(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        cfg = RiskConfig(rate_limit_budget_override=999)
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
            risk_config=cfg,
        )
        await eng.connect()
        try:
            # Should use override (999), not adapter's reported budget
            assert eng._effective_budget() == 999
        finally:
            await eng.ashutdown()


# ── instrument spec caching ──────────────────────────────────────────


class TestInstrumentSpecCaching:
    async def test_caches_fetched_spec(self, engine, mock_adapter):
        """The Engine delegates to the adapter for spec fetching; the adapter's
        own cache (MockAdapter._instrument_specs) returns the same object."""
        spec1 = await engine.fetch_instrument_spec(_instrument())
        spec2 = await engine.fetch_instrument_spec(_instrument())
        assert spec1 is spec2  # same object — adapter cached

    async def test_respects_adapter_spec_after_change(self, engine, mock_adapter):
        """The Engine always consults the adapter for the current spec — it does
        not hold a stale cache.  When the adapter returns a different spec, the
        next place_order uses the new spec."""
        # Add a spec with a high min_qty to the adapter
        mock_adapter.add_instrument_spec(_instrument(), _spec(min_qty=Decimal("100")))
        # The engine must pick up the adapter's current spec; quantity=1 < min_qty=100
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            await engine.place_order(_order(quantity=Decimal("1")))


# ── state mirror subscriptions ───────────────────────────────────────


class TestStateMirrorSubscriptions:
    async def test_fill_event_persisted_to_state_store(self, engine, mock_adapter):
        await engine.place_order(_order(client_order_id="fill-test"))
        fill = FillRecord(
            client_order_id="fill-test",
            platform_fill_id="pf-001",
            instrument=_instrument(),
            fill_quantity=Decimal("0.5"),
            fill_price=Decimal("50000"),
            fill_timestamp=_utcnow(),
            fee_currency="USDT",
            fee_amount=Decimal("1"),
            correlation_id="corr-001",
        )
        mock_adapter.inject_fill(fill)
        # Allow the asyncio task to run
        await asyncio.sleep(0.01)
        fills = await engine.get_fill_history(instrument=_instrument())
        assert len(fills) >= 1

    async def test_position_update_persisted(self, engine, mock_adapter):
        pos = Position(
            instrument=_instrument(),
            quantity=Decimal("1.5"),
            average_entry_price=Decimal("50000"),
            updated_at=_utcnow(),
            position_id="1",
        )
        mock_adapter.inject_position_update(pos)
        await asyncio.sleep(0.01)
        stored = await engine.get_positions(_instrument())
        assert len(stored) == 1
        assert stored[0].quantity == Decimal("1.5")
        assert stored[0].position_id == "1"

    async def test_balance_update_persisted(self, engine, mock_adapter):
        bal = Balance(
            currency="USDT",
            free=Decimal("9000"),
            used=Decimal("1000"),
            total=Decimal("10000"),
            updated_at=_utcnow(),
        )
        mock_adapter.inject_balance_update(bal)
        await asyncio.sleep(0.01)
        stored = await engine.get_balance("USDT")
        assert stored is not None
        assert stored.total == Decimal("10000")


# ── shutdown behaviour ───────────────────────────────────────────────


class TestShutdownBehavior:
    async def test_all_operations_blocked_after_shutdown(self, engine):
        await engine.ashutdown()
        with pytest.raises(EngineShutdownError):
            await engine.place_order(_order())
        with pytest.raises(EngineShutdownError):
            await engine.modify_order(OrderModification("any", price=Decimal("100")))
        with pytest.raises(EngineShutdownError):
            await engine.cancel_order("any")
        with pytest.raises(EngineShutdownError):
            await engine.get_order("any")
        with pytest.raises(EngineShutdownError):
            await engine.fetch_instrument_spec(_instrument())

    async def test_shutdown_is_idempotent(self, engine):
        await engine.ashutdown()
        await engine.ashutdown()


# ── reconcile ────────────────────────────────────────────────────────


class TestReconcile:
    async def test_reconcile_returns_result(self, engine):
        result = await engine.reconcile()
        assert result.is_clean  # empty state is clean

    async def test_reconcile_blocked_after_shutdown(self, engine):
        await engine.ashutdown()
        with pytest.raises(EngineShutdownError):
            await engine.reconcile()


class TestReconcileOnReconnect:
    """Section 6.1 / 9.4 — a reconnect automatically triggers a reconcile."""

    async def test_initial_connect_does_not_reconcile(self, mock_adapter, event_bus):
        """The first connect is not a reconnect — no automatic reconcile."""
        store = SQLiteStateStore(":memory:")
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
        )
        completed: list[ReconciliationCompleteEvent] = []
        event_bus.subscribe(ReconciliationCompleteEvent, completed.append)
        await eng.connect()
        assert eng._last_connected is True
        assert eng._reconcile_task is None  # nothing scheduled
        assert completed == []
        await eng.ashutdown()

    async def test_disconnect_then_reconnect_triggers_reconcile(
        self, engine, mock_adapter, event_bus
    ):
        """A False -> True transition (reconnect) schedules a reconcile."""
        completed: list[ReconciliationCompleteEvent] = []
        event_bus.subscribe(ReconciliationCompleteEvent, completed.append)

        await mock_adapter.disconnect()
        assert engine._last_connected is False

        await mock_adapter.connect()
        assert engine._reconcile_task is not None
        await engine._reconcile_task  # let the scheduled reconcile finish
        assert completed, "reconcile should have run on reconnect"

    async def test_reconnect_does_not_stack_reconciles(self, engine, mock_adapter, monkeypatch):
        """A second reconnect while a reconcile is in flight is not stacked."""
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0
        original_reconcile = engine.reconcile

        async def _slow_reconcile():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()  # hold the first reconcile open
            return await original_reconcile()

        monkeypatch.setattr(engine, "reconcile", _slow_reconcile)

        await mock_adapter.disconnect()
        await mock_adapter.connect()
        await started.wait()  # first reconcile is now in flight

        await mock_adapter.disconnect()
        await mock_adapter.connect()
        assert engine._reconcile_task is not None

        release.set()
        await engine._reconcile_task
        assert calls == 1  # second reconnect did not stack a second reconcile

    async def test_reconnect_after_shutdown_is_ignored(self, mock_adapter, event_bus):
        """Reconnect events arriving after shutdown schedule nothing."""
        store = SQLiteStateStore(":memory:")
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
        )
        await eng.connect()
        await eng.ashutdown()
        # Reconnect fires after teardown — must be ignored, not crash.
        await mock_adapter.connect()
        assert eng._reconcile_task is None


class TestReconcileMismatchCases:
    """Per-case mismatch detection and resolution (Section 6.3)."""

    async def test_position_quantity_mismatch_detected(self, engine, mock_adapter):
        """Case 1: local and platform positions differ in quantity."""
        # Seed a local position
        local_pos = Position(
            instrument=_instrument(),
            quantity=Decimal("1"),
            average_entry_price=Decimal("50000"),
            updated_at=_utcnow(),
            position_id="1",
        )
        await engine.state_store.upsert_position(local_pos)

        # Seed a different platform position
        platform_pos = Position(
            instrument=_instrument(),
            quantity=Decimal("2"),
            average_entry_price=Decimal("50000"),
            updated_at=_utcnow(),
            position_id="1",
        )
        mock_adapter.seed_position(platform_pos)

        result = await engine.reconcile()
        assert not result.is_clean
        assert len(result.position_mismatches) == 1
        assert result.position_mismatches[0].mismatch_type == "position_quantity"
        assert result.position_mismatches[0].instrument == _instrument()

        # Resolution: local should be overwritten with platform truth
        stored = await engine.get_positions(_instrument())
        assert len(stored) == 1
        assert stored[0].quantity == Decimal("2")

        # Halt should be entered for the instrument
        assert engine.halt_machine.is_instrument_halted(_instrument())

    async def test_balance_mismatch_detected(self, engine, mock_adapter):
        """Case 2: local and platform balances differ."""
        local_bal = Balance(
            currency="USDT",
            free=Decimal("1000"),
            used=Decimal("0"),
            total=Decimal("1000"),
            updated_at=_utcnow(),
        )
        await engine.state_store.upsert_balance(local_bal)

        platform_bal = Balance(
            currency="USDT",
            free=Decimal("900"),
            used=Decimal("100"),
            total=Decimal("1000"),
            updated_at=_utcnow(),
        )
        mock_adapter.seed_balance(platform_bal)

        result = await engine.reconcile()
        assert not result.is_clean
        assert len(result.balance_mismatches) == 1
        assert result.balance_mismatches[0].mismatch_type == "balance"

        # Resolution: local should be overwritten (corrected silently)
        stored = await engine.get_balance("USDT")
        assert stored is not None
        assert stored.free == Decimal("900")

        # Balance drift is corrected silently — it never halts (equity/margin
        # float with live P&L, so a delta is normal intraday movement).
        assert not engine.halt_machine.is_account_halted()

    async def test_orphan_on_platform_imported(self, engine, mock_adapter):
        """Case 3: order exists on platform but not locally — import it."""

        platform_order = OrderRecord(
            instrument=_instrument(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id="orphan-platform",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="pf-orphan-1",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-orphan",
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        mock_adapter.seed_order(platform_order)
        mock_adapter.seed_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("0"),
                updated_at=_utcnow(),
            )
        )

        result = await engine.reconcile()
        assert len(result.orphan_orders_on_platform) == 1
        assert result.orphan_orders_on_platform[0].client_order_id == "orphan-platform"

        # After reconcile, the order should exist locally
        stored = await engine.state_store.get_order("orphan-platform")
        assert stored is not None

    async def test_orphan_in_local_detected(self, engine, mock_adapter):
        """Case 4: order exists locally but not on platform."""
        # Seed order directly into the state store — bypassing the adapter
        # so the order only exists locally, not in MockAdapter's internal book.

        local_order = OrderRecord(
            instrument=_instrument(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id="orphan-local",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="pf-local-orphan",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-local-orphan",
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        await engine.state_store.upsert_order(local_order)

        # Seed an empty platform order book (only the position to avoid
        # position-mismatch noise).
        mock_adapter.seed_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("0"),
                updated_at=_utcnow(),
            )
        )

        rec_result = await engine.reconcile()
        assert len(rec_result.orphan_orders_in_local) == 1
        assert "orphan-local" in rec_result.orphan_orders_in_local

        # Resolution: the order must be removed from the local mirror
        assert await engine.state_store.get_order("orphan-local") is None

    async def test_partial_fill_discrepancy_detected(self, engine, mock_adapter):
        """Case 5: local and platform fill quantities differ for the same order."""
        # Establish a "clean through" watermark in the past so this pass
        # compares fills newer than it.  (Forward-only bootstrap otherwise
        # starts the watermark at "now" and skips pre-seeded fills.)
        await engine.state_store.set_reconcile_watermark(
            datetime.now(tz=UTC) - timedelta(minutes=5)
        )

        # Place an order locally
        await engine.place_order(_order(client_order_id="fill-disc"))

        # Seed a position so reconcile doesn't halt on position mismatch
        mock_adapter.seed_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("0"),
                updated_at=_utcnow(),
            )
        )

        # Seed a local fill
        local_fill = FillRecord(
            client_order_id="fill-disc",
            platform_fill_id="lf-001",
            instrument=_instrument(),
            fill_quantity=Decimal("0.3"),
            fill_price=Decimal("50000"),
            fill_timestamp=_utcnow(),
            fee_currency="USDT",
            fee_amount=Decimal("1"),
            correlation_id="corr-local",
        )
        await engine.state_store.upsert_fill(local_fill)

        # Seed a different platform fill
        platform_fill = FillRecord(
            client_order_id="fill-disc",
            platform_fill_id="pf-001",
            instrument=_instrument(),
            fill_quantity=Decimal("0.7"),
            fill_price=Decimal("50000"),
            fill_timestamp=_utcnow(),
            fee_currency="USDT",
            fee_amount=Decimal("1"),
            correlation_id="corr-platform",
        )
        mock_adapter.seed_fill(platform_fill)

        result = await engine.reconcile()
        assert len(result.partial_fill_discrepancies) == 1
        assert result.partial_fill_discrepancies[0].mismatch_type == "partial_fill"

        # Resolution: local fills should be overwritten with platform fills
        fills_after = await engine.get_fill_history()
        assert len(fills_after) == 1
        assert fills_after[0].fill_quantity == Decimal("0.7")

    async def test_clean_reconcile_clears_existing_halt(self, engine, mock_adapter):
        """A clean reconciliation pass clears an existing halt."""
        engine.halt_machine.enter_halt("instrument", _instrument(), "test", "halted")
        assert engine.halt_machine.is_instrument_halted(_instrument())

        result = await engine.reconcile()
        assert result.is_clean
        assert not engine.halt_machine.is_instrument_halted(_instrument())


class TestReconcileHaltScoping:
    """Only position/balance drift halts; orphan and partial-fill are corrected silently."""

    async def test_orphan_in_local_does_not_halt(self, engine, mock_adapter):
        local_order = OrderRecord(
            instrument=_instrument(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id="orphan-local",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="pf-local-orphan",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-local-orphan",
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        await engine.state_store.upsert_order(local_order)
        mock_adapter.seed_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("0"),
                updated_at=_utcnow(),
            )
        )

        result = await engine.reconcile()
        assert result.orphan_orders_in_local == ["orphan-local"]
        # Corrected without halting — orphan is an order artefact, not drift.
        assert engine.halt_machine.active_halts() == []

    async def test_partial_fill_does_not_halt(self, engine, mock_adapter):
        await engine.state_store.set_reconcile_watermark(
            datetime.now(tz=UTC) - timedelta(minutes=5)
        )
        await engine.place_order(_order(client_order_id="fill-disc"))
        mock_adapter.seed_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("0"),
                updated_at=_utcnow(),
            )
        )
        await engine.state_store.upsert_fill(
            FillRecord(
                client_order_id="fill-disc",
                platform_fill_id="lf-001",
                instrument=_instrument(),
                fill_quantity=Decimal("0.3"),
                fill_price=Decimal("50000"),
                fill_timestamp=_utcnow(),
                fee_currency="USDT",
                fee_amount=Decimal("1"),
                correlation_id="corr-local",
            )
        )
        mock_adapter.seed_fill(
            FillRecord(
                client_order_id="fill-disc",
                platform_fill_id="pf-001",
                instrument=_instrument(),
                fill_quantity=Decimal("0.7"),
                fill_price=Decimal("50000"),
                fill_timestamp=_utcnow(),
                fee_currency="USDT",
                fee_amount=Decimal("1"),
                correlation_id="corr-platform",
            )
        )

        result = await engine.reconcile()
        assert len(result.partial_fill_discrepancies) == 1
        assert engine.halt_machine.active_halts() == []


class TestReconcileTriState:
    """Unsupported datasets are skipped; other fetch failures abort the pass loudly."""

    async def test_unsupported_fetch_is_skipped(self, engine, mock_adapter):
        # A local position with an unsupported platform fetch must not be
        # flagged as local-only (the unsupported fetch is skipped, not empty).
        await engine.state_store.upsert_position(
            Position(
                instrument=_instrument(),
                quantity=Decimal("1"),
                average_entry_price=Decimal("50000"),
                updated_at=_utcnow(),
                position_id="1",
            )
        )
        mock_adapter.set_next_error(NotImplementedError("no bulk position fetch"))

        result = await engine.reconcile()
        assert result.position_mismatches == []
        assert not engine.halt_machine.is_instrument_halted(_instrument())

    async def test_fetch_error_aborts_pass(self, engine, mock_adapter):
        mock_adapter.set_next_error(RuntimeError("platform down"))
        with pytest.raises(ReconciliationError):
            await engine.reconcile()


class TestClearHalt:
    async def test_manual_clear(self, engine):
        engine.halt_machine.enter_halt("instrument", _instrument(), "reason", "detail")
        assert engine.halt_machine.is_instrument_halted(_instrument())

        cleared = await engine.clear_halt("instrument", instrument=_instrument())
        assert cleared is True
        assert not engine.halt_machine.is_instrument_halted(_instrument())

    async def test_manual_clear_non_halted_returns_false(self, engine):
        cleared = await engine.clear_halt("instrument", instrument=_instrument())
        assert cleared is False


class TestPeriodicReconcile:
    async def test_loop_task_created_when_enabled(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
            reconcile_interval_seconds=0.5,
        )
        await eng.connect()
        try:
            assert eng._reconcile_loop_task is not None
            assert not eng._reconcile_loop_task.done()
        finally:
            await eng.ashutdown()

    async def test_no_loop_task_when_disabled(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        eng = Engine(
            adapter=mock_adapter,
            state_store=store,
            event_bus=event_bus,
            get_reference_price=_ref_price,
            reconcile_interval_seconds=None,
        )
        await eng.connect()
        try:
            assert eng._reconcile_loop_task is None
        finally:
            await eng.ashutdown()

    def test_invalid_interval_raises(self, mock_adapter, event_bus):
        with pytest.raises(ValueError):
            Engine(adapter=mock_adapter, event_bus=event_bus, reconcile_interval_seconds=0)


# ── timeout idempotency (Section 9.2) ───────────────────────────────


class TestTimeoutIdempotency:
    async def test_timeout_with_order_on_platform_returns_existing(self, engine, mock_adapter):
        """When place_order times out but the order was actually placed,
        the engine queries the platform and returns the existing result."""
        mock_adapter.queue_place_order_response(TimeoutError())
        platform_result = OrderResult(
            client_order_id="timeout-found",
            platform_order_id="pf-timeout-001",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        mock_adapter.queue_get_order_response(platform_result)

        result = await engine.place_order(_order(client_order_id="timeout-found"))
        assert result.client_order_id == "timeout-found"
        assert result.platform_order_id == "pf-timeout-001"
        # ID must be added to known set
        assert "timeout-found" in engine._known_order_ids

    async def test_timeout_without_order_on_platform_propagates(self, engine, mock_adapter):
        """When place_order times out and the order is NOT on the platform,
        the timeout propagates to the caller."""
        mock_adapter.queue_place_order_response(TimeoutError())
        # No order on platform
        mock_adapter.queue_get_order_response(None)

        with pytest.raises(asyncio.TimeoutError):
            await engine.place_order(_order(client_order_id="timeout-miss"))

    async def test_timeout_does_not_consume_rate_limit_budget(self, engine, mock_adapter):
        """A timeout that finds the order on-platform should not decrement budget twice."""
        budget_before = engine._rate_limit_budget
        mock_adapter.queue_place_order_response(TimeoutError())
        platform_result = OrderResult(
            client_order_id="budget-timeout",
            platform_order_id="pf-budget",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        mock_adapter.queue_get_order_response(platform_result)

        await engine.place_order(_order(client_order_id="budget-timeout"))
        # Budget should NOT have been decremented (dispatch didn't succeed)
        assert engine._rate_limit_budget == budget_before


# ── edge cases ───────────────────────────────────────────────────────


class TestEdgeCases:
    async def test_place_order_with_tpsl(self, engine):
        order = _order(
            order_type=OrderType.STOP_LIMIT,
            price=Decimal("50000"),
            stop_price=Decimal("51000"),
            take_profit=TpSlAttachment(trigger_price=Decimal("52000")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("49000")),
        )
        result = await engine.place_order(order)
        assert result.status == OrderStatus.OPEN

    async def test_engine_without_reference_price_fn(self, mock_adapter, event_bus):
        store = SQLiteStateStore(":memory:")
        eng = Engine(adapter=mock_adapter, state_store=store, event_bus=event_bus)
        await eng.connect()
        try:
            # Should pass — price sanity skips when no reference price
            result = await eng.place_order(_order(price=Decimal("999999")))
            assert result.status == OrderStatus.OPEN
        finally:
            await eng.ashutdown()

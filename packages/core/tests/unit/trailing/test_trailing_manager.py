"""Unit tests for trailing lifecycle, routing, and position targeting."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from itertools import count

import pytest
import pytest_asyncio

from unified_trading_execution.adapter import (
    MarketDataCapability,
    PositionProtectionCapability,
    TrailingCapability,
)
from unified_trading_execution.errors import (
    InvalidOrderError,
    PlatformConnectionError,
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.events import EventBus, TrailingStateEvent
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.testing import MockAdapter
from unified_trading_execution.trailing.manager import (
    ALLOWED_TRAILING_TRANSITIONS,
    TrailingManager,
    ensure_transition_allowed,
    resolve_position_leg,
    resolve_routing,
)
from unified_trading_execution.trailing.types import (
    TERMINAL_TRAILING_PHASES,
    CandleTimeframe,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_inst(symbol="BTC", quote_currency="USDT"):
    return Instrument(
        symbol=symbol,
        quote_currency=quote_currency,
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_position(
    *,
    quantity="1",
    position_id="1",
    entry="50000",
    opened_at=NOW,
):
    return Position(
        instrument=make_inst(),
        quantity=Decimal(quantity),
        average_entry_price=Decimal(entry),
        updated_at=NOW,
        position_id=position_id,
        opened_at=opened_at,
    )


def make_spec(**overrides):
    values = {
        "offset_type": TrailingOffsetType.PRICE,
        "offset": Decimal("100"),
    }
    values.update(overrides)
    return TrailingSpec(**values)


def atr_spec():
    return TrailingSpec(
        offset_type=TrailingOffsetType.ATR,
        offset=Decimal("2"),
        atr_period=14,
        atr_timeframe=CandleTimeframe.H1,
    )


def make_protection(**overrides):
    values = {
        "supported": True,
        "can_set_stop": True,
        "can_set_take_profit": True,
        "can_clear_stop": True,
        "can_clear_take_profit": True,
        "preserves_unmentioned_side": True,
        "trigger_bases": frozenset({TrailingBasis.LAST}),
        "min_reprice_interval_seconds": 0.0,
        "reprice_gap": False,
    }
    values.update(overrides)
    return PositionProtectionCapability(**values)


def make_native(**overrides):
    values = {
        "supported": False,
        "offset_types": frozenset(),
        "bases": frozenset(),
        "activations": frozenset(),
        "supports_step": False,
        "supports_initial_stop": False,
        "supports_take_profit": False,
        "position_scoped": True,
        "min_reprice_interval_seconds": 0.0,
    }
    values.update(overrides)
    return TrailingCapability(**values)


def make_native_exact(**overrides):
    values = {
        "supported": True,
        "offset_types": frozenset({TrailingOffsetType.PRICE}),
        "bases": frozenset({TrailingBasis.LAST}),
        "activations": frozenset({TrailingActivation.IMMEDIATE, TrailingActivation.PRICE}),
        "supports_step": False,
        "supports_initial_stop": False,
        "supports_take_profit": False,
        "position_scoped": True,
        "min_reprice_interval_seconds": 0.0,
    }
    values.update(overrides)
    return TrailingCapability(**values)


def make_market(**overrides):
    values = {
        "ticker_bases": frozenset({TrailingBasis.LAST}),
        "min_ticker_interval_seconds": 0.0,
        "candle_timeframes": frozenset({CandleTimeframe.H1}),
        "streaming_ticker": False,
    }
    values.update(overrides)
    return MarketDataCapability(**values)


class CapabilityAdapter(MockAdapter):
    """MockAdapter with scripted capability declarations."""

    def __init__(self, event_bus, *, protection, native, market):
        super().__init__(event_bus=event_bus)
        self.protection = protection
        self.native = native
        self.market = market

    def position_protection_capability(self, instrument):
        return self.protection

    def trailing_capability(self, instrument):
        return self.native

    def market_data_capability(self, instrument):
        return self.market


@pytest_asyncio.fixture
async def store():
    s = SQLiteStateStore(":memory:")
    await s.initialize()
    yield s
    await s.close()


async def make_manager(
    store,
    *,
    protection=None,
    native=None,
    market=None,
    connect=True,
):
    bus = EventBus()
    adapter = CapabilityAdapter(
        bus,
        protection=protection or make_protection(),
        native=native or make_native(),
        market=market or make_market(),
    )
    if connect:
        await adapter.connect()
    ids = count(1)
    manager = TrailingManager(
        adapter,
        store,
        event_bus=bus,
        id_factory=lambda: f"id-{next(ids)}",
        clock=lambda: NOW,
    )
    return manager, adapter, bus


class TestAttachRouting:
    async def test_auto_falls_back_to_synthetic(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "native_unsupported"
        assert state.phase is TrailingPhase.ARMING
        assert state.position_id == "1"
        assert state.side is OrderSide.BUY
        assert state.entry_price == Decimal("50000")
        assert state.quantity == Decimal("1")
        assert state.position_opened_at == NOW
        assert state.created_at == NOW
        assert await store.get_trailing_state(state.trailing_id) == state

    async def test_auto_uses_native_exact_match(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, native=make_native_exact())

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert state.mode is TrailingMode.NATIVE
        assert state.routing_reason == "native_exact"
        assert state.resolved_basis is TrailingBasis.LAST
        assert state.protection_trigger_basis is TrailingBasis.LAST

    async def test_step_forces_synthetic(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, native=make_native_exact())

        state = await manager.attach(TrailingRequest(make_inst(), make_spec(step=Decimal("5"))))

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "native_unsupported_step"

    async def test_atr_offset_forces_synthetic(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, native=make_native_exact())

        state = await manager.attach(TrailingRequest(make_inst(), atr_spec()))

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "native_unsupported_atr"

    async def test_initial_stop_forces_synthetic(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, native=make_native_exact())

        spec = make_spec(initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("49000")))
        state = await manager.attach(TrailingRequest(make_inst(), spec))

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "native_unsupported_initial_stop"

    async def test_explicit_synthetic_skips_native(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, native=make_native_exact())

        state = await manager.attach(
            TrailingRequest(make_inst(), make_spec(), mode=TrailingMode.SYNTHETIC)
        )

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "synthetic_mode_requested"

    async def test_native_rejection_names_every_blocker(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(
            store,
            protection=make_protection(can_set_take_profit=False),
            native=make_native_exact(),
        )
        spec = make_spec(
            step=Decimal("5"),
            initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("49000")),
            take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("55000")),
        )

        with pytest.raises(TrailingNotSupportedError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), spec, mode=TrailingMode.NATIVE))

        message = str(excinfo.value)
        assert "step" in message
        assert "initial stop" in message
        assert "take profit" in message

    async def test_neither_mode_supported_lists_both(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, protection=make_protection(supported=False))

        with pytest.raises(TrailingNotSupportedError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        message = str(excinfo.value)
        assert "native" in message
        assert "position protection" in message

    async def test_unsupported_sampling_basis_is_rejected(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        with pytest.raises(TrailingNotSupportedError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), make_spec(basis=TrailingBasis.MARK)))

        message = str(excinfo.value)
        assert "mark" in message
        assert "last" in message

    async def test_atr_with_unknown_timeframe_is_unsupported(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, market=make_market(candle_timeframes=frozenset()))

        with pytest.raises(TrailingNotSupportedError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), atr_spec()))

        assert "ATR candles" in str(excinfo.value)


class TestPositionTargeting:
    async def test_single_leg_resolves(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert state.position_id == "1"

    async def test_explicit_position_id_targets_that_leg(self, store):
        await store.upsert_position(make_position(position_id="1", quantity="1"))
        await store.upsert_position(make_position(position_id="2", quantity="-2", entry="51000"))
        manager, _, _ = await make_manager(store)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec(), position_id="2"))

        assert state.position_id == "2"
        assert state.side is OrderSide.SELL
        assert state.quantity == Decimal("2")
        assert state.entry_price == Decimal("51000")

    async def test_ambiguous_legs_list_every_candidate(self, store):
        await store.upsert_position(make_position(position_id="1", quantity="1"))
        await store.upsert_position(make_position(position_id="2", quantity="-2", entry="51000"))
        manager, _, _ = await make_manager(store)

        with pytest.raises(TrailingStateError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        message = str(excinfo.value)
        assert "2 open positions" in message
        assert "'1'" in message
        assert "'2'" in message
        assert "long" in message
        assert "short" in message
        assert "entry 50000" in message
        assert "entry 51000" in message
        assert "opened" in message

    async def test_unknown_position_id_is_invalid_order(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        with pytest.raises(InvalidOrderError):
            await manager.attach(TrailingRequest(make_inst(), make_spec(), position_id="missing"))

    async def test_no_open_position_is_invalid_order(self, store):
        manager, _, _ = await make_manager(store)

        with pytest.raises(InvalidOrderError):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

    async def test_zero_quantity_leg_is_not_targeted(self, store):
        await store.upsert_position(make_position(position_id="1", quantity="0"))
        manager, _, _ = await make_manager(store)

        with pytest.raises(InvalidOrderError):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

    def test_leg_without_position_id_is_rejected(self):
        position = Position(
            instrument=make_inst(),
            quantity=Decimal("1"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
            position_id=None,
        )

        with pytest.raises(TrailingStateError):
            resolve_position_leg([position], make_inst(), None)


class TestLifecycle:
    async def test_duplicate_active_attach_is_rejected(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        await manager.attach(TrailingRequest(make_inst(), make_spec()))

        with pytest.raises(TrailingStateError):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

    async def test_cancelled_state_does_not_block_a_new_attach(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        first = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        await manager.cancel(first.trailing_id, reason="manual")

        second = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert second.trailing_id != first.trailing_id
        states = await manager.list()
        assert len(states) == 2

    async def test_cancel_transitions_and_filters(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        cancelled = await manager.cancel(state.trailing_id, reason="manual")

        assert cancelled.phase is TrailingPhase.CANCELLED
        assert cancelled.last_error == "manual"
        assert await manager.get(state.trailing_id) == cancelled
        assert await manager.list(active_only=True) == []
        assert [s.trailing_id for s in await manager.list()] == [state.trailing_id]

    async def test_cancel_terminal_state_is_rejected(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        await manager.cancel(state.trailing_id)

        with pytest.raises(TrailingStateError):
            await manager.cancel(state.trailing_id)

    async def test_cancel_unknown_state_is_rejected(self, store):
        manager, _, _ = await make_manager(store)

        with pytest.raises(TrailingStateError):
            await manager.cancel("missing")

    async def test_list_filters_by_instrument(self, store):
        instrument = make_inst()
        other = make_inst(symbol="ETH")
        manager, _, _ = await make_manager(store)
        await store.upsert_position(make_position())
        await store.upsert_position(
            Position(
                instrument=other,
                quantity=Decimal("1"),
                average_entry_price=Decimal("3000"),
                updated_at=NOW,
                position_id="9",
            )
        )
        btc = await manager.attach(TrailingRequest(instrument, make_spec()))
        eth = await manager.attach(TrailingRequest(other, make_spec()))

        assert [s.trailing_id for s in await manager.list(instrument=instrument)] == [
            btc.trailing_id
        ]
        assert [s.trailing_id for s in await manager.list(instrument=other)] == [eth.trailing_id]


class TestEventsAndAudit:
    async def test_attach_publishes_event_and_audit(self, store):
        await store.upsert_position(make_position())
        manager, _, bus = await make_manager(store)
        events: list[TrailingStateEvent] = []
        bus.subscribe(TrailingStateEvent, events.append)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert len(events) == 1
        event = events[0]
        assert event.trailing_id == state.trailing_id
        assert event.position_id == "1"
        assert event.mode is state.mode
        assert event.previous_phase is TrailingPhase.ARMING
        assert event.phase is TrailingPhase.ARMING
        assert event.reason == "attached"

        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.attached"
        ]
        assert len(audits) == 1
        audit = audits[0]
        assert audit.correlation_id == state.trailing_id
        assert audit.payload["position_id"] == "1"
        assert audit.payload["mode"] == state.mode.value
        assert audit.payload["reason"] == "attached"

    async def test_cancel_writes_audit(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        await manager.cancel(state.trailing_id, reason="manual")

        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.cancelled"
        ]
        assert len(audits) == 1
        assert audits[0].correlation_id == state.trailing_id
        assert audits[0].payload["reason"] == "manual"


class TestRequirements:
    async def test_all_methods_require_a_connected_engine(self, store):
        manager, _, _ = await make_manager(store, connect=False)

        with pytest.raises(PlatformConnectionError):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))
        with pytest.raises(PlatformConnectionError):
            await manager.get("id-1")
        with pytest.raises(PlatformConnectionError):
            await manager.list()
        with pytest.raises(PlatformConnectionError):
            await manager.cancel("id-1")


def test_every_lifecycle_edge_matches_the_table():
    for current in TrailingPhase:
        for target in TrailingPhase:
            if target in ALLOWED_TRAILING_TRANSITIONS[current]:
                ensure_transition_allowed(current, target)
            else:
                with pytest.raises(TrailingStateError):
                    ensure_transition_allowed(current, target)


@pytest.mark.parametrize("phase", sorted(TERMINAL_TRAILING_PHASES, key=lambda p: p.value))
def test_terminal_phases_have_no_exits(phase):
    assert ALLOWED_TRAILING_TRANSITIONS[phase] == frozenset()


def test_routing_accepts_matching_named_basis():
    request = TrailingRequest(make_inst(), make_spec(basis=TrailingBasis.LAST))

    mode, reason = resolve_routing(
        request,
        protection=make_protection(),
        native=make_native_exact(),
        market_data=make_market(),
    )

    assert mode is TrailingMode.NATIVE
    assert reason == "native_exact"


def test_routing_rejects_named_basis_before_mode_selection():
    request = TrailingRequest(
        make_inst(), make_spec(basis=TrailingBasis.MARK), mode=TrailingMode.NATIVE
    )

    with pytest.raises(TrailingNotSupportedError):
        resolve_routing(
            request,
            protection=make_protection(),
            native=make_native_exact(),
            market_data=make_market(),
        )

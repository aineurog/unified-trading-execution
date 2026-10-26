"""Unit tests for trailing lifecycle, routing, and position targeting."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import count
from typing import cast

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
    RateLimitError,
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.events import EventBus, TickerEvent, TrailingStateEvent
from unified_trading_execution.state.halt import HaltStateMachine
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
    TrailingConfig,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.trailing.worker import TrailingWorker
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Candle, Ticker
from unified_trading_execution.types.order import TpSlAttachment
from unified_trading_execution.types.position import Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)

# Shared across managers in one test: audit and event ids are globally unique
# in the store, so per manager counters collide on restart tests.
_IDS = count(1)


def _phase_value(phase: TrailingPhase) -> str:
    return phase.value


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
    opened_at: datetime | None = NOW,
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


def make_instrument_spec():
    return InstrumentSpec(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000"),
        min_notional=Decimal("1"),
        price_precision=2,
        qty_precision=3,
    )


def make_candles(width="1"):
    distance = Decimal(width)
    return [
        Candle(
            open_time=NOW - timedelta(hours=15 - index),
            close_time=NOW - timedelta(hours=14 - index),
            open=Decimal("50000"),
            high=Decimal("50000") + distance,
            low=Decimal("50000") - distance,
            close=Decimal("50000"),
        )
        for index in range(15)
    ]


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


class CountingAdapter(CapabilityAdapter):
    def __init__(self, event_bus, *, protection, native, market):
        super().__init__(event_bus, protection=protection, native=native, market=market)
        self.ticker_calls = 0

    async def fetch_ticker(self, instrument):
        self.ticker_calls += 1
        return await super().fetch_ticker(instrument)


class CountingCandlesAdapter(CapabilityAdapter):
    def __init__(self, event_bus, *, protection, native, market):
        super().__init__(event_bus, protection=protection, native=native, market=market)
        self.candle_calls = 0

    async def fetch_candles(self, instrument, timeframe, *, limit):
        self.candle_calls += 1
        return await super().fetch_candles(instrument, timeframe, limit=limit)


class NoNativePositionReadAdapter(CapabilityAdapter):
    async def get_position_trailing_stop(self, instrument, position_id):
        raise NotImplementedError("no position-scoped native read")


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
    config=None,
    clock=None,
    reserve_rate_limit=None,
    adapter_cls=CapabilityAdapter,
    connect=True,
):
    bus = EventBus()
    adapter = adapter_cls(
        bus,
        protection=protection or make_protection(),
        native=native or make_native(),
        market=market or make_market(),
    )
    if connect:
        await adapter.connect()
    instrument = make_inst()
    for position in await store.get_positions(instrument):
        adapter.seed_position(position)
        if position.position_id is not None:
            adapter.set_ticker(instrument, Ticker(last=position.average_entry_price))
    adapter.add_instrument_spec(instrument, make_instrument_spec())
    adapter.queue_candles_response(make_candles())
    manager = TrailingManager(
        adapter,
        store,
        halt_machine=HaltStateMachine(),
        reserve_rate_limit=reserve_rate_limit or (lambda: True),
        event_bus=bus,
        config=config,
        id_factory=lambda: f"id-{next(_IDS)}",
        clock=clock or (lambda: NOW),
    )
    return manager, adapter, bus


class TestAttachRouting:
    async def test_auto_falls_back_to_synthetic(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.routing_reason == "native_unsupported"
        assert state.phase is TrailingPhase.ARMED
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

    async def test_unknown_position_id_lists_open_ids(self, store):
        await store.upsert_position(make_position(position_id="1", quantity="1"))
        await store.upsert_position(make_position(position_id="2", quantity="0"))
        manager, _, _ = await make_manager(store)

        with pytest.raises(InvalidOrderError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), make_spec(), position_id="missing"))

        message = str(excinfo.value)
        assert "missing" in message
        assert "'1'" in message
        assert "'2'" not in message

    async def test_no_open_position_reports_mirror_lag(self, store):
        manager, _, _ = await make_manager(store)

        with pytest.raises(InvalidOrderError) as excinfo:
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        message = str(excinfo.value)
        assert "no open position" in message
        assert "mirror can lag" in message
        assert "retry" in message

    def test_duplicate_position_id_is_a_mirror_defect(self):
        legs = [make_position(position_id="1"), make_position(position_id="1", entry="51000")]

        with pytest.raises(TrailingStateError, match="mirror is defective"):
            resolve_position_leg(legs, make_inst(), "1")

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

        with pytest.raises(TrailingStateError, match="mirror is defective"):
            resolve_position_leg([position], make_inst(), None)

    def test_missing_id_on_any_leg_blocks_automatic_targeting(self):
        legs = [
            make_position(position_id="1", quantity="1"),
            Position(
                instrument=make_inst(),
                quantity=Decimal("2"),
                average_entry_price=Decimal("51000"),
                updated_at=NOW,
                position_id=None,
            ),
        ]

        with pytest.raises(TrailingStateError, match="mirror is defective"):
            resolve_position_leg(legs, make_inst(), None)

    def test_explicit_id_ignores_unrelated_leg_without_id(self):
        legs = [
            make_position(position_id="1", quantity="1"),
            Position(
                instrument=make_inst(),
                quantity=Decimal("2"),
                average_entry_price=Decimal("51000"),
                updated_at=NOW,
                position_id=None,
            ),
        ]

        leg, resolved = resolve_position_leg(legs, make_inst(), "1")

        assert leg.position_id == "1"
        assert resolved == "1"


class TestSyntheticAttach:
    async def test_immediate_attach_writes_stop_and_take_profit(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("55000")))

        state = await manager.attach(TrailingRequest(make_inst(), spec))

        assert state.phase is TrailingPhase.ARMED
        assert state.activated is True
        assert state.stop_owned is True
        assert state.take_profit_owned is True
        assert state.current_stop == Decimal("49900.00")
        assert state.current_take_profit == Decimal("55000.00")
        assert state.best_basis == Decimal("50000")
        assert len(adapter.protection_writes) == 1
        write = adapter.protection_writes[0]
        assert write.position_id == "1"
        assert write.side is OrderSide.BUY
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("49900.00")
        assert write.take_profit is not None
        assert write.take_profit.trigger_price == Decimal("55000.00")

    async def test_delayed_attach_without_initial_stop_writes_nothing(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        spec = make_spec(activation=TrailingActivation.PRICE, activation_price=Decimal("60000"))

        state = await manager.attach(TrailingRequest(make_inst(), spec))

        assert state.phase is TrailingPhase.ARMED
        assert state.activated is False
        assert state.current_stop is None
        assert state.current_take_profit is None
        assert state.stop_owned is False
        assert adapter.protection_writes == ()

    async def test_existing_stop_requires_takeover(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("49000"))
        )

        with pytest.raises(TrailingStateError, match="take_over_existing_protection"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert adapter.protection_writes == ()
        assert await manager.list() == []

    async def test_takeover_records_previous_and_writes_new_stop(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("49000"))
        )

        state = await manager.attach(
            TrailingRequest(make_inst(), make_spec(), take_over_existing_protection=True)
        )

        assert state.previous_stop_loss == Decimal("49000")
        assert state.current_stop == Decimal("49900.00")
        assert adapter.protection_writes[0].stop_loss is not None
        assert adapter.protection_writes[0].stop_loss.trigger_price == Decimal("49900.00")

    async def test_first_write_failure_persists_failed(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        adapter.set_next_set_protection_error(TrailingStateError("venue rejected"))

        with pytest.raises(TrailingStateError, match="venue rejected"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        states = await manager.list()
        assert len(states) == 1
        assert states[0].phase is TrailingPhase.FAILED
        assert states[0].last_error == "venue rejected"
        assert states[0].failure_count == 1

    async def test_attach_rejects_a_stop_inside_minimum_distance(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        adapter.add_instrument_spec(
            make_inst(),
            replace(make_instrument_spec(), min_stop_distance=Decimal("200")),
        )

        with pytest.raises(TrailingNotSupportedError, match="minimum stop distance"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert await manager.list() == []
        assert adapter.protection_writes == ()

    async def test_attach_stops_when_rate_limit_is_exhausted(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, reserve_rate_limit=lambda: False)

        with pytest.raises(RateLimitError, match="rate limit"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert await manager.list() == []
        assert adapter.protection_writes == ()

    async def test_crash_after_venue_write_blocks_a_second_active_attach(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        original = store.upsert_trailing_state
        armed_attempts = 0

        async def fail_before_armed(state):
            nonlocal armed_attempts
            if state.phase is TrailingPhase.ARMED:
                armed_attempts += 1
                raise RuntimeError("simulated crash")
            await original(state)

        store.upsert_trailing_state = fail_before_armed  # type: ignore[method-assign]
        try:
            with pytest.raises(RuntimeError, match="simulated crash"):
                await manager.attach(TrailingRequest(make_inst(), make_spec()))
        finally:
            store.upsert_trailing_state = original  # type: ignore[method-assign]

        assert armed_attempts == 1
        assert len(adapter.protection_writes) == 1
        with pytest.raises(TrailingStateError, match="already attached"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))
        assert len(await manager.list(active_only=True)) == 1


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

        second = await manager.attach(
            TrailingRequest(make_inst(), make_spec(), take_over_existing_protection=True)
        )

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

    async def test_cancel_clears_only_matching_owned_protection(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("55000")))
        state = await manager.attach(TrailingRequest(make_inst(), spec))

        cancelled = await manager.cancel(state.trailing_id, clear_protection=True)

        assert cancelled.phase is TrailingPhase.CANCELLED
        assert await adapter.get_position_tpsl(make_inst(), "1") == (None, None)
        clear = adapter.protection_writes[-1]
        assert clear.cleared_stop_loss is True
        assert clear.cleared_take_profit is True

    async def test_cancel_leaves_externally_changed_stop(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("48000"))
        )

        cancelled = await manager.cancel(state.trailing_id, clear_protection=True)

        assert cancelled.last_error == "protection_changed_externally"
        protection = await adapter.get_position_tpsl(make_inst(), "1")
        assert protection is not None
        assert protection[1] is not None
        assert protection[1].trigger_price == Decimal("48000")

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
        manager, adapter, _ = await make_manager(store)
        eth_position = (await store.get_positions(other))[0]
        adapter.seed_position(eth_position)
        adapter.set_ticker(other, Ticker(last=eth_position.average_entry_price))
        adapter.add_instrument_spec(other, make_instrument_spec())
        btc = await manager.attach(TrailingRequest(instrument, make_spec()))
        eth = await manager.attach(TrailingRequest(other, make_spec()))

        assert [s.trailing_id for s in await manager.list(instrument=instrument)] == [
            btc.trailing_id
        ]
        assert [s.trailing_id for s in await manager.list(instrument=other)] == [eth.trailing_id]


class TestReprice:
    async def test_favourable_move_tightens_and_adverse_move_does_not(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        updated = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))

        assert updated is not None
        assert updated.current_stop == Decimal("50400.00")
        assert updated.best_basis == Decimal("50500")
        assert updated.reprice_count == 1

        pulled_back = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("49500")))

        assert pulled_back is not None
        assert pulled_back.current_stop == Decimal("50400.00")
        assert pulled_back.best_basis == Decimal("50500")
        assert pulled_back.reprice_count == 1

    async def test_step_skips_small_move_then_accepts_larger_move(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec(step=Decimal("200"))))

        small = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50150")))

        assert small is not None
        assert small.current_stop == Decimal("49900.00")
        assert small.best_basis == Decimal("50150")

        larger = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50300")))

        assert larger is not None
        assert larger.current_stop == Decimal("50200.00")
        assert larger.reprice_count == 1

    async def test_min_reprice_interval_advances_basis_without_writing(self, store):
        await store.upsert_position(make_position())
        now = [NOW]
        manager, _, _ = await make_manager(
            store,
            protection=make_protection(min_reprice_interval_seconds=10),
            clock=lambda: now[0],
        )
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        now[0] = NOW + timedelta(seconds=5)
        skipped = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))

        assert skipped is not None
        assert skipped.current_stop == Decimal("49900.00")
        assert skipped.best_basis == Decimal("50500")

        now[0] = NOW + timedelta(seconds=11)
        written = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50600")))

        assert written is not None
        assert written.current_stop == Decimal("50500.00")
        assert written.reprice_count == 1

    async def test_reprice_skips_a_stop_inside_minimum_distance(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        repriced = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))
        assert repriced is not None
        assert repriced.current_stop == Decimal("50400.00")
        assert repriced.reprice_count == 1

        adapter.add_instrument_spec(
            make_inst(),
            replace(make_instrument_spec(), min_stop_distance=Decimal("200")),
        )
        manager._instrument_specs.clear()
        blocked = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50600")))

        assert blocked is not None
        assert blocked.current_stop == Decimal("50400.00")
        assert blocked.best_basis == Decimal("50600")
        assert blocked.reprice_count == 1
        assert len(adapter.protection_writes) == 2

    async def test_reprice_skips_when_rate_limit_is_exhausted(self, store):
        await store.upsert_position(make_position())
        allowed = [True]
        manager, adapter, _ = await make_manager(store, reserve_rate_limit=lambda: allowed[0])
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        allowed[0] = False

        skipped = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))

        assert skipped is not None
        assert skipped.current_stop == Decimal("49900.00")
        assert skipped.best_basis == Decimal("50500")
        assert skipped.reprice_count == 0
        assert len(adapter.protection_writes) == 1

    async def test_fetch_failures_degrade_then_recover(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store, config=TrailingConfig(fetch_failure_threshold=2))
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        still_armed = await manager.on_ticker_failure(state.trailing_id, "timeout")
        assert still_armed is not None
        assert still_armed.phase is TrailingPhase.ARMED

        degraded = await manager.on_ticker_failure(state.trailing_id, "timeout")
        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED
        assert degraded.last_error == "fetch: timeout"
        assert degraded.current_stop == Decimal("49900.00")

        recovered = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50000")))
        assert recovered is not None
        assert recovered.phase is TrailingPhase.ARMED
        assert recovered.last_error is None

    async def test_write_failures_degrade_keep_stop_then_recover(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(
            store, config=TrailingConfig(protection_failure_threshold=2)
        )
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        ticker = Ticker(last=Decimal("50500"))
        adapter.set_next_set_protection_error(RuntimeError("venue rejected"))
        first = await manager.on_ticker(state.trailing_id, ticker)
        assert first is not None
        assert first.phase is TrailingPhase.ARMED
        assert first.current_stop == Decimal("49900.00")
        assert first.failure_count == 1

        adapter.set_next_set_protection_error(RuntimeError("venue rejected"))
        second = await manager.on_ticker(state.trailing_id, ticker)
        assert second is not None
        assert second.phase is TrailingPhase.DEGRADED
        assert second.current_stop == Decimal("49900.00")
        assert second.failure_count == 2

        recovered = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50600")))
        assert recovered is not None
        assert recovered.phase is TrailingPhase.ARMED
        assert recovered.current_stop == Decimal("50500.00")
        assert recovered.failure_count == 0
        assert recovered.last_error is None

    async def test_worker_groups_instrument_and_publishes_once(self, store):
        await store.upsert_position(make_position())
        await store.upsert_position(make_position(position_id="2", quantity="2", entry="50000"))
        config = TrailingConfig(poll_interval_seconds=1.0, publish_ticker_events=True)
        manager, base_adapter, bus = await make_manager(
            store, config=config, adapter_cls=CountingAdapter
        )
        adapter = cast(CountingAdapter, base_adapter)
        await manager.attach(TrailingRequest(make_inst(), make_spec(), position_id="1"))
        await manager.attach(TrailingRequest(make_inst(), make_spec(), position_id="2"))
        adapter.ticker_calls = 0

        events: list[TickerEvent] = []
        bus.subscribe(TickerEvent, events.append)
        worker = TrailingWorker(manager, adapter, config)

        await worker.run_once(now=10.0)
        await worker.run_once(now=10.5)
        await worker.run_once(now=11.0)

        assert adapter.ticker_calls == 2
        assert len(events) == 2


class TestAtrAndActivation:
    async def test_price_activation_writes_first_stop(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        spec = make_spec(
            activation=TrailingActivation.PRICE,
            activation_price=Decimal("51000"),
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))

        assert state.phase is TrailingPhase.ARMED
        assert state.activated is False
        assert state.current_stop is None

        below = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))
        assert below is not None
        assert below.activated is False
        assert below.current_stop is None

        activated = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("51000")))
        assert activated is not None
        assert activated.activated is True
        assert activated.current_stop == Decimal("50900.00")
        assert activated.stop_owned is True
        assert activated.resolved_activation_price == Decimal("51000")

    async def test_activation_with_looser_initial_stop_keeps_existing_stop(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        spec = make_spec(
            offset=Decimal("2000"),
            activation=TrailingActivation.PRICE,
            activation_price=Decimal("51000"),
            initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("49500")),
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        assert state.current_stop == Decimal("49500.00")
        assert state.activated is False

        activated = await manager.on_ticker(state.trailing_id, Ticker(last=Decimal("51000")))

        assert activated is not None
        assert activated.activated is True
        assert activated.current_stop == Decimal("49500.00")
        assert activated.reprice_count == 0

    async def test_worker_refreshes_atr_at_candle_close(self, store):
        await store.upsert_position(make_position())
        wall = [NOW]
        config = TrailingConfig(poll_interval_seconds=1.0, candle_settle_seconds=0)
        manager, adapter, _ = await make_manager(store, config=config)
        state = await manager.attach(TrailingRequest(make_inst(), atr_spec()))
        assert state.atr_value == Decimal("2")

        adapter.queue_candles_response(make_candles(width="10"))
        worker = TrailingWorker(
            manager,
            adapter,
            config,
            clock=lambda: 0.0,
            wall_clock=lambda: wall[0],
        )

        await worker.run_once(now=10.0)
        await worker.run_once(now=11.0)
        still_initial = await manager.get(state.trailing_id)
        assert still_initial is not None
        assert still_initial.atr_value == Decimal("2")

        wall[0] = NOW + timedelta(hours=1)
        await worker.run_once(now=12.0)
        refreshed = await manager.get(state.trailing_id)
        assert refreshed is not None
        assert refreshed.atr_value == Decimal("20")
        assert refreshed.atr_as_of == wall[0]

    async def test_atr_take_profit_is_computed_once(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        spec = make_spec(
            take_profit=ProtectionLevel(ProtectionLevelType.ATR, Decimal("4")),
            atr_period=14,
            atr_timeframe=CandleTimeframe.H1,
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        assert state.current_take_profit == Decimal("50008.00")

        refreshed = await manager.on_atr(
            state.trailing_id, Decimal("20"), as_of=NOW + timedelta(hours=1)
        )

        assert refreshed is not None
        assert refreshed.atr_value == Decimal("20")
        assert refreshed.current_take_profit == Decimal("50008.00")

    async def test_one_candle_fetch_serves_two_states(self, store):
        positions = [
            make_position(position_id="1", entry="50000"),
            make_position(position_id="2", entry="50000"),
        ]
        for position in positions:
            await store.upsert_position(position)
        wall = [NOW]
        config = TrailingConfig(poll_interval_seconds=1.0, candle_settle_seconds=0)
        manager, base_adapter, _ = await make_manager(
            store, config=config, adapter_cls=CountingCandlesAdapter
        )
        adapter = cast(CountingCandlesAdapter, base_adapter)
        adapter.queue_candles_response(make_candles())
        first = await manager.attach(TrailingRequest(make_inst(), atr_spec(), position_id="1"))
        second = await manager.attach(TrailingRequest(make_inst(), atr_spec(), position_id="2"))
        assert adapter.candle_calls == 2

        adapter.queue_candles_response(make_candles(width="10"))
        worker = TrailingWorker(
            manager,
            adapter,
            config,
            clock=lambda: 0.0,
            wall_clock=lambda: wall[0],
        )
        wall[0] = NOW + timedelta(hours=1)
        await worker.run_once(now=10.0)

        assert adapter.candle_calls == 3
        first_after = await manager.get(first.trailing_id)
        second_after = await manager.get(second.trailing_id)
        assert first_after is not None
        assert second_after is not None
        assert first_after.atr_value == Decimal("20")
        assert second_after.atr_value == Decimal("20")

    async def test_worker_degrades_stale_atr_then_recovers(self, store):
        await store.upsert_position(make_position())
        wall = [NOW]
        config = TrailingConfig(
            poll_interval_seconds=1.0,
            candle_settle_seconds=0,
            atr_stale_after_candles=2,
        )
        manager, adapter, _ = await make_manager(store, config=config)
        state = await manager.attach(TrailingRequest(make_inst(), atr_spec()))
        worker = TrailingWorker(
            manager,
            adapter,
            config,
            clock=lambda: 0.0,
            wall_clock=lambda: wall[0],
        )

        adapter.queue_candles_response(RuntimeError("candles down"))
        wall[0] = NOW + timedelta(hours=1)
        await worker.run_once(now=10.0)
        first = await manager.get(state.trailing_id)
        assert first is not None
        assert first.phase is TrailingPhase.ARMED

        adapter.queue_candles_response(RuntimeError("candles down"))
        wall[0] = NOW + timedelta(hours=2)
        await worker.run_once(now=11.0)
        degraded = await manager.get(state.trailing_id)
        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED
        assert degraded.atr_value == Decimal("2")

        adapter.queue_candles_response(make_candles(width="10"))
        wall[0] = NOW + timedelta(hours=3)
        await worker.run_once(now=12.0)
        recovered = await manager.get(state.trailing_id)
        assert recovered is not None
        assert recovered.phase is TrailingPhase.ARMED
        assert recovered.atr_value == Decimal("20")


class TestPositionUpdates:
    async def test_partial_close_updates_quantity_without_rewriting_stop(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        await manager.attach(TrailingRequest(make_inst(), make_spec()))
        writes_before = len(adapter.protection_writes)

        updated = await manager.on_position_update(make_position(quantity="0.5"))

        assert updated is not None
        assert updated.quantity == Decimal("0.5")
        assert updated.current_stop == Decimal("49900.00")
        assert updated.phase is TrailingPhase.ARMED
        assert len(adapter.protection_writes) == writes_before

    async def test_quantity_increase_recomputes_profit_percent_activation(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        spec = make_spec(
            activation=TrailingActivation.PROFIT_PERCENT,
            activation_profit_percent=Decimal("10"),
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        assert state.resolved_activation_price == Decimal("55000")

        updated = await manager.on_position_update(make_position(quantity="2", entry="51000"))

        assert updated is not None
        assert updated.quantity == Decimal("2")
        assert updated.entry_price == Decimal("51000")
        assert updated.resolved_activation_price == Decimal("56100")
        assert updated.activated is False

    async def test_resize_rewrites_protection_at_the_new_size(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(
            store, protection=make_protection(reprice_gap=True)
        )
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("52000")))
        await manager.attach(TrailingRequest(make_inst(), spec))
        assert len(adapter.protection_writes) == 1
        adapter.update_position(make_position(quantity="0.5"), publish=False)

        updated = await manager.on_position_update(make_position(quantity="0.5"))

        assert updated is not None
        assert updated.quantity == Decimal("0.5")
        assert len(adapter.protection_writes) == 2
        write = adapter.protection_writes[-1]
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("49900.00")
        assert write.take_profit is not None
        assert write.take_profit.trigger_price == Decimal("52000.00")

        await manager.on_position_update(make_position(quantity="0.5"))
        assert len(adapter.protection_writes) == 2

    async def test_resize_failure_degrades_immediately(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(
            store, protection=make_protection(reprice_gap=True)
        )
        await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.set_next_set_protection_error(RuntimeError("resize down"))

        updated = await manager.on_position_update(make_position(quantity="2", entry="51000"))

        assert updated is not None
        assert updated.phase is TrailingPhase.DEGRADED
        assert updated.quantity == Decimal("2")
        assert updated.failure_count == 1
        assert updated.last_error is not None
        assert "resize" in updated.last_error

    async def test_zero_quantity_marks_the_state_done(self, store):
        await store.upsert_position(make_position())
        manager, adapter, bus = await make_manager(store)
        await manager.attach(TrailingRequest(make_inst(), make_spec()))
        events: list[TrailingStateEvent] = []
        bus.subscribe(TrailingStateEvent, events.append)
        closed_position = make_position(quantity="0")
        adapter.update_position(closed_position, publish=False)

        updated = await manager.on_position_update(closed_position)

        assert updated is not None
        assert updated.phase is TrailingPhase.DONE
        assert updated.quantity == Decimal("0")
        assert events[-1].phase is TrailingPhase.DONE
        assert events[-1].reason == "position_closed"
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.done"
        ]
        assert len(audits) == 1
        assert audits[0].payload["reason"] == "position_closed"

    async def test_side_flip_cancels_and_clears_the_owned_stop(self, store):
        await store.upsert_position(make_position())
        manager, adapter, bus = await make_manager(store)
        await manager.attach(TrailingRequest(make_inst(), make_spec()))
        flipped = make_position(quantity="-1")
        adapter.update_position(flipped, publish=False)
        events: list[TrailingStateEvent] = []
        bus.subscribe(TrailingStateEvent, events.append)

        updated = await manager.on_position_update(flipped)

        assert updated is not None
        assert updated.phase is TrailingPhase.CANCELLED
        assert updated.last_error is None
        assert events[-1].reason == "position_side_flipped"
        assert adapter.protection_writes[-1].cleared_stop_loss is True
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.cancelled"
        ]
        assert len(audits) == 1
        assert audits[0].payload["reason"] == "position_side_flipped"


class TestIntegrity:
    async def test_missing_stop_is_restored(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.seed_protection(make_inst(), "1")
        writes_before = len(adapter.protection_writes)

        restored = await manager.check_integrity(state.trailing_id)

        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.current_stop == Decimal("49900.00")
        assert len(adapter.protection_writes) == writes_before + 1
        write = adapter.protection_writes[-1]
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("49900.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert len(audits) == 1
        assert audits[0].payload["reason"] == "missing_stop"

    async def test_looser_external_stop_is_restored(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("49800"))
        )

        restored = await manager.check_integrity(state.trailing_id)

        assert restored is not None
        assert restored.current_stop == Decimal("49900.00")
        write = adapter.protection_writes[-1]
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("49900.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[0].payload["reason"] == "protection_changed_externally"

    async def test_tighter_external_stop_is_adopted_without_a_write(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("50050"))
        )
        writes_before = len(adapter.protection_writes)

        restored = await manager.check_integrity(state.trailing_id)

        assert restored is not None
        assert restored.current_stop == Decimal("50050")
        assert len(adapter.protection_writes) == writes_before
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[0].payload["reason"] == "adopted_tighter_stop"

    async def test_missing_take_profit_is_restored(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("52000")))
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        adapter.seed_protection(
            make_inst(), "1", stop_loss=TpSlAttachment(trigger_price=Decimal("49900"))
        )

        restored = await manager.check_integrity(state.trailing_id)

        assert restored is not None
        assert restored.current_stop == Decimal("49900")
        assert restored.current_take_profit == Decimal("52000.00")
        write = adapter.protection_writes[-1]
        assert write.take_profit is not None
        assert write.take_profit.trigger_price == Decimal("52000.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[0].payload["reason"] == "missing_take_profit"

    async def test_missing_position_marks_the_state_done(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.update_position(make_position(quantity="0"), publish=False)

        closed = await manager.check_integrity(state.trailing_id)

        assert closed is not None
        assert closed.phase is TrailingPhase.DONE
        assert closed.quantity == Decimal("0")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.done"
        ]
        assert len(audits) == 1

    async def test_unactivated_state_without_an_initial_stop_is_left_alone(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store)
        spec = make_spec(
            activation=TrailingActivation.PRICE,
            activation_price=Decimal("51000"),
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        writes_before = len(adapter.protection_writes)

        checked = await manager.check_integrity(state.trailing_id)

        assert checked is not None
        assert checked.phase is TrailingPhase.ARMED
        assert checked.current_stop is None
        assert len(adapter.protection_writes) == writes_before


class TestRestartRecovery:
    async def test_attach_refused_until_rehydration_completes(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.current_stop is not None

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=state.current_stop),
        )

        with pytest.raises(TrailingStateError, match="rehydration has not completed"):
            await second.attach(TrailingRequest(make_inst(), make_spec()))

        await second.rehydrate()

        with pytest.raises(TrailingStateError, match="already attached"):
            await second.attach(TrailingRequest(make_inst(), make_spec()))

    async def test_position_closed_while_offline_marks_done(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store)
        closed = make_position(quantity="0")
        await store.upsert_position(closed)
        adapter.update_position(closed, publish=False)

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DONE
        assert restored.quantity == Decimal("0")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.done"
        ]
        assert audits[-1].payload["reason"] == "position_closed_while_offline"

    async def test_missing_synthetic_stop_is_restored_on_rehydrate(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store)
        writes_before = len(adapter.protection_writes)

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.current_stop == Decimal("49900.00")
        assert len(adapter.protection_writes) == writes_before + 1
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "missing_stop"

    async def test_tighter_venue_stop_is_adopted_on_rehydrate(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("50050")),
        )
        writes_before = len(adapter.protection_writes)

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.current_stop == Decimal("50050")
        assert len(adapter.protection_writes) == writes_before
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "adopted_tighter_stop"

    async def test_rehydrate_reprices_the_synthetic_candidate(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.best_basis == Decimal("50000")

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("49900")),
        )
        adapter.set_ticker(make_inst(), Ticker(last=Decimal("50500")))

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.current_stop == Decimal("50400.00")
        assert restored.best_basis == Decimal("50500")
        assert restored.reprice_count == 1
        write = adapter.protection_writes[-1]
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("50400.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "rehydrate_reprice"

    async def test_rehydrate_respects_the_minimum_reprice_interval(self, store):
        await store.upsert_position(make_position())
        protection = make_protection(min_reprice_interval_seconds=10)
        clock = [NOW]
        first_reprice_at = NOW + timedelta(seconds=10)
        first, _, _ = await make_manager(store, protection=protection, clock=lambda: clock[0])
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))
        clock[0] = first_reprice_at
        repriced = await first.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))
        assert repriced is not None
        assert repriced.current_stop == Decimal("50400.00")
        assert repriced.last_reprice_at == first_reprice_at

        second, adapter, _ = await make_manager(
            store,
            protection=protection,
            clock=lambda: first_reprice_at + timedelta(seconds=5),
        )
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("50400")),
        )
        adapter.set_ticker(make_inst(), Ticker(last=Decimal("50600")))

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.current_stop == Decimal("50400.00")
        assert restored.best_basis == Decimal("50600")
        assert restored.reprice_count == 1
        assert adapter.protection_writes == ()

    async def test_looser_venue_stop_is_restored_on_rehydrate(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("49800")),
        )

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.current_stop == Decimal("49900.00")
        write = adapter.protection_writes[-1]
        assert write.stop_loss is not None
        assert write.stop_loss.trigger_price == Decimal("49900.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "protection_changed_externally"

    async def test_missing_owned_take_profit_is_restored_on_rehydrate(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("52000")))
        state = await first.attach(TrailingRequest(make_inst(), spec))

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("49900")),
        )

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.current_take_profit == Decimal("52000.00")
        write = adapter.protection_writes[-1]
        assert write.take_profit is not None
        assert write.take_profit.trigger_price == Decimal("52000.00")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "missing_take_profit"

    async def test_rearm_disabled_degrades_the_state(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.current_stop is not None

        second, adapter, _ = await make_manager(
            store, config=TrailingConfig(rearm_on_reconnect=False)
        )
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=state.current_stop),
        )

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DEGRADED
        assert restored.last_error == "rearm_disabled"

    async def test_dataset_gap_degrades_the_state(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, _, _ = await make_manager(store)
        await second.rehydrate(positions=[])

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DEGRADED
        assert restored.last_error == "position_dataset_inconsistent"

    async def test_position_replaced_on_open_time_change(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, _, _ = await make_manager(store)
        replacement = make_position(opened_at=NOW + timedelta(hours=1))
        await second.rehydrate(positions=[replacement])

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DEGRADED
        assert restored.last_error == "position_replaced"

    async def test_identity_unverified_when_open_time_is_missing(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, _, _ = await make_manager(store)
        replacement = make_position(opened_at=None)
        await second.rehydrate(positions=[replacement])

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DEGRADED
        assert restored.last_error == "position_identity_unverified"

    async def test_restart_side_flip_clears_owned_protection(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store)
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store)
        adapter.seed_protection(
            make_inst(),
            "1",
            stop_loss=TpSlAttachment(trigger_price=Decimal("49900")),
        )
        short = make_position(quantity="-1")
        await store.upsert_position(short)
        adapter.update_position(short, publish=False)

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.DEGRADED
        assert restored.last_error == "position_replaced"
        assert adapter.protection_writes[-1].cleared_stop_loss
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.clear"
        ]
        assert audits[-1].payload["reason"] == "cleared"


class TestNativeExecution:
    async def test_native_attach_places_the_trail(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert state.mode is TrailingMode.NATIVE
        assert state.phase is TrailingPhase.ARMED
        assert state.native_id is not None
        assert state.current_stop is None
        request = adapter.native_requests[-1]
        assert request.side is OrderSide.BUY
        assert request.quantity == Decimal("1")
        assert request.offset == Decimal("100")
        assert request.offset_type is TrailingOffsetType.PRICE
        assert request.basis is TrailingBasis.LAST
        assert request.client_id == state.trailing_id

    async def test_native_attach_requires_takeover_for_existing_trail(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        adapter.seed_native_trail(make_inst(), "1", offset=Decimal("250"))

        with pytest.raises(TrailingStateError, match="take_over_existing_protection"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert adapter.native_requests == ()
        assert await manager.list() == []

    async def test_native_takeover_replaces_trail_and_audits_previous_offset(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        adapter.seed_native_trail(
            make_inst(), "1", offset=Decimal("250"), current_stop=Decimal("49500")
        )

        state = await manager.attach(
            TrailingRequest(make_inst(), make_spec(), take_over_existing_protection=True)
        )

        assert state.phase is TrailingPhase.ARMED
        assert adapter.native_requests[-1].offset == Decimal("100")
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.takeover"
        ]
        assert audits[-1].payload["previous_native_offset"] == "250"
        assert audits[-1].payload["previous_native_stop"] == "49500"

    async def test_native_attach_fails_closed_without_position_read(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(
            store,
            native=make_native_exact(),
            adapter_cls=NoNativePositionReadAdapter,
        )

        with pytest.raises(TrailingNotSupportedError, match="position-scoped"):
            await manager.attach(TrailingRequest(make_inst(), make_spec()))

        assert await manager.list() == []

    async def test_native_attach_writes_owned_take_profit(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        spec = make_spec(take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("52000")))

        state = await manager.attach(TrailingRequest(make_inst(), spec))

        assert state.current_take_profit == Decimal("52000.00")
        assert state.take_profit_owned
        protection = await adapter.get_position_tpsl(make_inst(), "1")
        assert protection is not None
        assert protection[0] is not None
        assert protection[0].trigger_price == Decimal("52000.00")
        assert protection[1] is None

    async def test_rehydrate_keeps_an_existing_native_trail(self, store):
        await store.upsert_position(make_position())
        first, first_adapter, _ = await make_manager(store, native=make_native_exact())
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))

        second, adapter, _ = await make_manager(store, native=make_native_exact())
        seeded = await adapter.place_trailing_stop(first_adapter.native_requests[-1])
        await store.upsert_trailing_state(
            replace(
                state,
                native_id=seeded.native_id,
                current_stop=seeded.current_stop,
            )
        )
        requests_before = len(adapter.native_requests)

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.native_id == seeded.native_id
        assert len(adapter.native_requests) == requests_before

    async def test_rehydrate_rearms_a_missing_native_trail(self, store):
        await store.upsert_position(make_position())
        first, _, _ = await make_manager(store, native=make_native_exact())
        state = await first.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.native_id is not None

        second, adapter, _ = await make_manager(store, native=make_native_exact())

        await second.rehydrate()

        restored = await second.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.native_id is not None
        assert restored.native_id != state.native_id
        assert len(adapter.native_requests) == 1

    async def test_native_integrity_rearms_a_missing_trail(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.native_id is not None
        await adapter.cancel_trailing_stop(state.native_id, make_inst())
        requests_before = len(adapter.native_requests)

        restored = await manager.check_integrity(state.trailing_id)

        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.native_id is not None
        assert restored.native_id != state.native_id
        assert len(adapter.native_requests) == requests_before + 1
        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.integrity_restore"
        ]
        assert audits[-1].payload["reason"] == "native_rearmed"

    async def test_native_integrity_degrades_when_rearm_fails(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        await store.upsert_trailing_state(replace(state, native_id=None))
        adapter.set_next_native_error(PlatformConnectionError("venue down"))

        degraded = await manager.check_integrity(state.trailing_id)

        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED
        assert degraded.last_error is not None
        assert "venue down" in degraded.last_error

    async def test_native_integrity_degrades_when_read_fails(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(
            store,
            native=make_native_exact(),
            config=TrailingConfig(protection_failure_threshold=1),
        )
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        adapter.set_next_native_error(PlatformConnectionError("venue down"))

        degraded = await manager.check_integrity(state.trailing_id)

        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED
        assert degraded.last_error is not None
        assert "venue down" in degraded.last_error

    async def test_native_side_flip_cancels_the_venue_trail(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.native_id is not None

        cancelled = await manager.on_position_update(make_position(quantity="-1"))

        assert cancelled is not None
        assert cancelled.phase is TrailingPhase.CANCELLED
        assert await adapter.get_trailing_stop(state.native_id, make_inst()) is None

    async def test_native_cancel_removes_the_venue_trail(self, store):
        await store.upsert_position(make_position())
        manager, adapter, _ = await make_manager(store, native=make_native_exact())
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))
        assert state.native_id is not None

        cancelled = await manager.cancel(state.trailing_id)

        assert cancelled is not None
        assert cancelled.phase is TrailingPhase.CANCELLED
        assert await adapter.get_trailing_stop(state.native_id, make_inst()) is None


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
        assert event.phase is TrailingPhase.ARMED
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

    async def test_attach_writes_routing_audit(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)

        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        audits = [
            audit
            for audit in await store.query_audit_events()
            if audit.event_type == "trailing.routing"
        ]
        assert len(audits) == 1
        audit = audits[0]
        assert audit.correlation_id == state.trailing_id
        assert audit.payload["position_id"] == "1"
        assert audit.payload["mode"] == TrailingMode.SYNTHETIC.value
        assert audit.payload["reason"] == state.routing_reason
        assert audit.payload["current_stop"] == str(state.current_stop)

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


@pytest.mark.parametrize("phase", sorted(TERMINAL_TRAILING_PHASES, key=_phase_value))
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

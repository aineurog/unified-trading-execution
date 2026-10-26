"""Unit tests for trailing lifecycle, routing, and position targeting."""

from __future__ import annotations

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
from unified_trading_execution.trailing.worker import TrailingWorker
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
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Candle, Ticker
from unified_trading_execution.types.order import TpSlAttachment
from unified_trading_execution.types.position import Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


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
    ids = count(1)
    manager = TrailingManager(
        adapter,
        store,
        halt_machine=HaltStateMachine(),
        reserve_rate_limit=lambda: True,
        event_bus=bus,
        config=config,
        id_factory=lambda: f"id-{next(ids)}",
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
            TrailingRequest(
                make_inst(), make_spec(), take_over_existing_protection=True
            )
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
            TrailingRequest(
                make_inst(), make_spec(), take_over_existing_protection=True
            )
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
        spec = make_spec(
            take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("55000"))
        )
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

        updated = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50500"))
        )

        assert updated is not None
        assert updated.current_stop == Decimal("50400.00")
        assert updated.best_basis == Decimal("50500")
        assert updated.reprice_count == 1

        pulled_back = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("49500"))
        )

        assert pulled_back is not None
        assert pulled_back.current_stop == Decimal("50400.00")
        assert pulled_back.best_basis == Decimal("50500")
        assert pulled_back.reprice_count == 1

    async def test_step_skips_small_move_then_accepts_larger_move(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(store)
        state = await manager.attach(
            TrailingRequest(make_inst(), make_spec(step=Decimal("200")))
        )

        small = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50150"))
        )

        assert small is not None
        assert small.current_stop == Decimal("49900.00")
        assert small.best_basis == Decimal("50150")

        larger = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50300"))
        )

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
        skipped = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50500"))
        )

        assert skipped is not None
        assert skipped.current_stop == Decimal("49900.00")
        assert skipped.best_basis == Decimal("50500")

        now[0] = NOW + timedelta(seconds=11)
        written = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50600"))
        )

        assert written is not None
        assert written.current_stop == Decimal("50500.00")
        assert written.reprice_count == 1

    async def test_fetch_failures_degrade_then_recover(self, store):
        await store.upsert_position(make_position())
        manager, _, _ = await make_manager(
            store, config=TrailingConfig(fetch_failure_threshold=2)
        )
        state = await manager.attach(TrailingRequest(make_inst(), make_spec()))

        still_armed = await manager.on_ticker_failure(state.trailing_id, "timeout")
        assert still_armed is not None
        assert still_armed.phase is TrailingPhase.ARMED

        degraded = await manager.on_ticker_failure(state.trailing_id, "timeout")
        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED
        assert degraded.last_error == "fetch: timeout"
        assert degraded.current_stop == Decimal("49900.00")

        recovered = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50000"))
        )
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

        recovered = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50600"))
        )
        assert recovered is not None
        assert recovered.phase is TrailingPhase.ARMED
        assert recovered.current_stop == Decimal("50500.00")
        assert recovered.failure_count == 0
        assert recovered.last_error is None

    async def test_worker_groups_instrument_and_publishes_once(self, store):
        await store.upsert_position(make_position())
        await store.upsert_position(
            make_position(position_id="2", quantity="2", entry="50000")
        )
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

        below = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("50500"))
        )
        assert below is not None
        assert below.activated is False
        assert below.current_stop is None

        activated = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("51000"))
        )
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
            initial_stop=ProtectionLevel(
                ProtectionLevelType.PRICE, Decimal("49500")
            ),
        )
        state = await manager.attach(TrailingRequest(make_inst(), spec))
        assert state.current_stop == Decimal("49500.00")
        assert state.activated is False

        activated = await manager.on_ticker(
            state.trailing_id, Ticker(last=Decimal("51000"))
        )

        assert activated is not None
        assert activated.activated is True
        assert activated.current_stop == Decimal("49500.00")
        assert activated.reprice_count == 0

    async def test_worker_refreshes_atr_at_candle_close(self, store):
        await store.upsert_position(make_position())
        wall = [NOW]
        config = TrailingConfig(
            poll_interval_seconds=1.0, candle_settle_seconds=0
        )
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

"""Engine wiring tests for the trailing subsystem.

These exercise the boundary between ``Engine`` and ``TrailingManager``:
startup rehydration, direct position snapshots, worker lifecycle, position
update forwarding, and the shared rate-limit reservation.  Price math and
per-state lifecycle rules are covered by ``test_trailing_manager.py``.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TypeVar

import pytest
import pytest_asyncio

from unified_trading_execution.adapter import (
    MarketDataCapability,
    PositionProtectionCapability,
    RateLimits,
    TrailingCapability,
)
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import PlatformConnectionError, TrailingStateError
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.testing import MockAdapter
from unified_trading_execution.trailing import (
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.trailing.manager import TrailingManager
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.position import Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)
FUTURE_RESET = datetime(2099, 1, 1, tzinfo=UTC)


def make_inst(symbol: str = "BTC") -> Instrument:
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


def make_position(
    *,
    quantity: str = "1",
    position_id: str = "1",
    entry: str = "50000",
) -> Position:
    return Position(
        instrument=make_inst(),
        quantity=Decimal(quantity),
        average_entry_price=Decimal(entry),
        updated_at=NOW,
        position_id=position_id,
        opened_at=NOW,
    )


def make_spec() -> TrailingSpec:
    return TrailingSpec(
        offset_type=TrailingOffsetType.PRICE,
        offset=Decimal("100"),
    )


def make_instrument_spec() -> InstrumentSpec:
    return InstrumentSpec(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000"),
        min_notional=Decimal("1"),
        price_precision=2,
        qty_precision=3,
    )


def make_protection() -> PositionProtectionCapability:
    return PositionProtectionCapability(
        supported=True,
        can_set_stop=True,
        can_set_take_profit=True,
        can_clear_stop=True,
        can_clear_take_profit=True,
        preserves_unmentioned_side=True,
        trigger_bases=frozenset({TrailingBasis.LAST}),
        min_reprice_interval_seconds=0.0,
        reprice_gap=False,
    )


def make_market() -> MarketDataCapability:
    return MarketDataCapability(
        ticker_bases=frozenset({TrailingBasis.LAST}),
        min_ticker_interval_seconds=0.0,
        candle_timeframes=frozenset(),
        streaming_ticker=False,
    )


class TogglePositionsAdapter(MockAdapter):
    """MockAdapter whose direct position snapshot can be made to fail."""

    def __init__(self, *, event_bus: EventBus) -> None:
        super().__init__(event_bus=event_bus)
        self.fail_positions = False

    async def fetch_positions(self) -> list[Position]:
        if self.fail_positions:
            raise RuntimeError("position snapshot unavailable")
        return await super().fetch_positions()


def enable_native_trailing(adapter: MockAdapter) -> None:
    adapter.set_trailing_capability(
        make_inst(),
        TrailingCapability(
            supported=True,
            offset_types=frozenset({TrailingOffsetType.PRICE}),
            bases=frozenset({TrailingBasis.LAST}),
            activations=frozenset({TrailingActivation.IMMEDIATE}),
            supports_step=False,
            supports_initial_stop=False,
            supports_take_profit=False,
            position_scoped=True,
            min_reprice_interval_seconds=0.0,
        ),
    )


_AdapterT = TypeVar("_AdapterT", bound=MockAdapter)


def build_adapter(
    bus: EventBus,
    positions: list[Position],
    *,
    adapter_cls: type[_AdapterT] = MockAdapter,
) -> _AdapterT:
    adapter = adapter_cls(event_bus=bus)
    instrument = make_inst()
    adapter.add_instrument_spec(instrument, make_instrument_spec())
    adapter.set_position_protection_capability(instrument, make_protection())
    adapter.set_market_data_capability(instrument, make_market())
    adapter.set_ticker(instrument, Ticker(last=Decimal("50000")))
    adapter.set_rate_limits(
        RateLimits(
            requests_per_interval=1000,
            interval_seconds=60.0,
            remaining=1000,
            reset_at=FUTURE_RESET,
        )
    )
    for position in positions:
        adapter.seed_position(position)
    return adapter


async def make_engine(
    store: SQLiteStateStore,
    adapter: MockAdapter,
    bus: EventBus,
    *,
    risk_config: RiskConfig | None = None,
) -> Engine:
    engine = Engine(
        adapter=adapter,
        state_store=store,
        event_bus=bus,
        risk_config=risk_config,
        reconcile_interval_seconds=None,
    )
    await engine.connect()
    return engine


async def drain(engine: Engine) -> None:
    """Run engine-scheduled trailing work to completion."""
    for _ in range(100):
        pending = [task for task in engine._background_tasks if not task.done()]
        if not pending:
            await asyncio.sleep(0)
            pending = [task for task in engine._background_tasks if not task.done()]
            if not pending:
                return
        await asyncio.gather(*pending, return_exceptions=True)


async def wait_worker_running(engine: Engine, running: bool) -> None:
    for _ in range(200):
        await drain(engine)
        worker = engine._trailing_worker
        if worker is not None and worker.running is running:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"trailing worker did not become running={running}")


async def attach_synthetic(engine: Engine, *, position_id: str | None = None):
    return await engine.trailing.attach(
        TrailingRequest(make_inst(), make_spec(), position_id=position_id)
    )


@pytest_asyncio.fixture
async def store():
    state_store = SQLiteStateStore(":memory:")
    await state_store.initialize()
    try:
        yield state_store
    finally:
        await state_store.close()


@pytest_asyncio.fixture
async def persistent_store(tmp_path):
    """File-backed store so two engines can restart over the same state."""
    state_store = SQLiteStateStore(str(tmp_path / "trailing_state.db"))
    try:
        yield state_store
    finally:
        await state_store.close()


class TestStartupRehydration:
    async def test_property_refuses_before_connect(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = Engine(
            adapter=adapter,
            state_store=store,
            event_bus=bus,
            reconcile_interval_seconds=None,
        )

        with pytest.raises(PlatformConnectionError):
            _ = engine.trailing

    async def test_connect_rehydrates_from_direct_snapshot_and_starts_worker(
        self, persistent_store, monkeypatch
    ):
        store = persistent_store
        bus = EventBus()
        first = await make_engine(store, build_adapter(bus, [make_position()]), bus)
        state = await attach_synthetic(first)
        await first.ashutdown()

        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        rehydrate_calls: list[list[Position] | None] = []
        original_rehydrate = TrailingManager.rehydrate

        async def record_rehydrate(self, positions=None):
            rehydrate_calls.append(positions)
            await original_rehydrate(self, positions)

        monkeypatch.setattr(TrailingManager, "rehydrate", record_rehydrate)

        second = await make_engine(store, adapter, bus)
        try:
            assert second._trailing_ready is True
            assert second._trailing_worker is not None
            await wait_worker_running(second, True)
            assert rehydrate_calls == [[make_position()]]
            restored = await second.trailing.get(state.trailing_id)
            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
            assert restored.mode is TrailingMode.SYNTHETIC
        finally:
            await second.ashutdown()

    async def test_deferred_rehydration_retries_after_reconcile(self, persistent_store):
        store = persistent_store
        bus = EventBus()
        first = await make_engine(store, build_adapter(bus, [make_position()]), bus)
        state = await attach_synthetic(first)
        await first.ashutdown()

        bus = EventBus()
        adapter = build_adapter(bus, [make_position()], adapter_cls=TogglePositionsAdapter)
        adapter.fail_positions = True
        second = await make_engine(store, adapter, bus)
        try:
            assert second._trailing_ready is False
            assert second._trailing_worker is not None
            assert second._trailing_worker.running is False

            with pytest.raises(TrailingStateError, match="rehydration has not completed"):
                await attach_synthetic(second)

            adapter.fail_positions = False
            await second.reconcile()

            assert second._trailing_ready is True
            await wait_worker_running(second, True)
            restored = await second.trailing.get(state.trailing_id)
            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
        finally:
            await second.ashutdown()

    async def test_reconnect_retries_deferred_rehydration(self, persistent_store):
        store = persistent_store
        bus = EventBus()
        first = await make_engine(store, build_adapter(bus, [make_position()]), bus)
        await attach_synthetic(first)
        await first.ashutdown()

        bus = EventBus()
        adapter = build_adapter(bus, [make_position()], adapter_cls=TogglePositionsAdapter)
        adapter.fail_positions = True
        second = await make_engine(store, adapter, bus)
        try:
            assert second._trailing_ready is False

            adapter.fail_positions = False
            second._last_connected = False
            await adapter.disconnect()
            await adapter.connect()
            assert second._reconcile_task is not None
            await second._reconcile_task

            assert second._trailing_ready is True
            await wait_worker_running(second, True)
        finally:
            await second.ashutdown()


class TestWorkerLifecycle:
    async def test_attach_starts_worker_and_cancel_stops_it(self, store):
        bus = EventBus()
        engine = await make_engine(store, build_adapter(bus, [make_position()]), bus)
        try:
            assert engine._trailing_worker is not None
            assert engine._trailing_worker.running is False

            state = await attach_synthetic(engine)
            await wait_worker_running(engine, True)

            await engine.trailing.cancel(state.trailing_id, reason="manual")
            await wait_worker_running(engine, False)
        finally:
            await engine.ashutdown()

    async def test_worker_stops_only_after_last_synthetic_state(self, store):
        bus = EventBus()
        positions = [
            make_position(position_id="1", quantity="1"),
            make_position(position_id="2", quantity="2"),
        ]
        engine = await make_engine(store, build_adapter(bus, positions), bus)
        try:
            first = await attach_synthetic(engine, position_id="1")
            second = await attach_synthetic(engine, position_id="2")
            await wait_worker_running(engine, True)

            await engine.trailing.cancel(first.trailing_id)
            await drain(engine)
            assert engine._trailing_worker is not None
            assert engine._trailing_worker.running is True

            await engine.trailing.cancel(second.trailing_id)
            await wait_worker_running(engine, False)
        finally:
            await engine.ashutdown()

    async def test_native_state_does_not_start_the_poller(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        enable_native_trailing(adapter)
        engine = await make_engine(store, adapter, bus)
        try:
            state = await attach_synthetic(engine)
            assert state.mode is TrailingMode.NATIVE

            await drain(engine)
            assert engine._trailing_worker is not None
            assert engine._trailing_worker.running is False
        finally:
            await engine.ashutdown()

    async def test_disconnect_stops_worker_and_leaves_protection(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        state = await attach_synthetic(engine)
        await wait_worker_running(engine, True)

        await engine.disconnect()

        assert engine._trailing_worker is not None
        assert engine._trailing_worker.running is False
        assert engine._trailing_ready is False
        protection = await adapter.get_position_tpsl(make_inst(), "1")
        assert protection is not None
        assert protection[1] is not None
        assert protection[1].trigger_price == state.current_stop

        await engine.ashutdown()


class TestIntegrityTimer:
    async def test_timer_is_started_and_restores_a_missing_stop(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        try:
            state = await attach_synthetic(engine)
            await wait_worker_running(engine, True)
            timer = engine._trailing_integrity_task
            assert timer is not None
            assert not timer.done()

            adapter.seed_protection(make_inst(), "1", stop_loss=None)
            await engine._run_trailing_integrity_once()

            protection = await adapter.get_position_tpsl(make_inst(), "1")
            assert protection is not None
            assert protection[1] is not None
            assert protection[1].trigger_price == state.current_stop
        finally:
            await engine.ashutdown()

    async def test_ashutdown_stops_the_worker_and_integrity_timer(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        state = await attach_synthetic(engine)
        await wait_worker_running(engine, True)
        worker = engine._trailing_worker
        timer = engine._trailing_integrity_task
        assert worker is not None
        assert timer is not None

        await engine.ashutdown()
        if not timer.done():
            await asyncio.wait_for(timer, timeout=1.0)

        assert worker.running is False
        assert timer.done()
        protection = await adapter.get_position_tpsl(make_inst(), "1")
        assert protection is not None
        assert protection[1] is not None
        assert protection[1].trigger_price == state.current_stop


class TestPositionUpdateForwarding:
    async def test_ready_engine_forwards_position_updates(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        try:
            state = await attach_synthetic(engine)
            assert state.quantity == Decimal("1")

            adapter.update_position(make_position(quantity="0.5"))
            await drain(engine)

            updated = await engine.trailing.get(state.trailing_id)
            assert updated is not None
            assert updated.quantity == Decimal("0.5")
            assert updated.phase is TrailingPhase.ARMED
        finally:
            await engine.ashutdown()

    async def test_position_updates_are_not_forwarded_before_rehydration(self, persistent_store):
        store = persistent_store
        bus = EventBus()
        first = await make_engine(store, build_adapter(bus, [make_position()]), bus)
        await attach_synthetic(first)
        await first.ashutdown()

        bus = EventBus()
        adapter = build_adapter(bus, [make_position()], adapter_cls=TogglePositionsAdapter)
        adapter.fail_positions = True
        engine = await make_engine(store, adapter, bus)
        try:
            manager = engine._trailing_manager
            assert manager is not None
            forwarded: list[Position] = []

            async def record(position: Position) -> None:
                forwarded.append(position)

            manager.on_position_update = record  # type: ignore[method-assign]

            adapter.update_position(make_position(quantity="0.5"))
            await drain(engine)
            assert forwarded == []

            adapter.fail_positions = False
            await engine.reconcile()
            adapter.update_position(make_position(quantity="0.25"))
            await drain(engine)

            assert len(forwarded) == 1
            assert forwarded[0].quantity == Decimal("0.25")
        finally:
            await engine.ashutdown()


class TestRateLimitReservation:
    async def test_unknown_window_schedules_refresh_and_denies(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [])
        adapter.set_rate_limits(
            RateLimits(
                requests_per_interval=100,
                interval_seconds=60.0,
                remaining=7,
                reset_at=FUTURE_RESET,
            )
        )
        engine = await make_engine(store, adapter, bus)
        try:
            engine._rate_limit_reset_at = None
            engine._rate_limit_refresh_pending = False

            assert engine.reserve_rate_limit() is False
            assert engine._rate_limit_refresh_pending is True

            await drain(engine)

            assert engine._rate_limit_refresh_pending is False
            assert engine._rate_limit_reset_at == FUTURE_RESET
            assert engine._rate_limit_budget == 7
            assert engine.reserve_rate_limit() is True
            assert engine._rate_limit_budget == 6
        finally:
            await engine.ashutdown()

    async def test_stale_window_schedules_refresh_and_denies(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [])
        adapter.set_rate_limits(
            RateLimits(
                requests_per_interval=100,
                interval_seconds=60.0,
                remaining=5,
                reset_at=FUTURE_RESET,
            )
        )
        engine = await make_engine(store, adapter, bus)
        try:
            engine._rate_limit_reset_at = NOW
            engine._rate_limit_budget = 0
            engine._rate_limit_refresh_pending = False

            assert engine.reserve_rate_limit() is False
            assert engine._rate_limit_refresh_pending is True

            await drain(engine)

            assert engine._rate_limit_budget == 5
            assert engine._rate_limit_reset_at == FUTURE_RESET
            assert engine.reserve_rate_limit() is True
            assert engine._rate_limit_budget == 4
        finally:
            await engine.ashutdown()

    async def test_exhausted_current_window_denies_without_refreshing(self, store):
        bus = EventBus()
        engine = await make_engine(store, build_adapter(bus, []), bus)
        try:
            engine._rate_limit_reset_at = FUTURE_RESET
            engine._rate_limit_budget = 0
            engine._rate_limit_refresh_pending = False

            assert engine.reserve_rate_limit() is False
            assert engine._rate_limit_refresh_pending is False
            assert engine._rate_limit_budget == 0
        finally:
            await engine.ashutdown()

    async def test_budget_override_bypasses_unknown_window(self, store):
        bus = EventBus()
        engine = await make_engine(
            store,
            build_adapter(bus, []),
            bus,
            risk_config=RiskConfig(rate_limit_budget_override=2),
        )
        try:
            engine._rate_limit_reset_at = None
            engine._rate_limit_budget = 0
            engine._rate_limit_refresh_pending = False

            assert engine.reserve_rate_limit() is True
            assert engine.reserve_rate_limit() is True
            assert engine._rate_limit_refresh_pending is False
            assert engine._rate_limit_budget == 0
        finally:
            await engine.ashutdown()

    async def test_zero_budget_override_denies_without_refreshing(self, store):
        bus = EventBus()
        engine = await make_engine(
            store,
            build_adapter(bus, []),
            bus,
            risk_config=RiskConfig(rate_limit_budget_override=0),
        )
        try:
            engine._rate_limit_reset_at = None
            engine._rate_limit_refresh_pending = False

            assert engine.reserve_rate_limit() is False
            assert engine._rate_limit_refresh_pending is False
        finally:
            await engine.ashutdown()


class TestEndToEndLifecycle:
    async def test_favourable_move_tightens_and_adverse_move_never_loosens(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        try:
            state = await attach_synthetic(engine)
            assert state.current_stop == Decimal("49900.00")

            adapter.set_ticker(make_inst(), Ticker(last=Decimal("50500")))
            worker = engine._trailing_worker
            assert worker is not None
            base = time.monotonic()
            await worker.run_once(now=base + 10.0)
            tightened = await engine.trailing.get(state.trailing_id)
            assert tightened is not None
            assert tightened.current_stop == Decimal("50400.00")
            assert tightened.best_basis == Decimal("50500")

            adapter.set_ticker(make_inst(), Ticker(last=Decimal("49500")))
            await worker.run_once(now=base + 20.0)
            pulled_back = await engine.trailing.get(state.trailing_id)
            assert pulled_back is not None
            assert pulled_back.current_stop == Decimal("50400.00")
            assert pulled_back.best_basis == Decimal("50500")
            assert pulled_back.reprice_count == 1
        finally:
            await engine.ashutdown()

    async def test_restart_while_the_leg_remains_open(self, persistent_store):
        store = persistent_store
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        first = await make_engine(store, adapter, bus)
        state = await attach_synthetic(first)
        await first.ashutdown()

        bus = EventBus()
        restarted_adapter = build_adapter(bus, [make_position()])
        restarted = await make_engine(store, restarted_adapter, bus)
        try:
            restored = await restarted.trailing.get(state.trailing_id)
            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
            assert restored.current_stop == Decimal("49900.00")
            assert restarted._trailing_ready is True
            await wait_worker_running(restarted, True)

            protection = await restarted_adapter.get_position_tpsl(make_inst(), "1")
            assert protection is not None
            assert protection[1] is not None
            assert protection[1].trigger_price == Decimal("49900.00")
        finally:
            await restarted.ashutdown()

    async def test_two_hedged_legs_trail_independently(self, store):
        bus = EventBus()
        positions = [
            make_position(position_id="1", quantity="1", entry="50000"),
            make_position(position_id="2", quantity="-1", entry="51000"),
        ]
        adapter = build_adapter(bus, positions)
        engine = await make_engine(store, adapter, bus)
        try:
            long_state = await attach_synthetic(engine, position_id="1")
            short_state = await attach_synthetic(engine, position_id="2")
            assert long_state.current_stop == Decimal("49900.00")
            assert short_state.current_stop == Decimal("50100.00")

            adapter.set_ticker(make_inst(), Ticker(last=Decimal("51500")))
            worker = engine._trailing_worker
            assert worker is not None
            await worker.run_once(now=time.monotonic() + 10.0)

            long_after = await engine.trailing.get(long_state.trailing_id)
            short_after = await engine.trailing.get(short_state.trailing_id)
            assert long_after is not None
            assert short_after is not None
            assert long_after.position_id == "1"
            assert short_after.position_id == "2"
            assert long_after.current_stop == Decimal("51400.00")
            assert short_after.current_stop == Decimal("50100.00")
            assert long_after.reprice_count == 1
            assert short_after.reprice_count == 0
        finally:
            await engine.ashutdown()

    @pytest.mark.parametrize("mode", [TrailingMode.SYNTHETIC, TrailingMode.NATIVE])
    async def test_trailing_activity_leaves_orders_and_watermark_untouched(self, store, mode):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        if mode is TrailingMode.NATIVE:
            enable_native_trailing(adapter)
        engine = await make_engine(store, adapter, bus)
        try:
            baseline = await engine.reconcile()
            assert baseline.is_clean
            watermark = await store.get_reconcile_watermark()
            assert watermark is not None

            state = await engine.trailing.attach(
                TrailingRequest(make_inst(), make_spec(), mode=mode)
            )
            assert state.mode is mode
            if mode is TrailingMode.SYNTHETIC:
                await engine.trailing.on_ticker(state.trailing_id, Ticker(last=Decimal("50500")))

            assert await store.get_reconcile_watermark() == watermark
            assert await store.query_orders() == []
            assert await store.query_fills() == []

            after = await engine.reconcile()
            assert after.is_clean
            assert await store.query_orders() == []
            assert await store.query_fills() == []
        finally:
            await engine.ashutdown()

    async def test_disconnect_leaves_protection_in_place(self, store):
        bus = EventBus()
        adapter = build_adapter(bus, [make_position()])
        engine = await make_engine(store, adapter, bus)
        state = await attach_synthetic(engine)
        await wait_worker_running(engine, True)

        await engine.disconnect()

        protection = await adapter.get_position_tpsl(make_inst(), "1")
        assert protection is not None
        assert protection[1] is not None
        assert protection[1].trigger_price == state.current_stop
        assert engine._trailing_worker is not None
        assert engine._trailing_worker.running is False

        await engine.ashutdown()

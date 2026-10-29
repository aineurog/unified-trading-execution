"""Engine-level lifecycle tests for MT5 synthetic trailing.

These drive the real core ``Engine``/``TrailingManager``/``TrailingWorker`` with
an ``MT5Adapter`` connected to the scripted terminal in
``_scripted_terminal.py``.  They cover the MT5-specific acceptance gates:
exact-ticket writes, ticket isolation, ratcheting with the terminal minimum
stop distance, position updates, restart identity, integrity, and failure
policy.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import PlatformError, TrailingNotSupportedError
from unified_trading_execution.mt5 import MT5Adapter, MT5Config, MT5Engine
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing import (
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
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Position

from ._scripted_terminal import DEFAULT_SYMBOL, ScriptedLeg, ScriptedTerminal

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)
OPEN_EPOCH = int(NOW.timestamp())


def _instrument() -> Instrument:
    return Instrument(
        symbol="EUR",
        asset_class=AssetClass.MARGIN_FX,
        quote_currency="USD",
        platform_symbol=DEFAULT_SYMBOL,
    )


def _price_spec(**overrides: object) -> TrailingSpec:
    values: dict[str, object] = {
        "offset_type": TrailingOffsetType.PRICE,
        "offset": Decimal("0.0200"),
    }
    values.update(overrides)
    return TrailingSpec(**values)  # type: ignore[arg-type]


def _leg(ticket: int = 1001, **overrides: object) -> ScriptedLeg:
    values: dict[str, object] = {
        "ticket": ticket,
        "opened_at": OPEN_EPOCH,
    }
    values.update(overrides)
    return ScriptedLeg(**values)  # type: ignore[arg-type]


async def _never() -> None:
    await asyncio.Event().wait()


async def _make_engine(
    store: SQLiteStateStore,
    terminal: ScriptedTerminal,
    mt5: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: TrailingConfig | None = None,
) -> MT5Engine:
    terminal.install(mt5)
    adapter = MT5Adapter(MT5Config(login=12345678, password="test-password", server="Test-Demo"))
    monkeypatch.setattr(adapter, "_poll_loop", _never)
    engine = MT5Engine(
        adapter,
        state_store=store,
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        trailing_config=config,
        reconcile_interval_seconds=None,
        fill_settle_lag_seconds=0,
    )
    await engine.connect()
    return engine


def _freeze_clock(engine: Engine) -> list[datetime]:
    """Control the manager clock so the one-second reprice floor is testable."""
    now = [NOW]
    engine.trailing._clock = lambda: now[0]  # type: ignore[attr-defined]
    return now


async def _attach(
    engine: Engine,
    *,
    position_id: str | None = None,
    spec: TrailingSpec | None = None,
    mode: TrailingMode = TrailingMode.AUTO,
    take_over: bool = False,
):
    state = await engine.trailing.attach(
        TrailingRequest(
            _instrument(),
            spec or _price_spec(),
            position_id=position_id,
            mode=mode,
            take_over_existing_protection=take_over,
        )
    )
    await _quiesce_worker(engine)
    return state


async def _quiesce_worker(engine: Engine) -> None:
    """Stop the polling loop after attach's state event schedules a restart.

    The engine starts the worker from a scheduled ``TrailingStateEvent``
    handler, so a single ``stop()`` can race the still-pending sync.  Repeating
    stop + a loop turn drains every scheduled sync while keeping the worker
    stopped for deterministic manual cycles.
    """
    worker = engine._trailing_worker
    assert worker is not None
    for _ in range(3):
        await worker.stop()
        await asyncio.sleep(0)
        if not worker.running:
            return
    raise AssertionError("trailing worker kept restarting")


async def _run_worker(engine: Engine, offset_seconds: float) -> None:
    await _quiesce_worker(engine)
    worker = engine._trailing_worker
    assert worker is not None
    await worker.run_once(now=time.monotonic() + offset_seconds)


async def _drain(engine: Engine) -> None:
    for _ in range(100):
        pending = [task for task in engine._background_tasks if not task.done()]
        if not pending:
            await asyncio.sleep(0)
            pending = [task for task in engine._background_tasks if not task.done()]
            if not pending:
                return
        await asyncio.gather(*pending, return_exceptions=True)


def _position(
    *,
    quantity: str,
    position_id: str = "1001",
    average_entry_price: str = "1.1000",
) -> Position:
    return Position(
        instrument=_instrument(),
        quantity=Decimal(quantity),
        average_entry_price=Decimal(average_entry_price),
        updated_at=NOW,
        position_id=position_id,
        opened_at=NOW,
    )


async def _publish_position(engine: Engine, position: Position) -> None:
    adapter = engine.adapter
    assert isinstance(adapter, MT5Adapter)
    adapter._publish_position(position)
    await _drain(engine)


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


class TestAttach:
    async def test_attach_targets_one_ticket_and_leaves_other_legs_untouched(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(
            legs=[
                _leg(1001, volume=0.1, price_open=1.1000),
                _leg(1002, volume=0.2, price_open=1.1050),
                _leg(1003, side=1, volume=0.1, price_open=1.1100),
            ]
        )
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine, position_id="1001")

            assert state.mode is TrailingMode.SYNTHETIC
            assert state.phase is TrailingPhase.ARMED
            assert state.side is OrderSide.BUY
            assert state.position_id == "1001"
            assert state.resolved_basis is TrailingBasis.BID
            assert state.protection_trigger_basis is TrailingBasis.BID
            assert state.current_stop == Decimal("1.0800")

            assert terminal.legs[1001].stop_loss == 1.08
            assert terminal.legs[1002].stop_loss == 0.0
            assert terminal.legs[1003].stop_loss == 0.0
            assert len(terminal.sends) == 1
            assert terminal.sends[0]["position"] == 1001
            assert terminal.sends[0]["symbol"] == DEFAULT_SYMBOL
        finally:
            await engine.ashutdown()

    async def test_auto_resolves_synthetic_and_native_is_rejected(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001), _leg(1002, price_open=1.1050)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine, position_id="1001", mode=TrailingMode.AUTO)
            assert state.mode is TrailingMode.SYNTHETIC

            with pytest.raises(TrailingNotSupportedError, match="native trailing"):
                await _attach(engine, position_id="1002", mode=TrailingMode.NATIVE)

            assert terminal.legs[1002].stop_loss == 0.0
        finally:
            await engine.ashutdown()


class TestReprice:
    async def test_favourable_move_tightens_and_adverse_move_never_loosens(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine)

            terminal.bid = 1.1100
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            tightened = await engine.trailing.get(state.trailing_id)
            assert tightened is not None
            assert tightened.current_stop == Decimal("1.0900")
            assert tightened.best_basis == Decimal("1.1100")
            assert tightened.reprice_count == 1
            assert terminal.legs[1001].stop_loss == 1.09

            terminal.bid = 1.1000
            clock[0] = NOW + timedelta(seconds=20)
            await _run_worker(engine, 20.0)

            pulled_back = await engine.trailing.get(state.trailing_id)
            assert pulled_back is not None
            assert pulled_back.current_stop == Decimal("1.0900")
            assert pulled_back.best_basis == Decimal("1.1100")
            assert pulled_back.reprice_count == 1
            assert terminal.legs[1001].stop_loss == 1.09
        finally:
            await engine.ashutdown()

    async def test_step_skips_a_small_move_then_accepts_a_larger_one(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine, spec=_price_spec(step=Decimal("0.0200")))

            terminal.bid = 1.1100
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            skipped = await engine.trailing.get(state.trailing_id)
            assert skipped is not None
            assert skipped.current_stop == Decimal("1.0800")
            assert skipped.best_basis == Decimal("1.1100")
            assert skipped.reprice_count == 0
            assert terminal.legs[1001].stop_loss == 1.08

            terminal.bid = 1.1300
            clock[0] = NOW + timedelta(seconds=20)
            await _run_worker(engine, 20.0)

            accepted = await engine.trailing.get(state.trailing_id)
            assert accepted is not None
            assert accepted.current_stop == Decimal("1.1100")
            assert accepted.reprice_count == 1
            assert terminal.legs[1001].stop_loss == 1.11
        finally:
            await engine.ashutdown()

    async def test_reprice_skips_a_stop_inside_the_terminal_minimum_distance(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine)

            terminal.trade_stops_level = 300
            adapter = engine.adapter
            assert isinstance(adapter, MT5Adapter)
            adapter._spec_cache.clear()
            engine.trailing._instrument_specs.clear()  # type: ignore[attr-defined]

            terminal.bid = 1.1100
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            blocked = await engine.trailing.get(state.trailing_id)
            assert blocked is not None
            assert blocked.current_stop == Decimal("1.0800")
            assert blocked.best_basis == Decimal("1.1100")
            assert blocked.reprice_count == 0
            assert terminal.legs[1001].stop_loss == 1.08
        finally:
            await engine.ashutdown()

    async def test_reprice_waits_for_the_adapter_reprice_interval(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine)

            terminal.bid = 1.1050
            clock[0] = NOW + timedelta(milliseconds=500)
            await _run_worker(engine, 10.0)

            early = await engine.trailing.get(state.trailing_id)
            assert early is not None
            assert early.best_basis == Decimal("1.1050")
            assert early.current_stop == Decimal("1.0800")
            assert early.reprice_count == 0
            assert terminal.legs[1001].stop_loss == 1.08

            clock[0] = NOW + timedelta(seconds=2)
            await _run_worker(engine, 20.0)

            later = await engine.trailing.get(state.trailing_id)
            assert later is not None
            assert later.current_stop == Decimal("1.0850")
            assert later.reprice_count == 1
            assert terminal.legs[1001].stop_loss == 1.085
        finally:
            await engine.ashutdown()


class TestQuantization:
    async def test_attach_rounds_the_stop_down_to_the_tick(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)], bid=1.10505)
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)

            assert state.current_stop == Decimal("1.0850")
            assert terminal.legs[1001].stop_loss == 1.085
        finally:
            await engine.ashutdown()


class TestActivation:
    async def test_price_activation_arms_only_after_the_trigger(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            spec = _price_spec(
                activation=TrailingActivation.PRICE,
                activation_price=Decimal("1.1100"),
            )
            state = await _attach(engine, spec=spec)

            assert state.activated is False
            assert state.current_stop is None
            assert terminal.sends == []

            terminal.bid = 1.1150
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            activated = await engine.trailing.get(state.trailing_id)
            assert activated is not None
            assert activated.activated is True
            assert activated.current_stop == Decimal("1.0950")
            assert terminal.legs[1001].stop_loss == 1.095
        finally:
            await engine.ashutdown()

    async def test_profit_percent_activation_resolves_from_entry(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            spec = _price_spec(
                activation=TrailingActivation.PROFIT_PERCENT,
                activation_profit_percent=Decimal("1.0"),
            )
            state = await _attach(engine, spec=spec)
            assert state.resolved_activation_price == Decimal("1.1110")

            terminal.bid = 1.1200
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            activated = await engine.trailing.get(state.trailing_id)
            assert activated is not None
            assert activated.activated is True
            assert activated.current_stop == Decimal("1.1000")
        finally:
            await engine.ashutdown()

    async def test_initial_stop_and_take_profit_are_written_once(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            spec = _price_spec(
                initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("1.0900")),
                take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("1.1300")),
            )
            state = await _attach(engine, spec=spec)

            assert state.current_stop == Decimal("1.0900")
            assert state.current_take_profit == Decimal("1.1300")
            assert terminal.legs[1001].stop_loss == 1.09
            assert terminal.legs[1001].take_profit == 1.13

            terminal.bid = 1.1200
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)
            terminal.bid = 1.1300
            clock[0] = NOW + timedelta(seconds=20)
            await _run_worker(engine, 20.0)

            trailed = await engine.trailing.get(state.trailing_id)
            assert trailed is not None
            assert trailed.current_stop == Decimal("1.1100")
            assert trailed.current_take_profit == Decimal("1.1300")
            assert terminal.legs[1001].take_profit == 1.13
        finally:
            await engine.ashutdown()


class TestAtr:
    async def test_atr_offset_and_take_profit_use_closed_candles(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            spec = _price_spec(
                offset_type=TrailingOffsetType.ATR,
                offset=Decimal("5"),
                atr_period=2,
                atr_timeframe=CandleTimeframe.M5,
                take_profit=ProtectionLevel(ProtectionLevelType.ATR, Decimal("10")),
            )
            state = await _attach(engine, spec=spec)

            assert state.atr_value == Decimal("0.0020")
            assert state.current_stop == Decimal("1.0900")
            assert state.current_take_profit == Decimal("1.1200")
            assert terminal.legs[1001].stop_loss == 1.09
            assert terminal.legs[1001].take_profit == 1.12
            assert len(terminal.candle_fetches) == 1
            assert terminal.candle_fetches[0][0] == DEFAULT_SYMBOL
            assert terminal.candle_fetches[0][1] == 5
            assert terminal.candle_fetches[0][3] == 4
        finally:
            await engine.ashutdown()


class TestPositionUpdates:
    async def test_partial_close_keeps_the_price_stop(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            sends_before = len(terminal.sends)

            await _publish_position(engine, _position(quantity="0.05"))

            updated = await engine.trailing.get(state.trailing_id)
            assert updated is not None
            assert updated.phase is TrailingPhase.ARMED
            assert updated.quantity == Decimal("0.05")
            assert updated.current_stop == Decimal("1.0800")
            assert len(terminal.sends) == sends_before
            assert terminal.legs[1001].stop_loss == 1.08
        finally:
            await engine.ashutdown()

    async def test_full_close_marks_done_and_clears_only_the_owned_stop(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001, take_profit=1.2000)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            assert terminal.legs[1001].stop_loss == 1.08

            await _publish_position(engine, _position(quantity="0"))

            done = await engine.trailing.get(state.trailing_id)
            assert done is not None
            assert done.phase is TrailingPhase.DONE
            assert done.quantity == Decimal("0")
            assert terminal.legs[1001].stop_loss == 0.0
            assert terminal.legs[1001].take_profit == 1.20
            assert terminal.sends[-1]["position"] == 1001
            assert terminal.sends[-1]["sl"] == 0.0
            assert terminal.sends[-1]["tp"] == 1.20
        finally:
            await engine.ashutdown()

    async def test_side_flip_cancels_and_clears_only_the_owned_stop(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001, take_profit=1.2000)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            assert terminal.legs[1001].stop_loss == 1.08
            assert terminal.legs[1001].take_profit == 1.20

            await _publish_position(engine, _position(quantity="-0.1"))

            cancelled = await engine.trailing.get(state.trailing_id)
            assert cancelled is not None
            assert cancelled.phase is TrailingPhase.CANCELLED
            assert terminal.legs[1001].stop_loss == 0.0
            assert terminal.legs[1001].take_profit == 1.20
            assert terminal.sends[-1]["position"] == 1001
            assert terminal.sends[-1]["sl"] == 0.0
            assert terminal.sends[-1]["tp"] == 1.20
        finally:
            await engine.ashutdown()


class TestRestart:
    async def test_restart_rehydrates_the_same_ticket(
        self,
        persistent_store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        first = await _make_engine(persistent_store, terminal, mock_mt5_module, monkeypatch)
        state = await _attach(first)
        await first.ashutdown()

        restarted = await _make_engine(persistent_store, terminal, mock_mt5_module, monkeypatch)
        try:
            restored = await restarted.trailing.get(state.trailing_id)
            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
            assert restored.position_id == "1001"
            assert restored.current_stop == Decimal("1.0800")
            assert terminal.legs[1001].stop_loss == 1.08
        finally:
            await restarted.ashutdown()

    async def test_replaced_leg_is_degraded_and_not_adopted(
        self,
        persistent_store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        first = await _make_engine(persistent_store, terminal, mock_mt5_module, monkeypatch)
        state = await _attach(first)
        await first.ashutdown()

        terminal.legs[1001].opened_at += 60

        restarted = await _make_engine(persistent_store, terminal, mock_mt5_module, monkeypatch)
        try:
            replaced = await restarted.trailing.get(state.trailing_id)
            assert replaced is not None
            assert replaced.phase is TrailingPhase.DEGRADED
            assert replaced.last_error is not None
            assert "position_replaced" in replaced.last_error
            active = await restarted.trailing.list(active_only=True)
            assert [item.position_id for item in active] == ["1001"]
        finally:
            await restarted.ashutdown()


class TestIntegrity:
    async def test_missing_owned_stop_is_restored(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            terminal.legs[1001].stop_loss = 0.0

            restored = await engine.trailing.check_integrity(state.trailing_id)

            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
            assert restored.current_stop == Decimal("1.0800")
            assert terminal.legs[1001].stop_loss == 1.08
        finally:
            await engine.ashutdown()

    async def test_looser_external_stop_is_restored(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            terminal.legs[1001].stop_loss = 1.05

            restored = await engine.trailing.check_integrity(state.trailing_id)

            assert restored is not None
            assert restored.current_stop == Decimal("1.0800")
            assert terminal.legs[1001].stop_loss == 1.08
        finally:
            await engine.ashutdown()

    async def test_tighter_external_stop_is_adopted_without_a_write(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            sends_before = len(terminal.sends)
            terminal.legs[1001].stop_loss = 1.09

            adopted = await engine.trailing.check_integrity(state.trailing_id)

            assert adopted is not None
            assert adopted.current_stop == Decimal("1.0900")
            assert len(terminal.sends) == sends_before
            assert terminal.legs[1001].stop_loss == 1.09
        finally:
            await engine.ashutdown()

    async def test_missing_owned_take_profit_is_restored(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            spec = _price_spec(
                initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("1.0900")),
                take_profit=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("1.1300")),
            )
            state = await _attach(engine, spec=spec)
            assert terminal.legs[1001].take_profit == 1.13

            terminal.legs[1001].take_profit = 0.0

            restored = await engine.trailing.check_integrity(state.trailing_id)

            assert restored is not None
            assert restored.phase is TrailingPhase.ARMED
            assert restored.current_take_profit == Decimal("1.1300")
            assert terminal.legs[1001].take_profit == 1.13
            assert terminal.legs[1001].stop_loss == 1.09
        finally:
            await engine.ashutdown()


class TestFailurePolicy:
    async def test_ticker_failures_degrade_then_recover_with_protection_intact(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        config = TrailingConfig(fetch_failure_threshold=2)
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch, config=config)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine)
            terminal.ticker_available = False

            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)
            first = await engine.trailing.get(state.trailing_id)
            assert first is not None
            assert first.phase is TrailingPhase.ARMED

            clock[0] = NOW + timedelta(seconds=20)
            await _run_worker(engine, 20.0)
            degraded = await engine.trailing.get(state.trailing_id)
            assert degraded is not None
            assert degraded.phase is TrailingPhase.DEGRADED
            assert degraded.last_error is not None
            assert "fetch:" in degraded.last_error
            assert terminal.legs[1001].stop_loss == 1.08

            terminal.ticker_available = True
            clock[0] = NOW + timedelta(seconds=30)
            await _run_worker(engine, 30.0)
            recovered = await engine.trailing.get(state.trailing_id)
            assert recovered is not None
            assert recovered.phase is TrailingPhase.ARMED
            assert recovered.failure_count == 0
            assert recovered.last_error is None
        finally:
            await engine.ashutdown()

    async def test_protection_write_failures_degrade_then_recover(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        config = TrailingConfig(protection_failure_threshold=2)
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch, config=config)
        try:
            clock = _freeze_clock(engine)
            state = await _attach(engine)

            terminal.bid = 1.1100
            terminal.reject_next_send = True
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)
            first = await engine.trailing.get(state.trailing_id)
            assert first is not None
            assert first.phase is TrailingPhase.ARMED
            assert first.failure_count == 1
            assert first.current_stop == Decimal("1.0800")
            assert terminal.legs[1001].stop_loss == 1.08

            terminal.bid = 1.1200
            terminal.reject_next_send = True
            clock[0] = NOW + timedelta(seconds=20)
            await _run_worker(engine, 20.0)
            degraded = await engine.trailing.get(state.trailing_id)
            assert degraded is not None
            assert degraded.phase is TrailingPhase.DEGRADED
            assert degraded.failure_count == 2
            assert degraded.current_stop == Decimal("1.0800")
            assert terminal.legs[1001].stop_loss == 1.08

            terminal.bid = 1.1300
            clock[0] = NOW + timedelta(seconds=30)
            await _run_worker(engine, 30.0)
            recovered = await engine.trailing.get(state.trailing_id)
            assert recovered is not None
            assert recovered.phase is TrailingPhase.ARMED
            assert recovered.current_stop == Decimal("1.1100")
            assert recovered.failure_count == 0
            assert terminal.legs[1001].stop_loss == 1.11
        finally:
            await engine.ashutdown()

    async def test_failed_attach_leaves_no_active_state(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            terminal.reject_next_send = True

            with pytest.raises(PlatformError):
                await _attach(engine)

            states = await engine.trailing.list(active_only=False)
            assert len(states) == 1
            assert states[0].phase is TrailingPhase.FAILED
            active = await engine.trailing.list(active_only=True)
            assert active == []
            assert terminal.legs[1001].stop_loss == 0.0
        finally:
            await engine.ashutdown()


class TestReconciliationAndShutdown:
    async def test_trailing_creates_no_order_mirror_or_watermark_change(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            clock = _freeze_clock(engine)
            baseline = await engine.reconcile()
            assert baseline.is_clean
            watermark = await store.get_reconcile_watermark()
            assert watermark is not None

            await _attach(engine)
            terminal.bid = 1.1100
            clock[0] = NOW + timedelta(seconds=10)
            await _run_worker(engine, 10.0)

            assert await store.get_reconcile_watermark() == watermark
            assert await store.query_orders() == []
            assert await store.query_fills() == []

            after = await engine.reconcile()
            assert after.is_clean
            assert await store.query_orders() == []
        finally:
            await engine.ashutdown()

    async def test_shutdown_leaves_protection_at_the_terminal(
        self,
        store: SQLiteStateStore,
        mock_mt5_module: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        terminal = ScriptedTerminal(legs=[_leg(1001)])
        engine = await _make_engine(store, terminal, mock_mt5_module, monkeypatch)
        try:
            state = await _attach(engine)
            await _drain(engine)

            await engine.disconnect()

            assert terminal.legs[1001].stop_loss == 1.08
            assert state.current_stop == Decimal("1.0800")
            assert engine._trailing_worker is not None
            assert engine._trailing_worker.running is False
        finally:
            await engine.ashutdown()

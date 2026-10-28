"""Live testnet coverage for native and synthetic trailing stops.

These tests are skipped unless testnet credentials are configured.  They open
tiny derivative positions, exercise the native trailing surface against real
Bybit behaviour, and assert the routing, protection ownership, and state-store
invariants that synthetic trailing relies on.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.bybit.symbols import to_bybit_symbol
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import (
    PlatformError,
    TrailingStateError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing import (
    CandleTimeframe,
    NativeTrailingRequest,
    NativeTrailingResult,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.trailing.manager import TrailingManager
from unified_trading_execution.types.enums import (
    AssetClass,
    FillEntry,
    FillReason,
    OrderSide,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, TpSlAttachment
from unified_trading_execution.types.position import Position

from .conftest import cleanup_open_orders
from .helpers import (
    align_to_tick,
    build_unified_order,
    random_client_id,
    valid_qty_from_spec,
)


async def _wait_for_position(
    adapter: BybitAdapter,
    instrument: Instrument,
    *,
    timeout: float = 15.0,
) -> Position:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        positions = await adapter.fetch_positions()
        for position in positions:
            if position.instrument == instrument and position.quantity > 0:
                return cast(Position, position)
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"No open position appeared for {instrument.symbol}")
        await asyncio.sleep(0.5)


async def _wait_for_flat(
    adapter: BybitAdapter,
    instrument: Instrument,
    *,
    timeout: float = 15.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        positions = await adapter.fetch_positions()
        if not any(
            position.instrument == instrument and position.quantity != 0 for position in positions
        ):
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Position for {instrument.symbol} did not flatten")
        await asyncio.sleep(0.5)


async def _close_position(
    adapter: BybitAdapter,
    instrument: Instrument,
    quantity: Decimal,
) -> None:
    side = OrderSide.SELL if quantity > 0 else OrderSide.BUY
    close = build_unified_order(
        instrument,
        OrderType.MARKET,
        side,
        abs(quantity),
        client_order_id=random_client_id("close-trail"),
        reduce_only=True,
    )
    await adapter.place_order(close)


async def _flatten_instrument(adapter: BybitAdapter, instrument: Instrument) -> None:
    positions = await adapter.fetch_positions()
    for position in positions:
        if position.instrument == instrument and position.quantity != 0:
            await _close_position(adapter, instrument, position.quantity)
    await _wait_for_flat(adapter, instrument)


async def _open_long_position(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference_price: Decimal,
) -> tuple[Position, Decimal]:
    await _flatten_instrument(adapter, instrument)
    spec = await adapter.fetch_instrument_spec(instrument)
    quantity = valid_qty_from_spec(spec, reference_price)
    open_order = build_unified_order(
        instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        quantity,
        client_order_id=random_client_id("open-trail"),
    )
    await adapter.place_order(open_order)
    position = await _wait_for_position(adapter, instrument)
    return position, quantity


_POSITION_MODE_ERROR = "position idx not match position mode"


async def _reconcile_position_mode(
    adapter: BybitAdapter,
    instrument: Instrument,
    store: SQLiteStateStore,
) -> None:
    """Attach a store and seed it with the venue's per-symbol position mode.

    The adapter resolves ``positionIdx`` from stored intent, not from the
    venue, so an order placed before any mode intent is stored defaults to
    one-way and is rejected on a hedge-mode symbol.
    """
    await store.initialize()
    adapter.attach_state_store(store)
    mode_key = adapter._position_mode_key(instrument)
    mode = await adapter.get_position_mode(instrument)
    if mode is not None:
        await store.set_adapter_config(mode_key, mode.value)


async def _open_position_reconciling_mode(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference_price: Decimal,
    store: SQLiteStateStore,
) -> tuple[Position, Decimal]:
    """Open one long leg, retrying in hedge mode when the venue disagrees."""
    await _reconcile_position_mode(adapter, instrument, store)
    try:
        return await _open_long_position(adapter, instrument, reference_price)
    except PlatformError as exc:
        if _POSITION_MODE_ERROR not in str(exc):
            raise
        await store.set_adapter_config(adapter._position_mode_key(instrument), "hedge")
        return await _open_long_position(adapter, instrument, reference_price)


async def _wait_for_native_trail(
    adapter: BybitAdapter,
    instrument: Instrument,
    position_id: str,
    *,
    timeout: float = 5.0,
) -> NativeTrailingResult:
    """Poll the position snapshot until the just-written trail is visible.

    Testnet's ``get_positions`` snapshot lags a ``set_trading_stop`` that has
    already been accepted, so a one-shot read-back is racy.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await adapter.get_position_trailing_stop(instrument, position_id)
        if result is not None:
            return result
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"native trail for {instrument.symbol} did not appear")
        await asyncio.sleep(0.25)


async def _wait_for_no_native_trail(
    adapter: BybitAdapter,
    instrument: Instrument,
    position_id: str,
    *,
    timeout: float = 5.0,
) -> None:
    """Poll until the venue snapshot no longer reports a native trail."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await adapter.get_position_trailing_stop(instrument, position_id) is None:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"native trail for {instrument.symbol} did not clear")
        await asyncio.sleep(0.25)


def _native_id(position: Position) -> str:
    assert position.position_id is not None
    return f"{position.position_id}:trailing"


async def _audit_payloads(
    store: SQLiteStateStore,
    event_type: str,
) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], event.payload)
        for event in await store.query_audit_events(limit=200)
        if event.event_type == event_type
    ]


async def _wait_for_trailing_exit_fills(
    store: SQLiteStateStore,
    instrument: Instrument,
    *,
    exclude: frozenset[str],
    expected_quantity: Decimal,
    timeout: float = 30.0,
) -> list[FillRecord]:
    """Poll the persisted fills until the whole leg is closed by the trail."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        fills = [
            fill
            for fill in await store.query_fills(instrument=instrument, limit=1000)
            if fill.reason is FillReason.TRAILING_STOP
            and fill.entry is FillEntry.OUT
            and fill.platform_fill_id not in exclude
        ]
        covered = sum((fill.fill_quantity for fill in fills), Decimal("0"))
        if covered >= expected_quantity:
            return fills
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(
                f"trailing exit fills for {instrument.symbol} covered {covered} "
                f"of {expected_quantity}"
            )
        await asyncio.sleep(0.5)


async def _wait_for_trailing_phase(
    manager: TrailingManager,
    trailing_id: str,
    phase: TrailingPhase,
    *,
    timeout: float = 15.0,
) -> TrailingState:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        state = await manager.get(trailing_id)
        if state is not None and state.phase is phase:
            return state
        if asyncio.get_running_loop().time() >= deadline:
            current = state.phase.value if state is not None else "missing"
            raise AssertionError(
                f"trailing state {trailing_id} stayed {current}, expected {phase.value}"
            )
        await asyncio.sleep(0.25)


@dataclass(frozen=True)
class TrailingHarness:
    engine: Engine
    adapter: BybitAdapter
    store: SQLiteStateStore
    instrument: Instrument
    position: Position
    quantity: Decimal


@pytest.fixture
async def linear_position(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
    tmp_path: Path,
) -> AsyncIterator[tuple[Position, Decimal]]:
    """Open a minimal long position and always flatten it afterwards."""
    if linear_instrument.asset_class is not AssetClass.FUTURES:
        pytest.skip("trailing positions exist only on derivatives")

    store = SQLiteStateStore(str(tmp_path / "linear-position.db"))
    position, quantity = await _open_position_reconciling_mode(
        connected_adapter, linear_instrument, linear_reference_price, store
    )
    try:
        yield position, quantity
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                _native_id(position),
                linear_instrument,
            )
        with contextlib.suppress(Exception):
            await _flatten_instrument(connected_adapter, linear_instrument)
        with contextlib.suppress(Exception):
            await cleanup_open_orders(connected_adapter)


@pytest.fixture
async def inverse_position(
    connected_adapter: BybitAdapter,
    inverse_instrument: Instrument,
    inverse_reference_price: Decimal,
    tmp_path: Path,
) -> AsyncIterator[tuple[Position, Decimal]]:
    """Open a minimal long inverse position and always flatten it afterwards.

    Inverse symbols on the shared testnet account may be in hedge mode while
    linear symbols are one-way.  The adapter reads position mode from its state
    store, so attach a temporary store and reconcile the stored mode with the
    venue (without switching the platform) before placing the order.
    """
    if inverse_instrument.asset_class is not AssetClass.FUTURES:
        pytest.skip("trailing positions exist only on derivatives")

    store = SQLiteStateStore(str(tmp_path / "inverse-position.db"))
    position, quantity = await _open_position_reconciling_mode(
        connected_adapter, inverse_instrument, inverse_reference_price, store
    )

    try:
        yield position, quantity
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                _native_id(position),
                inverse_instrument,
            )
        with contextlib.suppress(Exception):
            await _flatten_instrument(connected_adapter, inverse_instrument)
        with contextlib.suppress(Exception):
            await cleanup_open_orders(connected_adapter)


@pytest.fixture
async def trailing_engine(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
    tmp_path: Path,
) -> AsyncIterator[TrailingHarness]:
    """A connected Engine over the live adapter, with one open linear position."""
    if linear_instrument.asset_class is not AssetClass.FUTURES:
        pytest.skip("trailing positions exist only on derivatives")

    await cleanup_open_orders(connected_adapter)
    store = SQLiteStateStore(str(tmp_path / "trailing-engine.db"))
    position, quantity = await _open_position_reconciling_mode(
        connected_adapter, linear_instrument, linear_reference_price, store
    )
    engine = Engine(
        adapter=connected_adapter,
        state_store=store,
        event_bus=EventBus(),
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        reconcile_interval_seconds=None,
    )
    await engine.connect()
    try:
        yield TrailingHarness(
            engine=engine,
            adapter=connected_adapter,
            store=store,
            instrument=linear_instrument,
            position=position,
            quantity=quantity,
        )
    finally:
        with contextlib.suppress(Exception):
            for active in await engine.trailing.list(active_only=True):
                await engine.trailing.cancel(active.trailing_id, clear_protection=True)
        with contextlib.suppress(Exception):
            await _flatten_instrument(connected_adapter, linear_instrument)
        await engine.ashutdown()


def _request(
    instrument: Instrument,
    position: Position,
    quantity: Decimal,
    offset: Decimal,
    *,
    basis: TrailingBasis = TrailingBasis.LAST,
    activation_price: Decimal | None = None,
) -> NativeTrailingRequest:
    assert position.position_id is not None
    return NativeTrailingRequest(
        instrument=instrument,
        position_id=position.position_id,
        side=OrderSide.BUY,
        quantity=quantity,
        offset=offset,
        offset_type=TrailingOffsetType.PRICE,
        basis=basis,
        activation_price=activation_price,
        client_id=random_client_id("trail"),
    )


async def test_native_trailing_place_replace_and_cancel(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_position: tuple[Position, Decimal],
) -> None:
    position, quantity = linear_position
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    last = await connected_adapter.fetch_ticker(linear_instrument)
    assert last is not None and last.last is not None

    initial = align_to_tick(last.last * Decimal("0.02"), spec.tick_size)
    tightened = align_to_tick(last.last * Decimal("0.01"), spec.tick_size)

    placed = await connected_adapter.place_trailing_stop(
        _request(linear_instrument, position, quantity, initial)
    )
    assert placed.position_scoped
    assert placed.confirmed_offset is not None
    stored = await _wait_for_native_trail(
        connected_adapter, linear_instrument, position.position_id
    )
    assert stored.confirmed_offset == initial

    replaced = await connected_adapter.place_trailing_stop(
        _request(linear_instrument, position, quantity, tightened, basis=TrailingBasis.MARK)
    )
    assert replaced.confirmed_offset is not None
    assert replaced.confirmed_offset <= initial
    stored_after = await _wait_for_native_trail(
        connected_adapter, linear_instrument, position.position_id
    )
    assert stored_after.confirmed_offset == tightened

    await connected_adapter.cancel_trailing_stop(
        replaced.native_id,
        linear_instrument,
    )
    await _wait_for_no_native_trail(connected_adapter, linear_instrument, position.position_id)


async def test_native_trailing_preserves_static_take_profit(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_position: tuple[Position, Decimal],
) -> None:
    position, quantity = linear_position
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    last = await connected_adapter.fetch_ticker(linear_instrument)
    assert last is not None and last.last is not None
    take_profit_price = align_to_tick(last.last * Decimal("1.3"), spec.tick_size)
    await connected_adapter.modify_position_tpsl(
        linear_instrument,
        position.position_id,
        take_profit=TpSlAttachment(trigger_price=take_profit_price),
    )
    before = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
    assert before is not None
    assert before[0] is not None

    offset = align_to_tick(last.last * Decimal("0.02"), spec.tick_size)
    try:
        await connected_adapter.place_trailing_stop(
            _request(linear_instrument, position, quantity, offset)
        )
        await _wait_for_native_trail(connected_adapter, linear_instrument, position.position_id)
        after = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
        assert after is not None
        assert after[0] is not None
        assert after[0].trigger_price == before[0].trigger_price
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                _native_id(position),
                linear_instrument,
            )
        with contextlib.suppress(Exception):
            await connected_adapter.clear_position_tpsl(
                linear_instrument,
                position.position_id,
                take_profit=True,
            )


async def test_static_protection_writes_preserve_the_unmentioned_side(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_position: tuple[Position, Decimal],
) -> None:
    """Verify the venue merges one-sided writes and one-sided clears."""
    position, _ = linear_position
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    last = await connected_adapter.fetch_ticker(linear_instrument)
    assert last is not None and last.last is not None

    take_profit_price = align_to_tick(last.last * Decimal("1.3"), spec.tick_size)
    stop_loss_price = align_to_tick(last.last * Decimal("0.9"), spec.tick_size)
    await connected_adapter.modify_position_tpsl(
        linear_instrument,
        position.position_id,
        take_profit=TpSlAttachment(trigger_price=take_profit_price),
        stop_loss=TpSlAttachment(trigger_price=stop_loss_price),
    )
    try:
        both = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
        assert both is not None
        assert both[0] is not None
        assert both[0].trigger_price == take_profit_price
        assert both[1] is not None
        assert both[1].trigger_price == stop_loss_price

        tightened_tp = align_to_tick(last.last * Decimal("1.25"), spec.tick_size)
        await connected_adapter.modify_position_tpsl(
            linear_instrument,
            position.position_id,
            take_profit=TpSlAttachment(trigger_price=tightened_tp),
        )
        updated = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
        assert updated is not None
        assert updated[0] is not None
        assert updated[0].trigger_price == tightened_tp
        assert updated[1] is not None
        assert updated[1].trigger_price == stop_loss_price

        await connected_adapter.clear_position_tpsl(
            linear_instrument,
            position.position_id,
            take_profit=True,
        )
        cleared = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
        assert cleared is not None
        assert cleared[0] is None
        assert cleared[1] is not None
        assert cleared[1].trigger_price == stop_loss_price
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.clear_position_tpsl(
                linear_instrument,
                position.position_id,
                take_profit=True,
                stop_loss=True,
            )


async def test_native_trailing_sends_the_activation_price(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_position: tuple[Position, Decimal],
) -> None:
    """N9: Bybit does not echo a pending activePrice on the position read.

    Verify the adapter sends it and the venue accepts the conditional trail;
    the absolute stop must never be reported until the activation triggers.
    """
    position, quantity = linear_position
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    last = await connected_adapter.fetch_ticker(linear_instrument)
    assert last is not None and last.last is not None

    offset = align_to_tick(last.last * Decimal("0.02"), spec.tick_size)
    activation = align_to_tick(last.last * Decimal("1.05"), spec.tick_size)
    with patch.object(
        connected_adapter._session,
        "set_trading_stop",
        wraps=connected_adapter._session.set_trading_stop,
    ) as set_trading_stop:
        placed = await connected_adapter.place_trailing_stop(
            _request(
                linear_instrument,
                position,
                quantity,
                offset,
                activation_price=activation,
            )
        )
    try:
        assert placed.position_scoped
        assert set_trading_stop.call_args is not None
        assert set_trading_stop.call_args.kwargs["activePrice"] == str(activation)
        stored = await connected_adapter.get_position_trailing_stop(
            linear_instrument, position.position_id
        )
        assert stored is None or stored.current_stop is None
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                placed.native_id,
                linear_instrument,
            )


async def test_native_trailing_works_on_inverse_perpetuals(
    connected_adapter: BybitAdapter,
    inverse_instrument: Instrument,
    inverse_position: tuple[Position, Decimal],
) -> None:
    position, quantity = inverse_position
    assert position.position_id is not None
    assert connected_adapter._instrument_to_category(inverse_instrument) == "inverse"

    spec = await connected_adapter.fetch_instrument_spec(inverse_instrument)
    last = await connected_adapter.fetch_ticker(inverse_instrument)
    assert last is not None and last.last is not None
    offset = align_to_tick(last.last * Decimal("0.02"), spec.tick_size)
    tightened = align_to_tick(last.last * Decimal("0.01"), spec.tick_size)

    placed = await connected_adapter.place_trailing_stop(
        _request(inverse_instrument, position, quantity, offset)
    )
    try:
        assert placed.position_scoped
        assert placed.native_id == _native_id(position)
        stored = await _wait_for_native_trail(
            connected_adapter, inverse_instrument, position.position_id
        )
        assert stored.confirmed_offset == offset
        assert stored.current_stop is None

        replaced = await connected_adapter.place_trailing_stop(
            _request(
                inverse_instrument,
                position,
                quantity,
                tightened,
                basis=TrailingBasis.MARK,
            )
        )
        assert replaced.confirmed_offset == tightened
        stored_after = await _wait_for_native_trail(
            connected_adapter, inverse_instrument, position.position_id
        )
        assert stored_after.confirmed_offset == tightened

        await connected_adapter.cancel_trailing_stop(
            replaced.native_id,
            inverse_instrument,
        )
        await _wait_for_no_native_trail(connected_adapter, inverse_instrument, position.position_id)
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                _native_id(position),
                inverse_instrument,
            )


async def test_native_trailing_is_rejected_on_spot(
    connected_adapter: BybitAdapter,
    spot_instrument: Instrument,
) -> None:
    request = NativeTrailingRequest(
        instrument=spot_instrument,
        position_id="0",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        offset=Decimal("100"),
        offset_type=TrailingOffsetType.PRICE,
        basis=TrailingBasis.LAST,
        activation_price=None,
        client_id=random_client_id("trail-spot"),
    )

    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_trailing_stop(request)
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.cancel_trailing_stop("BTCUSDT:0:trailing", spot_instrument)


async def test_synthetic_trailing_market_data_is_reachable(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> None:
    ticker = await connected_adapter.fetch_ticker(linear_instrument)
    assert ticker is not None
    assert ticker.last is not None
    assert ticker.mark is not None

    candles = await connected_adapter.fetch_candles(
        linear_instrument,
        CandleTimeframe.H1,
        limit=30,
    )
    assert len(candles) >= 15
    assert all(candle.close_time.tzinfo is not None for candle in candles)
    assert candles[-1].close_time <= datetime.now(UTC)


async def test_native_attach_arms_a_position_scoped_trail(
    trailing_engine: TrailingHarness,
) -> None:
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    spec = await adapter.fetch_instrument_spec(instrument)
    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.last is not None

    take_profit_price = align_to_tick(ticker.last * Decimal("1.3"), spec.tick_size)
    await adapter.modify_position_tpsl(
        instrument,
        position.position_id,
        take_profit=TpSlAttachment(trigger_price=take_profit_price),
    )
    before_tpsl = await adapter.get_position_tpsl(instrument, position.position_id)
    assert before_tpsl is not None
    before_open_orders = set((await adapter.fetch_open_orders()).keys())
    before_mirror = await harness.store.query_orders(limit=1000)
    before_watermark = await harness.store.get_reconcile_watermark()

    offset = align_to_tick(ticker.last * Decimal("0.02"), spec.tick_size)
    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(
                offset_type=TrailingOffsetType.PRICE,
                offset=offset,
                basis=TrailingBasis.LAST,
            ),
            position_id=position.position_id,
        )
    )

    assert state.mode is TrailingMode.NATIVE
    assert state.phase is TrailingPhase.ARMED
    assert state.native_id == _native_id(position)
    assert state.current_stop is None

    stored = await _wait_for_native_trail(adapter, instrument, position.position_id)
    assert stored.native_id == _native_id(position)
    assert stored.confirmed_offset == offset
    assert stored.current_stop is None
    assert stored.position_scoped

    assert await adapter.get_position_tpsl(instrument, position.position_id) == before_tpsl
    assert set((await adapter.fetch_open_orders()).keys()) == before_open_orders
    assert len(await harness.store.query_orders(limit=1000)) == len(before_mirror)
    assert await harness.store.get_reconcile_watermark() == before_watermark
    assert [
        active.trailing_id for active in await harness.engine.trailing.list(active_only=True)
    ] == [state.trailing_id]


async def test_native_trailing_trigger_closes_the_leg_without_an_orphan(
    trailing_engine: TrailingHarness,
) -> None:
    """A triggered native trail closes the leg and leaves nothing behind.

    The trail watches the mark price, which moves in real time on testnet, so a
    small retrace from the armed high watermark fires the venue's own close.
    The close must reach the store as a trailing exit fill and must not leak a
    phantom order into the mirror or leave a live order on the venue.
    """
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    spec = await adapter.fetch_instrument_spec(instrument)
    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.mark is not None

    before_order_ids = {
        order.client_order_id for order in await harness.store.query_orders(limit=1000)
    }
    before_fill_ids = frozenset(
        fill.platform_fill_id
        for fill in await harness.store.query_fills(instrument=instrument, limit=1000)
    )
    before_open_orders = set((await adapter.fetch_open_orders()).keys())

    offset = align_to_tick(spec.tick_size * 5, spec.tick_size)
    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(
                offset_type=TrailingOffsetType.PRICE,
                offset=offset,
                basis=TrailingBasis.MARK,
            ),
            position_id=position.position_id,
        )
    )

    assert state.mode is TrailingMode.NATIVE
    assert state.phase is TrailingPhase.ARMED
    await _wait_for_native_trail(adapter, instrument, position.position_id)

    await _wait_for_flat(adapter, instrument, timeout=120.0)

    exit_fills = await _wait_for_trailing_exit_fills(
        harness.store,
        instrument,
        exclude=before_fill_ids,
        expected_quantity=state.quantity,
    )
    # The venue's trailing close omits ``positionIdx``; the manager treats a
    # missing fill leg id as an unclaimed-leg match.  Any id it does carry must
    # address this leg (full ``{venue_symbol}:{idx}`` or bare ``idx``).
    leg_idx = position.position_id.rsplit(":", 1)[-1]
    assert all(fill.position_id in (None, position.position_id, leg_idx) for fill in exit_fills)

    done = await _wait_for_trailing_phase(
        harness.engine.trailing,
        state.trailing_id,
        TrailingPhase.DONE,
    )
    assert done.quantity == 0
    assert await adapter.get_position_trailing_stop(instrument, position.position_id) is None

    after_order_ids = {
        order.client_order_id for order in await harness.store.query_orders(limit=1000)
    }
    assert after_order_ids == before_order_ids
    assert set((await adapter.fetch_open_orders()).keys()) <= before_open_orders

    category = adapter._instrument_to_category(instrument)
    raw, _ = await adapter._run_request(
        adapter._session.get_open_orders,
        category=category,
        symbol=to_bybit_symbol(instrument),
    )
    venue_orders = (raw.get("result") or {}).get("list") or []
    assert not any(
        entry.get("stopOrderType") == "TrailingStop"
        or entry.get("createType") == "CreateByTrailingStop"
        for entry in venue_orders
    )


@pytest.mark.parametrize("clear_protection", [True, False])
async def test_native_cancel_always_removes_the_trail(
    trailing_engine: TrailingHarness,
    clear_protection: bool,
) -> None:
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    spec = await adapter.fetch_instrument_spec(instrument)
    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.last is not None
    take_profit_price = align_to_tick(ticker.last * Decimal("1.3"), spec.tick_size)
    await adapter.modify_position_tpsl(
        instrument,
        position.position_id,
        take_profit=TpSlAttachment(trigger_price=take_profit_price),
    )

    offset = align_to_tick(ticker.last * Decimal("0.02"), spec.tick_size)
    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=offset),
            position_id=position.position_id,
        )
    )
    assert state.mode is TrailingMode.NATIVE

    cancelled = await harness.engine.trailing.cancel(
        state.trailing_id,
        clear_protection=clear_protection,
    )

    assert cancelled.phase is TrailingPhase.CANCELLED
    await _wait_for_no_native_trail(adapter, instrument, position.position_id)
    after = await adapter.get_position_tpsl(instrument, position.position_id)
    assert after is not None
    assert after[0] is not None
    assert after[0].trigger_price == take_profit_price
    assert await harness.engine.trailing.list(active_only=True) == []
    assert await _audit_payloads(harness.store, "trailing.cancelled")
    assert await _audit_payloads(harness.store, "trailing.clear")


async def test_native_attach_requires_takeover_for_an_existing_native_trail(
    trailing_engine: TrailingHarness,
) -> None:
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    spec = await adapter.fetch_instrument_spec(instrument)
    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.last is not None

    previous_offset = align_to_tick(ticker.last * Decimal("0.02"), spec.tick_size)
    await adapter.place_trailing_stop(
        _request(instrument, position, harness.quantity, previous_offset)
    )
    previous = await _wait_for_native_trail(adapter, instrument, position.position_id)
    assert previous.confirmed_offset == previous_offset

    new_offset = align_to_tick(ticker.last * Decimal("0.01"), spec.tick_size)
    request = TrailingRequest(
        instrument,
        TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=new_offset),
        position_id=position.position_id,
    )

    with pytest.raises(TrailingStateError, match="native trailing stop"):
        await harness.engine.trailing.attach(request)

    assert await harness.engine.trailing.list(active_only=True) == []
    unchanged = await _wait_for_native_trail(adapter, instrument, position.position_id)
    assert unchanged.confirmed_offset == previous_offset

    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=new_offset),
            position_id=position.position_id,
            take_over_existing_protection=True,
        )
    )

    assert state.mode is TrailingMode.NATIVE
    assert state.phase is TrailingPhase.ARMED
    replaced = await _wait_for_native_trail(adapter, instrument, position.position_id)
    assert replaced.confirmed_offset == new_offset

    takeovers = await _audit_payloads(harness.store, "trailing.takeover")
    assert takeovers
    assert Decimal(str(takeovers[-1]["previous_native_offset"])) == previous_offset


async def test_native_attach_refuses_existing_static_stop_then_preserves_it_on_takeover(
    trailing_engine: TrailingHarness,
) -> None:
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    spec = await adapter.fetch_instrument_spec(instrument)
    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.last is not None
    static_stop = align_to_tick(ticker.last * Decimal("0.9"), spec.tick_size)
    await adapter.modify_position_tpsl(
        instrument,
        position.position_id,
        stop_loss=TpSlAttachment(trigger_price=static_stop),
    )

    offset = align_to_tick(ticker.last * Decimal("0.02"), spec.tick_size)
    request = TrailingRequest(
        instrument,
        TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=offset),
        position_id=position.position_id,
    )

    with pytest.raises(TrailingStateError, match="already has a stop"):
        await harness.engine.trailing.attach(request)

    assert await harness.engine.trailing.list(active_only=True) == []
    before = await adapter.get_position_tpsl(instrument, position.position_id)
    assert before is not None
    assert before[1] is not None
    assert before[1].trigger_price == static_stop

    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=offset),
            position_id=position.position_id,
            take_over_existing_protection=True,
        )
    )

    assert state.mode is TrailingMode.NATIVE
    assert state.phase is TrailingPhase.ARMED
    after = await adapter.get_position_tpsl(instrument, position.position_id)
    assert after is not None
    assert after[1] is not None
    assert after[1].trigger_price == static_stop

    takeovers = await _audit_payloads(harness.store, "trailing.takeover")
    assert takeovers
    assert Decimal(str(takeovers[-1]["previous_stop"])) == static_stop


async def test_percent_trailing_routes_to_synthetic_protection(
    trailing_engine: TrailingHarness,
) -> None:
    harness = trailing_engine
    adapter = harness.adapter
    instrument = harness.instrument
    position = harness.position
    assert position.position_id is not None

    state = await harness.engine.trailing.attach(
        TrailingRequest(
            instrument,
            TrailingSpec(
                offset_type=TrailingOffsetType.PERCENT,
                offset=Decimal("1"),
            ),
            position_id=position.position_id,
        )
    )

    assert state.mode is TrailingMode.SYNTHETIC
    assert state.phase is TrailingPhase.ARMED
    assert state.routing_reason == "native_unsupported_offset_type"
    assert state.native_id is None
    assert state.current_stop is not None
    assert await adapter.get_position_trailing_stop(instrument, position.position_id) is None

    worker = harness.engine._trailing_worker
    assert worker is not None
    await worker.stop()
    latest = await harness.engine.trailing.get(state.trailing_id)
    assert latest is not None
    assert latest.current_stop is not None

    protection = await adapter.get_position_tpsl(instrument, position.position_id)
    assert protection is not None
    assert protection[1] is not None
    assert protection[1].trigger_price == latest.current_stop

    await harness.engine.trailing.cancel(state.trailing_id, clear_protection=True)

    cleared = await adapter.get_position_tpsl(instrument, position.position_id)
    assert cleared is not None
    assert cleared[1] is None

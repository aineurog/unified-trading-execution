"""Demo-account integration tests for MT5 synthetic trailing.

MT5 declares no native trailing primitive, so every request routes to the
synthetic manager, which keeps a real position stop on the ticket.  These tests
connect a real ``MT5Engine`` to a demo terminal and prove the venue effects:

* two buys produce two tickets, each with its own stop; attaching to one never
  touches the other,
* hedged long and short legs attach independently on the bid and ask sides
  (skipped on a netting account, where opposing orders merge),
* a favourable tick tightens the venue stop, an adverse tick never loosens it,
* a partial close keeps the stop and persists the new quantity,
* attach preserves a take profit that trailing does not own,
* cancel clears only the owned stop,
* the state carries the position open time.

All tests are skipped cleanly when ``MT5_LOGIN`` / ``MT5_PASSWORD`` /
``MT5_SERVER`` are absent or the ``MetaTrader5`` package is unavailable.  The
broker symbol defaults to ``EURUSD`` and can be overridden via ``MT5_SYMBOL``.
Every test flattens its own legs and cancels its own orders.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TypeVar

import pytest
import pytest_asyncio

from unified_trading_execution.mt5 import MT5Adapter, MT5Config, MT5Engine
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import SQLiteStateStore
from unified_trading_execution.trailing import (
    TrailingBasis,
    TrailingConfig,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import TpSlAttachment
from unified_trading_execution.types.position import Position

from .helpers import (
    build_unified_order,
    cleanup_adapter,
    random_client_id,
    spec_price,
    spec_qty,
)

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_INSTRUMENT = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol=_BROKER_SYMBOL,
)

# The adapter advertises a one-second reprice floor for MT5.
_REPRICE_FLOOR_SECONDS = 1.0
# Stop distance in terminal points; wide enough for any broker stop level.
_OFFSET_POINTS = Decimal(200)
_WIDER_OFFSET_POINTS = Decimal(400)

_T = TypeVar("_T")


def _mt5_adapter(engine: MT5Engine) -> MT5Adapter:
    """The engine's adapter, narrowed to the implementation it always wires."""
    adapter = engine.adapter
    assert isinstance(adapter, MT5Adapter)
    return adapter


@pytest_asyncio.fixture
async def trailing_engine(mt5_config: MT5Config) -> AsyncIterator[MT5Engine]:
    """A connected engine with its own store on a flat demo position set."""
    store_path = Path(tempfile.gettempdir()) / f"ute_mt5_trailing_{uuid.uuid4().hex}.db"
    store = SQLiteStateStore(str(store_path))
    engine = MT5Engine(
        mt5_config,
        state_store=store,
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        trailing_config=TrailingConfig(poll_interval_seconds=1.0),
        reconcile_interval_seconds=None,
        fill_settle_lag_seconds=0,
    )
    await engine.connect()
    try:
        await cleanup_adapter(_mt5_adapter(engine))
        yield engine
    finally:
        with contextlib.suppress(Exception):
            await cleanup_adapter(_mt5_adapter(engine))
        with contextlib.suppress(Exception):
            await engine.ashutdown()
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            store_path.unlink()


async def _wait_for(
    factory: Callable[[], Awaitable[_T | None]],
    *,
    timeout: float = 20.0,
    message: str,
) -> _T:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = await factory()
        if result:
            return result
        if loop.time() >= deadline:
            pytest.fail(message)
        await asyncio.sleep(0.2)


async def _quiesce_worker(engine: MT5Engine) -> None:
    """Stop the polling worker so manual ticks stay deterministic.

    Every state event reschedules the worker, and a single ``stop()`` can race
    that pending sync, so stop and yield a few times.
    """
    worker = engine._trailing_worker
    assert worker is not None
    for _ in range(5):
        await worker.stop()
        await asyncio.sleep(0)
        if not worker.running:
            return
    pytest.fail("trailing worker kept restarting")


async def _tickets() -> set[int]:
    import MetaTrader5 as mt5

    legs = await asyncio.to_thread(mt5.positions_get, symbol=_BROKER_SYMBOL)
    return {int(leg.ticket) for leg in legs or ()}


async def _new_ticket(before: set[int], *, timeout: float) -> int | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        new = [ticket for ticket in await _tickets() if ticket not in before]
        if len(new) == 1:
            return new[0]
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(0.2)


async def _live_quote(adapter: MT5Adapter) -> tuple[Decimal, Decimal]:
    ticker = await adapter.fetch_ticker(_INSTRUMENT)
    if ticker is None or ticker.bid is None or ticker.ask is None:
        pytest.fail(f"no market quote for {_BROKER_SYMBOL}")
    return ticker.bid, ticker.ask


async def _market_order(
    adapter: MT5Adapter,
    *,
    side: OrderSide,
    qty: Decimal,
    take_profit: TpSlAttachment | None = None,
    position_id: str | None = None,
) -> None:
    result = await adapter.place_order(
        build_unified_order(
            _INSTRUMENT,
            OrderType.MARKET,
            side,
            qty,
            client_order_id=random_client_id("trail"),
            take_profit=take_profit,
            position_id=position_id,
        )
    )
    assert result.status is OrderStatus.FILLED, f"market {side.value} was not filled"


async def _open_leg(
    adapter: MT5Adapter,
    *,
    side: OrderSide,
    qty: Decimal,
    take_profit: TpSlAttachment | None = None,
    allow_merge: bool = False,
) -> int | None:
    before = await _tickets()
    await _market_order(adapter, side=side, qty=qty, take_profit=take_profit)
    ticket = await _new_ticket(before, timeout=5.0 if allow_merge else 8.0)
    if ticket is None and not allow_merge:
        pytest.fail("the market order did not produce a position leg")
    return ticket


async def _wait_for_mirror_leg(engine: MT5Engine, ticket: int) -> Position:
    async def find() -> Position | None:
        for leg in await engine.get_positions(_INSTRUMENT):
            if leg.position_id == str(ticket):
                return leg
        return None

    return await _wait_for(find, message=f"position {ticket} never reached the mirror")


async def _attach(
    engine: MT5Engine,
    ticket: int,
    *,
    offset_points: Decimal = _OFFSET_POINTS,
) -> TrailingState:
    tick_size = (await _mt5_adapter(engine).fetch_instrument_spec(_INSTRUMENT)).tick_size
    state = await engine.trailing.attach(
        TrailingRequest(
            _INSTRUMENT,
            TrailingSpec(
                offset_type=TrailingOffsetType.PRICE,
                offset=tick_size * offset_points,
            ),
            position_id=str(ticket),
            mode=TrailingMode.AUTO,
        )
    )
    await _quiesce_worker(engine)
    return state


async def _venue_tpsl(adapter: MT5Adapter, ticket: int) -> tuple[Decimal | None, Decimal | None]:
    result = await adapter.get_position_tpsl(_INSTRUMENT, str(ticket))
    assert result is not None, f"no open position {ticket}"
    take_profit, stop_loss = result
    return (
        take_profit.trigger_price if take_profit is not None else None,
        stop_loss.trigger_price if stop_loss is not None else None,
    )


async def _venue_stop(adapter: MT5Adapter, ticket: int) -> Decimal | None:
    _, stop_loss = await _venue_tpsl(adapter, ticket)
    return stop_loss


def _ticker_for(bid: Decimal, tick_size: Decimal) -> Ticker:
    return Ticker(bid=bid, ask=bid + tick_size * Decimal(20), mark=None)


async def test_two_buys_each_carry_their_own_stop(trailing_engine: MT5Engine) -> None:
    """Two tickets keep independent stops; attaching to one never touches the other."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)

    first = await _open_leg(adapter, side=OrderSide.BUY, qty=qty)
    assert first is not None
    await _wait_for_mirror_leg(trailing_engine, first)

    second = await _open_leg(adapter, side=OrderSide.BUY, qty=qty, allow_merge=True)
    if second is None:
        pytest.skip("account is netting-mode — the second buy merged into one leg")
    await _wait_for_mirror_leg(trailing_engine, second)

    first_state = await _attach(trailing_engine, first, offset_points=_OFFSET_POINTS)
    _, second_stop_before = await _venue_tpsl(adapter, second)
    assert second_stop_before is None, "attaching the first ticket wrote the second"

    second_state = await _attach(trailing_engine, second, offset_points=_WIDER_OFFSET_POINTS)

    assert first_state.mode is TrailingMode.SYNTHETIC
    assert first_state.position_id == str(first)
    assert second_state.position_id == str(second)
    assert first_state.current_stop is not None
    assert second_state.current_stop is not None
    assert first_state.current_stop > second_state.current_stop
    assert await _venue_stop(adapter, first) == first_state.current_stop
    assert await _venue_stop(adapter, second) == second_state.current_stop

    assert first_state.position_opened_at is not None
    assert first_state.position_opened_at.tzinfo is not None
    opened_ago = datetime.now(tz=UTC) - first_state.position_opened_at
    assert abs(opened_ago.total_seconds()) < 3600


async def test_hedged_legs_attach_independently(trailing_engine: MT5Engine) -> None:
    """A long and a short leg each get their own stop on the correct side."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)

    long_ticket = await _open_leg(adapter, side=OrderSide.BUY, qty=qty)
    assert long_ticket is not None
    short_ticket = await _open_leg(adapter, side=OrderSide.SELL, qty=qty, allow_merge=True)
    if short_ticket is None:
        pytest.skip("account is netting-mode — opposing orders net to zero")
    await _wait_for_mirror_leg(trailing_engine, long_ticket)
    await _wait_for_mirror_leg(trailing_engine, short_ticket)

    long_state = await _attach(trailing_engine, long_ticket)
    short_state = await _attach(trailing_engine, short_ticket)

    assert long_state.side is OrderSide.BUY
    assert short_state.side is OrderSide.SELL
    assert long_state.protection_trigger_basis is TrailingBasis.BID
    assert short_state.protection_trigger_basis is TrailingBasis.ASK
    assert long_state.current_stop is not None
    assert short_state.current_stop is not None
    assert long_state.current_stop < long_state.entry_price
    assert short_state.current_stop > short_state.entry_price
    assert await _venue_stop(adapter, long_ticket) == long_state.current_stop
    assert await _venue_stop(adapter, short_ticket) == short_state.current_stop


async def test_favourable_tick_tightens_and_adverse_does_not_loosen(
    trailing_engine: MT5Engine,
) -> None:
    """A higher bid reprices the venue stop up; a lower bid leaves it alone."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)
    ticket = await _open_leg(adapter, side=OrderSide.BUY, qty=qty)
    assert ticket is not None
    await _wait_for_mirror_leg(trailing_engine, ticket)

    attached = await _attach(trailing_engine, ticket)
    assert attached.current_stop is not None
    assert await _venue_stop(adapter, ticket) == attached.current_stop
    tick_size = (await adapter.fetch_instrument_spec(_INSTRUMENT)).tick_size

    await asyncio.sleep(_REPRICE_FLOOR_SECONDS + 0.3)
    await _quiesce_worker(trailing_engine)
    state = await trailing_engine.trailing.get(attached.trailing_id)
    assert state is not None
    assert state.current_stop is not None
    assert state.best_basis is not None

    # Anchor the synthetic best to the live quote so the new stop stays below
    # the real market (and therefore valid) even if price drifted since attach.
    live_bid, _ = await _live_quote(adapter)
    favourable_bid = max(state.best_basis, live_bid) + tick_size * Decimal(100)
    tightened = await trailing_engine.trailing.on_ticker(
        state.trailing_id, _ticker_for(favourable_bid, tick_size)
    )
    await _quiesce_worker(trailing_engine)

    assert tightened is not None
    assert tightened.current_stop is not None
    assert tightened.current_stop > state.current_stop
    assert tightened.best_basis == favourable_bid
    assert tightened.reprice_count >= 1
    assert await _venue_stop(adapter, ticket) == tightened.current_stop

    await asyncio.sleep(_REPRICE_FLOOR_SECONDS + 0.3)
    await _quiesce_worker(trailing_engine)
    assert tightened.best_basis is not None
    adverse_bid = tightened.best_basis - tick_size * Decimal(100)
    unchanged = await trailing_engine.trailing.on_ticker(
        attached.trailing_id, _ticker_for(adverse_bid, tick_size)
    )
    await _quiesce_worker(trailing_engine)

    assert unchanged is not None
    assert unchanged.current_stop == tightened.current_stop
    assert unchanged.best_basis == tightened.best_basis
    assert await _venue_stop(adapter, ticket) == tightened.current_stop


async def test_partial_close_keeps_the_stop(trailing_engine: MT5Engine) -> None:
    """Closing half the volume keeps protection and persists the new quantity."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)
    ticket = await _open_leg(adapter, side=OrderSide.BUY, qty=qty * 2)
    assert ticket is not None
    await _wait_for_mirror_leg(trailing_engine, ticket)

    state = await _attach(trailing_engine, ticket)
    assert state.current_stop is not None
    assert state.quantity == qty * 2

    await _market_order(adapter, side=OrderSide.SELL, qty=qty, position_id=str(ticket))

    async def reduced() -> TrailingState | None:
        current = await trailing_engine.trailing.get(state.trailing_id)
        if current is not None and current.quantity == qty:
            return current
        return None

    updated = await _wait_for(reduced, message="the partial close never reached the trailing state")
    await _quiesce_worker(trailing_engine)

    assert updated.phase is TrailingPhase.ARMED
    assert updated.current_stop is not None
    assert updated.current_stop >= state.current_stop
    assert await _venue_stop(adapter, ticket) == updated.current_stop


async def test_take_over_preserves_an_existing_take_profit(
    trailing_engine: MT5Engine,
) -> None:
    """Attach writes only the stop and leaves a venue take profit untouched."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)
    _, ask = await _live_quote(adapter)
    take_profit = await spec_price(adapter, _INSTRUMENT, ask * Decimal("1.002"))

    ticket = await _open_leg(
        adapter,
        side=OrderSide.BUY,
        qty=qty,
        take_profit=TpSlAttachment(trigger_price=take_profit),
    )
    assert ticket is not None
    await _wait_for_mirror_leg(trailing_engine, ticket)

    state = await _attach(trailing_engine, ticket)
    assert state.current_stop is not None
    venue_take_profit, venue_stop = await _venue_tpsl(adapter, ticket)
    assert venue_take_profit == take_profit
    assert venue_stop == state.current_stop


async def test_cancel_clears_only_the_owned_stop(trailing_engine: MT5Engine) -> None:
    """Cancel removes the trailing stop and leaves the venue take profit."""
    adapter = _mt5_adapter(trailing_engine)
    qty = await spec_qty(adapter, _INSTRUMENT)
    _, ask = await _live_quote(adapter)
    take_profit = await spec_price(adapter, _INSTRUMENT, ask * Decimal("1.002"))

    ticket = await _open_leg(
        adapter,
        side=OrderSide.BUY,
        qty=qty,
        take_profit=TpSlAttachment(trigger_price=take_profit),
    )
    assert ticket is not None
    await _wait_for_mirror_leg(trailing_engine, ticket)

    state = await _attach(trailing_engine, ticket)
    assert await _venue_stop(adapter, ticket) == state.current_stop

    cancelled = await trailing_engine.trailing.cancel(state.trailing_id, clear_protection=True)

    assert cancelled.phase is TrailingPhase.CANCELLED
    venue_take_profit, venue_stop = await _venue_tpsl(adapter, ticket)
    assert venue_stop is None
    assert venue_take_profit == take_profit
    active = await trailing_engine.trailing.list(active_only=True)
    assert state.trailing_id not in {item.trailing_id for item in active}

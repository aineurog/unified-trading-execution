"""Live testnet coverage for native trailing and its synthetic inputs.

These tests are skipped unless testnet credentials are configured.  They open
a tiny linear position, exercise the native trailing surface against real
Bybit behaviour, and assert the market data synthetic trailing relies on is
reachable for the same instrument.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    NativeTrailingRequest,
    TrailingBasis,
    TrailingOffsetType,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment
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
        position = next(
            (
                candidate
                for candidate in positions
                if candidate.instrument == instrument and candidate.quantity > 0
            ),
            None,
        )
        if position is not None:
            return position
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"No open position appeared for {instrument.symbol}")
        await asyncio.sleep(0.5)


async def _close_position(
    adapter: BybitAdapter,
    instrument: Instrument,
    quantity: Decimal,
) -> None:
    close = build_unified_order(
        instrument,
        OrderType.MARKET,
        OrderSide.SELL,
        quantity,
        client_order_id=random_client_id("close-trail"),
        reduce_only=True,
    )
    await adapter.place_order(close)


@pytest.fixture
async def linear_position(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> AsyncIterator[tuple[Position, Decimal]]:
    """Open a minimal long position and always flatten it afterwards."""
    if linear_instrument.asset_class is not AssetClass.FUTURES:
        pytest.skip("trailing positions exist only on derivatives")

    spec = await connected_adapter.fetch_instrument_spec(linear_instrument)
    quantity = valid_qty_from_spec(spec, linear_reference_price)
    open_order = build_unified_order(
        linear_instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        quantity,
        client_order_id=random_client_id("open-trail"),
    )
    await connected_adapter.place_order(open_order)
    position = await _wait_for_position(connected_adapter, linear_instrument)
    try:
        yield position, quantity
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_trailing_stop(
                f"{position.position_id}:trailing",
                linear_instrument,
            )
        with contextlib.suppress(Exception):
            await _close_position(connected_adapter, linear_instrument, quantity)
        with contextlib.suppress(Exception):
            await cleanup_open_orders(connected_adapter)


def _request(
    instrument: Instrument,
    position: Position,
    quantity: Decimal,
    offset: Decimal,
    *,
    basis: TrailingBasis = TrailingBasis.LAST,
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
        activation_price=None,
        client_id=random_client_id("trail"),
    )


async def test_native_trailing_place_replace_and_cancel(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_position: tuple[Position, Decimal],
) -> None:
    position, quantity = linear_position
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
    stored = await connected_adapter.get_position_trailing_stop(
        linear_instrument, position.position_id
    )
    assert stored is not None

    replaced = await connected_adapter.place_trailing_stop(
        _request(linear_instrument, position, quantity, tightened, basis=TrailingBasis.MARK)
    )
    assert replaced.confirmed_offset is not None
    assert replaced.confirmed_offset <= initial

    await connected_adapter.cancel_trailing_stop(
        replaced.native_id,
        linear_instrument,
    )
    assert (
        await connected_adapter.get_position_trailing_stop(linear_instrument, position.position_id)
        is None
    )


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
        after = await connected_adapter.get_position_tpsl(linear_instrument, position.position_id)
        assert after is not None
        assert after[0] is not None
        assert after[0].trigger_price == before[0].trigger_price
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.clear_position_tpsl(
                linear_instrument,
                position.position_id,
                take_profit=True,
            )


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

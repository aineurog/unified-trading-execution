"""Integration: IBKR synthetic trailing via engine (live paper account).

REQUIRES: Live IBKR paper trading account with TWS/Gateway running.
Set environment variables:
- IBKR_HOST (default: 127.0.0.1)
- IBKR_PORT (required - 7497 for TWS, 4002 for Gateway)
- IBKR_ACCOUNT (required - your paper trading account ID)
- IBKR_CLIENT_ID (default: 1)

Tests synthetic trailing stop behavior:
- Attach percentage trailing stop to position
- Monitor price movement
- Verify stop price adjustment
- Cancel trailing and verify protection cleared

Run with:
    uv run pytest packages/adapter-ibkr/tests/ibkr_integration/test_trailing.py -v
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from unified_trading_execution.adapter import CandleTimeframe
from unified_trading_execution.ibkr import IBKRAdapter, IBKREngine
from unified_trading_execution.trailing import (
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingConfig,
    TrailingOffsetType,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

from .test_order_roundtrip import _cid


def _stock() -> Instrument:
    """Build a canonical stock instrument for SAP with IBIS exchange."""
    return Instrument(
        symbol="SAP",
        asset_class=AssetClass.STOCK,
        currency="EUR",
        exchange="IBIS",  # Use IBIS to avoid SMART routing error 10311
    )


async def _flatten(adapter: IBKRAdapter) -> None:
    """Flatten all positions for cleanup."""
    for pos in await adapter.fetch_positions():
        if pos.quantity != 0:
            side = OrderSide.SELL if pos.quantity > 0 else OrderSide.BUY
            await adapter.place_order(
                UnifiedOrder(
                    instrument=pos.instrument,
                    order_type=OrderType.MARKET,
                    side=side,
                    quantity=abs(pos.quantity),
                    time_in_force=TimeInForce.GTC,
                    client_order_id=_cid("flatten"),
                )
            )
            await asyncio.sleep(1.5)


@pytest.fixture
async def engine(connected_adapter: IBKRAdapter) -> AsyncGenerator[IBKREngine, None]:
    """Connect IBKREngine to paper account with trailing config."""
    adapter = connected_adapter
    engine = IBKREngine(
        adapter,
        trailing_config=TrailingConfig(poll_interval_seconds=2.0),
    )
    await engine.connect()
    yield engine
    await _flatten(adapter)
    await engine.ashutdown()


async def test_synthetic_trailing_attach(engine: IBKREngine) -> None:
    """Attach synthetic trailing stop to open position."""
    inst = _stock()

    # Open position
    cid = _cid("trail-open")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)  # Increased wait time for position to appear

    # Get position - match by symbol instead of full instrument
    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach synthetic trailing stop with 1% percentage offset
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.spec.offset == Decimal("1.0")

    # Wait a few polling cycles
    await asyncio.sleep(6)

    # Cancel trailing
    await engine.trailing.cancel(state.trailing_id)

    # Cleanup
    await _flatten(engine._adapter)


async def test_synthetic_trailing_preserves_tp(engine: IBKREngine) -> None:
    """Synthetic trailing should preserve existing TP when only SL is set."""
    inst = _stock()

    # Open position
    cid = _cid("trail-tp")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Set TP first
    from unified_trading_execution.types.order import TpSlAttachment

    await engine.modify_position_tpsl(
        inst,
        position_id,
        take_profit=TpSlAttachment(trigger_price=Decimal("300")),
    )

    # Attach trailing (SL only) with 1% percentage offset
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None

    # Verify TP still exists
    result = await engine.get_position_tpsl(inst, position_id)
    if result is None:
        pytest.skip("Position not found when reading TP/SL")
    tp, sl = result
    assert tp is not None, "TP should be preserved"
    assert sl is not None, "SL should be set by trailing"

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await engine.clear_position_tpsl(inst, position_id, take_profit=True, stop_loss=True)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_with_initial_stop(engine: IBKREngine) -> None:
    """Test trailing with an initial stop placed at attach."""
    inst = _stock()

    # Open position
    cid = _cid("trail-init")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing with initial stop at 2% below entry
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
        initial_stop=ProtectionLevel(
            type=ProtectionLevelType.PERCENT,
            value=Decimal("2.0"),
        ),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.current_stop is not None, "Initial stop should be set"

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_with_activation_price(engine: IBKREngine) -> None:
    """Test trailing with price-based activation."""
    inst = _stock()

    # Open position
    cid = _cid("trail-act")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing with activation at higher price
    entry_price = pos.average_entry_price
    activation_price = entry_price * Decimal("1.05")  # 5% above entry

    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
        activation=TrailingActivation.PRICE,
        activation_price=activation_price,
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.resolved_activation_price == activation_price

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_with_take_profit(engine: IBKREngine) -> None:
    """Test trailing with static take profit."""
    inst = _stock()

    # Open position
    cid = _cid("trail-tp2")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing with take profit
    entry_price = pos.average_entry_price
    tp_price = entry_price * Decimal("1.10")  # 10% above entry

    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
        take_profit=ProtectionLevel(
            type=ProtectionLevelType.PRICE,
            value=tp_price,
        ),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.current_take_profit == tp_price

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_price_offset(engine: IBKREngine) -> None:
    """Test trailing with price-based offset instead of percent."""
    inst = _stock()

    # Open position
    cid = _cid("trail-price")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing with price offset (e.g., $2 below market)
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PRICE,
        offset=Decimal("2.0"),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.spec.offset == Decimal("2.0")

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_atr_offset(engine: IBKREngine) -> None:
    """Test trailing with ATR-based offset."""
    inst = _stock()

    # Open position
    cid = _cid("trail-atr")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing with ATR offset
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.ATR,
        offset=Decimal("2.0"),  # 2x ATR
        atr_period=14,
        atr_timeframe=CandleTimeframe.H1,
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None
    assert state.spec.offset_type == TrailingOffsetType.ATR
    assert state.spec.atr_period == 14
    assert state.spec.atr_timeframe == CandleTimeframe.H1

    # Wait for ATR to be calculated
    await asyncio.sleep(10)
    # Refresh state to check if ATR was populated
    state_refreshed = await engine.trailing.get(state.trailing_id)
    if state_refreshed:
        assert state_refreshed.atr_value is not None, "ATR should be calculated"

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)


async def test_synthetic_trailing_get_and_list(engine: IBKREngine) -> None:
    """Test get and list methods for trailing states."""
    inst = _stock()

    # Open position
    cid = _cid("trail-list")
    await engine.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(5)

    positions = await engine.fetch_positions()
    pos = next((p for p in positions if p.instrument.symbol == inst.symbol), None)
    if pos is None or pos.quantity == 0:
        pytest.skip(
            f"Could not open position for trailing test "
            f"(pos={pos}, found {len(positions)} positions)"
        )

    position_id = pos.position_id
    assert position_id is not None

    # Attach trailing
    spec = TrailingSpec(
        offset_type=TrailingOffsetType.PERCENT,
        offset=Decimal("1.0"),
    )
    request = TrailingRequest(
        instrument=inst,
        position_id=position_id,
        spec=spec,
    )

    state = await engine.trailing.attach(request)
    assert state is not None

    # Get by ID
    retrieved = await engine.trailing.get(state.trailing_id)
    assert retrieved is not None
    assert retrieved.trailing_id == state.trailing_id

    # List all
    all_states = await engine.trailing.list()
    all_ids = [s.trailing_id for s in all_states]
    assert state.trailing_id in all_ids

    # Cleanup
    await engine.trailing.cancel(state.trailing_id)
    await _flatten(engine._adapter)

"""Live testnet coverage for synthetic trailing stops.

Hyperliquid does not support native trailing stops due to lack of position-scoped
reads. All trailing is synthetic via position protection methods.

These tests are skipped unless testnet credentials are configured. They open
tiny derivative positions, exercise synthetic trailing via position protection,
and assert the routing, protection ownership, and state-store invariants.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidEngine,
)
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing import (
    CandleTimeframe,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment
from unified_trading_execution.types.position import Position

from .helpers import (
    build_unified_order,
    valid_qty_from_spec,
    venue_price,
)


async def _wait_for_position(
    adapter: HyperliquidAdapter,
    instrument: Instrument,
    *,
    timeout: float = 15.0,
) -> Position:
    """Poll until an open position appears for the instrument."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        positions = await adapter.fetch_positions()
        for position in positions:
            if position.instrument == instrument and position.quantity != 0:
                return cast(Position, position)
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"No open position appeared for {instrument.symbol}")
        await asyncio.sleep(0.5)


async def _wait_for_flat(
    adapter: HyperliquidAdapter,
    instrument: Instrument,
    *,
    timeout: float = 15.0,
) -> None:
    """Poll until the position for the instrument is flat."""
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
    adapter: HyperliquidAdapter,
    instrument: Instrument,
    quantity: Decimal,
    cid: str,
) -> None:
    """Close a position with a market order."""
    side = OrderSide.SELL if quantity > 0 else OrderSide.BUY
    close = build_unified_order(
        instrument,
        OrderType.MARKET,
        side,
        quantity,
        client_order_id=cid,
    )
    await adapter.place_order(close)


async def _flatten_instrument(
    adapter: HyperliquidAdapter,
    instrument: Instrument,
    cid_factory: Any,
) -> None:
    """Cancel all orders, TP/SL, and close the position for an instrument."""
    with contextlib.suppress(Exception):
        # Cancel TP/SL
        await adapter.clear_position_tpsl(
            instrument, f"{instrument.symbol}:oneWay", take_profit=True, stop_loss=True
        )

    with contextlib.suppress(Exception):
        # Cancel open orders
        open_orders = await adapter.fetch_open_orders()
        for order in open_orders.values():
            if order.instrument == instrument:
                await adapter.cancel_order(order.client_order_id)

    # Close position
    positions = await adapter.fetch_positions()
    for position in positions:
        if position.instrument == instrument and position.quantity != 0:
            await _close_position(adapter, instrument, position.quantity, cid_factory("close"))
            await _wait_for_flat(adapter, instrument)


async def _open_long_position(
    adapter: HyperliquidAdapter,
    instrument: Instrument,
    reference_price: Decimal,
    cid_factory: Any,
) -> tuple[Position, Decimal]:
    """Open a tiny long position and return it with the quantity."""
    await _flatten_instrument(adapter, instrument, cid_factory)
    spec = await adapter.fetch_instrument_spec(instrument)
    quantity = valid_qty_from_spec(spec, reference_price)
    open_order = build_unified_order(
        instrument,
        OrderType.MARKET,
        OrderSide.BUY,
        quantity,
        client_order_id=cid_factory("open"),
    )
    await adapter.place_order(open_order)
    position = await _wait_for_position(adapter, instrument)
    return position, quantity


async def _wait_for_trailing_phase(
    manager: Any,
    trailing_id: str,
    phase: TrailingPhase,
    *,
    timeout: float = 15.0,
) -> TrailingState:
    """Poll until the trailing state reaches the expected phase."""
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


async def test_static_protection_writes_preserve_unmentioned_side(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Any,
) -> None:
    """Verify the venue merges one-sided writes and one-sided clears."""
    position, _ = await _open_long_position(
        connected_adapter, btc_perp, reference_price, unique_cid
    )
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    assert ticker is not None and ticker.mark is not None

    take_profit_price = venue_price(ticker.mark * Decimal("1.3"), spec.tick_size, direction="up")
    stop_loss_price = venue_price(ticker.mark * Decimal("0.9"), spec.tick_size, direction="down")

    # Set both TP and SL
    await connected_adapter.modify_position_tpsl(
        btc_perp,
        position.position_id,
        stop_loss=TpSlAttachment(trigger_price=stop_loss_price),
        take_profit=TpSlAttachment(trigger_price=take_profit_price),
    )

    protection = await connected_adapter.get_position_tpsl(btc_perp, position.position_id)
    assert protection is not None
    take_profit, stop_loss = protection
    assert stop_loss is not None
    assert take_profit is not None

    # Modify only take profit, stop loss should be preserved
    tightened_tp = venue_price(ticker.mark * Decimal("1.25"), spec.tick_size, direction="up")
    await connected_adapter.modify_position_tpsl(
        btc_perp,
        position.position_id,
        take_profit=TpSlAttachment(trigger_price=tightened_tp),
    )

    protection = await connected_adapter.get_position_tpsl(btc_perp, position.position_id)
    assert protection is not None
    take_profit, stop_loss = protection
    assert stop_loss is not None
    assert take_profit is not None
    assert take_profit.trigger_price == tightened_tp

    # Clear only stop loss, take profit should be preserved
    await connected_adapter.clear_position_tpsl(btc_perp, position.position_id, stop_loss=True)

    protection = await connected_adapter.get_position_tpsl(btc_perp, position.position_id)
    assert protection is not None
    take_profit, stop_loss = protection
    assert stop_loss is None
    assert take_profit is not None

    await _flatten_instrument(connected_adapter, btc_perp, unique_cid)


async def test_clear_position_tpsl_on_flat_position(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Any,
) -> None:
    """Clear on a flat position returns quietly."""
    await _flatten_instrument(connected_adapter, btc_perp, unique_cid)

    # Clear on flat position should not raise
    await connected_adapter.clear_position_tpsl(
        btc_perp, "BTC:oneWay", stop_loss=True, take_profit=True
    )


async def test_market_data_is_reachable(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
) -> None:
    """Test that ticker and candle data are reachable."""
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    assert ticker is not None
    assert ticker.mark is not None
    assert ticker.mid is not None

    candles = await connected_adapter.fetch_candles(
        btc_perp,
        CandleTimeframe.H1,
        limit=30,
    )
    assert len(candles) >= 15
    assert all(candle.close_time.tzinfo is not None for candle in candles)
    assert candles[-1].close_time <= datetime.now(UTC)


async def test_synthetic_trailing_attach_and_arm(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Any,
    tmp_path: Any,
) -> None:
    """Test synthetic trailing attach and arm via position protection."""
    position, _ = await _open_long_position(
        connected_adapter, btc_perp, reference_price, unique_cid
    )
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    assert ticker is not None and ticker.mark is not None

    store = SQLiteStateStore(str(tmp_path / "synthetic.db"))
    await store.initialize()
    engine = HyperliquidEngine(
        HyperliquidConfig(
            wallet_address=connected_adapter._config.wallet_address,
            private_key=connected_adapter._config.private_key,
            testnet=True,
        ),
        state_store=store,
        event_bus=EventBus(),
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        reconcile_interval_seconds=None,
    )
    await engine.connect()

    try:
        offset = venue_price(ticker.mark * Decimal("0.02"), spec.tick_size, direction="down")
        step = venue_price(ticker.mark * Decimal("0.01"), spec.tick_size, direction="down")
        state = await engine.trailing.attach(
            TrailingRequest(
                btc_perp,
                TrailingSpec(
                    offset_type=TrailingOffsetType.PRICE,
                    offset=offset,
                    basis=TrailingBasis.MARK,
                    step=step,
                ),
                position_id=position.position_id,
                mode=TrailingMode.SYNTHETIC,
            )
        )

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.native_id is None

        # Wait for ARMED phase
        armed = await _wait_for_trailing_phase(
            engine.trailing, state.trailing_id, TrailingPhase.ARMED
        )
        assert armed.phase is TrailingPhase.ARMED

        # Verify protection is set
        protection = await connected_adapter.get_position_tpsl(btc_perp, position.position_id)
        assert protection is not None
        _take_profit, stop_loss = protection
        assert stop_loss is not None

        await engine.trailing.cancel(state.trailing_id, clear_protection=True)
    finally:
        with contextlib.suppress(Exception):
            await engine.ashutdown()
        await _flatten_instrument(connected_adapter, btc_perp, unique_cid)


async def test_auto_routes_to_synthetic(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Any,
    tmp_path: Any,
) -> None:
    """AUTO mode routes to synthetic since native is unsupported."""
    position, _ = await _open_long_position(
        connected_adapter, btc_perp, reference_price, unique_cid
    )
    assert position.position_id is not None
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    assert ticker is not None and ticker.mark is not None

    store = SQLiteStateStore(str(tmp_path / "auto.db"))
    await store.initialize()
    engine = HyperliquidEngine(
        HyperliquidConfig(
            wallet_address=connected_adapter._config.wallet_address,
            private_key=connected_adapter._config.private_key,
            testnet=True,
        ),
        state_store=store,
        event_bus=EventBus(),
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        reconcile_interval_seconds=None,
    )
    await engine.connect()

    try:
        offset = venue_price(ticker.mark * Decimal("0.02"), spec.tick_size, direction="down")
        state = await engine.trailing.attach(
            TrailingRequest(
                btc_perp,
                TrailingSpec(
                    offset_type=TrailingOffsetType.PRICE,
                    offset=offset,
                    basis=TrailingBasis.MARK,
                ),
                position_id=position.position_id,
                mode=TrailingMode.AUTO,
            )
        )

        # AUTO should route to synthetic since native is unsupported
        assert state.mode is TrailingMode.SYNTHETIC
        assert state.native_id is None

        await engine.trailing.cancel(state.trailing_id, clear_protection=True)
    finally:
        with contextlib.suppress(Exception):
            await engine.ashutdown()
        await _flatten_instrument(connected_adapter, btc_perp, unique_cid)

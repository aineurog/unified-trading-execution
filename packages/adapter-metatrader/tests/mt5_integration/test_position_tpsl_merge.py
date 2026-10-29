"""Position TP/SL merge integration tests.

MT5 treats an omitted ``tp``/``sl`` field on ``TRADE_ACTION_SLTP`` as zero,
and zero means "no level".  These tests prove that modifying or clearing one
side through ``modify_position_tpsl`` / ``clear_position_tpsl`` leaves the other
side untouched on a live demo account — the regression guarded by the unit
tests.

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
The instrument under test is EUR/USD; the broker symbol defaults to
``EURUSD`` and can be overridden via ``MT5_SYMBOL`` (e.g. ``EURUSD.m``).
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal

import pytest

from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderStatus, OrderType
from unified_trading_execution.types.instrument import Instrument
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


async def _live_quotes(adapter: MT5Adapter) -> tuple[Decimal, Decimal]:
    """Current (bid, ask) for the instrument under test."""
    ticker = await adapter.fetch_ticker(_INSTRUMENT)
    if ticker is None or ticker.bid is None or ticker.ask is None:
        pytest.fail(f"no market quote for {_BROKER_SYMBOL}")
    return ticker.bid, ticker.ask


def _find_position(positions: list[Position], instrument: Instrument) -> Position | None:
    """Return the leg for *instrument*, matching on identity fields."""
    for position in positions:
        if (
            position.instrument.symbol == instrument.symbol
            and position.instrument.asset_class == instrument.asset_class
        ):
            return position
    return None


async def _wait_for_position(
    adapter: MT5Adapter, instrument: Instrument, *, timeout: float = 10.0
) -> Position:
    """Poll ``fetch_positions`` until the freshly filled leg appears."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        position = _find_position(await adapter.fetch_positions(), instrument)
        if position is not None and position.position_id:
            return position
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(f"position for {instrument.symbol} did not appear")
        await asyncio.sleep(0.25)


async def test_modify_one_side_preserves_the_other(connected_adapter: MT5Adapter) -> None:
    """Changing SL leaves TP unchanged, and changing TP leaves SL unchanged."""
    _, ask = await _live_quotes(connected_adapter)
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    take_profit = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("1.002"))
    stop_loss = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.998"))
    try:
        result = await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.BUY,
                qty,
                client_order_id=random_client_id("merge"),
                take_profit=TpSlAttachment(trigger_price=take_profit),
                stop_loss=TpSlAttachment(trigger_price=stop_loss),
            )
        )
        assert result.status == OrderStatus.FILLED

        position = await _wait_for_position(connected_adapter, _INSTRUMENT)
        assert position.position_id is not None
        position_id = position.position_id

        initial = await connected_adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert initial is not None
        initial_tp, initial_sl = initial
        assert initial_tp is not None
        assert initial_sl is not None

        new_sl = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.996"))
        await connected_adapter.modify_position_tpsl(
            _INSTRUMENT, position_id, stop_loss=TpSlAttachment(trigger_price=new_sl)
        )

        after_sl = await connected_adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert after_sl is not None
        after_sl_tp, after_sl_sl = after_sl
        assert after_sl_tp is not None
        assert after_sl_tp.trigger_price == initial_tp.trigger_price
        assert after_sl_sl is not None
        assert after_sl_sl.trigger_price == new_sl

        new_tp = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("1.004"))
        await connected_adapter.modify_position_tpsl(
            _INSTRUMENT, position_id, take_profit=TpSlAttachment(trigger_price=new_tp)
        )

        after_tp = await connected_adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert after_tp is not None
        after_tp_tp, after_tp_sl = after_tp
        assert after_tp_tp is not None
        assert after_tp_tp.trigger_price == new_tp
        assert after_tp_sl is not None
        assert after_tp_sl.trigger_price == new_sl
    finally:
        await cleanup_adapter(connected_adapter)


async def test_clear_one_side_preserves_the_other(connected_adapter: MT5Adapter) -> None:
    """Clearing SL leaves TP, and clearing TP leaves SL unset on the venue."""
    _, ask = await _live_quotes(connected_adapter)
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    take_profit = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("1.002"))
    stop_loss = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.998"))
    try:
        result = await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.BUY,
                qty,
                client_order_id=random_client_id("clear"),
                take_profit=TpSlAttachment(trigger_price=take_profit),
                stop_loss=TpSlAttachment(trigger_price=stop_loss),
            )
        )
        assert result.status == OrderStatus.FILLED

        position = await _wait_for_position(connected_adapter, _INSTRUMENT)
        assert position.position_id is not None
        position_id = position.position_id

        await connected_adapter.clear_position_tpsl(_INSTRUMENT, position_id, stop_loss=True)

        after_sl = await connected_adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert after_sl is not None
        after_sl_tp, after_sl_sl = after_sl
        assert after_sl_tp is not None
        assert after_sl_tp.trigger_price == take_profit
        assert after_sl_sl is None

        await connected_adapter.clear_position_tpsl(_INSTRUMENT, position_id, take_profit=True)

        after_both = await connected_adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert after_both == (None, None)
    finally:
        await cleanup_adapter(connected_adapter)

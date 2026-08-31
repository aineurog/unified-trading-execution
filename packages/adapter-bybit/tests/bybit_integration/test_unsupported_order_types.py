"""Adapter-level pre-network rejection tests — UnsupportedOrderTypeError.

These verify ``build_place_order_payload`` / ``build_amend_payload`` raise
``UnsupportedOrderTypeError`` locally, before any Bybit API call is dispatched
for the rejected order.  A connected adapter is used so the instrument registry
is populated and a real follow-up order can prove the adapter's internal state
was not corrupted by the rejected attempt.

Because these use the ``connected_adapter`` fixture they skip (never fail) when
testnet credentials are missing.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import UnsupportedOrderTypeError
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

from .helpers import build_unified_order, make_instrument, random_client_id

_SPOT: Instrument = make_instrument("BTC", "USDT", AssetClass.SPOT)
_LINEAR: Instrument = make_instrument("BTC", "USDT", AssetClass.FUTURES, currency="USDT")


def _order(instrument: Instrument, order_type: OrderType, **kwargs) -> UnifiedOrder:
    return build_unified_order(
        instrument,
        order_type,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        **kwargs,
    )


async def _assert_rejected_state(adapter: BybitAdapter, client_id: str | None) -> None:
    """Assert the rejected attempt corrupted nothing in the adapter state."""
    assert client_id not in adapter._order_refs
    # A follow-up valid order on a different instrument still places.  A resting
    # LIMIT (well below market) is used so the order stays open and cancellable —
    # a MARKET order would fill instantly and ``cancel_order`` would race it.
    instrument = _LINEAR
    order = build_unified_order(
        instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("integrity"),
        price=Decimal("30000"),
    )
    result = await adapter.place_order(order)
    assert result.platform_order_id is not None
    await adapter.cancel_order(order.client_order_id)


async def test_stop_on_spot_rejected(connected_adapter: BybitAdapter) -> None:
    order = _order(_SPOT, OrderType.STOP, stop_price=Decimal("50000"))
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_stop_limit_on_spot_rejected(connected_adapter: BybitAdapter) -> None:
    order = _order(
        _SPOT,
        OrderType.STOP_LIMIT,
        price=Decimal("50000"),
        stop_price=Decimal("49000"),
    )
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_reduce_only_on_spot_rejected(connected_adapter: BybitAdapter) -> None:
    order = build_unified_order(
        _SPOT,
        OrderType.MARKET,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        reduce_only=True,
    )
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_day_tif_rejected(connected_adapter: BybitAdapter) -> None:
    order = build_unified_order(
        _LINEAR,
        OrderType.LIMIT,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        price=Decimal("30000"),
        time_in_force=TimeInForce.DAY,
    )
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_reduce_only_with_tp_sl_rejected(connected_adapter: BybitAdapter) -> None:
    from unified_trading_execution.types.order import TpSlAttachment

    order = build_unified_order(
        _LINEAR,
        OrderType.MARKET,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        reduce_only=True,
        take_profit=TpSlAttachment(trigger_price=Decimal("50000")),
    )
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_tp_sl_on_spot_non_limit_rejected(connected_adapter: BybitAdapter) -> None:
    from unified_trading_execution.types.order import TpSlAttachment

    order = build_unified_order(
        _SPOT,
        OrderType.MARKET,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        take_profit=TpSlAttachment(trigger_price=Decimal("50000")),
    )
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _assert_rejected_state(connected_adapter, order.client_order_id)


async def test_tp_sl_modification_on_spot_rejected(connected_adapter: BybitAdapter) -> None:
    from unified_trading_execution.types.order import OrderModification, TpSlAttachment

    # A valid resting spot limit to target for modification.
    instrument = _SPOT
    placed = build_unified_order(
        instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("unsupported"),
        price=Decimal("10000"),
    )
    await connected_adapter.place_order(placed)
    try:
        modification = OrderModification(
            client_order_id=placed.client_order_id,
            take_profit=TpSlAttachment(trigger_price=Decimal("12000")),
        )
        with pytest.raises(UnsupportedOrderTypeError):
            await connected_adapter.modify_order(modification)
    finally:
        await connected_adapter.cancel_order(placed.client_order_id)

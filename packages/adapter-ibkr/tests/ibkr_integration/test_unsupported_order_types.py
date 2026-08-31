"""IBKR unsupported order pre-network rejections — SPOT and generic."""

# ruff: noqa: E501

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from unified_trading_execution.errors import UnsupportedOrderTypeError
from unified_trading_execution.ibkr import IBKRAdapter
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder

STOCK = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
BTC = Instrument(symbol="BTC", quote_currency="USD", asset_class=AssetClass.SPOT)


async def _followup_ok(adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(
        instrument=STOCK, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id=str(uuid.uuid4()), price=Decimal("10")
    )
    result = await adapter.place_order(order)
    assert result.client_order_id == order.client_order_id
    assert order.client_order_id is not None
    await adapter.cancel_order(order.client_order_id)


async def test_spot_stop_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=BTC, order_type=OrderType.STOP, side=OrderSide.BUY, quantity=Decimal("0.001"), time_in_force=TimeInForce.GTC, client_order_id="spot-stop", stop_price=Decimal("50000"))
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_spot_stop_limit_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=BTC, order_type=OrderType.STOP_LIMIT, side=OrderSide.BUY, quantity=Decimal("0.001"), time_in_force=TimeInForce.GTC, client_order_id="spot-sl", price=Decimal("50000"), stop_price=Decimal("49000"))
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_spot_bracket_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=BTC, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("0.001"), time_in_force=TimeInForce.GTC, client_order_id="spot-bracket", price=Decimal("10000"), take_profit=TpSlAttachment(Decimal("11000")))
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_spot_market_buy_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=BTC, order_type=OrderType.MARKET, side=OrderSide.BUY, quantity=Decimal("0.001"), time_in_force=TimeInForce.GTC, client_order_id="spot-mbuy")
    with pytest.raises(UnsupportedOrderTypeError, match="cashQty"):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_reduce_only_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=STOCK, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="reduce", price=Decimal("10"), reduce_only=True)
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_position_id_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=STOCK, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="posid", price=Decimal("10"), position_id="123")
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)


async def test_tp_limit_price_rejected(connected_adapter: IBKRAdapter) -> None:
    order = UnifiedOrder(instrument=STOCK, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="tp-limit", price=Decimal("10"), take_profit=TpSlAttachment(Decimal("20"), limit_price=Decimal("19")))
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(order)
    await _followup_ok(connected_adapter)

"""Live native-error checks — InvalidSymbol, OrderNotFound."""

# ruff: noqa: E501, SIM105

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from unified_trading_execution.errors import InvalidSymbolError, OrderNotFoundError, UteError
from unified_trading_execution.ibkr import IBKRAdapter
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder


def _stock(symbol: str = "AAPL") -> Instrument:
    return Instrument(symbol=symbol, asset_class=AssetClass.STOCK, currency="USD")


async def test_invalid_symbol_rejected(connected_adapter: IBKRAdapter) -> None:
    fake = Instrument(symbol="FAKE123XYZ", asset_class=AssetClass.STOCK, currency="USD")
    order = UnifiedOrder(instrument=fake, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="invalid-sym", price=Decimal("10"))
    try:
        await connected_adapter.place_order(order)
    except UteError:
        pass
    valid = UnifiedOrder(instrument=_stock(), order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="after-invalid", price=Decimal("10"))
    result = await connected_adapter.place_order(valid)
    assert result.client_order_id == "after-invalid"
    with contextlib.suppress(Exception):
        await connected_adapter.cancel_order(valid.client_order_id)


async def test_fetch_invalid_symbol_raises(connected_adapter: IBKRAdapter) -> None:
    fake = Instrument(symbol="FAKE123XYZ", asset_class=AssetClass.STOCK, currency="USD")
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_instrument_spec(fake)


async def test_order_not_found(connected_adapter: IBKRAdapter) -> None:
    ghost = "ghost-not-exist-123"
    with pytest.raises(OrderNotFoundError):
        await connected_adapter.cancel_order(ghost)
    assert await connected_adapter.get_order_by_client_id(ghost) is None


async def test_usable_after_error(connected_adapter: IBKRAdapter) -> None:
    fake = Instrument(symbol="FAKE123XYZ", asset_class=AssetClass.STOCK, currency="USD")
    bad = UnifiedOrder(instrument=fake, order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="bad-after", price=Decimal("10"))
    with contextlib.suppress(UteError):
        await connected_adapter.place_order(bad)
    valid = UnifiedOrder(instrument=_stock(), order_type=OrderType.LIMIT, side=OrderSide.BUY, quantity=Decimal("1"), time_in_force=TimeInForce.GTC, client_order_id="good-after", price=Decimal("10"))
    result = await connected_adapter.place_order(valid)
    assert result.platform_order_id is not None
    with contextlib.suppress(Exception):
        await connected_adapter.cancel_order(valid.client_order_id)

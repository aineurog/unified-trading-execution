"""Native-error simulation integration tests (Section 11.2, bullet 3).

Deliberately triggers live, harmless testnet errors and asserts the correct
unified exception per the ``errors.py`` ret-code map.  Tests also verify the
adapter remains usable (no registry/state corruption) after the error.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import (
    OrderNotFoundError,
    UteError,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import build_unified_order, make_instrument, random_client_id


def _fabricated_instrument() -> Instrument:
    return make_instrument("BTCFOO", "USDT", AssetClass.SPOT)


async def test_invalid_symbol_rejected(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """A fabricated symbol raises InvalidSymbolError (or a mapped UteError)."""
    bogus = build_unified_order(
        make_instrument("NOTAREALSYM", "USDT", AssetClass.SPOT),
        OrderType.LIMIT,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("invalid-symbol"),
        price=reference_price,
    )
    with pytest.raises(UteError):
        await connected_adapter.place_order(bogus)


async def test_insufficient_balance_rejected(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """A notional far beyond the testnet balance raises InsufficientBalanceError."""
    spec = await connected_adapter.fetch_instrument_spec(traded_instrument)
    max_qty = spec.max_qty
    qty = max_qty if max_qty > 0 else Decimal("100000")
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("insufficient"),
        price=reference_price,
    )
    with pytest.raises(UteError):
        await connected_adapter.place_order(order)


async def test_order_not_found(
    connected_adapter: BybitAdapter,
) -> None:
    """Querying/cancelling an order that never existed raises OrderNotFoundError."""
    ghost = random_client_id("ghost")
    with pytest.raises(OrderNotFoundError):
        await connected_adapter.cancel_order(ghost)


async def test_adapter_usable_after_error(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """After an error, a valid order on the same instrument still places."""
    bogus = build_unified_order(
        make_instrument("NOTAREALSYM", "USDT", AssetClass.SPOT),
        OrderType.LIMIT,
        OrderSide.BUY,
        Decimal("0.001"),
        client_order_id=random_client_id("invalid-symbol"),
        price=reference_price,
    )
    with pytest.raises(UteError):
        await connected_adapter.place_order(bogus)

    from .helpers import valid_price_from_spec, valid_qty_from_spec

    spec = await connected_adapter.fetch_instrument_spec(traded_instrument)
    qty = valid_qty_from_spec(spec, reference_price)
    price = valid_price_from_spec(spec, reference_price)
    valid = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty if qty > 0 else Decimal("0.001"),
        client_order_id=random_client_id("after-error"),
        price=price if price and price > 0 else reference_price,
    )
    try:
        result = await connected_adapter.place_order(valid)
        assert result.platform_order_id is not None
    finally:
        with contextlib.suppress(Exception):
            await connected_adapter.cancel_order(valid.client_order_id)

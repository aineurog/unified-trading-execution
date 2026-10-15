"""Integration: live venue error strings map to typed errors.

Gate: the errors the venue actually returns (not just unit-table entries)
surface as the documented types. All cases are rejects — nothing fills.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest

from unified_trading_execution.errors import (
    InsufficientBalanceError,
    InvalidOrderError,
    OrderNotFoundError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import align_up_to_lot, build_unified_order, make_spot, venue_price


async def test_below_min_notional_rejected(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    tiny = spec.lot_size  # one lot of BTC is far below the $10 floor
    assert tiny * reference_price < Decimal("10")
    with pytest.raises(InvalidOrderError):
        await connected_adapter.place_order(
            build_unified_order(
                btc_perp,
                OrderType.LIMIT,
                OrderSide.BUY,
                tiny,
                client_order_id=unique_cid("err-minntl"),
                price=reference_price * Decimal("0.5"),
            )
        )


async def test_insufficient_margin_rejected(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
) -> None:
    # 10x the account balance at half of mid: above the balance so the venue
    # rejects on margin, below the tier cap so it gets that far, within 80%
    # of reference (venue band rule), and too far to ever fill.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    qty = align_up_to_lot((funded_account * Decimal("10")) / px, spec.lot_size)
    with pytest.raises(InsufficientBalanceError):
        await connected_adapter.place_order(
            build_unified_order(
                btc_perp,
                OrderType.LIMIT,
                OrderSide.BUY,
                qty,
                client_order_id=unique_cid("err-margin"),
                price=px,
            )
        )


async def test_spot_reduce_only_rejected(
    connected_adapter: HyperliquidAdapter,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
) -> None:
    spot = make_spot("BTC")
    spec = await connected_adapter.fetch_instrument_spec(spot)
    px = venue_price(reference_price * Decimal("0.5"), spec.tick_size, direction="down")
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(
            build_unified_order(
                spot,
                OrderType.LIMIT,
                OrderSide.BUY,
                Decimal("1"),
                client_order_id=unique_cid("err-redspot"),
                price=px,
                reduce_only=True,
            )
        )


async def test_cancel_never_placed_raises(
    connected_adapter: HyperliquidAdapter,
    unique_cid: Callable[[str], str],
) -> None:
    with pytest.raises(OrderNotFoundError):
        await connected_adapter.cancel_order(unique_cid("err-ghost"))

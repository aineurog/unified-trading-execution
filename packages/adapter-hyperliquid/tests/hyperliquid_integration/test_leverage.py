"""Integration: leverage/margin intents against live legs.

Gate: set → venue-verify → reconcile-noop; block guard refuses changes over an
open leg; over-cap raises typed errors; restore leaves 1x cross, no intents.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.hyperliquid import HyperliquidAdapter, MarginMode
from unified_trading_execution.hyperliquid.errors import LeverageExceedsMaxError
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument

from .helpers import (
    build_unified_order,
    restore_defaults,
    valid_qty_from_spec,
    wait_for_position,
)


async def test_set_verify_reconcile_noop(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    await connected_adapter.set_leverage(btc_perp, leverage=5)
    await connected_adapter.set_margin_mode(btc_perp, MarginMode.ISOLATED)

    # The venue reports no leverage entry while flat — open a leg to verify.
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    result = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("lev-verify"),
            time_in_force=TimeInForce.IOC,
        )
    )
    assert result.filled_quantity == qty
    await wait_for_position(connected_adapter, "BTC")

    assert await connected_adapter.get_leverage(btc_perp) == (5, False)
    assert await connected_adapter.get_margin_mode(btc_perp) == MarginMode.ISOLATED

    await connected_adapter.reconcile_user_intent()  # no drift → no-op, no raise

    await restore_defaults(connected_adapter, btc_perp)
    assert await connected_adapter.fetch_positions() == []


async def test_block_guard_refuses_change_over_open_leg(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    result = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("lev-guard"),
            time_in_force=TimeInForce.IOC,
        )
    )
    assert result.filled_quantity == qty
    await wait_for_position(connected_adapter, "BTC")

    with pytest.raises(PlatformError):
        await connected_adapter.set_leverage(btc_perp, leverage=5)

    await restore_defaults(connected_adapter, btc_perp)


async def test_over_cap_raises_typed_error(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    flattened_book: None,
) -> None:
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    assert spec.max_leverage is not None
    with pytest.raises(LeverageExceedsMaxError):
        await connected_adapter.set_leverage(btc_perp, leverage=int(spec.max_leverage) + 10)


async def test_isolated_top_up_roundtrip(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    await connected_adapter.set_leverage(btc_perp, leverage=5)
    await connected_adapter.set_margin_mode(btc_perp, MarginMode.ISOLATED)
    spec = await connected_adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    result = await connected_adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=unique_cid("lev-topup"),
            time_in_force=TimeInForce.IOC,
        )
    )
    assert result.filled_quantity == qty
    await wait_for_position(connected_adapter, "BTC")

    await connected_adapter.top_up_isolated_margin(btc_perp, amount_usdc=Decimal("5"))
    await restore_defaults(connected_adapter, btc_perp)

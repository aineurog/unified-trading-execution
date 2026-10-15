"""Integration: whole-position TP/SL attach/read/detach with real legs.

Gate: attach → read-back → detach roundtrip, deterministic construction
(one leg per side, reduce-only, ``position:``-namespace cloids — the
OPEN_ISSUES #5 lock-in), restart-recover across adapter instances.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from decimal import Decimal

import pytest

from unified_trading_execution.errors import OrderNotFoundError
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidConfig,
)
from unified_trading_execution.hyperliquid.orders import position_tpsl_cloid
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment

from .helpers import (
    build_unified_order,
    restore_defaults,
    valid_qty_from_spec,
    venue_price,
    wait_for_position,
)


async def _open_long(
    adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    cid: str,
) -> None:
    spec = await adapter.fetch_instrument_spec(btc_perp)
    qty = valid_qty_from_spec(spec, reference_price)
    result = await adapter.place_order(
        build_unified_order(
            btc_perp,
            OrderType.MARKET,
            OrderSide.BUY,
            qty,
            client_order_id=cid,
            time_in_force=TimeInForce.IOC,
        )
    )
    assert result.filled_quantity == qty
    await wait_for_position(adapter, "BTC")


async def test_attach_read_detach_roundtrip(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    await _open_long(connected_adapter, btc_perp, reference_price, unique_cid("tpsl-long"))
    tp = venue_price(reference_price * Decimal("1.5"), Decimal("0.1"), direction="up")
    sl = venue_price(reference_price * Decimal("0.5"), Decimal("0.1"), direction="down")

    await connected_adapter.modify_position_tpsl(
        btc_perp,
        "BTC:oneWay",
        take_profit=TpSlAttachment(trigger_price=tp),
        stop_loss=TpSlAttachment(trigger_price=sl),
    )
    got = await connected_adapter.get_position_tpsl(btc_perp, "BTC:oneWay")
    assert got is not None
    assert got[0] is not None and got[0].trigger_price == tp
    assert got[1] is not None and got[1].trigger_price == sl

    # Detach = cancel both legs by their deterministic cloid keys.
    opens = await connected_adapter.fetch_open_orders()
    tp_cloid = position_tpsl_cloid("BTC:oneWay", "take_profit")
    sl_cloid = position_tpsl_cloid("BTC:oneWay", "stop_loss")
    assert tp_cloid in opens and sl_cloid in opens
    await connected_adapter.cancel_order(tp_cloid)
    await connected_adapter.cancel_order(sl_cloid)
    got_after = await connected_adapter.get_position_tpsl(btc_perp, "BTC:oneWay")
    assert got_after is None or (got_after[0] is None and got_after[1] is None)


async def test_construction_is_one_leg_per_side(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
) -> None:
    """OPEN_ISSUES #5: assert OUR construction, not venue invariants."""
    await _open_long(connected_adapter, btc_perp, reference_price, unique_cid("tpsl-oneleg"))
    tp = venue_price(reference_price * Decimal("1.5"), Decimal("0.1"), direction="up")
    sl = venue_price(reference_price * Decimal("0.5"), Decimal("0.1"), direction="down")
    await connected_adapter.modify_position_tpsl(
        btc_perp,
        "BTC:oneWay",
        take_profit=TpSlAttachment(trigger_price=tp),
        stop_loss=TpSlAttachment(trigger_price=sl),
    )
    opens = await connected_adapter.fetch_open_orders()
    tp_key = position_tpsl_cloid("BTC:oneWay", "take_profit")
    sl_key = position_tpsl_cloid("BTC:oneWay", "stop_loss")
    assert [k for k in opens if k == tp_key] == [tp_key]
    assert [k for k in opens if k == sl_key] == [sl_key]
    # Deterministic cloids: same inputs always mint the same leg ids.
    assert position_tpsl_cloid("BTC:oneWay", "take_profit") == position_tpsl_cloid(
        "BTC:oneWay", "take_profit"
    )


async def test_modify_without_position_raises(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    reference_price: Decimal,
    flattened_book: None,
) -> None:
    with pytest.raises(OrderNotFoundError):
        await connected_adapter.modify_position_tpsl(
            btc_perp,
            "BTC:oneWay",
            take_profit=TpSlAttachment(trigger_price=reference_price),
        )


async def test_recover_rested_legs_after_restart(
    connected_adapter: HyperliquidAdapter,
    hyperliquid_config: HyperliquidConfig,
    btc_perp: Instrument,
    reference_price: Decimal,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    flattened_book: None,
    event_bus: EventBus,
) -> None:
    await _open_long(connected_adapter, btc_perp, reference_price, unique_cid("tpsl-restart"))
    tp = venue_price(reference_price * Decimal("1.5"), Decimal("0.1"), direction="up")
    await connected_adapter.modify_position_tpsl(
        btc_perp, "BTC:oneWay", take_profit=TpSlAttachment(trigger_price=tp)
    )
    # Fresh instance, cold oid index: rested legs must still resolve.
    fresh = HyperliquidAdapter(hyperliquid_config, event_bus=event_bus)
    await fresh.connect()
    try:
        got = await fresh.get_position_tpsl(btc_perp, "BTC:oneWay")
        assert got is not None and got[0] is not None
        assert got[0].trigger_price == tp
    finally:
        with contextlib.suppress(Exception):
            await fresh.disconnect()
    await restore_defaults(connected_adapter, btc_perp)

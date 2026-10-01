"""Integration: full user journey through the engine (testnet).

Capstone — mirrors not_push/hyperliquid/user_acceptance.py as committed,
rerunnable assertions: connect+streams, reads, LIMIT lifecycle, intent before
position, MARKET fill, position TP/SL, reconcile, restore-flat, sync engine.
Ends flat with zero opens and default venue state, or fails trying.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.hyperliquid import (
    HyperliquidEngine,
    MarginMode,
    SyncHyperliquidEngine,
)
from unified_trading_execution.types.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder

from .helpers import (
    flatten_all,
    restore_defaults,
    valid_qty_from_spec,
    venue_price,
    wait_for_position,
)


async def test_engine_roundtrip(
    connected_engine: HyperliquidEngine,
    btc_perp: Instrument,
    eth_perp: Instrument,
    unique_cid: Callable[[str], str],
    funded_account: Decimal,
    sync_engine_factory: Callable[[], SyncHyperliquidEngine],
) -> None:
    engine = connected_engine
    adapter = engine._adapter
    assert adapter.streams_connected

    # --- reads ---
    spec = await engine.fetch_instrument_spec(btc_perp)
    ticker = await engine.fetch_ticker(btc_perp)
    assert ticker is not None and ticker.bid is not None and ticker.ask is not None
    assert ticker.bid < ticker.ask
    balances = await engine.fetch_balances()
    assert "USDC" in balances
    limits = await engine.get_rate_limits()
    assert limits.remaining > 0

    await flatten_all(adapter)
    assert await engine.fetch_positions() == []

    # --- LIMIT lifecycle ---
    assert ticker.mid is not None
    mid = ticker.mid
    px = venue_price(mid * Decimal("0.5"), spec.tick_size, direction="down")
    # Size from the ORDER price, not mid — notional is qty times resting price.
    qty = valid_qty_from_spec(spec, px)
    cid = unique_cid("e2e-lim")
    placed = await adapter.place_order(
        UnifiedOrder(
            instrument=btc_perp,
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=qty,
            price=px,
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    assert placed.status == OrderStatus.OPEN
    seen = await adapter.get_order_by_client_id(cid)
    assert seen is not None and seen.status == OrderStatus.OPEN
    cancelled = await adapter.cancel_order(cid)
    assert cancelled.status == OrderStatus.CANCELLED

    # --- intent precedes the position (block guard demands flat book) ---
    await engine.set_leverage(btc_perp, leverage=5)
    await engine.set_margin_mode(btc_perp, MarginMode.ISOLATED)

    # --- MARKET fill + position TP/SL ---
    mkt = await adapter.place_order(
        UnifiedOrder(
            instrument=btc_perp,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=qty,
            time_in_force=TimeInForce.IOC,
            client_order_id=unique_cid("e2e-mkt"),
        )
    )
    assert mkt.status == OrderStatus.FILLED
    await wait_for_position(adapter, "BTC")

    # The venue reports leverage/mode entries only against a live leg.
    assert await engine.get_leverage(btc_perp) == (5, False)
    assert await engine.get_margin_mode(btc_perp) == MarginMode.ISOLATED

    tp = venue_price(mid * Decimal("1.5"), spec.tick_size, direction="up")
    sl = venue_price(mid * Decimal("0.5"), spec.tick_size, direction="down")
    await engine.modify_position_tpsl(
        btc_perp,
        "BTC:oneWay",
        take_profit=TpSlAttachment(trigger_price=tp),
        stop_loss=TpSlAttachment(trigger_price=sl),
    )
    got = await engine.get_position_tpsl(btc_perp, "BTC:oneWay")
    assert got is not None and got[0] is not None and got[1] is not None
    assert got[0].trigger_price == tp
    assert got[1].trigger_price == sl

    # --- reconcile (no drift → no-op) + restore flat + defaults ---
    await engine.reconcile_user_intent()
    await flatten_all(adapter)
    await restore_defaults(adapter, btc_perp)
    assert await engine.fetch_positions() == []
    assert await engine.fetch_open_orders() == {}

    # --- sync engine smoke on ETH ---
    sync = sync_engine_factory()
    sync.connect()
    try:
        sync_ticker = sync.fetch_ticker(eth_perp)
        assert sync_ticker.bid is not None and sync_ticker.ask is not None
        assert sync_ticker.bid < sync_ticker.ask
    finally:
        sync.shutdown()

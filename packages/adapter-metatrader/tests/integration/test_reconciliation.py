"""Integration tests for MT5 reconciliation, watermark bootstrapping, and halt management.

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config, MT5Engine
from unified_trading_execution.state import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderStatus, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import build_unified_order, cleanup_adapter, spec_price, spec_qty

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_INSTRUMENT = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol=_BROKER_SYMBOL,
)


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
    )


async def _live_bid_ask() -> tuple[Decimal, Decimal]:
    import MetaTrader5 as mt5

    tick = await asyncio.to_thread(mt5.symbol_info_tick, _BROKER_SYMBOL)
    if tick is None:
        pytest.fail(f"no market quote for {_BROKER_SYMBOL}")
    return Decimal(str(tick.bid)), Decimal(str(tick.ask))


async def test_reconciliation_bootstraps_watermark(
    mt5_config: MT5Config,
    event_bus: EventBus,
) -> None:
    """First reconciliation pass bootstraps the clean-through watermark and returns clean."""
    store_path = Path(tempfile.gettempdir()) / f"ute_mt5_reconcile_{uuid.uuid4().hex}.db"
    store = SQLiteStateStore(str(store_path))
    await store.initialize()
    engine = MT5Engine(mt5_config, event_bus=event_bus, state_store=store)
    await engine.connect()
    try:
        # Before reconciliation, watermark should be None
        assert await store.get_reconcile_watermark() is None

        # Run first reconciliation pass (bootstraps watermark to now, skips historical fills)
        result = await engine.reconcile()
        assert result.is_clean

        # Watermark should now be populated
        watermark = await store.get_reconcile_watermark()
        assert watermark is not None

        # Second pass should also be clean and advance watermark
        result2 = await engine.reconcile()
        assert result2.is_clean
    finally:
        with contextlib.suppress(Exception):
            await engine.disconnect()
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            store_path.unlink()


async def test_reconciliation_orphan_lifecycle(
    mt5_config: MT5Config,
    event_bus: EventBus,
) -> None:
    """Import a live platform order, then remove it after a platform-side cancel."""
    store_path = Path(tempfile.gettempdir()) / f"ute_mt5_orphans_{uuid.uuid4().hex}.db"
    store = SQLiteStateStore(str(store_path))
    await store.initialize()

    # Share one adapter: MT5.initialize()/shutdown() is process-global.
    adapter = MT5Adapter(mt5_config, event_bus=event_bus)
    engine = MT5Engine(adapter, event_bus=event_bus, state_store=store)
    await engine.connect()

    try:
        # Prepare valid resting limit order parameters
        bid, ask = await _live_bid_ask()
        qty = await spec_qty(adapter, _INSTRUMENT)
        price = await spec_price(adapter, _INSTRUMENT, ask * Decimal("0.90"))

        # -------------------------------------------------------------
        # Part 1: Platform Orphan (Order placed on platform, unknown to local engine)
        # -------------------------------------------------------------
        # Canonical UUIDs survive MT5's comment-based restart mapping.
        cid_platform = str(uuid.uuid4())
        order = build_unified_order(
            _INSTRUMENT,
            OrderType.LIMIT,
            OrderSide.BUY,
            qty,
            client_order_id=cid_platform,
            price=price,
        )

        # Place through a separate adapter so the engine's local mirror is unaware.
        result_plat = await adapter.place_order(order)
        assert result_plat.status == OrderStatus.OPEN
        assert result_plat.platform_order_id is not None

        # Verify engine's store does NOT know about this order yet
        assert await store.get_order(cid_platform) is None

        # The first pass must discover and import the platform-only order.
        res1 = await engine.reconcile()
        assert any(o.client_order_id == cid_platform for o in res1.orphan_orders_on_platform)

        # The imported order is now visible through the engine's active mirror.
        imported_order = await store.get_order(cid_platform)
        assert imported_order is not None
        assert imported_order.platform_order_id == result_plat.platform_order_id
        assert imported_order.status == OrderStatus.OPEN
        assert imported_order.instrument == _INSTRUMENT

        # Cancel through the platform adapter, bypassing the engine's mirror.
        await adapter.cancel_order(cid_platform)

        local_order_before = await store.get_order(cid_platform)
        assert local_order_before is not None
        assert local_order_before.status == OrderStatus.OPEN

        # The next pass must remove the stale active row from local state.
        res2 = await engine.reconcile()
        assert cid_platform in res2.orphan_orders_in_local

        assert await store.get_order(cid_platform) is None

        # Reconciliation removes only the active row; the append-only history remains.
        cursor = await store.conn.execute(
            "SELECT client_order_id FROM order_history WHERE client_order_id = ?",
            (cid_platform,),
        )
        history_rows = await cursor.fetchall()
        assert history_rows

    finally:
        with contextlib.suppress(Exception):
            await cleanup_adapter(adapter)
        with contextlib.suppress(Exception):
            await engine.disconnect()
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            store_path.unlink()

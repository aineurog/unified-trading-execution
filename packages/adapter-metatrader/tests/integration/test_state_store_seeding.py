"""Integration: connect() rebuilds client_order_id ↔ ticket maps from the store.

Simulates the engine's place-time persistence.  An open LIMIT order is
placed under one adapter, its ``OrderRecord`` is written to a real
``SQLiteStateStore`` (the exact ``dispatch_place_order`` → ``upsert_order``
path the engine uses), the adapter disconnects, and a fresh adapter with
empty maps connected to the SAME store must rebuild the mapping purely
from the store at ``connect()`` — independent of the ``U:`` comment round
trip.

This is the authoritative source for recovery; the comment scan is only a
cross-check (see ``_seed_mappings_from_state_store``).

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
The instrument under test is EUR/USD; the broker symbol defaults to
``EURUSD`` and can be overridden via ``MT5_SYMBOL`` (e.g. ``EURUSD.m``).
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
from uuid_extensions import uuid7

from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.state import SQLiteStateStore
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderModification

from .helpers import build_unified_order, cleanup_adapter, spec_price, spec_qty

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_INSTRUMENT = Instrument(symbol="EUR", quote_currency="USD", asset_class=AssetClass.MARGIN_FX)


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    """Alias EUR/USD to the live broker symbol (reverse alias is needed by
    the polling/recovery paths that resolve deal/position symbols)."""
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
        symbol_alias_table={"EUR/USD": _BROKER_SYMBOL},
    )


async def _live_ask() -> Decimal:
    import MetaTrader5 as mt5

    tick = await asyncio.to_thread(mt5.symbol_info_tick, _BROKER_SYMBOL)
    if tick is None:
        pytest.fail(f"no market quote for {_BROKER_SYMBOL}")
    return Decimal(str(tick.ask))


async def test_store_seeding_rebuilds_mapping_after_restart(
    mt5_config: MT5Config,
    event_bus: EventBus,
) -> None:
    """Maps are rebuilt from the store at connect, and the seeded ticket
    resolves modify/cancel — no comment round-trip involved."""
    store_path = Path(tempfile.gettempdir()) / f"ute_mt5_seed_{uuid.uuid4().hex}.db"
    store = SQLiteStateStore(str(store_path))
    await store.initialize()
    try:
        # Phase 1: place an open LIMIT under adapter A, then persist the order
        # exactly as the engine's dispatch_place_order does (upsert_order).
        first = MT5Adapter(mt5_config, event_bus=event_bus)
        first.attach_state_store(store)
        await first.connect()
        try:
            qty = await spec_qty(first, _INSTRUMENT)
            ask = await _live_ask()
            cid = str(uuid7())
            result = await first.place_order(
                build_unified_order(
                    _INSTRUMENT,
                    OrderType.LIMIT,
                    OrderSide.BUY,
                    qty,
                    client_order_id=cid,
                    price=await spec_price(first, _INSTRUMENT, ask * Decimal("0.90")),
                )
            )
            assert result.status == OrderStatus.OPEN
            assert result.platform_order_id is not None
            record = (await first.fetch_open_orders())[cid]
            assert record.platform_order_id == result.platform_order_id
            await store.upsert_order(record)
        finally:
            # Order stays OPEN on the terminal — only the process disconnects.
            await first.disconnect()

        # Phase 2: fresh adapter B, empty maps, SAME store → seed at connect.
        second = MT5Adapter(mt5_config, event_bus=event_bus)
        second.attach_state_store(store)
        await second.connect()
        try:
            ticket = int(result.platform_order_id)
            assert second._order_id_to_ticket.get(cid) == ticket
            assert second._ticket_to_order_id.get(ticket) == cid

            # Behavioural proof: modify_by_client_id resolves through the
            # seeded ticket even though this process never placed the order.
            ask = await _live_ask()
            new_price = await spec_price(second, _INSTRUMENT, ask * Decimal("0.85"))
            modified = await second.modify_order(
                OrderModification(client_order_id=cid, price=new_price)
            )
            assert modified.client_order_id == cid
            assert modified.platform_order_id == result.platform_order_id
        finally:
            await cleanup_adapter(second)
            await second.disconnect()
    finally:
        with contextlib.suppress(Exception):
            await store.close()
        with contextlib.suppress(Exception):
            store_path.unlink()

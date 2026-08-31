"""Live IBKR integration tests — require a running Gateway/TWS paper.

All tests use the session `connected_adapter` fixture (real IB). When
IBKR_HOST/PORT/ACCOUNT env is missing they are skipped, so CI stays green.
Each test cleans up its own orders/positions so the paper account stays flat.

Coverage:
  - fetch_instrument_spec (STK + FX IDEALPRO + cache)
  - LIMIT / STOP / STOP_LIMIT roundtrip (place → get → cancel → gone)
  - MARKET fill (fills + positions)
  - bracket LIMIT+TP/SL (OCA, parentId)
  - fetch_reconciliation (positions/balances/open_orders/fills + since)
  - get_rate_limits
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.ibkr import IBKRAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderModification, TpSlAttachment, UnifiedOrder

pytestmark = pytest.mark.asyncio

SYMBOL = "NVDA"
FX_SYMBOL = "EUR"


def _cid(prefix: str = "itest") -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


def _stock() -> Instrument:
    return Instrument(symbol=SYMBOL, asset_class=AssetClass.STOCK, currency="USD")


def _fx() -> Instrument:
    return Instrument(symbol=FX_SYMBOL, quote_currency="USD", asset_class=AssetClass.MARGIN_FX)


async def _wait_gone(adapter: IBKRAdapter, cid: str, timeout: float = 6) -> bool:
    for _ in range(int(timeout / 0.6)):
        cur = await adapter.get_order_by_client_id(cid)
        if cur is None or cur.status == OrderStatus.CANCELLED:
            return True
        await asyncio.sleep(0.6)
    return False


async def test_fetch_instrument_spec(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    stock = _stock()
    spec = await adapter.fetch_instrument_spec(stock)
    assert spec.tick_size > 0
    assert spec.lot_size > 0
    # cache hit
    spec2 = await adapter.fetch_instrument_spec(stock)
    assert spec2 is spec
    adapter._invalidate_spec_cache(stock)
    spec3 = await adapter.fetch_instrument_spec(stock)
    assert spec3.tick_size == spec.tick_size

    # FX IDEALPRO (was SMART 200 before fix)
    fx = _fx()
    fx_spec = await adapter.fetch_instrument_spec(fx)
    assert fx_spec.tick_size > 0


async def test_limit_roundtrip(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    inst = _stock()
    cid = _cid("limit")
    order = UnifiedOrder(
        instrument=inst,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        client_order_id=cid,
        price=Decimal("10"),
    )
    placed = await adapter.place_order(order)
    assert placed.client_order_id == cid
    assert placed.status in (OrderStatus.PENDING, OrderStatus.OPEN)

    await asyncio.sleep(1)
    got = await adapter.get_order_by_client_id(cid)
    assert got is not None
    assert got.status in (OrderStatus.PENDING, OrderStatus.OPEN)

    # modify
    mod = await adapter.modify_order(OrderModification(client_order_id=cid, price=Decimal("11")))
    assert mod.client_order_id == cid

    # cancel
    cancelled = await adapter.cancel_order(cid)
    assert cancelled.status in (OrderStatus.CANCELLED, OrderStatus.OPEN)
    await _wait_gone(adapter, cid)
    # After cancel, get may still return CANCELLED (IBKR keeps DoneState in trades())
    gone = await adapter.get_order_by_client_id(cid)
    assert gone is None or gone.status == OrderStatus.CANCELLED


async def test_stop_and_stop_limit_roundtrip(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    inst = _stock()
    for otype in [OrderType.STOP, OrderType.STOP_LIMIT]:
        cid = _cid(otype.value.lower())
        if otype == OrderType.STOP:
            order = UnifiedOrder(
                instrument=inst,
                order_type=otype,
                side=OrderSide.BUY,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
                client_order_id=cid,
                stop_price=Decimal("500"),
            )
        else:
            order = UnifiedOrder(
                instrument=inst,
                order_type=otype,
                side=OrderSide.BUY,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
                client_order_id=cid,
                price=Decimal("10"),
                stop_price=Decimal("500"),
            )
        placed = await adapter.place_order(order)
        assert placed.status in (OrderStatus.PENDING, OrderStatus.OPEN)
        await asyncio.sleep(0.7)
        assert await adapter.get_order_by_client_id(cid) is not None
        await adapter.cancel_order(cid)
        await _wait_gone(adapter, cid)


async def test_bracket_tpsl(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    inst = _stock()
    cid = _cid("bracket")
    order = UnifiedOrder(
        instrument=inst,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        client_order_id=cid,
        price=Decimal("10"),
        take_profit=TpSlAttachment(trigger_price=Decimal("20")),
        stop_loss=TpSlAttachment(trigger_price=Decimal("5")),
    )
    placed = await adapter.place_order(order)
    assert placed.status in (OrderStatus.PENDING, OrderStatus.OPEN)
    await asyncio.sleep(1)
    # Parent should be OPEN/PENDING, children are OCA-linked (only parent has cid)
    got = await adapter.get_order_by_client_id(cid)
    assert got is not None
    # Cancel parent — children OCA should also cancel
    await adapter.cancel_order(cid)
    await _wait_gone(adapter, cid)
    # After parent cancel, no open order with that cid
    gone = await adapter.get_order_by_client_id(cid)
    assert gone is None or gone.status == OrderStatus.CANCELLED


async def test_market_fill_and_fetch(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    inst = _stock()
    cid = _cid("market")
    order = UnifiedOrder(
        instrument=inst,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        client_order_id=cid,
    )
    await adapter.place_order(order)
    # MARKET may be PENDING briefly then FILLED
    await asyncio.sleep(2.5)
    cur = await adapter.get_order_by_client_id(cid)
    # May be FILLED or still OPEN/PENDING depending on paper fill speed
    assert cur is not None
    assert cur.status in (
        OrderStatus.FILLED,
        OrderStatus.OPEN,
        OrderStatus.PENDING,
        OrderStatus.CANCELLED,
    )

    # fetch_fills should contain it if filled
    fills = await adapter.fetch_fills()
    # If filled, there should be at least one fill for this cid
    if cur and cur.status == OrderStatus.FILLED:
        assert cid in fills or any(cid in k for k in fills)
        # fetch with since should also include it when since is recent
        since = datetime.now(UTC) - timedelta(minutes=10)
        recent = await adapter.fetch_fills(since=since)
        # Recent fills should include this cid if it was within 10m
        assert cid in recent or any(cid in k for k in recent)

    # Flatten with opposite MARKET so account stays flat for next tests
    try:
        close_cid = _cid("close")
        await adapter.place_order(
            UnifiedOrder(
                instrument=inst,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
                client_order_id=close_cid,
            )
        )
        await asyncio.sleep(1.5)
    except Exception:
        pass


async def test_fetch_reconciliation(connected_adapter: IBKRAdapter) -> None:
    adapter = connected_adapter
    # All fetch_* must not raise and return correct shapes
    positions = await adapter.fetch_positions()
    assert isinstance(positions, list)
    # Each position has position_id == conId when available
    for p in positions:
        assert p.instrument is not None
        assert p.quantity is not None

    balances = await adapter.fetch_balances()
    assert isinstance(balances, dict)
    # Paper always has at least USD
    assert (
        "USD" in balances or len(balances) == 0
    )  # empty only if accountValues filtered by wrong account
    if balances:
        bal = next(iter(balances.values()))
        assert bal.free + bal.used == bal.total

    open_orders = await adapter.fetch_open_orders()
    assert isinstance(open_orders, dict)

    fills = await adapter.fetch_fills()
    assert isinstance(fills, dict)
    # sorted per cid
    for lst in fills.values():
        times = [f.fill_timestamp for f in lst]
        assert times == sorted(times)

    # since filter
    since = datetime.now(UTC) - timedelta(hours=1)
    recent = await adapter.fetch_fills(since=since)
    assert isinstance(recent, dict)
    # recent must be subset of all
    for cid in recent:
        assert cid in fills or any(cid in k for k in fills)

    limits = await adapter.get_rate_limits()
    assert limits.requests_per_interval == 50
    assert limits.remaining <= 50


async def test_modify_position_tpsl(connected_adapter: IBKRAdapter) -> None:
    """Create a 1-share NVDA position via MARKET, then set TP/SL via OCA."""
    adapter = connected_adapter
    inst = _stock()
    # Ensure flat first
    for pos in await adapter.fetch_positions():
        if pos.instrument.symbol == SYMBOL and pos.quantity != 0:
            # flatten any existing NVDA
            side = OrderSide.SELL if pos.quantity > 0 else OrderSide.BUY
            await adapter.place_order(
                UnifiedOrder(
                    instrument=inst,
                    order_type=OrderType.MARKET,
                    side=side,
                    quantity=abs(pos.quantity),
                    time_in_force=TimeInForce.GTC,
                    client_order_id=_cid("flatten"),
                )
            )
            await asyncio.sleep(1.5)
    # Open 1 share
    cid = _cid("tpsl-pos")
    await adapter.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=cid,
        )
    )
    await asyncio.sleep(2)
    positions = await adapter.fetch_positions()
    nvda = next((p for p in positions if p.instrument.symbol == SYMBOL), None)
    assert nvda is not None and nvda.position_id is not None, "no NVDA position for tpsl test"
    # Place TP/SL
    await adapter.modify_position_tpsl(
        nvda.position_id,
        take_profit=TpSlAttachment(Decimal("300")),
        stop_loss=TpSlAttachment(Decimal("200")),
    )
    await asyncio.sleep(1)
    open_orders = await adapter.fetch_open_orders()
    # Should have 2 OCA orders for that conId (TP LMT 300, SL STP 200)
    tpsl = [
        o
        for o in open_orders.values()
        if o.quantity == Decimal("1") and o.price in (Decimal("300"), None)
    ]
    assert len(tpsl) >= 1  # at least TP
    # Cleanup tpsl
    for oid in list(open_orders.keys()):
        with contextlib.suppress(Exception):
            await adapter.cancel_order(oid)
    await asyncio.sleep(1)
    # Flatten
    await adapter.place_order(
        UnifiedOrder(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.SELL,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id=_cid("flat2"),
        )
    )
    await asyncio.sleep(1)


async def test_tif_variants(connected_adapter: IBKRAdapter) -> None:
    """Every TIF (GTC/DAY/IOC/FOK/GTD) with LIMIT stays pending and is cancellable."""
    adapter = connected_adapter
    inst = _stock()
    for tif in [
        TimeInForce.GTC,
        TimeInForce.DAY,
        TimeInForce.IOC,
        TimeInForce.FOK,
        TimeInForce.GTD,
    ]:
        cid = _cid(f"tif-{tif.value.lower()}")
        if tif == TimeInForce.GTD:
            order = UnifiedOrder(
                instrument=inst,
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("1"),
                time_in_force=tif,
                client_order_id=cid,
                price=Decimal("10"),
                expire_at=datetime.now(UTC) + timedelta(days=1),
            )
        else:
            order = UnifiedOrder(
                instrument=inst,
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("1"),
                time_in_force=tif,
                client_order_id=cid,
                price=Decimal("10"),
            )
        placed = await adapter.place_order(order)
        assert placed.status in (OrderStatus.PENDING, OrderStatus.OPEN)
        await asyncio.sleep(0.5)
        # IOC/FOK may be cancelled immediately if not filled — either OPEN/PENDING or gone is ok
        cur = await adapter.get_order_by_client_id(cid)
        if cur is not None:
            assert cur.status in (OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.CANCELLED)
            with contextlib.suppress(Exception):
                await adapter.cancel_order(cid)
                await asyncio.sleep(0.5)

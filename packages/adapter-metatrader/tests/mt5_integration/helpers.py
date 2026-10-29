"""Shared utilities for MT5 adapter integration tests.

These helpers build canonical core types and derive spec-compliant values so
tests never hard-code quantities/prices that a live instrument's filters may
reject.  All monetary values flow as ``Decimal`` — never ``float`` (Section
17.4 of the requirements).
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder
from unified_trading_execution.types.position import Position


def random_client_id(prefix: str) -> str:
    """A unique client order id for a test — avoids cross-test collisions."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def build_unified_order(
    instrument: Instrument,
    order_type: OrderType,
    side: OrderSide,
    quantity: Decimal,
    *,
    client_order_id: str,
    price: Decimal | None = None,
    stop_price: Decimal | None = None,
    time_in_force: TimeInForce = TimeInForce.GTC,
    reduce_only: bool = False,
    take_profit: TpSlAttachment | None = None,
    stop_loss: TpSlAttachment | None = None,
    position_id: str | None = None,
    expire_at: datetime | None = None,
) -> UnifiedOrder:
    """Construct a valid ``UnifiedOrder``, deriving any required price fields."""
    if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and price is None:
        raise ValueError(f"price is required for {order_type}")
    if order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and stop_price is None:
        raise ValueError(f"stop_price is required for {order_type}")
    return UnifiedOrder(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=quantity,
        time_in_force=time_in_force,
        client_order_id=client_order_id,
        price=price,
        stop_price=stop_price,
        reduce_only=reduce_only,
        take_profit=take_profit,
        stop_loss=stop_loss,
        position_id=position_id,
        expire_at=expire_at,
    )


def valid_qty_from_spec(spec: InstrumentSpec, _reference: Decimal | None = None) -> Decimal:
    """A spec-compliant quantity: >= min_qty, aligned to lot_size."""
    lot = spec.lot_size if spec.lot_size > 0 else Decimal("0.01")
    min_qty = spec.min_qty if spec.min_qty > 0 else lot

    steps = (min_qty / lot).to_integral_value(rounding="ROUND_CEILING")
    qty = steps * lot

    if spec.max_qty > 0 and qty > spec.max_qty:
        qty = spec.max_qty

    return qty


def valid_price_from_spec(spec: InstrumentSpec, reference: Decimal) -> Decimal:
    """A spec-compliant price aligned to ``tick_size`` near ``reference``."""
    if spec.tick_size <= 0:
        return reference
    steps = (reference / spec.tick_size).to_integral_value(rounding="ROUND_HALF_UP")
    aligned = steps * spec.tick_size
    return aligned if aligned > 0 else spec.tick_size


async def spec_qty(adapter: MT5Adapter, instrument: Instrument) -> Decimal:
    """A spec-compliant quantity for *instrument*."""
    spec = await adapter.fetch_instrument_spec(instrument)
    return valid_qty_from_spec(spec)


async def spec_price(adapter: MT5Adapter, instrument: Instrument, reference: Decimal) -> Decimal:
    """A spec-compliant price near ``reference`` for *instrument*."""
    spec = await adapter.fetch_instrument_spec(instrument)
    return valid_price_from_spec(spec, reference)


def position_for_symbol(
    positions: list[Position],
    instrument: Instrument,
) -> Position | None:
    """Find a position leg by base symbol/asset class.

    ``fetch_positions()`` returns one ``Position`` per terminal leg, each
    carrying the resolved ``Instrument`` (which includes ``platform_symbol``),
    so a caller-built instrument is matched on its identity fields instead.
    """
    for position in positions:
        candidate = position.instrument
        if (
            candidate.symbol == instrument.symbol
            and candidate.asset_class == instrument.asset_class
        ):
            return position
    return None


async def cleanup_adapter(adapter: MT5Adapter) -> None:
    """Cancel every open order and flatten every raw position leg.

    Read raw positions once per symbol because the adapter snapshot can contain
    multiple legs for that symbol. Close each ticket only once per pass; an
    un-targeted opposite order after a failed close could open new exposure.
    """
    import MetaTrader5 as mt5

    for client_order_id in await adapter.fetch_open_orders():
        with contextlib.suppress(Exception):
            await adapter.cancel_order(client_order_id)

    for _ in range(5):
        positions = await adapter.fetch_positions()
        instruments = {
            position.instrument.platform_symbol: position.instrument
            for position in positions
            if position.instrument.platform_symbol
        }
        legs: dict[int, tuple[Instrument, Any]] = {}
        for broker_symbol, instrument in instruments.items():
            raw = await asyncio.to_thread(mt5.positions_get, symbol=broker_symbol)
            for leg in raw or ():
                legs[int(leg.ticket)] = (instrument, leg)

        if not legs:
            return

        failures: list[str] = []
        for ticket, (instrument, leg) in legs.items():
            side = OrderSide.SELL if leg.type == 0 else OrderSide.BUY
            order = build_unified_order(
                instrument,
                OrderType.MARKET,
                side,
                Decimal(str(leg.volume)),
                client_order_id=random_client_id("clean"),
                position_id=str(ticket),
            )
            try:
                result = await adapter.place_order(order)
            except Exception as exc:
                failures.append(f"ticket {ticket}: {exc}")
                continue
            if result.status is not OrderStatus.FILLED:
                failures.append(f"ticket {ticket}: close returned {result.status.value}")

        if failures:
            raise RuntimeError("Failed to flatten MT5 positions: " + "; ".join(failures))
        await asyncio.sleep(1.0)

    remaining = await adapter.fetch_positions()
    if remaining:
        tickets = ", ".join(
            position.position_id for position in remaining if position.position_id is not None
        )
        raise RuntimeError(f"MT5 positions remain after cleanup: {tickets}")

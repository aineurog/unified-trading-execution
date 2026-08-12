"""Shared utilities for MT5 adapter integration tests.

These helpers build canonical core types and derive spec-compliant values so
tests never hard-code quantities/prices that a live instrument's filters may
reject.  All monetary values flow as ``Decimal`` — never ``float`` (Section
17.4 of the requirements).
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder


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

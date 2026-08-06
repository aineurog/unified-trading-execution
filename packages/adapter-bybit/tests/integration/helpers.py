"""Shared utilities for Bybit adapter integration tests.

These helpers build canonical core types and derive spec-compliant values so
tests never hard-code quantities/prices that a live instrument's filters may
reject.  All monetary values flow as ``Decimal`` — never ``float`` (Section
17.4).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    TpSlAttachment,
    UnifiedOrder,
)


def make_instrument(
    symbol: str,
    quote_currency: str,
    asset_class: AssetClass,
    *,
    currency: str | None = None,
) -> Instrument:
    """Build a canonical ``Instrument`` for a Bybit symbol."""
    is_futures = asset_class == AssetClass.FUTURES
    return Instrument(
        symbol=symbol,
        quote_currency=quote_currency,
        asset_class=asset_class,
        exchange=None,
        currency=currency if currency is not None else quote_currency,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1 if is_futures else None,
    )


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


def valid_qty_from_spec(spec: InstrumentSpec, reference: Decimal | None = None) -> Decimal:
    """A spec-compliant quantity: >= min_qty, satisfies min_notional, aligned to lot_size.

    ``reference`` is the current mid price and is used to enforce the minimum
    notional (``spec.min_notional``).  For spot this is ``minOrderAmt``
    (minimum quote-currency order value); for derivatives it is
    ``minNotionalValue``.  Both are stored in ``InstrumentSpec.min_notional``
    after the adapter parses the correct field per category.

    When ``reference`` is ``None``, the notional-based bump is skipped and the
    returned quantity is simply ``min_qty`` aligned to the lot boundary — safe
    for orders that will rest below market and never fill.
    """
    lot = spec.lot_size if spec.lot_size > 0 else Decimal("1")
    min_qty = spec.min_qty if spec.min_qty > 0 else lot

    # Start from min_qty aligned up to the next lot boundary.
    qty = align_up_to_lot(min_qty, lot)

    # If min_notional is set, bump qty until qty * reference >= min_notional.
    # Use a safety factor of 5x to absorb mid-price drift and tick-alignment
    # rounding that can bring the effective notional back under the threshold.
    if reference is not None and spec.min_notional > 0 and reference > 0:
        required_qty = (spec.min_notional * Decimal("5.0")) / reference
        if required_qty > qty:
            qty = align_up_to_lot(required_qty, lot)

    # Clamp to max_qty.
    if spec.max_qty > 0 and qty > spec.max_qty:
        qty = spec.max_qty

    return qty


def valid_price_from_spec(spec: InstrumentSpec, reference: Decimal) -> Decimal:
    """A spec-compliant price aligned to ``tick_size`` near ``reference``."""
    if spec.tick_size <= 0:
        return reference
    steps = (reference / spec.tick_size).to_integral_value(rounding="ROUND_UP")
    return steps * spec.tick_size


def align_down_to_tick(value: Decimal, tick_size: Decimal) -> Decimal:
    """Round ``value`` down to the nearest multiple of ``tick_size``."""
    if tick_size <= 0:
        return value
    steps = (value / tick_size).to_integral_value(rounding="ROUND_DOWN")
    return steps * tick_size


def align_to_tick(value: Decimal, tick_size: Decimal) -> Decimal:
    """Round ``value`` to the nearest multiple of ``tick_size``.

    Nearest (rather than a fixed up/down direction) preserves which side of the
    market a derived price sits on — a below-market resting price stays below,
    an above-market fill price stays above — while fixing the decimal precision
    Bybit rejects via 170134.
    """
    if tick_size <= 0:
        return value
    steps = (value / tick_size).to_integral_value(rounding="ROUND_HALF_UP")
    aligned = steps * tick_size
    return aligned if aligned > 0 else tick_size


def align_up_to_lot(value: Decimal, lot_size: Decimal) -> Decimal:
    """Round ``value`` up to the nearest multiple of ``lot_size``."""
    if lot_size <= 0:
        return value
    steps = (value / lot_size).to_integral_value(rounding="ROUND_CEILING")
    return steps * lot_size


@dataclass(frozen=True)
class InvalidOrderValues:
    """One deliberately invalid order parameter set, keyed by its name."""

    name: str
    order_type: OrderType
    quantity: Decimal | None = None
    price: Decimal | None = None


def invalid_values_from_spec(spec: InstrumentSpec, reference: Decimal) -> list[InvalidOrderValues]:
    """A table of deliberately invalid order parameters for the spec.

    Each entry violates exactly one instrument filter:
    - ``below_min_qty``: quantity below ``min_qty``
    - ``off_lot_step``: quantity not aligned to ``lot_size``
    - ``above_max_qty``: quantity above ``max_qty``
    - ``below_min_notional``: notional below ``min_notional``

    Notes:
    - All prices for quantity-violation cases are tick-aligned (``align_down_to_tick``)
      so that Bybit evaluates the quantity, not the price.
    - ``price_off_tick_step`` is omitted: Bybit silently rounds off-tick prices
      for derivatives rather than rejecting them, making the assertion
      non-deterministic. Spot does reject off-tick prices but that path is
      already covered by unit tests against the translation layer.
    - ``zero_price`` is omitted: ``UnifiedOrder.__post_init__`` rejects
      ``price=0`` before the adapter is called — this is a unit-level
      invariant, not a network-level test.
    """
    tick = spec.tick_size if spec.tick_size > 0 else Decimal("0.01")
    lot = spec.lot_size if spec.lot_size > 0 else Decimal("0.001")
    min_qty = spec.min_qty if spec.min_qty > 0 else lot

    # Tick-aligned price for qty-violation cases — ensures the price itself
    # is valid so Bybit evaluates the quantity filter, not the price filter.
    aligned_price = align_down_to_tick(reference, tick)
    if aligned_price <= 0:
        aligned_price = tick

    below_min_qty = max(min_qty / Decimal("10"), Decimal("0.0000001"))
    off_lot = align_up_to_lot(min_qty, lot) + lot / Decimal("2")
    above_max = (
        spec.max_qty * Decimal("10") if spec.max_qty > 0 else (reference * Decimal("100000"))
    )
    # below_min_notional: notional = qty * price. Use a qty that produces a
    # notional well below the threshold (1/10th of what is required).
    below_notional_qty: Decimal
    if spec.min_notional > 0 and aligned_price > 0:
        below_notional_qty = max(
            (spec.min_notional / Decimal("10")) / aligned_price,
            Decimal("0.0000001"),
        )
    else:
        below_notional_qty = max(min_qty / Decimal("100"), Decimal("0.0000001"))

    cases = [
        InvalidOrderValues(
            name="below_min_qty",
            order_type=OrderType.LIMIT,
            quantity=below_min_qty,
            price=aligned_price,
        ),
        InvalidOrderValues(
            name="off_lot_step",
            order_type=OrderType.LIMIT,
            quantity=off_lot,
            price=aligned_price,
        ),
        InvalidOrderValues(
            name="above_max_qty",
            order_type=OrderType.LIMIT,
            quantity=above_max,
            price=aligned_price,
        ),
    ]

    # Only include below_min_notional when the spec actually carries a non-zero
    # threshold — otherwise there is nothing to violate.
    if spec.min_notional > 0:
        cases.append(
            InvalidOrderValues(
                name="below_min_notional",
                order_type=OrderType.LIMIT,
                quantity=below_notional_qty,
                price=aligned_price,
            )
        )

    return cases


def order_ids_seen(adapter: Any) -> dict[str, set[str]]:
    """Snapshot the adapter's live/final order-id bookkeeping.

    Returns ``{"open": ..., "final": ...}`` from ``_open_order_ids`` and
    ``_final_order_ids`` so tests can assert finalize-only-on-close behaviour.
    """
    return {
        "open": set(adapter._open_order_ids),
        "final": set(adapter._final_order_ids),
    }


def assert_is_decimal(value: object, label: str) -> None:
    """Assert a value is a ``Decimal`` — never a ``float`` (Section 17.4)."""
    assert isinstance(value, Decimal), f"{label} must be a Decimal, got {type(value).__name__}"


def quantized_str(value: Decimal) -> str:
    """Render a Decimal without trailing zeros for payload building."""
    return format(value, "f")

"""Shared utilities for Hyperliquid adapter integration tests.

These helpers build canonical core types and derive spec-compliant values so
tests never hard-code quantities/prices that the live venue may reject. All
monetary values flow as ``Decimal`` — never ``float``.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)

if TYPE_CHECKING:
    from unified_trading_execution.hyperliquid import HyperliquidAdapter


def make_perp(coin: str) -> Instrument:
    """Build a canonical perp ``Instrument`` for a coin."""
    return Instrument(
        symbol=coin,
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def make_spot(base: str) -> Instrument:
    """Build a canonical spot ``Instrument`` for a base currency (quote USDC)."""
    return Instrument(
        symbol=base,
        quote_currency="USDC",
        asset_class=AssetClass.SPOT,
        currency="USDC",
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
        expire_at=expire_at,
    )


def valid_qty_from_spec(
    spec: InstrumentSpec,
    reference: Decimal | None = None,
    *,
    target_notional: Decimal = Decimal("11"),
) -> Decimal:
    """A spec-compliant quantity: >= lot, satisfies the $10 venue minimum.

    ``reference`` is the current mid price; ``target_notional`` (default $11)
    absorbs mid-price drift and tick-alignment rounding that could bring the
    effective notional back under the $10 floor.
    """
    lot = spec.lot_size if spec.lot_size > 0 else Decimal("1")
    qty = align_up_to_lot(spec.min_qty if spec.min_qty > 0 else lot, lot)
    if reference is not None and reference > 0:
        required = target_notional / reference
        if required > qty:
            qty = align_up_to_lot(required, lot)
    if spec.max_qty > 0 and qty > spec.max_qty:
        qty = spec.max_qty
    return qty


def valid_price_from_spec(spec: InstrumentSpec, reference: Decimal) -> Decimal:
    """A spec-compliant price aligned up to ``tick_size`` near ``reference``."""
    if spec.tick_size <= 0:
        return reference
    steps = (reference / spec.tick_size).to_integral_value(rounding="ROUND_CEILING")
    return steps * spec.tick_size


def venue_price(raw: Decimal, tick: Decimal, *, direction: str = "down") -> Decimal:
    """A venue-legal price: <= 5 significant figures, floored/ceiled to tick.

    ``direction`` is ``"down"`` (resting below market, TP math) or ``"up"``.
    """
    if tick <= 0:
        raise ValueError("tick must be positive")
    quantum = Decimal(1).scaleb(raw.adjusted() - 4)
    truncated = (raw // quantum) * quantum
    steps = truncated / tick
    rounded = (
        steps.to_integral_value(rounding="ROUND_FLOOR")
        if direction == "down"
        else steps.to_integral_value(rounding="ROUND_CEILING")
    )
    return rounded * tick


def align_up_to_lot(value: Decimal, lot_size: Decimal) -> Decimal:
    """Round ``value`` up to the nearest multiple of ``lot_size``."""
    if lot_size <= 0:
        return value
    steps = (value / lot_size).to_integral_value(rounding="ROUND_CEILING")
    return steps * lot_size


async def wait_for_status(
    adapter: HyperliquidAdapter,
    client_order_id: str,
    *wanted: OrderStatus,
    timeout: float = 30.0,
) -> OrderResult:
    """Poll ``get_order_by_client_id`` until ``status`` is one of ``wanted``."""
    deadline = asyncio.get_running_loop().time() + timeout
    last: OrderResult | None = None
    while True:
        last = await adapter.get_order_by_client_id(client_order_id)
        if last is not None and last.status in wanted:
            return last
        if asyncio.get_running_loop().time() >= deadline:
            seen = last.status if last is not None else None
            raise TimeoutError(
                f"Timed out waiting for {client_order_id} in "
                f"{[s.value for s in wanted]}; last seen {seen}"
            )
        await asyncio.sleep(0.5)


async def wait_for_fill(
    adapter: HyperliquidAdapter,
    client_order_id: str,
    *,
    timeout: float = 30.0,
) -> OrderResult:
    """Poll until the order reads back FILLED (full fill expected at market)."""
    return await wait_for_status(adapter, client_order_id, OrderStatus.FILLED, timeout=timeout)


async def wait_for_position(
    adapter: HyperliquidAdapter,
    coin: str,
    *,
    timeout: float = 30.0,
) -> Decimal:
    """Poll ``fetch_positions`` until a nonzero leg for ``coin`` appears."""
    deadline = asyncio.get_running_loop().time() + timeout
    qty = Decimal("0")
    while True:
        for leg in await adapter.fetch_positions():
            if leg.position_id == f"{coin}:oneWay":
                qty = leg.quantity
                break
        else:
            qty = Decimal("0")
        if qty != 0:
            return qty
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting for a {coin} position leg")
        await asyncio.sleep(0.5)


async def wait_for_flat(
    adapter: HyperliquidAdapter,
    *,
    timeout: float = 60.0,
) -> None:
    """Poll ``fetch_positions`` until no nonzero leg remains."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        legs = [leg for leg in await adapter.fetch_positions() if leg.quantity != 0]
        if not legs:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting for a flat book; left {legs!r}")
        await asyncio.sleep(0.5)


async def flatten_all(adapter: HyperliquidAdapter) -> None:
    """Best-effort full cleanup: cancel every open, close every leg.

    Detaches position TP/SL first (resting trigger legs block nothing but pollute
    the book), then cancels plain opens, then market-closes each nonzero leg.
    Each leg is closed on its OWN coin — a leg must never be closed with another
    instrument's symbol, which would size the close against the wrong market.
    Silent-tolerant — teardown must never fail the test it protects.
    """
    # Position TP/SL legs surface in fetch_open_orders keyed by raw cloid,
    # so the cancel sweep below detaches them — no separate detach call.
    with contextlib.suppress(Exception):
        opens = await adapter.fetch_open_orders()
        for client_order_id in list(opens):
            with contextlib.suppress(Exception):
                await adapter.cancel_order(client_order_id)
    with contextlib.suppress(Exception):
        legs = await adapter.fetch_positions()
        for leg in legs:
            qty = abs(leg.quantity)
            if qty <= 0:
                continue
            coin = (leg.position_id or "").split(":", 1)[0]
            if not coin:
                continue
            with contextlib.suppress(Exception):
                await adapter.place_order(
                    UnifiedOrder(
                        instrument=make_perp(coin),
                        order_type=OrderType.MARKET,
                        side=OrderSide.SELL if leg.quantity > 0 else OrderSide.BUY,
                        quantity=qty,
                        time_in_force=TimeInForce.IOC,
                        client_order_id=(f"hlit-flat-{uuid.uuid4().hex[:8]}-{coin}"),
                    )
                )


async def restore_defaults(
    adapter: HyperliquidAdapter,
    instrument: Instrument,
) -> None:
    """Return the venue leg to 1x cross and clear stored intents.

    Flatten first (the venue rejects cross<->isolated flips against an open
    leg), then set 1x + cross through the public setters, then remove the
    intents so no test residue survives.
    """
    from unified_trading_execution.hyperliquid import MarginMode

    await flatten_all(adapter)
    await wait_for_flat(adapter)
    with contextlib.suppress(Exception):
        await adapter.set_margin_mode(instrument, MarginMode.CROSS)
    with contextlib.suppress(Exception):
        await adapter.set_leverage(instrument, leverage=1)
    with contextlib.suppress(Exception):
        await adapter.remove_leverage(instrument)
    with contextlib.suppress(Exception):
        await adapter.remove_margin_mode(instrument)


def modify_price(
    client_order_id: str,
    price: Decimal,
) -> OrderModification:
    """A price-only ``OrderModification`` for a resting limit."""
    return OrderModification(client_order_id=client_order_id, price=price)


def utcnow() -> datetime:
    """Timezone-aware UTC now for ``since`` filters."""
    return datetime.now(UTC)


def assert_is_decimal(value: object, label: str) -> None:
    """Assert a value is a ``Decimal`` — never a ``float``."""
    assert isinstance(value, Decimal), f"{label} must be a Decimal, got {type(value).__name__}"

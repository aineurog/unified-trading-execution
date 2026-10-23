"""Pure trailing-stop and ATR calculations.  No I/O.

Every function here is a deterministic calculation over values the caller has
already obtained.  The manager owns sampling, persistence, and venue calls.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Literal

from unified_trading_execution.errors import (
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.trailing.types import (
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingOffsetType,
    TrailingSpec,
)
from unified_trading_execution.types.enums import OrderSide
from unified_trading_execution.types.market_data import Candle

_HUNDRED = Decimal("100")
_ONE = Decimal("1")


def _is_long(side: OrderSide) -> bool:
    return side is OrderSide.BUY


def _require_atr(atr: Decimal | None, purpose: str) -> Decimal:
    if atr is None:
        raise ValueError(f"{purpose} requires an ATR value")
    return atr


# ============================================================
# Wilder ATR
# ============================================================


def true_range(candle: Candle, previous_close: Decimal) -> Decimal:
    """True range of ``candle`` relative to the previous candle's close."""
    return max(
        candle.high - candle.low,
        abs(candle.high - previous_close),
        abs(candle.low - previous_close),
    )


def wilder_atr(candles: Sequence[Candle], period: int) -> Decimal:
    """Wilder ATR seeded by the first ``period`` true ranges.

    Requires ``period + 1`` closed candles, oldest first.  Fewer candles raise
    ``TrailingNotSupportedError``; the period is never shortened silently.
    """
    if period < 2:
        raise ValueError(f"ATR period must be >= 2, got {period}")

    required = period + 1
    if len(candles) < required:
        raise TrailingNotSupportedError(
            f"Wilder ATR({period}) needs {required} closed candles, got {len(candles)}"
        )

    ranges = [
        true_range(candles[index], candles[index - 1].close) for index in range(1, len(candles))
    ]
    atr = sum(ranges[:period], Decimal("0")) / Decimal(period)
    for value in ranges[period:]:
        atr = ((atr * (period - 1)) + value) / Decimal(period)
    return atr


# ============================================================
# Basis, activation, and candidate stop
# ============================================================


def update_best_basis(side: OrderSide, best_basis: Decimal | None, basis: Decimal) -> Decimal:
    """Advance the most favourable basis seen since attach."""
    if best_basis is None:
        return basis
    return max(best_basis, basis) if _is_long(side) else min(best_basis, basis)


def resolve_activation_price(
    spec: TrailingSpec, side: OrderSide, entry_price: Decimal
) -> Decimal | None:
    """Return the absolute activation price, or ``None`` for immediate."""
    if spec.activation is TrailingActivation.IMMEDIATE:
        return None
    if spec.activation is TrailingActivation.PRICE:
        price = spec.activation_price
        if price is None:
            raise ValueError("activation=PRICE requires activation_price")
        return price

    pct = spec.activation_profit_percent
    if pct is None:
        raise ValueError("activation=PROFIT_PERCENT requires activation_profit_percent")
    return profit_percent_activation_price(entry_price, side, pct)


def profit_percent_activation_price(
    entry_price: Decimal, side: OrderSide, percent: Decimal
) -> Decimal:
    """Resolve a profit target percentage to an absolute activation price."""
    ratio = percent / _HUNDRED
    factor = _ONE + ratio if _is_long(side) else _ONE - ratio
    return entry_price * factor


def is_activation_reached(
    side: OrderSide, basis: Decimal, activation_price: Decimal | None
) -> bool:
    """Whether ``basis`` has reached the activation price on the given side."""
    if activation_price is None:
        return True
    return basis >= activation_price if _is_long(side) else basis <= activation_price


def candidate_stop(
    *,
    side: OrderSide,
    best_basis: Decimal,
    spec: TrailingSpec,
    atr: Decimal | None = None,
) -> Decimal:
    """Trailing stop candidate for the current best basis."""
    if spec.offset_type is TrailingOffsetType.PRICE:
        return best_basis - spec.offset if _is_long(side) else best_basis + spec.offset

    if spec.offset_type is TrailingOffsetType.PERCENT:
        ratio = spec.offset / _HUNDRED
        factor = _ONE - ratio if _is_long(side) else _ONE + ratio
        return best_basis * factor

    value = _require_atr(atr, "an ATR trailing offset")
    distance = spec.offset * value
    return best_basis - distance if _is_long(side) else best_basis + distance


# ============================================================
# Static protection levels
# ============================================================


def initial_stop_price(
    level: ProtectionLevel,
    side: OrderSide,
    entry_price: Decimal,
    *,
    atr: Decimal | None = None,
) -> Decimal:
    """Resolve an initial stop level against the entry price."""
    if level.type is ProtectionLevelType.PRICE:
        return level.value

    if level.type is ProtectionLevelType.PERCENT:
        ratio = level.value / _HUNDRED
        factor = _ONE - ratio if _is_long(side) else _ONE + ratio
        return entry_price * factor

    value = _require_atr(atr, "an ATR initial stop")
    distance = level.value * value
    return entry_price - distance if _is_long(side) else entry_price + distance


def take_profit_price(
    level: ProtectionLevel,
    side: OrderSide,
    entry_price: Decimal,
    *,
    atr: Decimal | None = None,
) -> Decimal:
    """Resolve a static take-profit level against the entry price."""
    if level.type is ProtectionLevelType.PRICE:
        return level.value

    if level.type is ProtectionLevelType.PERCENT:
        ratio = level.value / _HUNDRED
        factor = _ONE + ratio if _is_long(side) else _ONE - ratio
        return entry_price * factor

    value = _require_atr(atr, "an ATR take profit")
    distance = level.value * value
    return entry_price + distance if _is_long(side) else entry_price - distance


def validate_level_direction(
    *,
    side: OrderSide,
    role: Literal["initial_stop", "take_profit"],
    level_price: Decimal,
    reference_price: Decimal,
) -> None:
    """Reject a protection level on the wrong side of the position.

    An initial stop is compared with the current basis and must sit below a
    long or above a short.  A take profit is compared with the entry price and
    must sit above a long or below a short.
    """
    if role == "initial_stop":
        valid = level_price < reference_price if _is_long(side) else level_price > reference_price
    else:
        valid = level_price > reference_price if _is_long(side) else level_price < reference_price
    if not valid:
        raise TrailingStateError(
            f"{role} {level_price} is on the wrong side of {reference_price} "
            f"for a {side.value} position"
        )


# ============================================================
# Rounding, minimum distance, and ratchet
# ============================================================


def quantize_price(value: Decimal, tick_size: Decimal, rounding: str) -> Decimal:
    """Quantize ``value`` to ``tick_size`` with an explicit rounding mode."""
    return (value / tick_size).to_integral_value(rounding=rounding) * tick_size


def quantize_stop(value: Decimal, tick_size: Decimal, side: OrderSide) -> Decimal:
    """Round a stop away from the market: long down, short up."""
    rounding = ROUND_DOWN if _is_long(side) else ROUND_UP
    return quantize_price(value, tick_size, rounding)


def quantize_take_profit(value: Decimal, tick_size: Decimal, side: OrderSide) -> Decimal:
    """Round a take profit in the profitable direction: long up, short down."""
    rounding = ROUND_UP if _is_long(side) else ROUND_DOWN
    return quantize_price(value, tick_size, rounding)


def respects_min_stop_distance(
    *,
    side: OrderSide,
    candidate: Decimal,
    basis: Decimal,
    min_stop_distance: Decimal | None,
) -> bool:
    """Check the candidate against the live basis, not the best basis."""
    if min_stop_distance is None:
        return True
    if _is_long(side):
        return candidate <= basis - min_stop_distance
    return candidate >= basis + min_stop_distance


def ratchet_accepts(
    *,
    side: OrderSide,
    current_stop: Decimal | None,
    candidate: Decimal,
    step: Decimal | None = None,
) -> bool:
    """Whether ``candidate`` tightens the stop and clears the step threshold."""
    if current_stop is None:
        return True

    if _is_long(side):
        if candidate <= current_stop:
            return False
        return step is None or candidate >= current_stop + step

    if candidate >= current_stop:
        return False
    return step is None or candidate <= current_stop - step

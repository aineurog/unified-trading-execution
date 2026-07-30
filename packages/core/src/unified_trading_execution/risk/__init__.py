"""Risk-check validator chain — Section 7.

Stateless, synchronous, ordered chain run before every order dispatch.
Fail-fast: the first failing validator rejects the order immediately;
later validators do not run.

All five validators are pure functions — they take pre-fetched data as
arguments and either return None (pass) or raise a typed exception (fail).
None of them performs I/O.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InsufficientBalanceError,
    InvalidSymbolError,
    RateLimitError,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import UnifiedOrder

logger = logging.getLogger(__name__)


# ============================================================
# Reference-price callback
# ============================================================


class ReferencePriceFn(Protocol):
    """User-injected callback that returns the current reference price for an instrument.

    Return None if no price is currently available (e.g. market closed).
    The engine does not own a market-data pipeline — this is an external dependency.
    """

    def __call__(self, instrument: Instrument) -> Decimal | None: ...


# ============================================================
# Configuration
# ============================================================


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """Validator thresholds — user-configurable per engine instance.

    Thresholds are the *only* thing configurable. Individual validators
    cannot be disabled in v1 — a deliberate safety stance for real capital.
    """

    # Step 2: size / notional bounds
    max_order_size: Decimal = Decimal("Inf")  # global default — no limit
    max_order_notional: Decimal = Decimal("Inf")
    per_instrument_max_size: dict[Instrument, Decimal] = field(default_factory=dict)
    per_instrument_max_notional: dict[Instrument, Decimal] = field(default_factory=dict)

    # Step 3: fat-finger price deviation
    max_price_deviation_pct: Decimal = Decimal("5.0")  # e.g. 5.0 = 5 %

    # Step 5: rate-limit budget override (None = use adapter's reported limits)
    rate_limit_budget_override: int | None = None


# ============================================================
# Validator 1 — Symbol / instrument validity
# ============================================================


def validate_symbol_validity(
    order: UnifiedOrder,
    instrument_spec: InstrumentSpec | None,
) -> None:
    """Confirm the instrument is known and tradable on the target adapter.

    Raises InvalidSymbolError if no InstrumentSpec has been fetched.
    """
    if instrument_spec is None:
        raise InvalidSymbolError(
            f"Instrument {order.instrument.symbol} is not tradable on this platform"
        )


# ============================================================
# Validator 2 — Order size / quantity bounds
# ============================================================


def validate_order_size(
    order: UnifiedOrder,
    instrument_spec: InstrumentSpec,
    config: RiskConfig,
) -> None:
    """Validate quantity against platform limits and user-configured caps.

    Checks:
      - Quantity is >= min_qty
      - Quantity is <= max_qty (platform)
      - Quantity is <= user-configured per-instrument cap (if set)
      - Quantity is <= user-configured global cap
      - Quantity respects qty_precision (no sub-tick amounts)
      - Notional (qty * price) is <= user-configured cap (if set)
    """
    qty = order.quantity
    spec = instrument_spec

    if qty < spec.min_qty:
        raise InvalidSymbolError(
            f"Order quantity {qty} is below minimum {spec.min_qty} "
            f"for {order.instrument.symbol}"
        )
    if qty > spec.max_qty:
        raise InvalidSymbolError(
            f"Order quantity {qty} exceeds platform maximum {spec.max_qty} "
            f"for {order.instrument.symbol}"
        )

    # Precision check
    qty_tick = Decimal("1") / (10**spec.qty_precision)
    if qty % qty_tick != 0:
        raise InvalidSymbolError(
            f"Order quantity {qty} violates qty_precision={spec.qty_precision} "
            f"(tick={qty_tick}) for {order.instrument.symbol}"
        )

    # Per-instrument user cap
    per_inst_max = config.per_instrument_max_size.get(order.instrument)
    if per_inst_max is not None and qty > per_inst_max:
        raise InvalidSymbolError(
            f"Order quantity {qty} exceeds configured max {per_inst_max} "
            f"for {order.instrument.symbol}"
        )

    # Global user cap
    if qty > config.max_order_size:
        raise InvalidSymbolError(f"Order quantity {qty} exceeds global max {config.max_order_size}")

    # Notional check (if order has a price)
    if order.price is not None:
        notional = qty * order.price
        per_inst_notional = config.per_instrument_max_notional.get(order.instrument)
        if per_inst_notional is not None and notional > per_inst_notional:
            raise InvalidSymbolError(
                f"Order notional {notional} exceeds configured max {per_inst_notional} "
                f"for {order.instrument.symbol}"
            )
        if notional > config.max_order_notional:
            raise InvalidSymbolError(
                f"Order notional {notional} exceeds global max {config.max_order_notional}"
            )

    # Minimum notional
    if order.price is not None:
        notional = qty * order.price
        if notional < spec.min_notional:
            raise InvalidSymbolError(
                f"Order notional {notional} is below minimum {spec.min_notional} "
                f"for {order.instrument.symbol}"
            )


# ============================================================
# Validator 3 — Price sanity (fat-finger protection)
# ============================================================


def _check_price_deviation(
    label: str,
    price: Decimal,
    reference_price: Decimal,
    max_pct: Decimal,
    instrument: Instrument,
) -> None:
    deviation = abs(price - reference_price) / reference_price * 100
    if deviation > max_pct:
        raise InvalidSymbolError(
            f"{label} {price} deviates {deviation:.2f}% from reference "
            f"{reference_price} for {instrument.symbol} "
            f"(max allowed: {max_pct}%)"
        )


def validate_price_sanity(
    order: UnifiedOrder,
    reference_price: Decimal | None,
    config: RiskConfig,
) -> None:
    """Validate order prices against a reference price to catch fat-finger errors.

    If no reference price is available, this validator skips with a logged
    warning rather than blocking the order (Section 7, step 3).

    Applies to: the order's limit/stop price and any TP/SL trigger prices.
    TP/SL orders are NOT special-cased — they go through the same check.
    """
    if reference_price is None:
        logger.warning(
            "Price sanity check skipped for %s — no reference price available",
            order.instrument.symbol,
        )
        return

    max_pct = config.max_price_deviation_pct

    if order.price is not None:
        _check_price_deviation("Price", order.price, reference_price, max_pct, order.instrument)

    if order.stop_price is not None:
        _check_price_deviation(
            "Stop price",
            order.stop_price,
            reference_price,
            max_pct,
            order.instrument,
        )

    if order.take_profit is not None:
        _check_price_deviation(
            "TP trigger",
            order.take_profit.trigger_price,
            reference_price,
            max_pct,
            order.instrument,
        )
        if order.take_profit.limit_price is not None:
            _check_price_deviation(
                "TP limit",
                order.take_profit.limit_price,
                reference_price,
                max_pct,
                order.instrument,
            )

    if order.stop_loss is not None:
        _check_price_deviation(
            "SL trigger",
            order.stop_loss.trigger_price,
            reference_price,
            max_pct,
            order.instrument,
        )
        if order.stop_loss.limit_price is not None:
            _check_price_deviation(
                "SL limit",
                order.stop_loss.limit_price,
                reference_price,
                max_pct,
                order.instrument,
            )


# ============================================================
# Validator 4 — Duplicate / idempotent submission check
# ============================================================


def validate_no_duplicate(
    client_order_id: str,
    known_order_ids: frozenset[str],
) -> None:
    """Reject if the client_order_id is already known (active or terminal).

    Genuine timeouts are handled via the status-check-and-retry path
    (Section 9.2), not by blind resubmission through this validator.
    """
    if client_order_id in known_order_ids:
        raise DuplicateOrderIdError(f"client_order_id '{client_order_id}' is already in use")


# ============================================================
# Validator 5 — Rate-limit self-throttling
# ============================================================


def validate_rate_limit(
    remaining_budget: int,
) -> None:
    """Check the engine's tracked call budget before dispatch.

    The engine must never fire a request purely to let the platform itself
    reject it. Budget is tracked by the engine from adapter-reported limits.
    """
    if remaining_budget <= 0:
        raise RateLimitError("Rate-limit budget exhausted — cannot dispatch")


# ============================================================
# Chain runner
# ============================================================


def run_risk_checks(
    order: UnifiedOrder,
    *,
    instrument_spec: InstrumentSpec | None,
    reference_price: Decimal | None,
    known_order_ids: frozenset[str],
    remaining_budget: int,
    config: RiskConfig | None = None,
) -> None:
    """Run all five validators in order. Fail-fast — raises on first failure.

    Args:
        order: The validated UnifiedOrder (client_order_id must be set).
        instrument_spec: Cached InstrumentSpec from the adapter, or None.
        reference_price: Current reference price, or None if unavailable.
        known_order_ids: Active and terminal client_order_ids to check for duplicates.
        remaining_budget: Tracked remaining calls in the current rate-limit window.
        config: Threshold configuration (uses defaults when None).

    Raises:
        InvalidSymbolError, DuplicateOrderIdError, RateLimitError
    """
    cfg = config or RiskConfig()

    # Step 1: Symbol validity (cheapest check)
    validate_symbol_validity(order, instrument_spec)

    # Step 2: Order size / quantity bounds
    validate_order_size(order, instrument_spec, cfg)  # type: ignore[arg-type]  # spec non-None after step 1

    # Step 3: Price sanity (fat-finger protection)
    validate_price_sanity(order, reference_price, cfg)

    # Step 4: Duplicate / idempotent submission
    if order.client_order_id is None:
        raise InvalidSymbolError("client_order_id must be set before risk checks")
    validate_no_duplicate(order.client_order_id, known_order_ids)

    # Step 5: Rate-limit self-throttling (last, right before dispatch)
    validate_rate_limit(remaining_budget)

"""Trailing-stop data types and enums.

This is a **leaf module**: it may import only the standard library and
``unified_trading_execution.types``.  It must never import ``adapter``,
``engine``, ``state``, or ``events`` — adapters import these types, never the
reverse.  The import-linter contract ``trailing-types-is-leaf`` and the
layering test enforce this.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from unified_trading_execution.types.enums import OrderSide
from unified_trading_execution.types.instrument import Instrument


class TrailingMode(StrEnum):
    """How a trailing state is executed.

    ``AUTO`` is request-only.  A resolved :class:`TrailingState` always carries
    ``NATIVE`` or ``SYNTHETIC``.
    """

    AUTO = "auto"
    NATIVE = "native"
    SYNTHETIC = "synthetic"


class TrailingOffsetType(StrEnum):
    """Unit of the trailing distance."""

    PRICE = "price"
    PERCENT = "percent"
    ATR = "atr"


class ProtectionLevelType(StrEnum):
    """Unit of a static protection level (initial stop or take profit)."""

    PRICE = "price"
    PERCENT = "percent"
    ATR = "atr"


class TrailingBasis(StrEnum):
    """Price field the trail is measured from."""

    LAST = "last"
    MARK = "mark"
    MID = "mid"
    BID = "bid"
    ASK = "ask"


class TrailingActivation(StrEnum):
    """When the stop starts to trail."""

    IMMEDIATE = "immediate"
    PRICE = "price"
    PROFIT_PERCENT = "profit_percent"


class TrailingPhase(StrEnum):
    """Lifecycle phase of one trailing state.  Closed set."""

    ARMING = "arming"
    ARMED = "armed"
    TRIGGERED = "triggered"
    DONE = "done"
    CANCELLED = "cancelled"
    DEGRADED = "degraded"
    FAILED = "failed"


class CandleTimeframe(StrEnum):
    """Closed candle timeframes available to ATR.  Closed set."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"


# Phases that keep the active-uniqueness index busy and allow cancel().  The
# partial SQL index in migration 011 uses the same set.
ACTIVE_TRAILING_PHASES: frozenset[TrailingPhase] = frozenset(
    {
        TrailingPhase.ARMING,
        TrailingPhase.ARMED,
        TrailingPhase.TRIGGERED,
        TrailingPhase.DEGRADED,
    }
)

# Phases from which cancel() is not allowed.
TERMINAL_TRAILING_PHASES: frozenset[TrailingPhase] = frozenset(
    {
        TrailingPhase.DONE,
        TrailingPhase.CANCELLED,
        TrailingPhase.FAILED,
    }
)


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware (UTC)")


@dataclass(frozen=True, slots=True)
class ProtectionLevel:
    """One static protection level computed once at attach.

    ``PRICE`` is an absolute price, ``PERCENT`` a distance from the entry price,
    and ``ATR`` a multiple of the ATR measured from the entry price.  The
    percent-below-100 and side-direction checks need runtime data and are done
    by the manager at attach, not here.
    """

    type: ProtectionLevelType
    value: Decimal

    def __post_init__(self) -> None:
        if self.value <= 0:
            raise ValueError(f"ProtectionLevel.value must be > 0, got {self.value}")


def _is_atr(level: ProtectionLevel | None) -> bool:
    return level is not None and level.type is ProtectionLevelType.ATR


@dataclass(frozen=True, slots=True)
class TrailingSpec:
    """Validated request parameters for one trailing state.

    Every rule that only depends on these fields is enforced in
    ``__post_init__``.  Rules that need the position side, entry price, or a
    sampled basis are checked by the manager at attach.

    An ``initial_stop`` places protection at attach even when activation is
    not immediate.  Without one, no stop exists until activation is met.  A
    ``take_profit`` is a static level, written at attach and never trailed.
    An ATR initial stop is measured from the entry price; an ATR trailing
    distance is measured from the best basis seen since attach.

    A ``PERCENT`` initial stop must be below 100, and its direction relative
    to the basis is checked at attach.  A ``PERCENT`` take profit has no
    upper bound.
    """

    offset_type: TrailingOffsetType
    offset: Decimal
    basis: TrailingBasis | None = None
    activation: TrailingActivation = TrailingActivation.IMMEDIATE
    activation_price: Decimal | None = None
    activation_profit_percent: Decimal | None = None
    initial_stop: ProtectionLevel | None = None
    take_profit: ProtectionLevel | None = None
    step: Decimal | None = None
    atr_period: int | None = None
    atr_timeframe: CandleTimeframe | None = None

    def __post_init__(self) -> None:
        if self.offset <= 0:
            raise ValueError(f"offset must be > 0, got {self.offset}")
        if self.offset_type is TrailingOffsetType.PERCENT and not (
            Decimal("0") < self.offset < Decimal("100")
        ):
            raise ValueError(f"percent offset must be in (0, 100), got {self.offset}")
        if self.step is not None and self.step < 0:
            raise ValueError(f"step must be >= 0, got {self.step}")

        self._validate_activation()
        self._validate_atr()

    def _validate_activation(self) -> None:
        if self.activation is TrailingActivation.IMMEDIATE:
            if self.activation_price is not None or self.activation_profit_percent is not None:
                raise ValueError(
                    "activation=IMMEDIATE forbids activation_price and activation_profit_percent"
                )
        elif self.activation is TrailingActivation.PRICE:
            if self.activation_price is None or self.activation_price <= 0:
                raise ValueError("activation=PRICE requires activation_price > 0")
            if self.activation_profit_percent is not None:
                raise ValueError("activation=PRICE forbids activation_profit_percent")
        else:  # PROFIT_PERCENT
            pct = self.activation_profit_percent
            if pct is None or not (Decimal("0") < pct < Decimal("100")):
                raise ValueError(
                    "activation=PROFIT_PERCENT requires 0 < "
                    f"activation_profit_percent < 100, got {pct}"
                )
            if self.activation_price is not None:
                raise ValueError("activation=PROFIT_PERCENT forbids activation_price")

    def _validate_atr(self) -> None:
        uses_atr = (
            self.offset_type is TrailingOffsetType.ATR
            or _is_atr(self.initial_stop)
            or _is_atr(self.take_profit)
        )
        atr_set = self.atr_period is not None and self.atr_timeframe is not None
        if uses_atr and not atr_set:
            raise ValueError("ATR values require both atr_period and atr_timeframe")
        if not uses_atr and (self.atr_period is not None or self.atr_timeframe is not None):
            raise ValueError("atr_period/atr_timeframe are set but no ATR value is requested")
        if self.atr_period is not None and self.atr_period < 2:
            raise ValueError(f"atr_period must be >= 2, got {self.atr_period}")


@dataclass(frozen=True, slots=True)
class TrailingRequest:
    """One attach request.  Position resolution happens in the manager."""

    instrument: Instrument
    spec: TrailingSpec
    position_id: str | None = None
    mode: TrailingMode = TrailingMode.AUTO
    take_over_existing_protection: bool = False


@dataclass(frozen=True, slots=True)
class NativeTrailingRequest:
    """A native trailing-stop placement or replacement for one position leg."""

    instrument: Instrument
    position_id: str
    side: OrderSide
    quantity: Decimal
    offset: Decimal
    offset_type: TrailingOffsetType
    basis: TrailingBasis | None
    activation_price: Decimal | None
    client_id: str


@dataclass(frozen=True, slots=True)
class NativeTrailingResult:
    """The venue's view of a native trailing stop."""

    native_id: str
    confirmed_offset: Decimal | None
    current_stop: Decimal | None
    activated: bool
    position_scoped: bool


@dataclass(frozen=True, slots=True)
class TrailingConfig:
    """Runtime tuning for polling, ATR refresh, and failure policy."""

    poll_interval_seconds: float = 1.0
    max_concurrent_fetches: int = 4
    fetch_failure_threshold: int = 5
    protection_failure_threshold: int = 3
    rearm_on_reconnect: bool = True
    candle_settle_seconds: float = 2.0
    atr_stale_after_candles: int = 2
    publish_ticker_events: bool = False

    def __post_init__(self) -> None:
        if not math.isfinite(self.poll_interval_seconds) or self.poll_interval_seconds < 0.1:
            raise ValueError("poll_interval_seconds must be a finite value >= 0.1")
        if self.max_concurrent_fetches < 1:
            raise ValueError("max_concurrent_fetches must be >= 1")
        if self.fetch_failure_threshold < 1:
            raise ValueError("fetch_failure_threshold must be >= 1")
        if self.protection_failure_threshold < 1:
            raise ValueError("protection_failure_threshold must be >= 1")
        if not math.isfinite(self.candle_settle_seconds) or self.candle_settle_seconds < 0:
            raise ValueError("candle_settle_seconds must be a finite value >= 0")
        if self.atr_stale_after_candles < 1:
            raise ValueError("atr_stale_after_candles must be >= 1")


@dataclass(frozen=True, slots=True)
class TrailingState:
    """Persisted state for one trailing position leg.

    This is a record, not a live object: it never holds a strategy, callback,
    adapter handle, or lock.  ``stop_owned`` and ``take_profit_owned`` describe
    static levels only; a native trail is identified by ``native_id`` and does
    not set either flag.

    ``activated`` records that the manager observed the activation condition.
    In native mode it also means the venue accepted the trailing configuration.
    """

    trailing_id: str
    instrument: Instrument
    position_id: str
    mode: TrailingMode
    phase: TrailingPhase
    spec: TrailingSpec
    side: OrderSide
    entry_price: Decimal
    quantity: Decimal
    created_at: datetime
    updated_at: datetime

    position_opened_at: datetime | None = None
    routing_reason: str | None = None
    resolved_basis: TrailingBasis | None = None
    protection_trigger_basis: TrailingBasis | None = None
    current_stop: Decimal | None = None
    current_take_profit: Decimal | None = None
    stop_owned: bool = False
    take_profit_owned: bool = False
    previous_stop_loss: Decimal | None = None
    previous_take_profit: Decimal | None = None
    best_basis: Decimal | None = None
    resolved_activation_price: Decimal | None = None
    activated: bool = False
    atr_value: Decimal | None = None
    atr_as_of: datetime | None = None
    native_id: str | None = None
    last_reprice_at: datetime | None = None
    reprice_count: int = 0
    last_error: str | None = None
    failure_count: int = 0

    def __post_init__(self) -> None:
        if self.mode is TrailingMode.AUTO:
            raise ValueError("TrailingState.mode must be resolved (never AUTO)")
        if self.quantity < 0:
            raise ValueError(f"quantity must be an absolute size >= 0, got {self.quantity}")
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        for field_name in ("position_opened_at", "atr_as_of", "last_reprice_at"):
            value = getattr(self, field_name)
            if value is not None:
                _require_aware(value, field_name)

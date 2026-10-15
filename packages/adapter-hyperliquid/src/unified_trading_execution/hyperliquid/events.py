"""Hyperliquid adapter-specific event types.

These live in the adapter package, not core, because they carry
platform-specific payloads (per-asset leverage, per-asset margin mode).
They are published on the shared ``EventBus`` so engine-level subscribers
(e.g. reconciliation) can observe them without importing adapter code.

Shapes mirror ``unified_trading_execution.bybit.events`` where the venues
agree, and diverge where Hyperliquid does: leverage is a single integer
(no buy/sell split), and margin mode is per asset (not account-wide), so
the mode events carry ``instrument`` where Bybit's do not.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from unified_trading_execution.events import Event
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.types.instrument import Instrument


@dataclass(frozen=True, slots=True)
class LeverageAppliedEvent(Event):
    """Stored leverage intent was successfully applied to the platform."""

    instrument: Instrument
    leverage: int


@dataclass(frozen=True, slots=True)
class LeverageApplyFailedEvent(Event):
    """Stored leverage intent could not be applied to the platform."""

    instrument: Instrument
    leverage: int
    reason: str


@dataclass(frozen=True, slots=True)
class LeverageDriftEvent(Event):
    """Platform leverage differs from stored intent."""

    instrument: Instrument
    stored: int
    platform: int
    action_taken: Literal["reapplied", "notified", "halted"]


@dataclass(frozen=True, slots=True)
class MarginModeChangedEvent(Event):
    """Per-asset margin mode was changed.

    Unlike Bybit's account-wide mode event, this carries ``instrument``.
    ``previous`` is None when no mode was known (no stored intent, no leg).
    """

    instrument: Instrument
    previous: MarginMode | None
    current: MarginMode


@dataclass(frozen=True, slots=True)
class MarginModeDriftEvent(Event):
    """Platform margin mode differs from stored intent."""

    instrument: Instrument
    stored: MarginMode
    platform: MarginMode
    action_taken: Literal["reapplied", "notified", "halted"]


@dataclass(frozen=True, slots=True)
class MarginModeApplyFailedEvent(Event):
    """Stored margin-mode intent could not be applied to the platform."""

    instrument: Instrument
    mode: MarginMode
    reason: str

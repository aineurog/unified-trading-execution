"""Bybit adapter-specific event types (Section 6).

These live in the adapter package, not core, because they carry
platform-specific payloads (leverage, margin mode).  They are published on
the shared ``EventBus`` so engine-level subscribers (e.g. reconciliation)
can observe them without importing adapter code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from unified_trading_execution.bybit.margin import MarginMode
from unified_trading_execution.events import Event
from unified_trading_execution.types.instrument import Instrument


@dataclass(frozen=True, slots=True)
class LeverageAppliedEvent(Event):
    """Stored leverage intent was successfully applied to the platform."""

    instrument: Instrument
    buy_leverage: int
    sell_leverage: int


@dataclass(frozen=True, slots=True)
class LeverageApplyFailedEvent(Event):
    """Stored leverage intent could not be applied to the platform."""

    instrument: Instrument
    buy_leverage: int
    sell_leverage: int
    reason: str


@dataclass(frozen=True, slots=True)
class LeverageDriftEvent(Event):
    """Platform leverage differs from stored intent."""

    instrument: Instrument
    stored_buy: int
    stored_sell: int
    platform_buy: int
    platform_sell: int
    action_taken: Literal["reapplied", "notified", "halted"]


@dataclass(frozen=True, slots=True)
class MarginModeChangedEvent(Event):
    """Margin mode was changed for an instrument."""

    instrument: Instrument
    previous: MarginMode | None
    current: MarginMode

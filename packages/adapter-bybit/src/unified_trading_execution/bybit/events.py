"""Bybit adapter-specific event types (Section 6).

These live in the adapter package, not core, because they carry
platform-specific payloads (leverage, margin mode).  They are published on
the shared ``EventBus`` so engine-level subscribers (e.g. reconciliation)
can observe them without importing adapter code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from unified_trading_execution.bybit.enums import MarginMode, PositionMode
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
    """Account-wide margin mode was changed.

    Bybit UTA margin mode is account-wide — no instrument field.
    """

    previous: MarginMode | None
    current: MarginMode


@dataclass(frozen=True, slots=True)
class PositionModeAppliedEvent(Event):
    """Stored position mode intent was successfully applied to the platform."""

    instrument: Instrument
    mode: PositionMode


@dataclass(frozen=True, slots=True)
class PositionModeApplyFailedEvent(Event):
    """Stored position mode intent could not be applied on connect."""

    instrument: Instrument
    mode: PositionMode
    reason: str


@dataclass(frozen=True, slots=True)
class PositionModeDriftEvent(Event):
    """Platform position mode differs from stored intent."""

    instrument: Instrument
    stored: PositionMode
    platform: PositionMode
    action_taken: Literal["reapplied", "notified", "halted"]

"""Binance Futures configuration events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from unified_trading_execution.binance.enums import MarginMode, PositionMode
from unified_trading_execution.events import Event
from unified_trading_execution.types.instrument import Instrument

FuturesSetting = Literal["leverage", "margin_mode", "position_mode"]
DriftAction = Literal["reapplied", "notified", "halted"]


@dataclass(frozen=True, slots=True)
class FuturesSettingAppliedEvent(Event):
    setting: FuturesSetting
    instrument: Instrument | None
    value: str


@dataclass(frozen=True, slots=True)
class FuturesSettingApplyFailedEvent(Event):
    setting: FuturesSetting
    instrument: Instrument | None
    value: str
    reason: str


@dataclass(frozen=True, slots=True)
class FuturesSettingDriftEvent(Event):
    setting: FuturesSetting
    instrument: Instrument | None
    stored_value: str
    platform_value: str
    action_taken: DriftAction


@dataclass(frozen=True, slots=True)
class FuturesMarginModeChangedEvent(Event):
    instrument: Instrument
    previous: MarginMode | None
    current: MarginMode


@dataclass(frozen=True, slots=True)
class FuturesPositionModeChangedEvent(Event):
    previous: PositionMode | None
    current: PositionMode

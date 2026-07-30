"""Halt state machine — Section 6.4.

States: ACTIVE → HALTED → CLEARED.
Scope: per-instrument or account-wide.

Configurable per adapter instance:
  - auto_halt_enabled (default True)
  - closing_orders_permitted (default True)
  - clear_mode: AUTOMATIC or MANUAL (default AUTOMATIC)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from unified_trading_execution.types.enums import HaltClearMode
from unified_trading_execution.types.instrument import Instrument


@dataclass
class HaltConfig:
    """Per-adapter configuration for halt behaviour."""

    auto_halt_enabled: bool = True
    closing_orders_permitted: bool = True
    clear_mode: HaltClearMode = HaltClearMode.AUTOMATIC


@dataclass
class _HaltEntry:
    scope: Literal["instrument", "account"]
    instrument: Instrument | None
    reason: str
    detail: str


class HaltStateMachine:
    """Tracks which instruments/accounts are halted and enforces halt rules.

    Not thread-safe — single-threaded asyncio usage.
    """

    def __init__(self, config: HaltConfig | None = None) -> None:
        self._config = config or HaltConfig()
        self._instrument_halts: dict[str, _HaltEntry] = {}  # keyed by symbol
        self._account_halted: _HaltEntry | None = None

    @property
    def config(self) -> HaltConfig:
        return self._config

    # ---- Queries ----

    def is_account_halted(self) -> bool:
        return self._account_halted is not None

    def is_instrument_halted(self, instrument: Instrument) -> bool:
        return instrument.symbol in self._instrument_halts

    def is_halted(self, instrument: Instrument | None = None) -> bool:
        """Check if either the specific instrument or the account is halted."""
        if self._account_halted is not None:
            return True
        if instrument is not None and instrument.symbol in self._instrument_halts:
            return True
        return False

    def can_place_order(self, instrument: Instrument, reduce_only: bool = False) -> bool:
        """Whether a new order can be placed for the given instrument.

        If closing_orders_permitted is True, reduce-only orders are always allowed.
        """
        if not self.is_halted(instrument):
            return True
        if reduce_only and self._config.closing_orders_permitted:
            return True
        return False

    def active_halts(self) -> list[_HaltEntry]:
        """All currently active halts (instrument + account)."""
        result: list[_HaltEntry] = list(self._instrument_halts.values())
        if self._account_halted is not None:
            result.append(self._account_halted)
        return result

    # ---- Mutations ----

    def enter_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        reason: str,
        detail: str,
    ) -> bool:
        """Enter a halt. Returns True if this is a new halt (state changed)."""
        if not self._config.auto_halt_enabled:
            return False

        entry = _HaltEntry(scope=scope, instrument=instrument, reason=reason, detail=detail)

        if scope == "account":
            if self._account_halted is not None:
                return False  # already halted
            self._account_halted = entry
            return True

        if instrument is None:
            raise ValueError("instrument required for instrument-scoped halt")
        key = instrument.symbol
        if key in self._instrument_halts:
            return False  # already halted
        self._instrument_halts[key] = entry
        return True

    def try_clear_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None = None,
        *,
        reconciliation_is_clean: bool = False,
        manual_clear: bool = False,
    ) -> bool:
        """Attempt to clear a halt. Returns True if the halt was cleared.

        In AUTOMATIC mode: clears when reconciliation_is_clean is True.
        In MANUAL mode: clears only when manual_clear is True.
        """
        entry = None
        if scope == "account":
            entry = self._account_halted
        elif instrument is not None:
            entry = self._instrument_halts.get(instrument.symbol)

        if entry is None:
            return False  # not halted

        if self._config.clear_mode == HaltClearMode.AUTOMATIC:
            if not reconciliation_is_clean:
                return False
        else:  # MANUAL
            if not manual_clear:
                return False

        if scope == "account":
            self._account_halted = None
        elif instrument is not None:
            del self._instrument_halts[instrument.symbol]

        return True

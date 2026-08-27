"""Position and Balance — the state mirror's core data types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from unified_trading_execution.types.instrument import Instrument


@dataclass(frozen=True, slots=True)
class Position:
    """A single position leg. Positive quantity = long, negative = short.

    ``position_id`` is the platform-assigned identifier for this specific leg
    (MT5 ticket, Bybit positionIdx, cTrader positionId), unique within the
    instrument.  ``None`` for a derived/synthetic position.
    """

    instrument: Instrument
    quantity: Decimal
    average_entry_price: Decimal
    updated_at: datetime  # UTC, timezone-aware
    position_id: str | None = None  # platform leg id, unique within the instrument

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware (UTC)")


@dataclass(frozen=True, slots=True)
class Balance:
    """Current balance in a single currency. total == free + used enforced at construction."""

    currency: str
    free: Decimal  # available for new orders
    used: Decimal  # locked in open orders / margin
    total: Decimal  # free + used
    updated_at: datetime  # UTC, timezone-aware

    def __post_init__(self) -> None:
        if self.free + self.used != self.total:
            raise ValueError(
                f"Balance invariant violated: free ({self.free}) + used ({self.used}) "
                f"!= total ({self.total}) for currency {self.currency}"
            )
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware (UTC)")

    @property
    def available_ratio(self) -> Decimal:
        """Fraction of balance that is free for new orders. 1.0 = all available."""
        if self.total == 0:
            return Decimal("1.0")
        return self.free / self.total

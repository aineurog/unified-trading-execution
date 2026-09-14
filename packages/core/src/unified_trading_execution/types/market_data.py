"""Market-data types — a single-instrument price snapshot (Ticker)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Ticker:
    """A single-instrument price snapshot.

    Each field is ``None`` when the platform does not provide it: spot has no
    ``mark``, MT5/forex have no ``mark``, and a market that has not traded yet
    has no ``last``.  Adapters return ``None`` (rather than an all-``None``
    ``Ticker``) when there is genuinely no quote for the instrument.
    """

    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    mark: Decimal | None = None

    @property
    def mid(self) -> Decimal | None:
        """Best-effort mid: ``(bid + ask) / 2``, else ``last``, else ``mark``."""
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / Decimal("2")
        return self.last if self.last is not None else self.mark

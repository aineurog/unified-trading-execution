"""Market-data types — a price snapshot (Ticker) and a closed OHLC candle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True, slots=True)
class Candle:
    """One closed OHLC candle, used for ATR.

    ``open_time`` and ``close_time`` are timezone-aware.  The forming bar is
    never represented by this type: adapters return closed candles only.
    """

    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        if self.open_time.tzinfo is None or self.close_time.tzinfo is None:
            raise ValueError("candle times must be timezone-aware (UTC)")
        if self.close_time <= self.open_time:
            raise ValueError("close_time must be after open_time")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")

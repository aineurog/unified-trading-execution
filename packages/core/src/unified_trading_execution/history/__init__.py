"""History accessors — read-only, filterable queries over the audit trail (Section 10.2).

Each function takes a StateStore instance and optional filters (instrument,
time range). They are usable without an Engine — only a connection to the
state store is required. The Engine class exposes these same queries as
methods that delegate to the StateStore directly.
"""

from __future__ import annotations

from datetime import datetime

from unified_trading_execution.events import HaltEvent, ReconciliationEvent
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


async def query_order_history(
    store: StateStore,
    *,
    instrument: Instrument | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[OrderRecord]:
    """Return order records filtered by instrument and/or time range.

    Filters are conjunctive — all supplied criteria must match. When no
    filters are given, returns the most recent orders up to *limit*.
    """
    return await store.query_orders(
        instrument=instrument,
        start=start,
        end=end,
        limit=limit,
    )


async def query_fill_history(
    store: StateStore,
    *,
    instrument: Instrument | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[FillRecord]:
    """Return fill records filtered by instrument and/or time range."""
    return await store.query_fills(
        instrument=instrument,
        start=start,
        end=end,
        limit=limit,
    )


async def query_position_history(
    store: StateStore,
    *,
    instrument: Instrument | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[Position]:
    """Return position snapshots filtered by instrument and/or time range."""
    return await store.query_positions(
        instrument=instrument,
        start=start,
        end=end,
        limit=limit,
    )


async def query_balance_history(
    store: StateStore,
    *,
    currency: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[Balance]:
    """Return balance snapshots filtered by currency and/or time range."""
    return await store.query_balances(
        currency=currency,
        start=start,
        end=end,
        limit=limit,
    )


async def query_reconciliation_events(
    store: StateStore,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[ReconciliationEvent]:
    """Return reconciliation events within the given time range."""
    return await store.query_reconciliation_events(
        start=start,
        end=end,
        limit=limit,
    )


async def query_halt_events(
    store: StateStore,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[HaltEvent]:
    """Return halt entry/clear events within the given time range."""
    return await store.query_halt_events(
        start=start,
        end=end,
        limit=limit,
    )


__all__ = [
    "query_balance_history",
    "query_fill_history",
    "query_halt_events",
    "query_order_history",
    "query_position_history",
    "query_reconciliation_events",
]

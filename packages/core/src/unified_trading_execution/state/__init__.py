"""StateStore interface — the pluggable backend for the state mirror.

The default v1 implementation is SQLite via aiosqlite. The interface is
designed so a future Postgres or Redis backend can be swapped in with
zero changes to core logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from unified_trading_execution.events import HaltEvent, ReconciliationEvent
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


class StateStore(ABC):
    """Pluggable storage backend for the state mirror and audit trail.

    All writes to the state store that form a logical group (e.g., a fill
    that also updates a position) occur within a single transaction.
    """

    # ---- Single-record write ----

    @abstractmethod
    async def upsert_position(self, position: Position) -> None:
        """Insert or update a single position record."""
        ...

    @abstractmethod
    async def upsert_balance(self, balance: Balance) -> None:
        """Insert or update a single balance record."""
        ...

    @abstractmethod
    async def upsert_order(self, order: OrderRecord) -> None:
        """Insert or update a single order record."""
        ...

    # ---- Audit trail write (append-only) ----

    @abstractmethod
    async def write_audit_event(self, event: "AuditEvent") -> None:  # noqa: F821 — circular import avoided
        """Append a single audit event. Must reject attempts to overwrite."""
        ...

    # ---- Single-record read ----

    @abstractmethod
    async def get_position(self, instrument: Instrument) -> Position | None:
        """Read a single position record, or None."""
        ...

    @abstractmethod
    async def get_balance(self, currency: str) -> Balance | None:
        """Read a single balance record, or None."""
        ...

    @abstractmethod
    async def get_order(self, client_order_id: str) -> OrderRecord | None:
        """Read a single order record, or None."""
        ...

    # ---- Filtered queries ----

    @abstractmethod
    async def query_orders(self, *, instrument: Instrument | None = None,
                           start: datetime | None = None,
                           end: datetime | None = None,
                           limit: int = 1000) -> list[OrderRecord]:
        """Query orders, optionally filtered by instrument and/or time range."""
        ...

    @abstractmethod
    async def query_fills(self, *, instrument: Instrument | None = None,
                          start: datetime | None = None,
                          end: datetime | None = None,
                          limit: int = 1000) -> list[FillRecord]:
        """Query fills, optionally filtered by instrument and/or time range."""
        ...

    @abstractmethod
    async def query_positions(self, *, instrument: Instrument | None = None,
                              start: datetime | None = None,
                              end: datetime | None = None,
                              limit: int = 1000) -> list[Position]:
        """Query position history, optionally filtered by instrument and/or time range."""
        ...

    @abstractmethod
    async def query_balances(self, *, currency: str | None = None,
                             start: datetime | None = None,
                             end: datetime | None = None,
                             limit: int = 1000) -> list[Balance]:
        """Query balance history, optionally filtered by currency and/or time range."""
        ...

    @abstractmethod
    async def query_reconciliation_events(self, *, start: datetime | None = None,
                                          end: datetime | None = None,
                                          limit: int = 1000) -> list[ReconciliationEvent]:
        """Query reconciliation events, optionally filtered by time range."""
        ...

    @abstractmethod
    async def query_halt_events(self, *, start: datetime | None = None,
                                end: datetime | None = None,
                                limit: int = 1000) -> list[HaltEvent]:
        """Query halt entry/clear events, optionally filtered by time range."""
        ...

    # ---- Lifecycle ----

    @abstractmethod
    async def initialize(self) -> None:
        """Create tables if needed and run any pending schema migrations."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Flush pending writes and close the connection."""
        ...

    @property
    @abstractmethod
    def path(self) -> str:
        """Resolved filesystem path for file-based backends; empty string otherwise."""
        ...


class SQLiteStateStore(StateStore):
    """Default v1 StateStore implementation backed by SQLite via aiosqlite.

    Section 6.2 of the requirements: file-based, zero external infrastructure,
    WAL journal mode, synchronous=NORMAL, single writer per instance.

    Construction:
        store = SQLiteStateStore("./ute_data/bybit_futures_acct123.db")
        await store.initialize()
    """

    def __init__(self, db_path: str) -> None:
        self._path = db_path

    @property
    def path(self) -> str:
        return self._path

    # The remaining methods are the implementation the dev owns.
    # See the StateStore ABC above for the complete contract.

    async def initialize(self) -> None:
        raise NotImplementedError("SQLiteStateStore.initialize")

    async def close(self) -> None:
        raise NotImplementedError("SQLiteStateStore.close")

    async def upsert_position(self, position: Position) -> None:
        raise NotImplementedError

    async def upsert_balance(self, balance: Balance) -> None:
        raise NotImplementedError

    async def upsert_order(self, order: OrderRecord) -> None:
        raise NotImplementedError

    async def write_audit_event(self, event: "AuditEvent") -> None:  # noqa: F821
        raise NotImplementedError

    async def get_position(self, instrument: Instrument) -> Position | None:
        raise NotImplementedError

    async def get_balance(self, currency: str) -> Balance | None:
        raise NotImplementedError

    async def get_order(self, client_order_id: str) -> OrderRecord | None:
        raise NotImplementedError

    async def query_orders(self, *, instrument=None, start=None, end=None, limit=1000):
        raise NotImplementedError

    async def query_fills(self, *, instrument=None, start=None, end=None, limit=1000):
        raise NotImplementedError

    async def query_positions(self, *, instrument=None, start=None, end=None, limit=1000):
        raise NotImplementedError

    async def query_balances(self, *, currency=None, start=None, end=None, limit=1000):
        raise NotImplementedError

    async def query_reconciliation_events(self, *, start=None, end=None, limit=1000):
        raise NotImplementedError

    async def query_halt_events(self, *, start=None, end=None, limit=1000):
        raise NotImplementedError

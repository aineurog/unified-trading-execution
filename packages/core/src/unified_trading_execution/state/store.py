"""StateStore ABC and SQLite implementation — Sections 6.2, 17.11."""

from __future__ import annotations

import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import aiosqlite

from unified_trading_execution.events import (
    AuditEvent,
    HaltEvent,
    ReconciliationEvent,
    ReconciliationMismatch,
)
from unified_trading_execution.types.enums import (
    LIVE_ORDER_STATUSES,
    AssetClass,
    FillEntry,
    FillReason,
    OptionRight,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord, TpSlAttachment
from unified_trading_execution.types.position import Balance, Position

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

# Key under which the reconciliation "clean through" watermark is persisted in
# the ``reconciliation_state`` singleton table.
_RECONCILE_WATERMARK_KEY = "last_clean_through"


def _slug(component: str) -> str:
    """Sanitize one filename component to `[a-z0-9_]` (pass through junk).

    Keeps the resolved path filesystem-safe regardless of how a platform /
    account identifier is spelled.
    """
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in component).lower()
    return cleaned.strip("_") or "unknown"


def default_state_store_path(platform_name: str, account_id: str) -> str:
    """Resolve the default ``StateStore`` location per Section 6.2.

    Returns ``./unified_trading_execution_data/<platform>_<account>.db``
    relative to the process working directory — deterministic regardless of
    the working directory's name (predictable and human-inspectable, never
    hidden or written to a system location).  ``<platform>`` and ``<account>``
    are slugified so the resulting filename is always filesystem-safe.

    Example with ``platform_name="bybit"``, ``account_id="acct123"``:
    ``./unified_trading_execution_data/bybit_acct123.db``.
    """
    filename = f"{_slug(platform_name)}_{_slug(account_id)}.db"
    return os.path.join("./unified_trading_execution_data", filename)


# ============================================================
# Instrument serialisation helpers
# ============================================================

# One row of the current-state ``halts`` table: scope + instrument identity +
# reason/detail.  ``scope`` is a closed set, typed literally so callers can
# hand it straight to the halt state machine.
ActiveHaltRow = tuple[Literal["instrument", "account"], Instrument | None, str, str]


def _serialise_instrument(i: Instrument) -> dict[str, Any]:
    return {
        "symbol": i.symbol,
        "quote_currency": i.quote_currency,
        "asset_class": i.asset_class.value,
        "exchange": i.exchange,
        "currency": i.currency,
        "expiry": i.expiry.isoformat() if i.expiry else None,
        "strike": str(i.strike) if i.strike is not None else None,
        "option_right": i.option_right.value if i.option_right else None,
        "multiplier": i.multiplier,
        "platform_symbol": i.platform_symbol,
    }


def _deserialise_instrument(d: dict[str, Any]) -> Instrument:
    return Instrument(
        symbol=d["symbol"],
        quote_currency=d.get("quote_currency"),
        asset_class=AssetClass(d["asset_class"]),
        exchange=d.get("exchange"),
        currency=d.get("currency"),
        expiry=datetime.strptime(d["expiry"], "%Y-%m-%d").date() if d.get("expiry") else None,
        strike=Decimal(d["strike"]) if d.get("strike") else None,
        option_right=OptionRight(d["option_right"]) if d.get("option_right") else None,
        multiplier=d.get("multiplier"),
        platform_symbol=d.get("platform_symbol"),
    )


def _serialise_decimal_list(items: list[Decimal]) -> str:
    return json.dumps([str(x) for x in items])


# ============================================================
# StateStore ABC
# ============================================================


class StateStore(ABC):
    """Pluggable storage backend for the state mirror and audit trail."""

    @abstractmethod
    async def upsert_position(self, position: Position) -> None: ...
    @abstractmethod
    async def delete_position(self, instrument: Instrument, position_id: str) -> None: ...
    @abstractmethod
    async def upsert_balance(self, balance: Balance) -> None: ...
    @abstractmethod
    async def upsert_order(self, order: OrderRecord) -> None: ...
    @abstractmethod
    async def upsert_fill(self, fill: FillRecord) -> None: ...
    async def upsert_fills_batch(self, fills: list[FillRecord]) -> None:
        """Batched insert — default implementation calls upsert_fill in a loop."""
        for fill in fills:
            await self.upsert_fill(fill)

    @abstractmethod
    async def delete_orders_by_client_ids(self, client_order_ids: list[str]) -> None: ...
    @abstractmethod
    async def delete_fills_by_client_ids(
        self, client_order_ids: list[str], *, since: datetime | None = None
    ) -> None: ...

    @abstractmethod
    async def write_audit_event(self, event: AuditEvent) -> None: ...
    @abstractmethod
    async def write_reconciliation_event(self, event: ReconciliationEvent) -> None: ...
    @abstractmethod
    async def write_halt_event(self, event: HaltEvent) -> None: ...

    @abstractmethod
    async def get_positions(self, instrument: Instrument) -> list[Position]: ...
    @abstractmethod
    async def get_net_position(self, instrument: Instrument) -> Position | None: ...
    @abstractmethod
    async def get_balance(self, currency: str) -> Balance | None: ...
    @abstractmethod
    async def get_order(self, client_order_id: str) -> OrderRecord | None: ...

    async def query_open_orders(self, *, limit: int = 1000) -> list[OrderRecord]:
        """Return orders currently live on the platform (PENDING/OPEN/PARTIALLY_FILLED).

        Used by reconciliation to compare open order sets.  The default
        implementation filters ``query_orders`` by live status; SQLite
        overrides this with a status-filtered query.
        """
        orders = await self.query_orders(limit=limit)
        return [o for o in orders if o.status in LIVE_ORDER_STATUSES]

    @abstractmethod
    async def query_orders(
        self,
        *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[OrderRecord]: ...
    @abstractmethod
    async def query_fills(
        self,
        *,
        instrument: Instrument | None = None,
        position_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FillRecord]: ...
    @abstractmethod
    async def query_positions(
        self,
        *,
        instrument: Instrument | None = None,
        limit: int = 1000,
    ) -> list[Position]: ...
    @abstractmethod
    async def query_balances(
        self,
        *,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Balance]: ...
    @abstractmethod
    async def query_reconciliation_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[ReconciliationEvent]: ...
    @abstractmethod
    async def query_halt_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[HaltEvent]: ...

    @abstractmethod
    async def query_audit_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[AuditEvent]: ...

    @abstractmethod
    async def get_adapter_config(self, key: str) -> str | None: ...
    @abstractmethod
    async def set_adapter_config(self, key: str, value: str) -> None: ...
    @abstractmethod
    async def delete_adapter_config(self, key: str) -> None: ...
    @abstractmethod
    async def list_adapter_config(self, prefix: str) -> dict[str, str]: ...

    async def get_reconcile_watermark(self) -> datetime | None:
        """Return the persisted reconciliation "clean through" watermark, or None.

        Concrete default returns None; SQLite overrides this with the value
        persisted in the ``reconciliation_state`` table.  Backends that do not
        persist a watermark simply reconcile forward-only from first connect.
        """
        return None

    async def set_reconcile_watermark(self, watermark: datetime) -> None:
        """Persist the reconciliation "clean through" watermark.

        Concrete default is a no-op; SQLite overrides this to write the value
        into the ``reconciliation_state`` table.
        """
        return None

    async def get_active_halts(self) -> list[ActiveHaltRow]:
        """Return currently-active halts as ``(scope, instrument, reason, detail)``.

        Concrete default returns an empty list (no halt persistence).  SQLite
        overrides this to read the ``halts`` current-state table so halts
        survive a restart.
        """
        return []

    async def upsert_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        reason: str,
        detail: str,
    ) -> None:
        """Record an active halt in the current-state ``halts`` table.

        Concrete default is a no-op; SQLite overrides this to persist the halt.
        """
        return None

    async def delete_halt(
        self, scope: Literal["instrument", "account"], instrument: Instrument | None
    ) -> None:
        """Remove an active halt from the current-state ``halts`` table.

        Concrete default is a no-op; SQLite overrides this to persist the clear.
        """
        return None

    @abstractmethod
    async def initialize(self) -> None: ...
    @abstractmethod
    async def close(self) -> None: ...

    @property
    @abstractmethod
    def conn(self) -> Any:
        """Escape hatch: the raw DB connection for ad-hoc queries.

        Used sparingly by engine-level reconciliation logic (e.g., DELETE
        of orphan orders). Backends that don't expose a connection object
        raise NotImplementedError.
        """
        ...

    async def flush(self) -> None:
        """Ensure all pending writes are durable before teardown.

        The default is a no-op. SQLiteStateStore overrides this with a
        WAL checkpoint to force pending WAL frames into the main database
        file before the engine disconnects.
        """
        return

    @property
    @abstractmethod
    def path(self) -> str: ...


# ============================================================
# SQLiteStateStore
# ============================================================


class SQLiteStateStore(StateStore):
    """Default v1 StateStore backed by SQLite via aiosqlite.

    WAL journal mode, synchronous=NORMAL, foreign keys enabled.
    Schema migrations run automatically on initialize().
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._path = db_path
        self._conn: aiosqlite.Connection | None = None
        # Single aiosqlite connection ⇒ concurrent writers must not interleave
        # their `BEGIN`/`COMMIT` spans. Held for the whole duration of every
        # mutating operation so a second `BEGIN` can never fire while another
        # transaction is open (the "cannot start a transaction within a
        # transaction" crash).
        self._write_lock = asyncio.Lock()

    @property
    def path(self) -> str:
        return self._path

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("SQLiteStateStore not initialised — call initialize() first")
        return self._conn

    # ---- Lifecycle ----

    async def initialize(self) -> None:
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path, isolation_level=None)
        self._conn.row_factory = aiosqlite.Row
        await self._run_migrations()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def flush(self) -> None:
        """Force WAL checkpoint so pending writes are durable on disk."""
        if self._conn is not None:
            async with self._write_lock:
                # TRUNCATE mode writes the WAL back into the main DB, then
                # resets the WAL to zero bytes — ideal for orderly shutdown.
                await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def _run_migrations(self) -> None:
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        cursor = await self.conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        row = await cursor.fetchone()
        assert row is not None, "SELECT with aggregation returned no row"
        current = row[0]

        sql_files = sorted(
            [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")],
        )
        for fname in sql_files:
            version = int(fname.split("_")[0])
            if version <= current:
                continue
            sql_path = MIGRATIONS_DIR / fname
            sql = sql_path.read_text()
            await self.conn.executescript(sql)
            now = datetime.now(tz=UTC).isoformat()
            await self.conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                (version, now),
            )
            await self.conn.commit()

    # ---- Upserts ----

    async def upsert_position(self, position: Position) -> None:
        if position.position_id is None:
            raise ValueError("position_id is required to persist a position")
        async with self._write_lock:
            i = _serialise_instrument(position.instrument)
            now = position.updated_at.isoformat()
            await self.conn.execute("BEGIN")
            try:
                await self.conn.execute(
                    """INSERT OR REPLACE INTO positions
                       (symbol, quote_currency, asset_class, exchange, currency,
                        expiry, strike, option_right, multiplier, platform_symbol,
                        position_id, quantity, average_entry_price, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        i["symbol"],
                        i["quote_currency"],
                        i["asset_class"],
                        i["exchange"],
                        i["currency"],
                        i["expiry"],
                        i["strike"],
                        i["option_right"],
                        i["multiplier"],
                        i["platform_symbol"],
                        position.position_id,
                        str(position.quantity),
                        str(position.average_entry_price),
                        now,
                    ),
                )
            except BaseException:
                await self.conn.rollback()
                raise
            else:
                await self.conn.commit()

    async def delete_position(self, instrument: Instrument, position_id: str) -> None:
        async with self._write_lock:
            i = _serialise_instrument(instrument)
            await self.conn.execute(
                "DELETE FROM positions WHERE symbol=? AND asset_class=? AND position_id=?",
                (i["symbol"], i["asset_class"], position_id),
            )

    async def upsert_balance(self, balance: Balance) -> None:
        async with self._write_lock:
            now = balance.updated_at.isoformat()
            await self.conn.execute("BEGIN")
            try:
                await self.conn.execute(
                    "INSERT OR REPLACE INTO balances (currency, free, used, total, updated_at) VALUES (?,?,?,?,?)",
                    (
                        balance.currency,
                        str(balance.free),
                        str(balance.used),
                        str(balance.total),
                        now,
                    ),
                )
                await self.conn.execute(
                    "INSERT INTO balance_history (currency, free, used, total, recorded_at) VALUES (?,?,?,?,?)",
                    (
                        balance.currency,
                        str(balance.free),
                        str(balance.used),
                        str(balance.total),
                        now,
                    ),
                )
            except BaseException:
                await self.conn.rollback()
                raise
            else:
                await self.conn.commit()

    async def upsert_order(self, order: OrderRecord) -> None:
        async with self._write_lock:
            i = _serialise_instrument(order.instrument)
            now = datetime.now(tz=UTC).isoformat()
            await self.conn.execute("BEGIN")
            try:
                # Current state: latest snapshot per client_order_id.
                await self.conn.execute(
                    """INSERT OR REPLACE INTO orders
                       (client_order_id, symbol, quote_currency, asset_class, exchange, currency,
                        expiry, strike, option_right, multiplier, platform_symbol,
                        order_type, side, quantity, time_in_force, price, stop_price,
                        reduce_only, client_tag,
                        take_profit_trigger, take_profit_limit,
                        stop_loss_trigger, stop_loss_limit,
                        platform_order_id, status, filled_quantity, average_fill_price,
                        correlation_id, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        order.client_order_id,
                        i["symbol"],
                        i["quote_currency"],
                        i["asset_class"],
                        i["exchange"],
                        i["currency"],
                        i["expiry"],
                        i["strike"],
                        i["option_right"],
                        i["multiplier"],
                        i["platform_symbol"],
                        order.order_type.value,
                        order.side.value,
                        str(order.quantity),
                        order.time_in_force.value,
                        str(order.price) if order.price else None,
                        str(order.stop_price) if order.stop_price else None,
                        1 if order.reduce_only else 0,
                        order.client_tag,
                        str(order.take_profit.trigger_price) if order.take_profit else None,
                        str(order.take_profit.limit_price)
                        if order.take_profit and order.take_profit.limit_price
                        else None,
                        str(order.stop_loss.trigger_price) if order.stop_loss else None,
                        str(order.stop_loss.limit_price)
                        if order.stop_loss and order.stop_loss.limit_price
                        else None,
                        order.platform_order_id,
                        order.status.value,
                        str(order.filled_quantity),
                        str(order.average_fill_price) if order.average_fill_price else None,
                        order.correlation_id,
                        order.created_at.isoformat(),
                        order.updated_at.isoformat(),
                    ),
                )
                # Append-only lifecycle snapshot: preserves terminal transitions
                # and orders later removed from `orders` by orphan resolution.
                await self.conn.execute(
                    """INSERT INTO order_history
                       (client_order_id, symbol, quote_currency, asset_class, exchange, currency,
                        expiry, strike, option_right, multiplier, platform_symbol,
                        order_type, side, quantity, time_in_force, price, stop_price,
                        reduce_only, client_tag,
                        take_profit_trigger, take_profit_limit,
                        stop_loss_trigger, stop_loss_limit,
                        platform_order_id, status, filled_quantity, average_fill_price,
                        correlation_id, created_at, updated_at, recorded_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        order.client_order_id,
                        i["symbol"],
                        i["quote_currency"],
                        i["asset_class"],
                        i["exchange"],
                        i["currency"],
                        i["expiry"],
                        i["strike"],
                        i["option_right"],
                        i["multiplier"],
                        i["platform_symbol"],
                        order.order_type.value,
                        order.side.value,
                        str(order.quantity),
                        order.time_in_force.value,
                        str(order.price) if order.price else None,
                        str(order.stop_price) if order.stop_price else None,
                        1 if order.reduce_only else 0,
                        order.client_tag,
                        str(order.take_profit.trigger_price) if order.take_profit else None,
                        str(order.take_profit.limit_price)
                        if order.take_profit and order.take_profit.limit_price
                        else None,
                        str(order.stop_loss.trigger_price) if order.stop_loss else None,
                        str(order.stop_loss.limit_price)
                        if order.stop_loss and order.stop_loss.limit_price
                        else None,
                        order.platform_order_id,
                        order.status.value,
                        str(order.filled_quantity),
                        str(order.average_fill_price) if order.average_fill_price else None,
                        order.correlation_id,
                        order.created_at.isoformat(),
                        order.updated_at.isoformat(),
                        now,
                    ),
                )
            except BaseException:
                await self.conn.rollback()
                raise
            else:
                await self.conn.commit()

    async def upsert_fill(self, fill: FillRecord) -> None:
        async with self._write_lock:
            i = _serialise_instrument(fill.instrument)
            await self.conn.execute(
                """INSERT INTO fills
                   (client_order_id, platform_fill_id, symbol, quote_currency, asset_class,
                    exchange, currency, expiry, strike, option_right, multiplier,
                    platform_symbol, fill_quantity, fill_price, fill_timestamp,
                    fee_currency, fee_amount, correlation_id, position_id, reason, entry)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(platform_fill_id) DO UPDATE SET
                           client_order_id = excluded.client_order_id,
                           symbol = excluded.symbol,
                           quote_currency = excluded.quote_currency,
                           asset_class = excluded.asset_class,
                           exchange = excluded.exchange,
                           currency = excluded.currency,
                           expiry = excluded.expiry,
                           strike = excluded.strike,
                           option_right = excluded.option_right,
                           multiplier = excluded.multiplier,
                           platform_symbol = excluded.platform_symbol,
                           fill_quantity = excluded.fill_quantity,
                           fill_price = excluded.fill_price,
                           fill_timestamp = excluded.fill_timestamp,
                           fee_currency = excluded.fee_currency,
                           fee_amount = excluded.fee_amount,
                           correlation_id = excluded.correlation_id,
                           position_id = excluded.position_id,
                           reason = excluded.reason,
                           entry = excluded.entry""",
                (
                    fill.client_order_id,
                    fill.platform_fill_id,
                    i["symbol"],
                    i["quote_currency"],
                    i["asset_class"],
                    i["exchange"],
                    i["currency"],
                    i["expiry"],
                    i["strike"],
                    i["option_right"],
                    i["multiplier"],
                    i["platform_symbol"],
                    str(fill.fill_quantity),
                    str(fill.fill_price),
                    fill.fill_timestamp.isoformat(),
                    fill.fee_currency,
                    str(fill.fee_amount) if fill.fee_amount else None,
                    fill.correlation_id,
                    fill.position_id,
                    fill.reason.value if fill.reason else None,
                    fill.entry.value if fill.entry else None,
                ),
            )

    async def upsert_fills_batch(self, fills: list[FillRecord]) -> None:
        """Batched insert for reconciliation writes — single transaction."""
        async with self._write_lock:
            await self.conn.execute("BEGIN")
            try:
                for fill in fills:
                    await self.upsert_fill_unlocked(fill)
            except BaseException:
                await self.conn.rollback()
                raise
            else:
                await self.conn.commit()

    async def upsert_fill_unlocked(self, fill: FillRecord) -> None:
        """Non-locking insert of a single fill — callers must hold the write lock.

        Used by ``upsert_fills_batch`` so each row does not re-acquire the lock
        (the batch already holds it).  Not part of the public ``StateStore`` API.
        """
        i = _serialise_instrument(fill.instrument)
        await self.conn.execute(
            """INSERT INTO fills
               (client_order_id, platform_fill_id, symbol, quote_currency, asset_class,
                exchange, currency, expiry, strike, option_right, multiplier,
                platform_symbol, fill_quantity, fill_price, fill_timestamp,
                fee_currency, fee_amount, correlation_id, position_id, reason, entry)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(platform_fill_id) DO UPDATE SET
                       client_order_id = excluded.client_order_id,
                       symbol = excluded.symbol,
                       quote_currency = excluded.quote_currency,
                       asset_class = excluded.asset_class,
                       exchange = excluded.exchange,
                       currency = excluded.currency,
                       expiry = excluded.expiry,
                       strike = excluded.strike,
                       option_right = excluded.option_right,
                       multiplier = excluded.multiplier,
                       platform_symbol = excluded.platform_symbol,
                       fill_quantity = excluded.fill_quantity,
                       fill_price = excluded.fill_price,
                       fill_timestamp = excluded.fill_timestamp,
                       fee_currency = excluded.fee_currency,
                       fee_amount = excluded.fee_amount,
                       correlation_id = excluded.correlation_id,
                       position_id = excluded.position_id,
                       reason = excluded.reason,
                       entry = excluded.entry""",
            (
                fill.client_order_id,
                fill.platform_fill_id,
                i["symbol"],
                i["quote_currency"],
                i["asset_class"],
                i["exchange"],
                i["currency"],
                i["expiry"],
                i["strike"],
                i["option_right"],
                i["multiplier"],
                i["platform_symbol"],
                str(fill.fill_quantity),
                str(fill.fill_price),
                fill.fill_timestamp.isoformat(),
                fill.fee_currency,
                str(fill.fee_amount) if fill.fee_amount else None,
                fill.correlation_id,
                fill.position_id,
                fill.reason.value if fill.reason else None,
                fill.entry.value if fill.entry else None,
            ),
        )

    # ---- Locked deletes (reconciliation) ----

    async def delete_orders_by_client_ids(self, client_order_ids: list[str]) -> None:
        """Delete current-state order rows by client id, holding the write lock.

        Used by reconciliation's orphan-in-local resolution.  Bypassing the
        lock (as raw ``conn`` access did) let these interleave with an open
        ``BEGIN`` from a concurrent upsert.
        """
        async with self._write_lock:
            for client_order_id in client_order_ids:
                await self.conn.execute(
                    "DELETE FROM orders WHERE client_order_id = ?",
                    (client_order_id,),
                )

    async def delete_fills_by_client_ids(
        self, client_order_ids: list[str], *, since: datetime | None = None
    ) -> None:
        """Delete fill rows by client id, holding the write lock.

        When *since* is given, only fills with ``fill_timestamp >= since`` are
        removed, so reconciliation can correct a window of fills without
        disturbing pre-watermark history.
        """
        async with self._write_lock:
            for client_order_id in client_order_ids:
                if since is None:
                    await self.conn.execute(
                        "DELETE FROM fills WHERE client_order_id = ?",
                        (client_order_id,),
                    )
                else:
                    await self.conn.execute(
                        "DELETE FROM fills WHERE client_order_id = ? AND fill_timestamp >= ?",
                        (client_order_id, since.isoformat()),
                    )

    # ---- Audit trail (append-only) ----

    async def write_audit_event(self, event: AuditEvent) -> None:
        """Append a single audit event. Rejects event_id collisions."""
        async with self._write_lock:
            await self.conn.execute(
                "INSERT INTO audit_events (event_id, timestamp, adapter_name, account_id, correlation_id, event_type, payload_json) VALUES (?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.adapter_name,
                    event.account_id,
                    event.correlation_id,
                    event.event_type,
                    json.dumps(event.payload),
                ),
            )

    async def write_reconciliation_event(self, event: ReconciliationEvent) -> None:
        mismatches_json = json.dumps(
            [
                {
                    "mismatch_type": m.mismatch_type,
                    "instrument": _serialise_instrument(m.instrument) if m.instrument else None,
                    "local_value": m.local_value,
                    "platform_value": m.platform_value,
                }
                for m in event.mismatches
            ]
        )
        async with self._write_lock:
            await self.conn.execute(
                "INSERT INTO reconciliation_events (event_id, timestamp, adapter_name, account_id, correlation_id, mismatches_json, duration_ms) VALUES (?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.adapter_name,
                    event.account_id,
                    event.correlation_id,
                    mismatches_json,
                    event.duration_ms,
                ),
            )

    async def write_halt_event(self, event: HaltEvent) -> None:
        inst = event.instrument
        async with self._write_lock:
            i = _serialise_instrument(inst) if inst else None
            await self.conn.execute(
                """INSERT INTO halt_events
                   (event_id, timestamp, adapter_name, account_id, correlation_id,
                    action, scope, symbol, quote_currency, asset_class, exchange,
                    currency, expiry, strike, option_right, multiplier, platform_symbol,
                    reason, detail, cleared_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.adapter_name,
                    event.account_id,
                    event.correlation_id,
                    event.action,
                    event.scope,
                    i["symbol"] if i else None,
                    i["quote_currency"] if i else None,
                    i["asset_class"] if i else None,
                    i["exchange"] if i else None,
                    i["currency"] if i else None,
                    i["expiry"] if i else None,
                    i["strike"] if i else None,
                    i["option_right"] if i else None,
                    i["multiplier"] if i else None,
                    i["platform_symbol"] if i else None,
                    event.reason,
                    event.detail,
                    event.cleared_by,
                ),
            )

    # ---- Single-record reads ----

    async def get_positions(self, instrument: Instrument) -> list[Position]:
        async with self._write_lock:
            i = _serialise_instrument(instrument)
            cursor = await self.conn.execute(
                "SELECT quantity, average_entry_price, updated_at, position_id "
                "FROM positions WHERE symbol=? AND asset_class=?",
                (i["symbol"], i["asset_class"]),
            )
            return [
                Position(
                    instrument=instrument,
                    quantity=Decimal(row["quantity"]),
                    average_entry_price=Decimal(row["average_entry_price"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                    position_id=row["position_id"],
                )
                for row in await cursor.fetchall()
            ]

    async def get_net_position(self, instrument: Instrument) -> Position | None:
        legs = await self.get_positions(instrument)
        net_quantity = sum((leg.quantity for leg in legs), start=Decimal("0"))
        if net_quantity == 0:
            return None
        weighted = sum(
            (leg.quantity * leg.average_entry_price for leg in legs),
            start=Decimal("0"),
        )
        return Position(
            instrument=instrument,
            quantity=net_quantity,
            average_entry_price=weighted / net_quantity,
            updated_at=max(leg.updated_at for leg in legs),
        )

    async def get_balance(self, currency: str) -> Balance | None:
        async with self._write_lock:
            cursor = await self.conn.execute(
                "SELECT free, used, total, updated_at FROM balances WHERE currency=?",
                (currency,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return Balance(
                currency=currency,
                free=Decimal(row["free"]),
                used=Decimal(row["used"]),
                total=Decimal(row["total"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )

    async def get_order(self, client_order_id: str) -> OrderRecord | None:
        async with self._write_lock:
            cursor = await self.conn.execute(
                "SELECT * FROM orders WHERE client_order_id=?",
                (client_order_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return self._row_to_order_record(row)

    # ---- Adapter config (key/value, Section 2.1) ----

    async def get_adapter_config(self, key: str) -> str | None:
        async with self._write_lock:
            cursor = await self.conn.execute(
                "SELECT value FROM adapter_config WHERE key=?",
                (key,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return str(row[0])

    async def set_adapter_config(self, key: str, value: str) -> None:
        now = datetime.now(tz=UTC).isoformat()
        async with self._write_lock:
            await self.conn.execute(
                "INSERT OR REPLACE INTO adapter_config (key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, now),
            )

    async def delete_adapter_config(self, key: str) -> None:
        async with self._write_lock:
            await self.conn.execute("DELETE FROM adapter_config WHERE key=?", (key,))

    async def list_adapter_config(self, prefix: str) -> dict[str, str]:
        async with self._write_lock:
            cursor = await self.conn.execute(
                "SELECT key, value FROM adapter_config WHERE key LIKE ?",
                (prefix + "%",),
            )
            rows = await cursor.fetchall()
            return {str(row["key"]): str(row["value"]) for row in rows}

    async def get_reconcile_watermark(self) -> datetime | None:
        async with self._write_lock:
            cursor = await self.conn.execute(
                "SELECT value FROM reconciliation_state WHERE key=?",
                (_RECONCILE_WATERMARK_KEY,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(str(row["value"]))

    async def set_reconcile_watermark(self, watermark: datetime) -> None:
        now = datetime.now(tz=UTC).isoformat()
        async with self._write_lock:
            await self.conn.execute(
                "INSERT OR REPLACE INTO reconciliation_state (key, value, updated_at) VALUES (?,?,?)",
                (_RECONCILE_WATERMARK_KEY, watermark.isoformat(), now),
            )

    # ---- Active-halt persistence (Section 6.4) ----

    async def get_active_halts(self) -> list[ActiveHaltRow]:
        """Read the current-state ``halts`` table for active halts."""
        async with self._write_lock:
            cursor = await self.conn.execute("SELECT * FROM halts")
            results: list[ActiveHaltRow] = []
            for row in await cursor.fetchall():
                instrument = None
                if row["symbol"]:
                    instrument = _deserialise_instrument(dict(row))
                results.append((row["scope"], instrument, row["reason"], row["detail"]))
            return results

    async def upsert_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        reason: str,
        detail: str,
    ) -> None:
        """Persist an active halt; idempotent via the (scope, symbol, asset_class) key."""
        i = _serialise_instrument(instrument) if instrument is not None else None
        now = datetime.now(tz=UTC).isoformat()
        async with self._write_lock:
            await self.conn.execute(
                """INSERT OR REPLACE INTO halts
                   (scope, symbol, quote_currency, asset_class, exchange, currency,
                    expiry, strike, option_right, multiplier, platform_symbol,
                    reason, detail, entered_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    scope,
                    i["symbol"] if i else "",
                    i["quote_currency"] if i else None,
                    i["asset_class"] if i else "",
                    i["exchange"] if i else None,
                    i["currency"] if i else None,
                    i["expiry"] if i else None,
                    i["strike"] if i else None,
                    i["option_right"] if i else None,
                    i["multiplier"] if i else None,
                    i["platform_symbol"] if i else None,
                    reason,
                    detail,
                    now,
                ),
            )

    async def delete_halt(
        self, scope: Literal["instrument", "account"], instrument: Instrument | None
    ) -> None:
        """Remove an active halt from the ``halts`` table."""
        async with self._write_lock:
            if instrument is None:
                await self.conn.execute("DELETE FROM halts WHERE scope = ?", (scope,))
            else:
                i = _serialise_instrument(instrument)
                await self.conn.execute(
                    "DELETE FROM halts WHERE scope = ? AND symbol = ? AND asset_class = ?",
                    (scope, i["symbol"], i["asset_class"]),
                )

    # ---- Filtered queries ----

    async def query_orders(
        self,
        *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[OrderRecord]:
        query = "SELECT * FROM orders WHERE 1=1"
        params: list[Any] = []
        if instrument is not None:
            query += " AND symbol=? AND asset_class=?"
            params.extend([instrument.symbol, instrument.asset_class.value])
        if start is not None:
            query += " AND created_at >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND created_at <= ?"
            params.append(end.isoformat())
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            return [self._row_to_order_record(r) for r in await cursor.fetchall()]

    async def query_open_orders(self, *, limit: int = 1000) -> list[OrderRecord]:
        """Return orders currently live (PENDING/OPEN/PARTIALLY_FILLED)."""
        query = "SELECT * FROM orders WHERE status IN (?,?,?) ORDER BY created_at DESC LIMIT ?"
        params: list[Any] = [
            OrderStatus.PENDING.value,
            OrderStatus.OPEN.value,
            OrderStatus.PARTIALLY_FILLED.value,
            limit,
        ]
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            return [self._row_to_order_record(r) for r in await cursor.fetchall()]

    async def query_fills(
        self,
        *,
        instrument: Instrument | None = None,
        position_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FillRecord]:
        query = "SELECT * FROM fills WHERE 1=1"
        params: list[Any] = []
        if instrument is not None:
            query += " AND symbol=? AND asset_class=?"
            params.extend([instrument.symbol, instrument.asset_class.value])
        if position_id is not None:
            query += " AND position_id=?"
            params.append(position_id)
        if start is not None:
            query += " AND fill_timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND fill_timestamp <= ?"
            params.append(end.isoformat())
        query += " ORDER BY fill_timestamp DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            return [self._row_to_fill_record(r) for r in await cursor.fetchall()]

    async def query_positions(
        self,
        *,
        instrument: Instrument | None = None,
        limit: int = 1000,
    ) -> list[Position]:
        query = "SELECT * FROM positions WHERE 1=1"
        params: list[Any] = []
        if instrument is not None:
            query += " AND symbol=? AND asset_class=?"
            params.extend([instrument.symbol, instrument.asset_class.value])
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            results: list[Position] = []
            for row in await cursor.fetchall():
                inst = _deserialise_instrument(dict(row))
                results.append(
                    Position(
                        instrument=inst,
                        quantity=Decimal(row["quantity"]),
                        average_entry_price=Decimal(row["average_entry_price"]),
                        updated_at=datetime.fromisoformat(row["updated_at"]),
                        position_id=row["position_id"],
                    )
                )
            return results

    async def query_balances(
        self,
        *,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Balance]:
        query = "SELECT * FROM balance_history WHERE 1=1"
        params: list[Any] = []
        if currency is not None:
            query += " AND currency=?"
            params.append(currency)
        if start is not None:
            query += " AND recorded_at >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND recorded_at <= ?"
            params.append(end.isoformat())
        query += " ORDER BY recorded_at DESC, id DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            return [
                Balance(
                    currency=row["currency"],
                    free=Decimal(row["free"]),
                    used=Decimal(row["used"]),
                    total=Decimal(row["total"]),
                    updated_at=datetime.fromisoformat(row["recorded_at"]),
                )
                for row in await cursor.fetchall()
            ]

    async def query_reconciliation_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[ReconciliationEvent]:
        query = "SELECT * FROM reconciliation_events WHERE 1=1"
        params: list[Any] = []
        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end.isoformat())
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            results: list[ReconciliationEvent] = []
            for row in await cursor.fetchall():
                mm_data = json.loads(row["mismatches_json"])
                mismatches = tuple(
                    ReconciliationMismatch(
                        mismatch_type=m["mismatch_type"],
                        instrument=_deserialise_instrument(m["instrument"])
                        if m["instrument"]
                        else None,
                        local_value=m["local_value"],
                        platform_value=m["platform_value"],
                    )
                    for m in mm_data
                )
                results.append(
                    ReconciliationEvent(
                        event_id=row["event_id"],
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                        adapter_name=row["adapter_name"],
                        account_id=row["account_id"],
                        correlation_id=row["correlation_id"],
                        mismatches=mismatches,
                        duration_ms=row["duration_ms"],
                    )
                )
            return results

    async def query_halt_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[HaltEvent]:
        query = "SELECT * FROM halt_events WHERE 1=1"
        params: list[Any] = []
        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end.isoformat())
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            results: list[HaltEvent] = []
            for row in await cursor.fetchall():
                inst = None
                if row["symbol"] is not None and row["asset_class"] is not None:
                    inst = _deserialise_instrument(dict(row))
                results.append(
                    HaltEvent(
                        event_id=row["event_id"],
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                        adapter_name=row["adapter_name"],
                        account_id=row["account_id"],
                        correlation_id=row["correlation_id"],
                        action=row["action"],
                        scope=row["scope"],
                        instrument=inst,
                        reason=row["reason"],
                        detail=row["detail"],
                        cleared_by=row["cleared_by"],
                    )
                )
            return results

    async def query_audit_events(
        self, *, start: datetime | None = None, end: datetime | None = None, limit: int = 1000
    ) -> list[AuditEvent]:
        """Return filtered audit-trail records, newest first.

        Filters are conjunctive — all supplied criteria must match. When no
        filters are given, returns the most recent audit events up to *limit*.
        """
        query = "SELECT * FROM audit_events WHERE 1=1"
        params: list[Any] = []
        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end.isoformat())
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        async with self._write_lock:
            cursor = await self.conn.execute(query, params)
            results: list[AuditEvent] = []
            for row in await cursor.fetchall():
                results.append(
                    AuditEvent(
                        event_id=row["event_id"],
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                        adapter_name=row["adapter_name"],
                        account_id=row["account_id"],
                        correlation_id=row["correlation_id"],
                        event_type=row["event_type"],
                        payload=json.loads(row["payload_json"]),
                    )
                )
            return results

    # ---- Row deserialisation ----

    def _row_to_order_record(self, row: aiosqlite.Row) -> OrderRecord:
        inst = _deserialise_instrument(dict(row))
        tp = None
        if row["take_profit_trigger"] is not None:
            tp = TpSlAttachment(
                trigger_price=Decimal(row["take_profit_trigger"]),
                limit_price=Decimal(row["take_profit_limit"]) if row["take_profit_limit"] else None,
            )
        sl = None
        if row["stop_loss_trigger"] is not None:
            sl = TpSlAttachment(
                trigger_price=Decimal(row["stop_loss_trigger"]),
                limit_price=Decimal(row["stop_loss_limit"]) if row["stop_loss_limit"] else None,
            )
        return OrderRecord(
            instrument=inst,
            order_type=OrderType(row["order_type"]),
            side=OrderSide(row["side"]),
            quantity=Decimal(row["quantity"]),
            time_in_force=TimeInForce(row["time_in_force"]),
            client_order_id=row["client_order_id"],
            price=Decimal(row["price"]) if row["price"] else None,
            stop_price=Decimal(row["stop_price"]) if row["stop_price"] else None,
            reduce_only=bool(row["reduce_only"]),
            client_tag=row["client_tag"],
            take_profit=tp,
            stop_loss=sl,
            platform_order_id=row["platform_order_id"],
            status=OrderStatus(row["status"]),
            filled_quantity=Decimal(row["filled_quantity"]),
            average_fill_price=Decimal(row["average_fill_price"])
            if row["average_fill_price"]
            else None,
            correlation_id=row["correlation_id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _row_to_fill_record(self, row: aiosqlite.Row) -> FillRecord:
        inst = _deserialise_instrument(dict(row))
        return FillRecord(
            client_order_id=row["client_order_id"],
            platform_fill_id=row["platform_fill_id"],
            instrument=inst,
            fill_quantity=Decimal(row["fill_quantity"]),
            fill_price=Decimal(row["fill_price"]),
            fill_timestamp=datetime.fromisoformat(row["fill_timestamp"]),
            fee_currency=row["fee_currency"],
            fee_amount=Decimal(row["fee_amount"]) if row["fee_amount"] else None,
            correlation_id=row["correlation_id"],
            position_id=row["position_id"],
            reason=FillReason(row["reason"]) if row["reason"] else None,
            entry=FillEntry(row["entry"]) if row["entry"] else None,
        )

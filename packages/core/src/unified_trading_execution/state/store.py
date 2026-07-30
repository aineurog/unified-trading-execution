"""StateStore ABC and SQLite implementation — Sections 6.2, 17.11."""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiosqlite

from unified_trading_execution.events import (
    AuditEvent,
    HaltEvent,
    ReconciliationEvent,
    ReconciliationMismatch,
)
from unified_trading_execution.types.enums import (
    AssetClass,
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


# ============================================================
# Instrument serialisation helpers
# ============================================================


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
        "broker_symbol_override": i.broker_symbol_override,
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
    async def write_audit_event(self, event: AuditEvent) -> None: ...
    @abstractmethod
    async def write_reconciliation_event(self, event: ReconciliationEvent) -> None: ...
    @abstractmethod
    async def write_halt_event(self, event: HaltEvent) -> None: ...

    @abstractmethod
    async def get_position(self, instrument: Instrument) -> Position | None: ...
    @abstractmethod
    async def get_balance(self, currency: str) -> Balance | None: ...
    @abstractmethod
    async def get_order(self, client_order_id: str) -> OrderRecord | None: ...

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
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FillRecord]: ...
    @abstractmethod
    async def query_positions(
        self,
        *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
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
    async def initialize(self) -> None: ...
    @abstractmethod
    async def close(self) -> None: ...

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
            # TRUNCATE mode writes the WAL back into the main DB, then
            # resets the WAL to zero bytes — ideal for orderly shutdown.
            await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def _run_migrations(self) -> None:
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        cursor = await self.conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        row = await cursor.fetchone()
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
        i = _serialise_instrument(position.instrument)
        now = position.updated_at.isoformat()
        await self.conn.execute("BEGIN")
        try:
            await self.conn.execute(
                """INSERT OR REPLACE INTO positions
                   (symbol, quote_currency, asset_class, exchange, currency,
                    expiry, strike, option_right, multiplier, broker_symbol_override,
                    quantity, average_entry_price, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                    i["broker_symbol_override"],
                    str(position.quantity),
                    str(position.average_entry_price),
                    now,
                ),
            )
            await self.conn.execute(
                """INSERT INTO position_history
                   (symbol, quote_currency, asset_class, exchange, currency,
                    expiry, strike, option_right, multiplier, broker_symbol_override,
                    quantity, average_entry_price, recorded_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                    i["broker_symbol_override"],
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

    async def upsert_balance(self, balance: Balance) -> None:
        now = balance.updated_at.isoformat()
        await self.conn.execute("BEGIN")
        try:
            await self.conn.execute(
                "INSERT OR REPLACE INTO balances (currency, free, used, total, updated_at) VALUES (?,?,?,?,?)",
                (balance.currency, str(balance.free), str(balance.used), str(balance.total), now),
            )
            await self.conn.execute(
                "INSERT INTO balance_history (currency, free, used, total, recorded_at) VALUES (?,?,?,?,?)",
                (balance.currency, str(balance.free), str(balance.used), str(balance.total), now),
            )
        except BaseException:
            await self.conn.rollback()
            raise
        else:
            await self.conn.commit()

    async def upsert_order(self, order: OrderRecord) -> None:
        i = _serialise_instrument(order.instrument)
        await self.conn.execute(
            """INSERT OR REPLACE INTO orders
               (client_order_id, symbol, quote_currency, asset_class, exchange, currency,
                expiry, strike, option_right, multiplier, broker_symbol_override,
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
                i["broker_symbol_override"],
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

    async def upsert_fill(self, fill: FillRecord) -> None:
        i = _serialise_instrument(fill.instrument)
        await self.conn.execute(
            """INSERT INTO fills
               (client_order_id, platform_fill_id, symbol, quote_currency, asset_class,
                exchange, currency, expiry, strike, option_right, multiplier,
                broker_symbol_override, fill_quantity, fill_price, fill_timestamp,
                fee_currency, fee_amount, correlation_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                i["broker_symbol_override"],
                str(fill.fill_quantity),
                str(fill.fill_price),
                fill.fill_timestamp.isoformat(),
                fill.fee_currency,
                str(fill.fee_amount) if fill.fee_amount else None,
                fill.correlation_id,
            ),
        )

    async def upsert_fills_batch(self, fills: list[FillRecord]) -> None:
        """Batched insert for reconciliation writes — single transaction."""
        await self.conn.execute("BEGIN")
        try:
            for fill in fills:
                await self.upsert_fill(fill)
        except BaseException:
            await self.conn.rollback()
            raise
        else:
            await self.conn.commit()

    # ---- Audit trail (append-only) ----

    async def write_audit_event(self, event: AuditEvent) -> None:
        """Append a single audit event. Rejects event_id collisions."""
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
        await self.conn.execute(
            "INSERT INTO halt_events (event_id, timestamp, adapter_name, account_id, correlation_id, action, scope, symbol, asset_class, reason, detail, cleared_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event.event_id,
                event.timestamp.isoformat(),
                event.adapter_name,
                event.account_id,
                event.correlation_id,
                event.action,
                event.scope,
                inst.symbol if inst else None,
                inst.asset_class.value if inst else None,
                event.reason,
                event.detail,
                event.cleared_by,
            ),
        )

    # ---- Single-record reads ----

    async def get_position(self, instrument: Instrument) -> Position | None:
        i = _serialise_instrument(instrument)
        cursor = await self.conn.execute(
            "SELECT quantity, average_entry_price, updated_at FROM positions WHERE symbol=? AND asset_class=?",
            (i["symbol"], i["asset_class"]),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return Position(
            instrument=instrument,
            quantity=Decimal(row["quantity"]),
            average_entry_price=Decimal(row["average_entry_price"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    async def get_balance(self, currency: str) -> Balance | None:
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
        cursor = await self.conn.execute(
            "SELECT * FROM orders WHERE client_order_id=?",
            (client_order_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return self._row_to_order_record(row)

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
        cursor = await self.conn.execute(query, params)
        return [self._row_to_order_record(r) for r in await cursor.fetchall()]

    async def query_fills(
        self,
        *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FillRecord]:
        query = "SELECT * FROM fills WHERE 1=1"
        params: list[Any] = []
        if instrument is not None:
            query += " AND symbol=? AND asset_class=?"
            params.extend([instrument.symbol, instrument.asset_class.value])
        if start is not None:
            query += " AND fill_timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND fill_timestamp <= ?"
            params.append(end.isoformat())
        query += " ORDER BY fill_timestamp DESC LIMIT ?"
        params.append(limit)
        cursor = await self.conn.execute(query, params)
        return [self._row_to_fill_record(r) for r in await cursor.fetchall()]

    async def query_positions(
        self,
        *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Position]:
        query = "SELECT * FROM position_history WHERE 1=1"
        params: list[Any] = []
        if instrument is not None:
            query += " AND symbol=? AND asset_class=?"
            params.extend([instrument.symbol, instrument.asset_class.value])
        if start is not None:
            query += " AND recorded_at >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND recorded_at <= ?"
            params.append(end.isoformat())
        query += " ORDER BY recorded_at DESC, id DESC LIMIT ?"
        params.append(limit)
        cursor = await self.conn.execute(query, params)
        results: list[Position] = []
        for row in await cursor.fetchall():
            inst = _deserialise_instrument(dict(row))
            results.append(
                Position(
                    instrument=inst,
                    quantity=Decimal(row["quantity"]),
                    average_entry_price=Decimal(row["average_entry_price"]),
                    updated_at=datetime.fromisoformat(row["recorded_at"]),
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
        cursor = await self.conn.execute(query, params)
        results: list[HaltEvent] = []
        for row in await cursor.fetchall():
            inst = None
            if row["symbol"] is not None and row["asset_class"] is not None:
                inst = Instrument(
                    symbol=row["symbol"],
                    quote_currency=None,
                    asset_class=AssetClass(row["asset_class"]),
                    exchange=None,
                    currency=None,
                    expiry=None,
                    strike=None,
                    option_right=None,
                    multiplier=None,
                )
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
        )

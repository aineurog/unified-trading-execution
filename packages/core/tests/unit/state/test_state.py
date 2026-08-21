"""Unit tests for StateStore, reconciliation, and halt state machine — Sections 6.2–6.4, 17.11."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from dataclasses import replace
from decimal import Decimal

import pytest
import pytest_asyncio

from unified_trading_execution.events import AuditEvent
from unified_trading_execution.state.halt import HaltConfig, HaltStateMachine
from unified_trading_execution.state.reconciliation import reconcile
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import (
    AssetClass,
    HaltClearMode,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_inst(symbol="BTC"):
    return Instrument(
        symbol=symbol,
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_position(symbol="BTC", qty="0.5"):
    return Position(
        instrument=make_inst(symbol),
        quantity=Decimal(qty),
        average_entry_price=Decimal("50000"),
        updated_at=NOW,
    )


def make_balance(currency="USDT", free="9000", used="1000"):
    return Balance(
        currency=currency,
        free=Decimal(free),
        used=Decimal(used),
        total=Decimal(free) + Decimal(used),
        updated_at=NOW,
    )


def make_order(client_order_id="abc", status=OrderStatus.OPEN):
    return OrderRecord(
        instrument=make_inst(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
        price=Decimal("50000"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="plat-123",
        status=status,
        filled_quantity=Decimal("0"),
        average_fill_price=None,
        correlation_id="corr-1",
        created_at=NOW,
        updated_at=NOW,
    )


def make_fill(client_order_id="abc", qty="0.001", price="50000"):
    return FillRecord(
        client_order_id=client_order_id,
        platform_fill_id=f"fill-{client_order_id}",
        instrument=make_inst(),
        fill_quantity=Decimal(qty),
        fill_price=Decimal(price),
        fill_timestamp=NOW,
        fee_currency="USDT",
        fee_amount=Decimal("0.05"),
        correlation_id="corr-1",
    )


# ============================================================
# Fixtures
# ============================================================


@pytest_asyncio.fixture
async def store():
    s = SQLiteStateStore(":memory:")
    await s.initialize()
    yield s
    await s.close()


# ============================================================
# SQLiteStateStore — lifecycle
# ============================================================


class TestSQLiteStoreLifecycle:
    @pytest.mark.asyncio
    async def test_initialize_and_close(self):
        s = SQLiteStateStore(":memory:")
        await s.initialize()
        assert s._conn is not None
        await s.close()
        assert s._conn is None

    @pytest.mark.asyncio
    async def test_path_property(self, store):
        assert store.path == ":memory:"

    @pytest.mark.asyncio
    async def test_conn_raises_before_initialize(self):
        s = SQLiteStateStore(":memory:")
        with pytest.raises(RuntimeError, match="not initialised"):
            _ = s.conn


# ============================================================
# SQLiteStateStore — default location (Section 6.2)
# ============================================================


class TestDefaultStateStorePath:
    def test_encodes_project_dir_platform_and_account(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        from unified_trading_execution.state.store import default_state_store_path

        path = default_state_store_path("bybit", "acct123")
        project = tmp_path.name
        assert os.path.join(f"./{project}_data", "bybit_acct123.db") == path

    def test_slugs_unsafe_identifiers(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        from unified_trading_execution.state.store import default_state_store_path

        path = default_state_store_path("Bybit Pro!", "Acct/1")
        assert path.endswith("bybit_pro_acct_1.db")
        assert " " not in path
        assert path.endswith(".db")

    @pytest.mark.asyncio
    async def test_initialize_creates_parent_dir(self, tmp_path):
        target = str(tmp_path / "nested" / "dir" / "store.db")
        s = SQLiteStateStore(target)
        await s.initialize()
        assert (tmp_path / "nested" / "dir" / "store.db").exists()
        await s.close()


# ============================================================
# SQLiteStateStore — positions
# ============================================================


class TestSQLiteStorePositions:
    @pytest.mark.asyncio
    async def test_upsert_and_get_position(self, store):
        pos = make_position()
        await store.upsert_position(pos)
        got = await store.get_position(pos.instrument)
        assert got is not None
        assert got.quantity == Decimal("0.5")

    @pytest.mark.asyncio
    async def test_get_position_nonexistent(self, store):
        got = await store.get_position(make_inst("ETH"))
        assert got is None

    @pytest.mark.asyncio
    async def test_upsert_position_overwrites(self, store):
        pos1 = make_position(qty="0.5")
        await store.upsert_position(pos1)
        pos2 = make_position(qty="1.0")
        await store.upsert_position(pos2)
        got = await store.get_position(pos2.instrument)
        assert got.quantity == Decimal("1.0")

    @pytest.mark.asyncio
    async def test_position_history_recorded(self, store):
        await store.upsert_position(make_position(qty="0.5"))
        await store.upsert_position(make_position(qty="1.0"))
        history = await store.query_positions(instrument=make_inst())
        assert len(history) == 2
        assert history[0].quantity == Decimal("1.0")  # most recent first

    @pytest.mark.asyncio
    async def test_query_positions_filtered_by_time(self, store):
        await store.upsert_position(make_position(qty="0.5"))
        history = await store.query_positions(
            instrument=make_inst(),
            start=datetime(2025, 1, 1, tzinfo=UTC),
        )
        assert len(history) == 1


# ============================================================
# SQLiteStateStore — balances
# ============================================================


class TestSQLiteStoreBalances:
    @pytest.mark.asyncio
    async def test_upsert_and_get_balance(self, store):
        bal = make_balance()
        await store.upsert_balance(bal)
        got = await store.get_balance("USDT")
        assert got is not None
        assert got.free == Decimal("9000")

    @pytest.mark.asyncio
    async def test_get_balance_nonexistent(self, store):
        got = await store.get_balance("BTC")
        assert got is None

    @pytest.mark.asyncio
    async def test_balance_history_recorded(self, store):
        await store.upsert_balance(make_balance(free="9000"))
        await store.upsert_balance(make_balance(free="8000"))
        history = await store.query_balances(currency="USDT")
        assert len(history) == 2

    @pytest.mark.asyncio
    async def test_query_balances_filtered_by_currency(self, store):
        await store.upsert_balance(make_balance("USDT"))
        await store.upsert_balance(make_balance("BTC", "1", "0"))
        history = await store.query_balances(currency="BTC")
        assert len(history) == 1
        assert history[0].currency == "BTC"


# ============================================================
# SQLiteStateStore — orders
# ============================================================


class TestSQLiteStoreOrders:
    @pytest.mark.asyncio
    async def test_upsert_and_get_order(self, store):
        order = make_order()
        await store.upsert_order(order)
        got = await store.get_order("abc")
        assert got is not None
        assert got.status == OrderStatus.OPEN

    @pytest.mark.asyncio
    async def test_get_order_nonexistent(self, store):
        got = await store.get_order("nonexistent")
        assert got is None

    @pytest.mark.asyncio
    async def test_upsert_order_updates(self, store):
        await store.upsert_order(make_order(status=OrderStatus.OPEN))
        await store.upsert_order(make_order(status=OrderStatus.FILLED))
        got = await store.get_order("abc")
        assert got.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_query_orders_filtered_by_instrument(self, store):
        await store.upsert_order(make_order("abc"))
        await store.upsert_order(make_order("def"))
        results = await store.query_orders(instrument=make_inst())
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_query_orders_filtered_by_time(self, store):
        await store.upsert_order(make_order("abc"))
        results = await store.query_orders(
            start=datetime(2025, 1, 1, tzinfo=UTC),
        )
        assert len(results) == 1


# ============================================================
# SQLiteStateStore — fills
# ============================================================


class TestSQLiteStoreFills:
    @pytest.mark.asyncio
    async def test_upsert_and_query_fills(self, store):
        fill = make_fill()
        await store.upsert_fill(fill)
        results = await store.query_fills()
        assert len(results) == 1
        assert results[0].fill_quantity == Decimal("0.001")

    @pytest.mark.asyncio
    async def test_upsert_fill_updates_existing_platform_fill(self, store):
        first = make_fill("first", qty="0.001")
        second = replace(
            make_fill("second", qty="0.002"),
            platform_fill_id=first.platform_fill_id,
        )

        await store.upsert_fill(first)
        await store.upsert_fill(second)

        results = await store.query_fills()
        assert len(results) == 1
        assert results[0].platform_fill_id == first.platform_fill_id
        assert results[0].client_order_id == "second"
        assert results[0].fill_quantity == Decimal("0.002")

    @pytest.mark.asyncio
    async def test_query_fills_filtered_by_instrument(self, store):
        await store.upsert_fill(make_fill("abc"))
        await store.upsert_fill(make_fill("def"))
        results = await store.query_fills(instrument=make_inst())
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_query_fills_filtered_by_time(self, store):
        await store.upsert_fill(make_fill("abc"))
        results = await store.query_fills(
            start=datetime(2025, 1, 1, tzinfo=UTC),
        )
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_batched_fills_insert(self, store):
        fills = [make_fill("abc"), make_fill("def")]
        await store.upsert_fills_batch(fills)
        results = await store.query_fills()
        assert len(results) == 2


# ============================================================
# SQLiteStateStore — order_history (append-only lifecycle)
# ============================================================


class TestSQLiteStoreOrderHistory:
    @pytest.mark.asyncio
    async def test_upsert_appends_history_snapshot(self, store):
        await store.upsert_order(make_order(status=OrderStatus.OPEN))
        await store.upsert_order(make_order(status=OrderStatus.FILLED))
        cursor = await store.conn.execute(
            "SELECT COUNT(*) AS n FROM order_history WHERE client_order_id='abc'"
        )
        row = await cursor.fetchone()
        assert row["n"] == 2

    @pytest.mark.asyncio
    async def test_orders_keeps_latest_snapshot(self, store):
        await store.upsert_order(make_order(status=OrderStatus.OPEN))
        await store.upsert_order(make_order(status=OrderStatus.FILLED))
        got = await store.get_order("abc")
        assert got is not None
        assert got.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    async def test_orphan_removal_preserves_history(self, store):
        """Deleting an orphan from `orders` must not destroy its lifecycle log."""
        await store.upsert_order(make_order(status=OrderStatus.OPEN))
        await store.delete_orders_by_client_ids(["abc"])
        assert await store.get_order("abc") is None
        cursor = await store.conn.execute(
            "SELECT COUNT(*) AS n FROM order_history WHERE client_order_id='abc'"
        )
        row = await cursor.fetchone()
        assert row["n"] == 1


# ============================================================
# SQLiteStateStore — query_open_orders (live statuses only)
# ============================================================


class TestSQLiteStoreOpenOrders:
    @pytest.mark.asyncio
    async def test_only_live_statuses_returned(self, store):
        for status in (
            OrderStatus.PENDING,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ):
            await store.upsert_order(make_order(client_order_id=f"cid-{status.value}", status=status))
        open_orders = await store.query_open_orders()
        cids = {o.client_order_id for o in open_orders}
        assert cids == {"cid-PENDING", "cid-OPEN", "cid-PARTIALLY_FILLED"}

    @pytest.mark.asyncio
    async def test_terminal_orders_not_returned(self, store):
        await store.upsert_order(make_order(client_order_id="closed", status=OrderStatus.FILLED))
        assert await store.query_open_orders() == []


# ============================================================
# SQLiteStateStore — reconciliation watermark
# ============================================================


class TestReconcileWatermark:
    @pytest.mark.asyncio
    async def test_default_is_none(self, store):
        assert await store.get_reconcile_watermark() is None

    @pytest.mark.asyncio
    async def test_roundtrip(self, store):
        wm = datetime(2026, 7, 27, 8, 0, 0, tzinfo=UTC)
        await store.set_reconcile_watermark(wm)
        got = await store.get_reconcile_watermark()
        assert got == wm
        assert got.tzinfo is not None  # stored timezone-aware

    @pytest.mark.asyncio
    async def test_overwrites(self, store):
        await store.set_reconcile_watermark(datetime(2026, 1, 1, tzinfo=UTC))
        later = datetime(2026, 7, 28, tzinfo=UTC)
        await store.set_reconcile_watermark(later)
        assert await store.get_reconcile_watermark() == later


# ============================================================
# SQLiteStateStore — delete_fills_by_client_ids (window-bounded)
# ============================================================


class TestDeleteFillsSince:
    @pytest.mark.asyncio
    async def test_delete_without_since_removes_all(self, store):
        await store.upsert_fill(make_fill("abc", qty="0.1"))
        await store.delete_fills_by_client_ids(["abc"])
        assert await store.query_fills() == []

    @pytest.mark.asyncio
    async def test_delete_with_since_preserves_older_fills(self, store):
        # Distinct timestamps so the window filter can tell them apart.
        from datetime import timedelta

        old = FillRecord(
            client_order_id="abc",
            platform_fill_id="fill-old",
            instrument=make_inst(),
            fill_quantity=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_timestamp=NOW - timedelta(hours=1),
            fee_currency="USDT",
            fee_amount=Decimal("0.05"),
            correlation_id="corr-old",
        )
        new = FillRecord(
            client_order_id="abc",
            platform_fill_id="fill-new",
            instrument=make_inst(),
            fill_quantity=Decimal("0.2"),
            fill_price=Decimal("50000"),
            fill_timestamp=NOW,
            fee_currency="USDT",
            fee_amount=Decimal("0.05"),
            correlation_id="corr-new",
        )
        await store.upsert_fill(old)
        await store.upsert_fill(new)

        await store.delete_fills_by_client_ids(["abc"], since=NOW - timedelta(minutes=30))

        remaining = await store.query_fills()
        assert len(remaining) == 1
        assert remaining[0].platform_fill_id == "fill-old"


# ============================================================
# SQLiteStateStore — audit events
# ============================================================


class TestSQLiteStoreAudit:
    @pytest.mark.asyncio
    async def test_write_audit_event(self, store):
        await store.write_audit_event(
            AuditEvent(
                event_id="evt-1",
                timestamp=NOW,
                adapter_name="bybit",
                account_id="acct-1",
                correlation_id="corr-1",
                event_type="order.placed",
                payload={"client_order_id": "abc"},
            )
        )
        # Verify it's written by querying the table directly
        cursor = await store.conn.execute("SELECT * FROM audit_events WHERE event_id=?", ("evt-1",))
        row = await cursor.fetchone()
        assert row is not None
        assert row["event_type"] == "order.placed"

    @pytest.mark.asyncio
    async def test_write_audit_event_rejects_duplicate_event_id(self, store):
        event = AuditEvent(
            event_id="evt-dup",
            timestamp=NOW,
            adapter_name="bybit",
            account_id="acct-1",
            correlation_id="corr-1",
            event_type="order.placed",
            payload={},
        )
        await store.write_audit_event(event)
        with pytest.raises(Exception):  # UNIQUE constraint
            await store.write_audit_event(event)

    @pytest.mark.asyncio
    async def test_reconciliation_event_roundtrip(self, store):
        from unified_trading_execution.events import ReconciliationEvent, ReconciliationMismatch

        m = ReconciliationMismatch(
            mismatch_type="position_quantity",
            instrument=make_inst(),
            local_value="0.5",
            platform_value="1.0",
        )
        evt = ReconciliationEvent(
            event_id="rec-1",
            timestamp=NOW,
            adapter_name="bybit",
            account_id="acct-1",
            correlation_id=None,
            mismatches=(m,),
            duration_ms=12.3,
        )
        await store.write_reconciliation_event(evt)
        results = await store.query_reconciliation_events()
        assert len(results) == 1
        assert results[0].mismatches[0].mismatch_type == "position_quantity"

    @pytest.mark.asyncio
    async def test_halt_event_roundtrip(self, store):
        from unified_trading_execution.events import HaltEvent

        evt = HaltEvent(
            event_id="halt-1",
            timestamp=NOW,
            adapter_name="bybit",
            account_id="acct-1",
            correlation_id=None,
            action="entered",
            scope="instrument",
            instrument=make_inst(),
            reason="position_quantity_mismatch",
            detail="qty drift",
            cleared_by=None,
        )
        await store.write_halt_event(evt)
        results = await store.query_halt_events()
        assert len(results) == 1
        assert results[0].action == "entered"
        # The full instrument identity round-trips — including quote_currency,
        # which Instrument now requires for SPOT.
        assert results[0].instrument == evt.instrument
        assert results[0].instrument.quote_currency == "USDT"


# ============================================================
# SQLiteStateStore — WAL and performance
# ============================================================


class TestSQLiteStorePerformance:
    @pytest.mark.asyncio
    async def test_wal_mode_enabled(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        s = SQLiteStateStore(db_path)
        await s.initialize()
        try:
            cursor = await s.conn.execute("PRAGMA journal_mode")
            row = await cursor.fetchone()
            assert row[0].upper() == "WAL"
        finally:
            await s.close()

    @pytest.mark.asyncio
    async def test_synchronous_normal(self, store):
        cursor = await store.conn.execute("PRAGMA synchronous")
        row = await cursor.fetchone()
        assert row[0] == 1  # NORMAL

    @pytest.mark.asyncio
    async def test_foreign_keys_enabled(self, store):
        cursor = await store.conn.execute("PRAGMA foreign_keys")
        row = await cursor.fetchone()
        assert row[0] == 1


# ============================================================
# Reconciliation — 5 mismatch cases (Section 6.3)
# ============================================================


class TestReconciliation:
    """Each mismatch case from Section 6.3 has its own distinct test."""

    def test_case1_position_quantity_mismatch(self):
        inst = make_inst()
        local = {inst: make_position(qty="0.5")}
        platform = {inst: make_position(qty="1.0")}
        result = reconcile(
            local_positions=local,
            platform_positions=platform,
            local_balances={},
            platform_balances={},
            local_orders={},
            platform_orders={},
            local_fills={},
            platform_fills={},
        )
        assert len(result.position_mismatches) == 1
        assert result.position_mismatches[0].mismatch_type == "position_quantity"
        assert result.position_mismatches[0].local_value == "0.5"
        assert result.position_mismatches[0].platform_value == "1.0"

    def test_case2_balance_mismatch(self):
        local = {"USDT": make_balance(free="9000")}
        platform = {"USDT": make_balance(free="8000")}
        result = reconcile(
            local_positions={},
            platform_positions={},
            local_balances=local,
            platform_balances=platform,
            local_orders={},
            platform_orders={},
            local_fills={},
            platform_fills={},
        )
        assert len(result.balance_mismatches) == 1
        assert result.balance_mismatches[0].mismatch_type == "balance"

    def test_case3_orphan_order_on_platform(self):
        platform_orders = {"abc": make_order("abc")}
        result = reconcile(
            local_positions={},
            platform_positions={},
            local_balances={},
            platform_balances={},
            local_orders={},
            platform_orders=platform_orders,
            local_fills={},
            platform_fills={},
        )
        assert len(result.orphan_orders_on_platform) == 1
        assert result.orphan_orders_on_platform[0].client_order_id == "abc"

    def test_case4_orphan_order_in_local(self):
        local_orders = {"abc": make_order("abc")}
        result = reconcile(
            local_positions={},
            platform_positions={},
            local_balances={},
            platform_balances={},
            local_orders=local_orders,
            platform_orders={},
            local_fills={},
            platform_fills={},
        )
        assert len(result.orphan_orders_in_local) == 1
        assert result.orphan_orders_in_local[0] == "abc"

    def test_case5_partial_fill_discrepancy(self):
        local_fills = {"abc": [make_fill("abc", qty="0.001")]}
        platform_fills = {"abc": [make_fill("abc", qty="0.002")]}
        result = reconcile(
            local_positions={},
            platform_positions={},
            local_balances={},
            platform_balances={},
            local_orders={},
            platform_orders={},
            local_fills=local_fills,
            platform_fills=platform_fills,
        )
        assert len(result.partial_fill_discrepancies) == 1
        assert result.partial_fill_discrepancies[0].mismatch_type == "partial_fill"

    def test_clean_reconciliation(self):
        inst = make_inst()
        pos = {inst: make_position()}
        bal = {"USDT": make_balance()}
        orders = {"abc": make_order()}
        fills = {"abc": [make_fill("abc")]}
        result = reconcile(
            local_positions=pos,
            platform_positions=pos,
            local_balances=bal,
            platform_balances=bal,
            local_orders=orders,
            platform_orders=orders,
            local_fills=fills,
            platform_fills=fills,
        )
        assert result.is_clean
        assert len(result.all_mismatches) == 0

    def test_all_mismatches_combined(self):
        """When multiple mismatches occur, all_mismatches returns them all."""
        inst = make_inst()
        result = reconcile(
            local_positions={inst: make_position(qty="0.5")},
            platform_positions={inst: make_position(qty="1.0")},
            local_balances={"USDT": make_balance(free="9000")},
            platform_balances={"USDT": make_balance(free="8000")},
            local_orders={},
            platform_orders={"orphan": make_order("orphan")},
            local_fills={},
            platform_fills={},
        )
        assert not result.is_clean
        # position mismatch + balance mismatch + orphan on platform
        assert len(result.all_mismatches) >= 3

    def test_position_absence_is_quantity_zero(self):
        """A local position with no platform counterpart is a quantity drift.

        Presence/absence is normalised to quantity 0 on the absent side so an
        open-on-one-side / flat-on-the-other is detected (not skipped).
        """
        inst = make_inst()
        local = {inst: make_position(qty="0.5")}
        result = reconcile(
            local_positions=local,
            platform_positions={},  # platform has no open position
            local_balances={},
            platform_balances={},
            local_orders={},
            platform_orders={},
            local_fills={},
            platform_fills={},
        )
        assert len(result.position_mismatches) == 1
        assert result.position_mismatches[0].platform_value == "absent"

    def test_balance_absence_is_zero(self):
        """A local balance with no platform counterpart is a balance drift."""
        local = {"USDT": make_balance(free="9000")}
        result = reconcile(
            local_positions={},
            platform_positions={},
            local_balances=local,
            platform_balances={},
            local_orders={},
            platform_orders={},
            local_fills={},
            platform_fills={},
        )
        assert len(result.balance_mismatches) == 1
        assert result.balance_mismatches[0].platform_value == "absent"

    def test_none_platform_dataset_is_skipped(self):
        """A ``None`` platform dataset (unsupported fetch) is skipped entirely.

        It must never be mistaken for an empty platform, which would falsely
        flag every local entry as local-only.
        """
        inst = make_inst()
        local_positions = {inst: make_position(qty="0.5")}
        local_balances = {"USDT": make_balance(free="9000")}
        result = reconcile(
            local_positions=local_positions,
            platform_positions=None,  # unsupported
            local_balances=local_balances,
            platform_balances=None,  # unsupported
            local_orders={},
            platform_orders=None,
            local_fills={},
            platform_fills=None,
        )
        assert result.position_mismatches == []
        assert result.balance_mismatches == []
        assert result.is_clean


# ============================================================
# Halt state machine — configurability (Section 6.4)
# ============================================================


class TestHaltStateMachine:
    """Every configurability combination from Section 6.4 is tested."""

    def test_default_config(self):
        hsm = HaltStateMachine()
        assert hsm.config.auto_halt_enabled is True
        assert hsm.config.closing_orders_permitted is True
        assert hsm.config.clear_mode == HaltClearMode.AUTOMATIC

    # ---- Enter / clear halts ----

    def test_enter_instrument_halt(self):
        hsm = HaltStateMachine()
        inst = make_inst()
        changed = hsm.enter_halt("instrument", inst, "position_quantity_mismatch", "drift")
        assert changed is True
        assert hsm.is_instrument_halted(inst)
        assert hsm.is_halted(inst)

    def test_enter_account_halt(self):
        hsm = HaltStateMachine()
        changed = hsm.enter_halt("account", None, "balance_mismatch", "USDT off")
        assert changed is True
        assert hsm.is_account_halted()
        assert hsm.is_halted()  # no instrument needed

    def test_duplicate_instrument_halt_is_noop(self):
        hsm = HaltStateMachine()
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "reason", "detail")
        changed = hsm.enter_halt("instrument", inst, "reason2", "detail2")
        assert changed is False

    def test_duplicate_account_halt_is_noop(self):
        hsm = HaltStateMachine()
        hsm.enter_halt("account", None, "reason", "detail")
        changed = hsm.enter_halt("account", None, "reason2", "detail2")
        assert changed is False

    # ---- Auto clear ----

    def test_automatic_clear_on_clean_reconciliation(self):
        hsm = HaltStateMachine(HaltConfig(clear_mode=HaltClearMode.AUTOMATIC))
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "position_quantity_mismatch", "drift")
        cleared = hsm.try_clear_halt("instrument", inst, reconciliation_is_clean=True)
        assert cleared is True
        assert not hsm.is_halted(inst)

    def test_automatic_does_not_clear_when_still_mismatched(self):
        hsm = HaltStateMachine(HaltConfig(clear_mode=HaltClearMode.AUTOMATIC))
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "position_quantity_mismatch", "drift")
        cleared = hsm.try_clear_halt("instrument", inst, reconciliation_is_clean=False)
        assert cleared is False
        assert hsm.is_halted(inst)

    # ---- Manual clear ----

    def test_manual_clear_requires_explicit_ack(self):
        hsm = HaltStateMachine(HaltConfig(clear_mode=HaltClearMode.MANUAL))
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "position_quantity_mismatch", "drift")
        # Clean reconciliation alone is not enough
        cleared = hsm.try_clear_halt("instrument", inst, reconciliation_is_clean=True)
        assert cleared is False
        # Explicit manual clear works
        cleared = hsm.try_clear_halt(
            "instrument", inst, reconciliation_is_clean=True, manual_clear=True
        )
        assert cleared is True

    def test_manual_clear_without_manual_flag_fails(self):
        hsm = HaltStateMachine(HaltConfig(clear_mode=HaltClearMode.MANUAL))
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "reason", "detail")
        cleared = hsm.try_clear_halt("instrument", inst, manual_clear=False)
        assert cleared is False

    # ---- Closing orders permitted ----

    def test_closing_orders_permitted_default(self):
        hsm = HaltStateMachine()
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "reason", "detail")
        # reduce_only orders allowed
        assert hsm.can_place_order(inst, reduce_only=True) is True
        # non-reduce_only orders blocked
        assert hsm.can_place_order(inst, reduce_only=False) is False

    def test_closing_orders_blocked_when_disabled(self):
        hsm = HaltStateMachine(HaltConfig(closing_orders_permitted=False))
        inst = make_inst()
        hsm.enter_halt("instrument", inst, "reason", "detail")
        assert hsm.can_place_order(inst, reduce_only=True) is False

    # ---- Auto-halt disabled ----

    def test_auto_halt_disabled_ignores_enter(self):
        hsm = HaltStateMachine(HaltConfig(auto_halt_enabled=False))
        inst = make_inst()
        changed = hsm.enter_halt("instrument", inst, "reason", "detail")
        assert changed is False
        assert not hsm.is_halted(inst)

    # ---- Clear non-halted is noop ----

    def test_clear_non_halted_instrument(self):
        hsm = HaltStateMachine()
        cleared = hsm.try_clear_halt("instrument", make_inst(), reconciliation_is_clean=True)
        assert cleared is False

    def test_clear_non_halted_account(self):
        hsm = HaltStateMachine()
        cleared = hsm.try_clear_halt("account", None, reconciliation_is_clean=True)
        assert cleared is False

    # ---- Account halt affects all instruments ----

    def test_account_halt_affects_all_instruments(self):
        hsm = HaltStateMachine()
        hsm.enter_halt("account", None, "balance_mismatch", "USDT")
        assert hsm.is_halted(make_inst("BTC"))
        assert hsm.is_halted(make_inst("ETH"))

    def test_account_halt_cannot_be_cleared_by_instrument_clear(self):
        hsm = HaltStateMachine()
        hsm.enter_halt("account", None, "balance_mismatch", "USDT")
        cleared = hsm.try_clear_halt("instrument", make_inst(), reconciliation_is_clean=True)
        assert cleared is False
        assert hsm.is_account_halted()

    # ---- active_halts ----

    def test_active_halts_lists_all(self):
        hsm = HaltStateMachine()
        hsm.enter_halt("instrument", make_inst("BTC"), "reason", "detail")
        hsm.enter_halt("instrument", make_inst("ETH"), "reason", "detail")
        halts = hsm.active_halts()
        assert len(halts) == 2

    def test_active_halts_includes_account(self):
        hsm = HaltStateMachine()
        hsm.enter_halt("account", None, "reason", "detail")
        halts = hsm.active_halts()
        assert len(halts) == 1
        assert halts[0].scope == "account"


# ============================================================
# SQLiteStateStore — adapter_config (Section 2.1)
# ============================================================


class TestAdapterConfig:
    @pytest.mark.asyncio
    async def test_migration_creates_adapter_config_table(self):
        """002_adapter_config.sql runs on initialize and creates the table."""
        s = SQLiteStateStore(":memory:")
        await s.initialize()
        try:
            cursor = await s.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='adapter_config'"
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "adapter_config"
        finally:
            await s.close()

    @pytest.mark.asyncio
    async def test_set_and_get(self, store):
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, store):
        assert await store.get_adapter_config("leverage.NOPE") is None

    @pytest.mark.asyncio
    async def test_set_overwrites_existing(self, store):
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        await store.set_adapter_config("leverage.BTCUSDT", "50")
        assert await store.get_adapter_config("leverage.BTCUSDT") == "50"

    @pytest.mark.asyncio
    async def test_set_updates_updated_at(self, store):
        async def read_updated_at() -> str:
            cursor = await store.conn.execute(
                "SELECT updated_at FROM adapter_config WHERE key=?", ("leverage.BTCUSDT",)
            )
            row = await cursor.fetchone()
            assert row is not None
            return row[0]

        await store.set_adapter_config("leverage.BTCUSDT", "10")
        first = datetime.fromisoformat(await read_updated_at())
        assert first.tzinfo is not None  # stored as timezone-aware ISO-8601 UTC
        await asyncio.sleep(0.001)
        await store.set_adapter_config("leverage.BTCUSDT", "20")
        second = datetime.fromisoformat(await read_updated_at())
        assert second > first

    @pytest.mark.asyncio
    async def test_delete_removes_key(self, store):
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        await store.delete_adapter_config("leverage.BTCUSDT")
        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    @pytest.mark.asyncio
    async def test_delete_missing_is_noop(self, store):
        await store.delete_adapter_config("leverage.NOPE")  # should not raise

    @pytest.mark.asyncio
    async def test_list_by_prefix(self, store):
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        await store.set_adapter_config("leverage.ETHUSDT", "20")
        await store.set_adapter_config("margin_mode.BTCUSDT", "cross")
        result = await store.list_adapter_config("leverage.")
        assert result == {"leverage.BTCUSDT": "10", "leverage.ETHUSDT": "20"}

    @pytest.mark.asyncio
    async def test_list_when_empty(self, store):
        assert await store.list_adapter_config("leverage.") == {}

    @pytest.mark.asyncio
    async def test_list_excludes_non_matching(self, store):
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        await store.set_adapter_config("margin_mode.BTCUSDT", "cross")
        assert await store.list_adapter_config("leverage.") == {"leverage.BTCUSDT": "10"}

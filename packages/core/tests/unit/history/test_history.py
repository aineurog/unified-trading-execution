"""Unit tests for history/ accessors — verify genuine filterability (Section 10.2).

Each accessor must apply filters to the underlying queries, not just ignore
them and return everything. Tests seed data with known instrument/time values
then assert that only matching records come back.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from unified_trading_execution.state.store import SQLiteStateStore

# Module under test
from unified_trading_execution import history
from unified_trading_execution.events import HaltEvent, ReconciliationEvent
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


# ── helpers ──────────────────────────────────────────────────────────

def _instrument(symbol: str = "BTCUSDT") -> Instrument:
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


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _order(
    *,
    client_order_id: str = "test-001",
    instrument: Instrument | None = None,
    created_at: datetime | None = None,
) -> OrderRecord:
    return OrderRecord(
        instrument=instrument or _instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=1,
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
        price=50000,
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id=f"pf-{client_order_id}",
        status=OrderStatus.OPEN,
        filled_quantity=0,
        average_fill_price=None,
        correlation_id="corr-1",
        created_at=created_at or _utcnow(),
        updated_at=_utcnow(),
    )


def _fill(
    *,
    client_order_id: str = "test-001",
    platform_fill_id: str = "f-001",
    instrument: Instrument | None = None,
    fill_timestamp: datetime | None = None,
) -> FillRecord:
    return FillRecord(
        client_order_id=client_order_id,
        platform_fill_id=platform_fill_id,
        instrument=instrument or _instrument(),
        fill_quantity=1,
        fill_price=50000,
        fill_timestamp=fill_timestamp or _utcnow(),
        fee_currency="USDT",
        fee_amount=1,
        correlation_id="corr-1",
    )


# ── fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
async def store():
    s = SQLiteStateStore(":memory:")
    await s.initialize()
    yield s
    await s.close()


# ── order history filterability ──────────────────────────────────────


class TestQueryOrderHistory:
    async def test_no_filters_returns_all(self, store):
        await store.upsert_order(_order(client_order_id="o-1"))
        await store.upsert_order(_order(client_order_id="o-2"))
        results = await history.query_order_history(store)
        assert len(results) == 2

    async def test_instrument_filter(self, store):
        btc = _instrument("BTCUSDT")
        eth = _instrument("ETHUSDT")
        await store.upsert_order(_order(client_order_id="btc", instrument=btc))
        await store.upsert_order(_order(client_order_id="eth", instrument=eth))

        results = await history.query_order_history(store, instrument=btc)
        assert len(results) == 1
        assert results[0].client_order_id == "btc"

    async def test_time_range_filter(self, store):
        now = _utcnow()
        t1 = now - timedelta(hours=3)
        t2 = now - timedelta(hours=2)
        t3 = now - timedelta(hours=1)

        await store.upsert_order(_order(client_order_id="old", created_at=t1))
        await store.upsert_order(_order(client_order_id="mid", created_at=t2))
        await store.upsert_order(_order(client_order_id="new", created_at=t3))

        results = await history.query_order_history(
            store, start=t2 - timedelta(minutes=30), end=t2 + timedelta(minutes=30),
        )
        assert len(results) == 1
        assert results[0].client_order_id == "mid"

    async def test_combined_filters(self, store):
        btc = _instrument("BTCUSDT")
        eth = _instrument("ETHUSDT")
        now = _utcnow()
        t_old = now - timedelta(hours=3)
        t_new = now - timedelta(hours=1)

        await store.upsert_order(_order(client_order_id="btc-old", instrument=btc, created_at=t_old))
        await store.upsert_order(_order(client_order_id="eth-old", instrument=eth, created_at=t_old))
        await store.upsert_order(_order(client_order_id="btc-new", instrument=btc, created_at=t_new))

        results = await history.query_order_history(
            store,
            instrument=btc,
            start=now - timedelta(hours=2),
        )
        assert len(results) == 1
        assert results[0].client_order_id == "btc-new"

    async def test_limit(self, store):
        for i in range(5):
            await store.upsert_order(_order(client_order_id=f"limit-{i}"))
        results = await history.query_order_history(store, limit=2)
        assert len(results) == 2


# ── fill history filterability ───────────────────────────────────────


class TestQueryFillHistory:
    async def test_instrument_filter(self, store):
        btc = _instrument("BTCUSDT")
        eth = _instrument("ETHUSDT")
        await store.upsert_fill(_fill(client_order_id="f1", platform_fill_id="pf1", instrument=btc))
        await store.upsert_fill(_fill(client_order_id="f2", platform_fill_id="pf2", instrument=eth))

        results = await history.query_fill_history(store, instrument=btc)
        assert len(results) == 1
        assert results[0].client_order_id == "f1"

    async def test_time_range_filter(self, store):
        now = _utcnow()
        t1 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)

        await store.upsert_fill(_fill(client_order_id="early", platform_fill_id="fe", fill_timestamp=t1))
        await store.upsert_fill(_fill(client_order_id="late", platform_fill_id="fl", fill_timestamp=t2))

        results = await history.query_fill_history(
            store, start=now - timedelta(minutes=90),
        )
        assert len(results) == 1
        assert results[0].client_order_id == "late"


# ── position history filterability ───────────────────────────────────


class TestQueryPositionHistory:
    async def test_instrument_filter(self, store):
        btc = _instrument("BTCUSDT")
        eth = _instrument("ETHUSDT")
        await store.upsert_position(Position(
            instrument=btc, quantity=1, average_entry_price=50000,
            updated_at=_utcnow(),
        ))
        await store.upsert_position(Position(
            instrument=eth, quantity=2, average_entry_price=3000,
            updated_at=_utcnow(),
        ))

        results = await history.query_position_history(store, instrument=btc)
        assert len(results) == 1
        assert results[0].instrument.symbol == "BTCUSDT"

    async def test_no_filters_returns_all(self, store):
        await store.upsert_position(Position(
            instrument=_instrument("BTCUSDT"), quantity=1,
            average_entry_price=50000, updated_at=_utcnow(),
        ))
        await store.upsert_position(Position(
            instrument=_instrument("ETHUSDT"), quantity=2,
            average_entry_price=3000, updated_at=_utcnow(),
        ))
        results = await history.query_position_history(store)
        assert len(results) >= 2


# ── balance history filterability ────────────────────────────────────


class TestQueryBalanceHistory:
    async def test_currency_filter(self, store):
        await store.upsert_balance(Balance(
            currency="USDT", free=1000, used=0, total=1000, updated_at=_utcnow(),
        ))
        await store.upsert_balance(Balance(
            currency="BTC", free=1, used=0, total=1, updated_at=_utcnow(),
        ))

        results = await history.query_balance_history(store, currency="USDT")
        assert len(results) == 1
        assert results[0].currency == "USDT"

    async def test_no_filter_returns_all(self, store):
        await store.upsert_balance(Balance(
            currency="USDT", free=1000, used=0, total=1000, updated_at=_utcnow(),
        ))
        await store.upsert_balance(Balance(
            currency="BTC", free=1, used=0, total=1, updated_at=_utcnow(),
        ))
        results = await history.query_balance_history(store)
        assert len(results) >= 2

    async def test_wrong_currency_returns_empty(self, store):
        await store.upsert_balance(Balance(
            currency="USDT", free=1000, used=0, total=1000, updated_at=_utcnow(),
        ))
        results = await history.query_balance_history(store, currency="ETH")
        assert results == []


# ── reconciliation events filterability ──────────────────────────────


class TestQueryReconciliationEvents:
    async def test_time_range_filter(self, store):
        now = _utcnow()
        t1 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)

        await store.write_reconciliation_event(ReconciliationEvent(
            event_id="rec-1", timestamp=t1, adapter_name="mock",
            account_id="acc", correlation_id=None, mismatches=(), duration_ms=100,
        ))
        await store.write_reconciliation_event(ReconciliationEvent(
            event_id="rec-2", timestamp=t2, adapter_name="mock",
            account_id="acc", correlation_id=None, mismatches=(), duration_ms=200,
        ))

        results = await history.query_reconciliation_events(
            store, start=now - timedelta(minutes=90),
        )
        assert len(results) == 1
        assert results[0].event_id == "rec-2"


# ── halt events filterability ────────────────────────────────────────


class TestQueryHaltEvents:
    async def test_time_range_filter(self, store):
        now = _utcnow()
        t1 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)

        await store.write_halt_event(HaltEvent(
            event_id="h-1", timestamp=t1, adapter_name="mock",
            account_id="acc", correlation_id=None,
            action="entered", scope="instrument", instrument=_instrument(),
            reason="test", detail="", cleared_by=None,
        ))
        await store.write_halt_event(HaltEvent(
            event_id="h-2", timestamp=t2, adapter_name="mock",
            account_id="acc", correlation_id=None,
            action="cleared", scope="instrument", instrument=_instrument(),
            reason="test", detail="", cleared_by="automatic",
        ))

        results = await history.query_halt_events(
            store, start=now - timedelta(minutes=90),
        )
        assert len(results) == 1
        assert results[0].event_id == "h-2"

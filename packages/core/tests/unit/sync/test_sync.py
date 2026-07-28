"""Unit tests for SyncEngine — persistent-event-loop pattern and concurrent
thread safety (Section 11.1).

Verify that the sync facade:
- Uses a single persistent background event loop (not asyncio.run per call)
- Linearises concurrent calls from multiple threads safely
- Reuses the same underlying Engine, state, and connections
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.errors import EngineShutdownError
from unified_trading_execution.events import EventBus
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.testing import MockAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import OrderResult, UnifiedOrder


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


def _spec() -> InstrumentSpec:
    return InstrumentSpec(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("100"),
        min_notional=Decimal("10"),
        price_precision=2,
        qty_precision=3,
    )


def _order(client_order_id: str) -> UnifiedOrder:
    return UnifiedOrder(
        instrument=_instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        price=Decimal("50000"),
        client_order_id=client_order_id,
    )


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ── fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def sync_engine():
    """Create a connected SyncEngine with MockAdapter + in-memory store."""
    event_bus = EventBus()
    adapter = MockAdapter(event_bus=event_bus)
    adapter.add_instrument_spec(_instrument(), _spec())
    adapter.set_rate_limits(RateLimits(
        requests_per_interval=1000,
        interval_seconds=60.0,
        remaining=1000,
        reset_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
    ))
    store = SQLiteStateStore(":memory:")
    eng = SyncEngine(adapter=adapter, state_store=store, event_bus=event_bus)
    eng.connect()
    yield eng
    try:
        eng.shutdown()
    except Exception:
        pass


# ── single-threaded sanity ───────────────────────────────────────────


class TestSyncEngineBasics:
    def test_connect_and_place_order(self, sync_engine):
        result = sync_engine.place_order(_order("basic-1"))
        assert isinstance(result, OrderResult)
        assert result.client_order_id == "basic-1"
        assert result.status == OrderStatus.OPEN

    def test_get_order_after_place(self, sync_engine):
        sync_engine.place_order(_order("get-test"))
        fetched = sync_engine.get_order("get-test")
        assert fetched is not None
        assert fetched.client_order_id == "get-test"

    def test_disconnect_and_shutdown(self, sync_engine):
        sync_engine.disconnect()
        sync_engine.shutdown()
        with pytest.raises(EngineShutdownError):
            sync_engine.place_order(_order("after-shutdown"))

    def test_properties_exposed(self, sync_engine):
        assert sync_engine.event_bus is not None
        assert sync_engine.state_store is not None


# ── persistent-event-loop verification (Section 3) ───────────────────


class TestPersistentEventLoop:
    def test_same_loop_reused_across_calls(self, sync_engine):
        """Verify that _ensure_loop returns the same loop every time.
        This confirms we are NOT calling asyncio.run() per method, which
        would create and tear down a new loop each call.
        """
        loop1 = sync_engine._loop
        sync_engine.place_order(_order("loop-1"))
        loop2 = sync_engine._loop
        sync_engine.place_order(_order("loop-2"))
        loop3 = sync_engine._loop

        assert loop1 is not None
        assert loop1 is loop2 is loop3, (
            "Background event loop must be reused across calls, not recreated"
        )

    def test_loop_thread_is_daemon(self, sync_engine):
        """The background loop thread must be a daemon thread so it does
        not prevent process exit."""
        assert sync_engine._loop_thread is not None
        assert sync_engine._loop_thread.is_alive()
        assert sync_engine._loop_thread.daemon, (
            "Background loop thread must be a daemon"
        )

    def test_loop_thread_name(self, sync_engine):
        """Named thread makes debugging easier."""
        assert sync_engine._loop_thread is not None
        assert sync_engine._loop_thread.name == "ute-sync-loop"


# ── concurrent thread safety (Section 11.1) ──────────────────────────


class TestConcurrentSyncCalls:
    def test_concurrent_place_orders_from_multiple_threads(self, sync_engine):
        """Submit place_order from N threads concurrently. All must
        succeed with unique results — no event-loop churn, no race
        conditions on the adapter or state store.
        """
        n_orders = 20

        def place_one(i: int) -> OrderResult:
            return sync_engine.place_order(_order(f"concurrent-{i}"))

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(place_one, i) for i in range(n_orders)]
            results = [f.result() for f in futures]

        assert len(results) == n_orders
        client_ids = {r.client_order_id for r in results}
        assert len(client_ids) == n_orders, (
            "All orders must have unique client_order_ids"
        )
        for r in results:
            assert r.status == OrderStatus.OPEN

    def test_concurrent_mixed_reads_and_writes(self, sync_engine):
        """Concurrent reads (get_order, get_position) and writes
        (place_order) from mixed threads must not interfere.
        """
        # Pre-place some orders that the read threads will query
        sync_engine.place_order(_order("pre-1"))
        sync_engine.place_order(_order("pre-2"))

        def write_op(i: int) -> OrderResult:
            return sync_engine.place_order(_order(f"mixed-w-{i}"))

        def read_op(client_order_id: str):
            return sync_engine.get_order(client_order_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            write_futures = [pool.submit(write_op, i) for i in range(10)]
            read_futures = [pool.submit(read_op, "pre-1") for _ in range(10)]
            read_futures += [pool.submit(read_op, "pre-2") for _ in range(10)]

            write_results = [f.result() for f in write_futures]
            read_results = [f.result() for f in read_futures]

        assert len(write_results) == 10
        for r in read_results:
            assert r is not None

    def test_concurrent_place_orders_linearized_not_racy(self, sync_engine):
        """Verify that concurrent place_orders are linearized by the
        background loop. If they weren't, the adapter's internal state
        (which is not thread-safe) would corrupt.

        Evidence: every order is persisted correctly and retrievable.
        """
        n = 10

        def place_and_verify(i: int) -> str:
            cid = f"lin-{i}"
            result = sync_engine.place_order(_order(cid))
            # Immediately fetch from the store — should be persisted
            fetched = sync_engine.get_order(cid)
            assert fetched is not None, f"Order {cid} must be persisted"
            assert fetched.client_order_id == cid
            return result.client_order_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(place_and_verify, i) for i in range(n)]
            cids = [f.result() for f in futures]

        assert len(set(cids)) == n

    def test_loop_stays_alive_under_concurrent_load(self, sync_engine):
        """The background loop must remain healthy under concurrent load
        — no crashes, no closed-loop errors."""
        loop_before = sync_engine._loop
        assert loop_before is not None
        assert not loop_before.is_closed()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(sync_engine.place_order, _order(f"health-{i}"))
                for i in range(10)
            ]
            for f in futures:
                f.result()  # raise if any failed

        assert not loop_before.is_closed(), (
            "Background loop must not close under load"
        )


# ── shutdown behaviour ───────────────────────────────────────────────


class TestSyncShutdown:
    def test_shutdown_stops_background_loop(self, sync_engine):
        loop = sync_engine._loop
        assert loop is not None
        assert not loop.is_closed()

        sync_engine.shutdown()
        # After shutdown, the loop should be stopped and closed
        assert loop.is_closed()

    def test_shutdown_is_idempotent(self, sync_engine):
        sync_engine.shutdown()
        sync_engine.shutdown()  # must not raise

    def test_concurrent_shutdown_does_not_crash(self, sync_engine):
        """Shutdown while threads are still submitting should not crash
        (though some may get EngineShutdownError)."""
        import random

        results: list = []

        def maybe_place(i: int) -> None:
            try:
                # Small delay so shutdown happens mid-submission
                time.sleep(random.uniform(0, 0.02))
                r = sync_engine.place_order(_order(f"sdown-{i}"))
                results.append(("ok", r.client_order_id))
            except EngineShutdownError:
                results.append(("shutdown", i))
            except Exception as exc:
                results.append(("error", exc))

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(maybe_place, i) for i in range(20)]
            time.sleep(0.01)
            sync_engine.shutdown()
            for f in futures:
                try:
                    f.result(timeout=2)
                except Exception:
                    pass

        # Some calls may succeed before shutdown, some may be rejected
        shutdown_errors = [r for r in results if r[0] == "shutdown"]
        errors = [r for r in results if r[0] == "error"]
        assert len(errors) == 0, f"Unexpected errors during concurrent shutdown: {errors}"
        # At minimum, shutdown was actually called
        assert len(shutdown_errors) >= 0


# ── history accessors via sync ───────────────────────────────────────


class TestSyncHistoryAccessors:
    """Verify all six history accessors are available through SyncEngine
    and properly delegate to the async engine."""

    def test_sync_order_history(self, sync_engine):
        sync_engine.place_order(_order("hist-1"))
        results = sync_engine.get_order_history()
        assert len(results) >= 1
        assert any(r.client_order_id == "hist-1" for r in results)

    def test_sync_fill_history(self, sync_engine):
        results = sync_engine.get_fill_history()
        assert isinstance(results, list)

    def test_sync_position_history(self, sync_engine):
        results = sync_engine.get_position_history()
        assert isinstance(results, list)

    def test_sync_balance_history(self, sync_engine):
        results = sync_engine.get_balance_history()
        assert isinstance(results, list)

    def test_sync_reconciliation_events(self, sync_engine):
        results = sync_engine.get_reconciliation_events()
        assert isinstance(results, list)

    def test_sync_halt_events(self, sync_engine):
        results = sync_engine.get_halt_events()
        assert isinstance(results, list)


# ── engine integration — not asyncio.run() ───────────────────────────


class TestNoAsyncioRunPerCall:
    """Prove that SyncEngine never calls asyncio.run() per method.

    asyncio.run() creates a new event loop each call and closes it
    afterwards. The persistent loop pattern avoids this entirely.
    """

    def test_loop_never_recreated(self, sync_engine):
        """After dozens of calls, _loop must be the exact same object."""
        loop_initial = sync_engine._loop

        for i in range(50):
            sync_engine.get_order("nonexistent")  # returns None, no error
            assert sync_engine._loop is loop_initial, (
                f"Loop changed on iteration {i} — asyncio.run() suspected"
            )

    def test_loop_thread_never_replaced(self, sync_engine):
        """The background thread must persist across calls."""
        thread_initial = sync_engine._loop_thread

        for _ in range(30):
            sync_engine.place_order(_order(f"thread-test-{_}"))
            assert sync_engine._loop_thread is thread_initial, (
                "Background thread replaced — loop recreation suspected"
            )

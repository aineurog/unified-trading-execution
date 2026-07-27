"""Sync facade — thin blocking wrapper over the async Engine.

The sync API is NOT implemented by calling asyncio.run() per method — that
creates and tears down a new event loop on every call, breaking connection
reuse and severely harming performance. Instead, a single persistent
background event loop is created at construction; each sync method submits
work to that loop via asyncio.run_coroutine_threadsafe and blocks the
calling thread until it completes.

Both APIs share the same underlying Engine, state, and connections — there
are never two divergent codepaths to maintain (Section 3 of the requirements).
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from decimal import Decimal
from typing import Callable

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import EngineShutdownError
from unified_trading_execution.events import EventBus, HaltEvent, ReconciliationEvent
from unified_trading_execution.state import StateStore
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position


class SyncEngine:
    """Blocking (synchronous) wrapper around the async Engine.

    Construction:
        engine = SyncEngine(
            adapter=BybitAdapter(...),
            state_store=SQLiteStateStore("path/to/db"),
            get_reference_price=my_price_fn,  # optional
        )
        engine.connect()

    Usage:
        order = UnifiedOrder(...)
        result = engine.place_order(order)
        engine.disconnect()

    Thread safety: concurrent sync calls from multiple threads are safe —
    they are linearized by the background event loop. The async core remains
    single-threaded.
    """

    def __init__(
        self,
        adapter: Adapter,
        state_store: StateStore,
        *,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self._async_engine = Engine(
            adapter=adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._shutdown = False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Lazily start the persistent background event loop on first use."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(
                target=self._loop.run_forever,
                name="ute-sync-loop",
                daemon=True,
            )
            self._loop_thread.start()
        return self._loop

    def _run(self, coro):
        """Submit a coroutine to the persistent loop and block until done."""
        self._check_not_shutdown()
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    # ---- Lifecycle ----

    def connect(self) -> None:
        """Connect adapter and initialise state store (blocking)."""
        self._run(self._async_engine.connect())

    def disconnect(self) -> None:
        """Disconnect adapter gracefully (blocking)."""
        self._run(self._async_engine.disconnect())

    def shutdown(self) -> None:
        """Ordered teardown: flush audit, disconnect, close state store, stop loop."""
        if self._shutdown:
            return
        self._shutdown = True
        self._async_engine.shutdown()
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._loop_thread is not None and self._loop_thread.is_alive():
                self._loop_thread.join(timeout=5)
            self._loop.close()

    # ---- Order operations ----

    def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Place an order through the full pipeline (blocking)."""
        return self._run(self._async_engine.place_order(order))

    def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing order (blocking)."""
        return self._run(self._async_engine.modify_order(modification))

    def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an order by client_order_id (blocking)."""
        return self._run(self._async_engine.cancel_order(client_order_id))

    def get_order(self, client_order_id: str) -> OrderResult | None:
        """Query an order's status from the platform (blocking)."""
        return self._run(self._async_engine.get_order(client_order_id))

    # ---- Instrument metadata ----

    def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch and cache trading rules (blocking)."""
        return self._run(self._async_engine.fetch_instrument_spec(instrument))

    # ---- State mirror access ----

    def get_position(self, instrument: Instrument) -> Position | None:
        """Read mirrored position (blocking)."""
        return self._run(self._async_engine.get_position(instrument))

    def get_all_positions(self) -> list[Position]:
        """Read all mirrored positions (blocking)."""
        return self._run(self._async_engine.get_all_positions())

    def get_balance(self, currency: str) -> Balance | None:
        """Read mirrored balance (blocking)."""
        return self._run(self._async_engine.get_balance(currency))

    def get_all_balances(self) -> list[Balance]:
        """Read all mirrored balances (blocking)."""
        return self._run(self._async_engine.get_all_balances())

    # ---- History accessors ----

    def get_order_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OrderRecord]:
        """Query order history (blocking)."""
        return self._run(self._async_engine.get_order_history(
            instrument=instrument, start=start, end=end,
        ))

    def get_fill_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[FillRecord]:
        """Query fill history (blocking)."""
        return self._run(self._async_engine.get_fill_history(
            instrument=instrument, start=start, end=end,
        ))

    def get_position_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Position]:
        """Query position history (blocking)."""
        return self._run(self._async_engine.get_position_history(
            instrument=instrument, start=start, end=end,
        ))

    def get_balance_history(
        self,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Balance]:
        """Query balance history (blocking)."""
        return self._run(self._async_engine.get_balance_history(
            currency=currency, start=start, end=end,
        ))

    def get_reconciliation_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[ReconciliationEvent]:
        """Query reconciliation events (blocking)."""
        return self._run(self._async_engine.get_reconciliation_events(
            start=start, end=end,
        ))

    def get_halt_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[HaltEvent]:
        """Query halt entry/clear events (blocking)."""
        return self._run(self._async_engine.get_halt_events(
            start=start, end=end,
        ))

    # ---- Properties ----

    @property
    def event_bus(self) -> EventBus:
        """The event bus — subscribe from any thread before connecting."""
        return self._async_engine.event_bus

    @property
    def state_store(self) -> StateStore:
        """The state store — for direct access to its path (e.g. backups)."""
        return self._async_engine.state_store

    # ---- Internal ----

    def _check_not_shutdown(self) -> None:
        if self._shutdown:
            raise EngineShutdownError("SyncEngine has been shut down and is permanently unusable.")

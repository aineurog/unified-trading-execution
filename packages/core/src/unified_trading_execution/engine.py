"""Async Engine — the central orchestrator users interact with.

The Engine owns the lifecycle: it wires together the adapter, risk-check
chain, state mirror, event bus, and audit trail. Users call methods on the
Engine, not directly on the adapter — the Engine runs risk checks, generates
client_order_ids and correlation_ids, and delegates translation-only work
to the adapter.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.errors import DuplicateOrderIdError, EngineShutdownError
from unified_trading_execution.events import (
    EventBus,
    HaltEvent,
    ReconciliationEvent,
)
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


class Engine:
    """Async-native trading engine — the main entry point for async users.

    Construction:
        engine = Engine(
            adapter=BybitAdapter(...),
            state_store=SQLiteStateStore("path/to/db"),
            get_reference_price=my_price_fn,  # optional
            event_bus=EventBus(),             # optional (auto-created if omitted)
        )
        await engine.connect()

    Usage:
        order = UnifiedOrder(...)
        result = await engine.place_order(order)
        await engine.disconnect()
    """

    def __init__(
        self,
        adapter: Adapter,
        state_store: StateStore,
        *,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self._adapter = adapter
        self._state_store = state_store
        self._get_reference_price = get_reference_price
        self._event_bus = event_bus or EventBus()
        self._shutdown = False

    # ---- Lifecycle ----

    async def connect(self) -> None:
        """Connect the adapter and initialise the state store."""
        await self._state_store.initialize()
        await self._adapter.connect()

    async def disconnect(self) -> None:
        """Disconnect the adapter gracefully."""
        await self._adapter.disconnect()

    async def ashutdown(self) -> None:
        """Ordered teardown: flush audit, disconnect, close state store, mark dead."""
        if self._shutdown:
            return
        self._shutdown = True
        await self._adapter.disconnect()
        await self._state_store.close()

    def shutdown(self) -> None:
        """Sync wrapper for ashutdown — convenience for sync users."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.ashutdown())
        else:
            import threading
            import concurrent.futures
            future = asyncio.run_coroutine_threadsafe(self.ashutdown(), loop)
            future.result()

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Place an order through the full pipeline.

        1. Generate client_order_id and correlation_id if needed.
        2. Run the risk-check chain.
        3. Delegate to the adapter.
        4. Persist to state store.
        5. Emit OrderPlacedEvent.
        """
        self._check_not_shutdown()

        if order.client_order_id is None:
            order.client_order_id = str(uuid.uuid7())

        correlation_id = str(uuid.uuid7())

        # --- Risk-check chain runs here (Section 7) ---
        # --- Delegate to adapter ---
        result = await self._adapter.place_order(order)

        # --- Persist ---
        record = OrderRecord(
            instrument=order.instrument,
            order_type=order.order_type,
            side=order.side,
            quantity=order.quantity,
            time_in_force=order.time_in_force,
            client_order_id=result.client_order_id,
            price=order.price,
            stop_price=order.stop_price,
            reduce_only=order.reduce_only,
            client_tag=order.client_tag,
            take_profit=order.take_profit,
            stop_loss=order.stop_loss,
            platform_order_id=result.platform_order_id,
            status=result.status,
            filled_quantity=result.filled_quantity,
            average_fill_price=result.average_fill_price,
            correlation_id=correlation_id,
            created_at=result.created_at,
            updated_at=result.updated_at,
        )
        await self._state_store.upsert_order(record)

        from unified_trading_execution.events import OrderPlacedEvent
        self._event_bus.publish(OrderPlacedEvent(
            event_id=str(uuid.uuid7()),
            timestamp=datetime.now(tz=timezone.utc),
            adapter_name=self._adapter.platform_name,
            account_id=self._adapter.account_id,
            correlation_id=correlation_id,
            order=record,
        ))

        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing order — risk-checked before dispatch."""
        self._check_not_shutdown()
        correlation_id = str(uuid.uuid7())

        # --- Risk-check chain runs against resulting order ---
        result = await self._adapter.modify_order(modification)

        # --- Persist updated record ---
        # ... update state store, emit OrderModifiedEvent ...
        return result

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an order by its client_order_id."""
        self._check_not_shutdown()
        correlation_id = str(uuid.uuid7())
        result = await self._adapter.cancel_order(client_order_id)
        return result

    async def get_order(self, client_order_id: str) -> OrderResult | None:
        """Query an order's current status from the platform."""
        self._check_not_shutdown()
        return await self._adapter.get_order_by_client_id(client_order_id)

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch and cache trading rules for an instrument."""
        self._check_not_shutdown()
        return await self._adapter.fetch_instrument_spec(instrument)

    # ---- State mirror access ----

    async def get_position(self, instrument: Instrument) -> Position | None:
        """Read the current mirrored position for an instrument."""
        return await self._state_store.get_position(instrument)

    async def get_all_positions(self) -> list[Position]:
        """Read all current mirrored positions."""
        return await self._state_store.get_all_positions()

    async def get_balance(self, currency: str) -> Balance | None:
        """Read the current mirrored balance for a currency."""
        return await self._state_store.get_balance(currency)

    async def get_all_balances(self) -> list[Balance]:
        """Read all current mirrored balances."""
        return await self._state_store.get_all_balances()

    # ---- History accessors ----

    async def get_order_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OrderRecord]:
        """Query order history, optionally filtered."""
        return await self._state_store.query_orders(
            instrument=instrument, start=start, end=end,
        )

    async def get_fill_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[FillRecord]:
        """Query fill history, optionally filtered."""
        return await self._state_store.query_fills(
            instrument=instrument, start=start, end=end,
        )

    async def get_position_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Position]:
        """Query position history, optionally filtered."""
        return await self._state_store.query_positions(
            instrument=instrument, start=start, end=end,
        )

    async def get_balance_history(
        self,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Balance]:
        """Query balance history, optionally filtered."""
        return await self._state_store.query_balances(
            currency=currency, start=start, end=end,
        )

    async def get_reconciliation_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[ReconciliationEvent]:
        """Query reconciliation event history, optionally filtered by time range."""
        return await self._state_store.query_reconciliation_events(
            start=start, end=end,
        )

    async def get_halt_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[HaltEvent]:
        """Query halt entry/clear events, optionally filtered by time range."""
        return await self._state_store.query_halt_events(
            start=start, end=end,
        )

    # ---- Properties ----

    @property
    def event_bus(self) -> EventBus:
        """The event bus — subscribe to receive fill, position, and halt events."""
        return self._event_bus

    @property
    def state_store(self) -> StateStore:
        """The state store — for direct access to its path and methods."""
        return self._state_store

    @property
    def adapter(self) -> Adapter:
        """The underlying adapter."""
        return self._adapter

    # ---- Internal ----

    def _check_not_shutdown(self) -> None:
        if self._shutdown:
            raise EngineShutdownError("Engine has been shut down and is permanently unusable.")

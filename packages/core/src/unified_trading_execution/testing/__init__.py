"""Public mock adapter — shipped with the core package, not a private test-only fixture.

Both the project's own unit tests and any integrator's own strategy tests are
expected to depend on this module. It is the officially supported way to test
code built against this engine without hitting a real testnet.
"""

from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timezone
from decimal import Decimal

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.types.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
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


def _new_id() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class MockAdapter(Adapter):
    """Full Adapter ABC implementation with scriptable responses for testing.

    Every async method that can succeed or fail draws from an internal queue
    of scripted responses. Tests push expected results onto these queues
    before calling the method. If the queue is empty, the adapter returns
    a sensible default rather than crashing — but a test that hasn't
    scripted what it needs will get an obviously dummy result.

    Error injection: call ``set_next_error(exc)`` to make the next call to
    *any* async method raise that exception. The error is consumed after one
    use and does not affect subsequent calls.

    Event injection: ``inject_fill()``, ``inject_position_update()``,
    ``inject_balance_update()`` publish events to the EventBus exactly as a
    real adapter's websocket handler would.

    The adapter is not thread-safe — single-threaded asyncio usage only.
    """

    def __init__(
        self,
        *,
        event_bus: EventBus,
        platform_name: str = "mock",
        account_id: str = "mock-account",
    ) -> None:
        self._event_bus = event_bus
        self._platform_name = platform_name
        self._account_id = account_id
        self._connected = False

        # Internal order book: client_order_id -> OrderRecord
        self._orders: dict[str, OrderRecord] = {}

        # Scriptable response queues — one per method
        self._place_order_queue: deque[OrderResult | BaseException] = deque()
        self._modify_order_queue: deque[OrderResult | BaseException] = deque()
        self._cancel_order_queue: deque[OrderResult | BaseException] = deque()
        self._get_order_queue: deque[OrderResult | BaseException | None] = deque()

        # Seeded instrument specs: Instrument -> InstrumentSpec
        self._instrument_specs: dict[Instrument, InstrumentSpec] = {}

        # Seeded platform state for reconciliation
        self._positions: dict[Instrument, Position] = {}
        self._balances: dict[str, Balance] = {}
        self._fills: list[FillRecord] = []

        # Configurable capabilities
        self._supported_order_types: frozenset[OrderType] = frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )
        self._rate_limits = RateLimits(
            requests_per_interval=100,
            interval_seconds=60.0,
            remaining=100,
            reset_at=_utcnow(),
        )

        # Single-shot error injection
        self._next_error: BaseException | None = None

    # ---- Helpers for test authors ----

    @property
    def event_bus(self) -> EventBus:
        """The EventBus this adapter publishes to — for test assertions."""
        return self._event_bus

    @property
    def orders(self) -> dict[str, OrderRecord]:
        """The internal order book — for test assertions."""
        return dict(self._orders)

    # -- Error injection --

    def set_next_error(self, exc: BaseException) -> None:
        """Make the next call to any async method raise this exception.

        Consumed after one use. Set to None to clear.
        """
        self._next_error = exc

    # -- Response queueing --

    def queue_place_order_response(self, result: OrderResult | BaseException) -> None:
        """Queue a response for the next ``place_order()`` call."""
        self._place_order_queue.append(result)

    def queue_modify_order_response(self, result: OrderResult | BaseException) -> None:
        """Queue a response for the next ``modify_order()`` call."""
        self._modify_order_queue.append(result)

    def queue_cancel_order_response(self, result: OrderResult | BaseException) -> None:
        """Queue a response for the next ``cancel_order()`` call."""
        self._cancel_order_queue.append(result)

    def queue_get_order_response(self, result: OrderResult | BaseException | None) -> None:
        """Queue a response for the next ``get_order_by_client_id()`` call."""
        self._get_order_queue.append(result)

    # -- Event injection (simulates websocket stream) --

    def inject_fill(self, fill: FillRecord) -> None:
        """Publish a FillEvent to the bus as if received from a websocket stream."""
        self._event_bus.publish(
            FillEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self._platform_name,
                account_id=self._account_id,
                correlation_id=fill.correlation_id,
                fill=fill,
            )
        )

    def inject_position_update(self, position: Position) -> None:
        """Publish a PositionUpdateEvent to the bus."""
        self._event_bus.publish(
            PositionUpdateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self._platform_name,
                account_id=self._account_id,
                correlation_id=None,
                position=position,
            )
        )

    def inject_balance_update(self, balance: Balance) -> None:
        """Publish a BalanceUpdateEvent to the bus."""
        self._event_bus.publish(
            BalanceUpdateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self._platform_name,
                account_id=self._account_id,
                correlation_id=None,
                balance=balance,
            )
        )

    # -- Instrument specs --

    def add_instrument_spec(self, instrument: Instrument, spec: InstrumentSpec) -> None:
        """Seed a response for ``fetch_instrument_spec()``."""
        self._instrument_specs[instrument] = spec

    # -- Capability configuration --

    def set_rate_limits(self, rl: RateLimits) -> None:
        """Override the rate-limit state returned by ``get_rate_limits()``."""
        self._rate_limits = rl

    def set_supported_order_types(self, types: frozenset[OrderType]) -> None:
        """Override the set returned by ``supported_order_types()``."""
        self._supported_order_types = types

    # -- Order book seeding --

    def seed_order(self, order: OrderRecord) -> None:
        """Pre-seed an order in the internal book (e.g., for get_order_by_client_id)."""
        self._orders[order.client_order_id] = order

    def set_connected(self, connected: bool) -> None:
        """Force the connection state (without publishing an event)."""
        self._connected = connected

    # ---- Identification ----

    @property
    def platform_name(self) -> str:
        return self._platform_name

    @property
    def account_id(self) -> str:
        return self._account_id

    # ---- Connection lifecycle ----

    async def connect(self) -> None:
        if (err := self._consume_error()) is not None:
            raise err
        self._connected = True
        self._event_bus.publish(
            ConnectionStateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self._platform_name,
                account_id=self._account_id,
                correlation_id=None,
                connected=True,
            )
        )

    async def disconnect(self) -> None:
        if (err := self._consume_error()) is not None:
            raise err
        self._connected = False
        self._event_bus.publish(
            ConnectionStateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self._platform_name,
                account_id=self._account_id,
                correlation_id=None,
                connected=False,
            )
        )

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        if (err := self._consume_error()) is not None:
            raise err
        if self._place_order_queue:
            response = self._place_order_queue.popleft()
            if isinstance(response, BaseException):
                raise response
            result = response
        else:
            # Default: create a stub ACCEPTED result
            now = _utcnow()
            result = OrderResult(
                client_order_id=order.client_order_id or _new_id(),
                platform_order_id=f"mock-{_new_id()[:8]}",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=now,
                updated_at=now,
            )
        # Record in internal book
        self._orders[result.client_order_id] = _order_request_to_record(order, result)
        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        if (err := self._consume_error()) is not None:
            raise err
        if self._modify_order_queue:
            response = self._modify_order_queue.popleft()
            if isinstance(response, BaseException):
                raise response
            return response
        existing = self._orders.get(modification.client_order_id)
        if existing is None:
            raise OrderNotFoundError(modification.client_order_id)
        # Default: return updated record
        updated = OrderRecord(
            instrument=existing.instrument,
            order_type=existing.order_type,
            side=existing.side,
            quantity=modification.quantity
            if modification.quantity is not None
            else existing.quantity,
            time_in_force=existing.time_in_force,
            client_order_id=existing.client_order_id,
            price=modification.price if modification.price is not None else existing.price,
            stop_price=modification.stop_price
            if modification.stop_price is not None
            else existing.stop_price,
            reduce_only=existing.reduce_only,
            client_tag=existing.client_tag,
            take_profit=modification.take_profit
            if modification.take_profit is not None
            else existing.take_profit,
            stop_loss=modification.stop_loss
            if modification.stop_loss is not None
            else existing.stop_loss,
            platform_order_id=existing.platform_order_id,
            status=existing.status,
            filled_quantity=existing.filled_quantity,
            average_fill_price=existing.average_fill_price,
            correlation_id=existing.correlation_id,
            created_at=existing.created_at,
            updated_at=_utcnow(),
        )
        self._orders[existing.client_order_id] = updated
        return OrderResult(
            client_order_id=updated.client_order_id,
            platform_order_id=updated.platform_order_id,
            status=updated.status,
            filled_quantity=updated.filled_quantity,
            average_fill_price=updated.average_fill_price,
            created_at=updated.created_at,
            updated_at=updated.updated_at,
        )

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        if (err := self._consume_error()) is not None:
            raise err
        if self._cancel_order_queue:
            response = self._cancel_order_queue.popleft()
            if isinstance(response, BaseException):
                raise response
            return response
        existing = self._orders.get(client_order_id)
        if existing is None:
            raise OrderNotFoundError(client_order_id)
        # Default: mark cancelled
        now = _utcnow()
        cancelled = OrderRecord(
            instrument=existing.instrument,
            order_type=existing.order_type,
            side=existing.side,
            quantity=existing.quantity,
            time_in_force=existing.time_in_force,
            client_order_id=existing.client_order_id,
            price=existing.price,
            stop_price=existing.stop_price,
            reduce_only=existing.reduce_only,
            client_tag=existing.client_tag,
            take_profit=existing.take_profit,
            stop_loss=existing.stop_loss,
            platform_order_id=existing.platform_order_id,
            status=OrderStatus.CANCELLED,
            filled_quantity=existing.filled_quantity,
            average_fill_price=existing.average_fill_price,
            correlation_id=existing.correlation_id,
            created_at=existing.created_at,
            updated_at=now,
        )
        self._orders[existing.client_order_id] = cancelled
        return OrderResult(
            client_order_id=cancelled.client_order_id,
            platform_order_id=cancelled.platform_order_id,
            status=cancelled.status,
            filled_quantity=cancelled.filled_quantity,
            average_fill_price=cancelled.average_fill_price,
            created_at=cancelled.created_at,
            updated_at=cancelled.updated_at,
        )

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        if (err := self._consume_error()) is not None:
            raise err
        if self._get_order_queue:
            response = self._get_order_queue.popleft()
            if isinstance(response, BaseException):
                raise response
            return response
        # Default: look up in internal book
        record = self._orders.get(client_order_id)
        if record is None:
            return None
        return OrderResult(
            client_order_id=record.client_order_id,
            platform_order_id=record.platform_order_id,
            status=record.status,
            filled_quantity=record.filled_quantity,
            average_fill_price=record.average_fill_price,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        if (err := self._consume_error()) is not None:
            raise err
        spec = self._instrument_specs.get(instrument)
        if spec is None:
            raise InvalidSymbolError(f"No spec seeded for {instrument.symbol}")
        return spec

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        return self._supported_order_types

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        if (err := self._consume_error()) is not None:
            raise err
        return self._rate_limits

    # ---- Reconciliation data (optional Adapter ABC methods) ----

    async def fetch_positions(self) -> dict[Instrument, Position]:
        if (err := self._consume_error()) is not None:
            raise err
        return dict(self._positions)

    async def fetch_balances(self) -> dict[str, Balance]:
        if (err := self._consume_error()) is not None:
            raise err
        return dict(self._balances)

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        if (err := self._consume_error()) is not None:
            raise err
        return {
            cid: rec
            for cid, rec in self._orders.items()
            if rec.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
        }

    async def fetch_fills(
        self, *, since: datetime | None = None
    ) -> dict[str, list[FillRecord]]:
        if (err := self._consume_error()) is not None:
            raise err
        result: dict[str, list[FillRecord]] = {}
        for fill in self._fills:
            if since is not None and fill.fill_timestamp < since:
                continue
            result.setdefault(fill.client_order_id, []).append(fill)
        return result

    # -- Seeding helpers for reconciliation tests --

    def seed_position(self, position: Position) -> None:
        """Pre-seed a position for fetch_positions()."""
        self._positions[position.instrument] = position

    def seed_balance(self, balance: Balance) -> None:
        """Pre-seed a balance for fetch_balances()."""
        self._balances[balance.currency] = balance

    def seed_fill(self, fill: FillRecord) -> None:
        """Pre-seed a fill for fetch_fills()."""
        self._fills.append(fill)

    # ---- Internal ----

    def _consume_error(self) -> BaseException | None:
        err = self._next_error
        self._next_error = None
        return err


def _order_request_to_record(order: UnifiedOrder, result: OrderResult) -> OrderRecord:
    """Convert a UnifiedOrder + OrderResult into a full OrderRecord."""
    return OrderRecord(
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
        correlation_id=f"mock-{_new_id()[:8]}",
        created_at=result.created_at,
        updated_at=result.updated_at,
    )

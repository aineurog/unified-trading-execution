"""Internal event bus — the v2 extensibility mechanism present from v1.

A simple synchronous pub/sub within the async event loop — no queueing,
no persistence, no cross-process delivery in v1.

The EventBus itself publishes events only. Audit-trail writes happen in the
Engine: after ``event_bus.publish(event)`` returns, the Engine calls
``state_store.write_audit_event(...)`` directly in the same coroutine — not
via a subscriber (which would require async callbacks, breaking the
synchronous callback contract).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


@dataclass(frozen=True, slots=True)
class Event:
    """Base type for all events published on the bus."""

    event_id: str  # UUID7
    timestamp: datetime  # UTC, timezone-aware
    adapter_name: str
    account_id: str
    correlation_id: str | None  # None for events not tied to a user action


@dataclass(frozen=True, slots=True)
class FillEvent(Event):
    fill: FillRecord


@dataclass(frozen=True, slots=True)
class PositionUpdateEvent(Event):
    position: Position


@dataclass(frozen=True, slots=True)
class BalanceUpdateEvent(Event):
    balance: Balance


@dataclass(frozen=True, slots=True)
class ConnectionStateEvent(Event):
    connected: bool


@dataclass(frozen=True, slots=True)
class OrderPlacedEvent(Event):
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class OrderModifiedEvent(Event):
    order: OrderRecord
    previous: OrderRecord


@dataclass(frozen=True, slots=True)
class OrderCancelledEvent(Event):
    client_order_id: str
    instrument: Instrument


@dataclass(frozen=True, slots=True)
class ReconciliationCompleteEvent(Event):
    mismatches: tuple[ReconciliationMismatch, ...]  # empty tuple = clean


@dataclass(frozen=True, slots=True)
class HaltEnteredEvent(Event):
    scope: Literal["instrument", "account"]
    instrument: Instrument | None  # None when scope="account"
    reason: str  # machine-readable, e.g. "position_quantity_mismatch"
    detail: str  # human-readable


@dataclass(frozen=True, slots=True)
class HaltClearedEvent(Event):
    scope: Literal["instrument", "account"]
    instrument: Instrument | None
    cleared_by: Literal["automatic", "manual"]


@dataclass(frozen=True, slots=True)
class ReconciliationMismatch:
    mismatch_type: Literal[
        "position_quantity",
        "balance",
        "orphan_on_platform",
        "orphan_in_local",
        "partial_fill",
    ]
    instrument: Instrument | None
    local_value: str  # JSON-serialized
    platform_value: str  # JSON-serialized


@dataclass(frozen=True, slots=True)
class ReconciliationEvent:
    """Audit-trail record for a reconciliation pass — persisted in state store.

    Distinct from ReconciliationCompleteEvent, which is the bus event.
    """

    event_id: str  # UUID7
    timestamp: datetime  # UTC
    adapter_name: str
    account_id: str
    correlation_id: str | None
    mismatches: tuple[ReconciliationMismatch, ...]  # empty = clean
    duration_ms: float


@dataclass(frozen=True, slots=True)
class HaltEvent:
    """Audit-trail record for a halt entry or clear — persisted in state store.

    Distinct from HaltEnteredEvent / HaltClearedEvent, which are bus events.
    """

    event_id: str  # UUID7
    timestamp: datetime  # UTC
    adapter_name: str
    account_id: str
    correlation_id: str | None
    action: Literal["entered", "cleared"]
    scope: Literal["instrument", "account"]
    instrument: Instrument | None
    reason: str  # machine-readable, e.g. "position_quantity_mismatch"
    detail: str  # human-readable
    cleared_by: Literal["automatic", "manual"] | None  # None when action="entered"


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """Audit-trail record for an order lifecycle event — persisted in state store
    (Section 17.11).

    Distinct from the bus events (OrderPlacedEvent, OrderModifiedEvent,
    OrderCancelledEvent). This is the immutable, typed audit record written
    directly by dispatch/ after the bus publish succeeds.
    """

    event_id: str  # UUID7
    timestamp: datetime  # UTC
    adapter_name: str
    account_id: str
    correlation_id: str
    event_type: str  # "order.placed" | "order.modified" | "order.cancelled"
    payload: dict  # structured metadata, e.g. {"client_order_id": "..."}


Subscriber = Callable[[Event], None]


class EventBus:
    """Synchronous pub/sub within the async event loop.

    Subscribers must not raise — an exception in a subscriber is logged
    and does not prevent remaining subscribers from running.

    Dispatch is O(subscribers) with no intermediate queues and no async
    fan-out within a single publish call.
    """

    def __init__(self) -> None:
        self._subscribers: dict[type[Event], list[Subscriber]] = defaultdict(list)

    def subscribe(self, event_type: type[Event], callback: Subscriber) -> None:
        """Register a callback for an event type.

        Subscribers are called in registration order. Subscribers for
        a supertype (e.g., Event) receive all events.
        """
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: type[Event], callback: Subscriber) -> None:
        try:
            self._subscribers[event_type].remove(callback)
        except (KeyError, ValueError):
            pass

    def publish(self, event: Event) -> None:
        """Publish an event to all matching subscribers synchronously."""
        for event_type, subscribers in self._subscribers.items():
            if isinstance(event, event_type):
                for callback in subscribers:
                    try:
                        callback(event)
                    except Exception:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.exception(
                            "Subscriber %s raised for event %s",
                            callback,
                            event.event_id,
                        )

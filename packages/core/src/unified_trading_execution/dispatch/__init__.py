"""Dispatch orchestration — order placement, modification, cancellation flow.

Each function in this module is a pure async orchestrator: it takes explicit
dependencies (adapter, state store, event bus, etc.) and orchestrates the
full lifecycle of a single dispatch operation. Functions are stateless — all
mutable state is owned by the Engine.

The Engine calls these functions after preparing the pre-dispatch context
(instrument spec, reference price, known order IDs, rate-limit budget).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from uuid_extensions import uuid7
from decimal import Decimal
from typing import Callable, Awaitable

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import UnsupportedOrderTypeError
from unified_trading_execution.events import (
    AuditEvent,
    EventBus,
    OrderCancelledEvent,
    OrderModifiedEvent,
    OrderPlacedEvent,
)
from unified_trading_execution.risk import RiskConfig, run_risk_checks
from unified_trading_execution.state import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)

logger = logging.getLogger(__name__)


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ── place order ────────────────────────────────────────────────────


async def dispatch_place_order(
    *,
    adapter: Adapter,
    state_store: StateStore,
    event_bus: EventBus,
    risk_config: RiskConfig,
    halt_machine: HaltStateMachine,
    instrument_spec: InstrumentSpec,
    reference_price: Decimal | None,
    known_order_ids: frozenset[str],
    rate_limit_budget: int,
    order: UnifiedOrder,
) -> OrderResult:
    """Run the full place-order pipeline.

    The caller (Engine) must have already fetched/resolved *instrument_spec*,
    *reference_price*, *known_order_ids*, and *rate_limit_budget* before
    calling this function.  The function itself is side-effect-free except
    for the adapter call, state-store writes, and event publishes.
    """

    # -- 0. Validate order type is supported by the adapter ---------
    if order.order_type not in adapter.supported_order_types():
        raise UnsupportedOrderTypeError(
            f"{order.order_type.value} is not supported by {adapter.platform_name}"
        )

    # -- 1. Generate IDs --------------------------------------------
    if order.client_order_id is None:
        order.client_order_id = _new_id()
    correlation_id = _new_id()

    # -- 2. Risk-check chain (Section 7) ----------------------------
    run_risk_checks(
        order,
        instrument_spec=instrument_spec,
        reference_price=reference_price,
        known_order_ids=known_order_ids,
        remaining_budget=rate_limit_budget,
        config=risk_config,
    )

    # -- 3. Halt check (Section 6.4) --------------------------------
    if not halt_machine.can_place_order(order.instrument, order.reduce_only):
        from unified_trading_execution.errors import InstrumentHaltedError
        raise InstrumentHaltedError(
            f"Cannot place order for {order.instrument.symbol}: instrument or account is halted"
        )

    # -- 4. Delegate to adapter -------------------------------------
    result = await adapter.place_order(order)

    # -- 5. Persist to state store ----------------------------------
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
    await state_store.upsert_order(record)

    # -- 6. Publish event -------------------------------------------
    event = OrderPlacedEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        order=record,
    )
    event_bus.publish(event)

    # -- 7. Write audit event (after publish — Section 17.12) ------
    await state_store.write_audit_event(AuditEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        event_type="order.placed",
        payload={
            "client_order_id": result.client_order_id,
            "platform_order_id": result.platform_order_id,
            "status": result.status.value,
        },
    ))

    return result


# ── modify order ───────────────────────────────────────────────────


async def dispatch_modify_order(
    *,
    adapter: Adapter,
    state_store: StateStore,
    event_bus: EventBus,
    risk_config: RiskConfig,
    halt_machine: HaltStateMachine,
    get_instrument_spec: Callable[[Instrument], Awaitable[InstrumentSpec]],
    reference_price: Decimal | None,
    known_order_ids: frozenset[str],
    rate_limit_budget: int,
    modification: OrderModification,
) -> OrderResult:
    """Run the full modify-order pipeline.

    Constructs the "would-be" order after modification, runs risk checks
    on it, then delegates to the adapter, persists the updated record,
    and publishes an OrderModifiedEvent.
    """

    # -- 0. Fetch existing order ------------------------------------
    existing = await state_store.get_order(modification.client_order_id)
    if existing is None:
        from unified_trading_execution.errors import OrderNotFoundError
        raise OrderNotFoundError(modification.client_order_id)

    correlation_id = _new_id()

    # -- 1. Construct would-be order for risk checks ----------------
    would_be = UnifiedOrder(
        instrument=existing.instrument,
        order_type=existing.order_type,
        side=existing.side,
        quantity=modification.quantity if modification.quantity is not None else existing.quantity,
        time_in_force=existing.time_in_force,
        client_order_id=existing.client_order_id,
        price=modification.price if modification.price is not None else existing.price,
        stop_price=modification.stop_price if modification.stop_price is not None else existing.stop_price,
        reduce_only=existing.reduce_only,
        client_tag=existing.client_tag,
        take_profit=modification.take_profit if modification.take_profit is not None else existing.take_profit,
        stop_loss=modification.stop_loss if modification.stop_loss is not None else existing.stop_loss,
    )

    # -- 2. Validate order type -------------------------------------
    if would_be.order_type not in adapter.supported_order_types():
        raise UnsupportedOrderTypeError(
            f"{would_be.order_type.value} is not supported by {adapter.platform_name}"
        )

    # -- 3. Fetch instrument spec -----------------------------------
    instrument_spec = await get_instrument_spec(would_be.instrument)

    # -- 4. Risk-check chain ----------------------------------------
    run_risk_checks(
        would_be,
        instrument_spec=instrument_spec,
        reference_price=reference_price,
        known_order_ids=known_order_ids,
        remaining_budget=rate_limit_budget,
        config=risk_config,
    )

    # -- 5. Halt check (modifying to increase exposure is blocked) --
    if not halt_machine.can_place_order(would_be.instrument, would_be.reduce_only):
        from unified_trading_execution.errors import InstrumentHaltedError
        raise InstrumentHaltedError(
            f"Cannot modify order for {would_be.instrument.symbol}: instrument or account is halted"
        )

    # -- 6. Delegate to adapter -------------------------------------
    result = await adapter.modify_order(modification)

    # -- 7. Persist updated record ----------------------------------
    updated = OrderRecord(
        instrument=existing.instrument,
        order_type=existing.order_type,
        side=existing.side,
        quantity=modification.quantity if modification.quantity is not None else existing.quantity,
        time_in_force=existing.time_in_force,
        client_order_id=existing.client_order_id,
        price=modification.price if modification.price is not None else existing.price,
        stop_price=modification.stop_price if modification.stop_price is not None else existing.stop_price,
        reduce_only=existing.reduce_only,
        client_tag=existing.client_tag,
        take_profit=modification.take_profit if modification.take_profit is not None else existing.take_profit,
        stop_loss=modification.stop_loss if modification.stop_loss is not None else existing.stop_loss,
        platform_order_id=result.platform_order_id,
        status=result.status,
        filled_quantity=result.filled_quantity,
        average_fill_price=result.average_fill_price,
        correlation_id=existing.correlation_id,
        created_at=existing.created_at,
        updated_at=result.updated_at,
    )
    await state_store.upsert_order(updated)

    # -- 8. Publish event -------------------------------------------
    event = OrderModifiedEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        order=updated,
        previous=existing,
    )
    event_bus.publish(event)

    # -- 9. Write audit event ---------------------------------------
    await state_store.write_audit_event(AuditEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        event_type="order.modified",
        payload={"client_order_id": modification.client_order_id},
    ))

    return result


# ── cancel order ───────────────────────────────────────────────────


async def dispatch_cancel_order(
    *,
    adapter: Adapter,
    state_store: StateStore,
    event_bus: EventBus,
    client_order_id: str,
) -> OrderResult:
    """Run the full cancel-order pipeline.

    Cancel is always permitted (no risk checks, no halt checks — Section 7
    and 6.4). The engine persists the updated status and publishes an
    OrderCancelledEvent.
    """

    correlation_id = _new_id()

    # -- 0. Fetch existing order (must exist locally) ----------------
    existing = await state_store.get_order(client_order_id)
    if existing is None:
        from unified_trading_execution.errors import OrderNotFoundError
        raise OrderNotFoundError(client_order_id)

    # -- 1. Delegate to adapter -------------------------------------
    result = await adapter.cancel_order(client_order_id)

    # -- 2. Persist updated record ----------------------------------
    updated = OrderRecord(
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
        platform_order_id=result.platform_order_id,
        status=result.status,
        filled_quantity=result.filled_quantity,
        average_fill_price=result.average_fill_price,
        correlation_id=existing.correlation_id,
        created_at=existing.created_at,
        updated_at=result.updated_at,
    )
    await state_store.upsert_order(updated)

    # -- 3. Publish event -------------------------------------------
    event = OrderCancelledEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        client_order_id=client_order_id,
        instrument=existing.instrument,
    )
    event_bus.publish(event)

    # -- 4. Write audit event ---------------------------------------
    await state_store.write_audit_event(AuditEvent(
        event_id=_new_id(),
        timestamp=_utcnow(),
        adapter_name=adapter.platform_name,
        account_id=adapter.account_id,
        correlation_id=correlation_id,
        event_type="order.cancelled",
        payload={"client_order_id": client_order_id},
    ))

    return result

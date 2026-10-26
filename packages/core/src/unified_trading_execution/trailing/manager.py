"""Trailing-stop manager: attachment lifecycle, routing, and position targeting.

The manager owns one :class:`~unified_trading_execution.trailing.types.TrailingState`
per position leg.  It resolves the leg from the state mirror, chooses native or
synthetic execution from the adapter's declared capabilities, persists the
state, and publishes the audit record and lifecycle event.

Execution lives outside this module: attachment persists ``ARMING`` before any
venue call, and the protection write confirms the stop and advances the phase.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from uuid_extensions import uuid7

from unified_trading_execution.adapter import (
    Adapter,
    MarketDataCapability,
    PositionProtectionCapability,
    TrailingCapability,
)
from unified_trading_execution.errors import (
    InvalidOrderError,
    PlatformConnectionError,
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.events import AuditEvent, EventBus, TrailingStateEvent
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.trailing.types import (
    ACTIVE_TRAILING_PHASES,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import OrderSide
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Position

# Every lifecycle edge the manager may take.  Terminal phases have no exits.
ALLOWED_TRAILING_TRANSITIONS: dict[TrailingPhase, frozenset[TrailingPhase]] = {
    TrailingPhase.ARMING: frozenset(
        {
            TrailingPhase.ARMED,
            TrailingPhase.DEGRADED,
            TrailingPhase.FAILED,
            TrailingPhase.CANCELLED,
            TrailingPhase.DONE,
        }
    ),
    TrailingPhase.ARMED: frozenset(
        {
            TrailingPhase.TRIGGERED,
            TrailingPhase.DEGRADED,
            TrailingPhase.CANCELLED,
            TrailingPhase.DONE,
        }
    ),
    TrailingPhase.TRIGGERED: frozenset(
        {
            TrailingPhase.DEGRADED,
            TrailingPhase.CANCELLED,
            TrailingPhase.DONE,
        }
    ),
    TrailingPhase.DEGRADED: frozenset(
        {
            TrailingPhase.ARMED,
            TrailingPhase.CANCELLED,
            TrailingPhase.DONE,
            TrailingPhase.FAILED,
        }
    ),
    TrailingPhase.DONE: frozenset(),
    TrailingPhase.CANCELLED: frozenset(),
    TrailingPhase.FAILED: frozenset(),
}

_NATIVE_BLOCKER_LABELS: dict[str, str] = {
    "native_unsupported": "native trailing",
    "native_unsupported_offset_type": "offset type",
    "native_unsupported_atr": "ATR offset",
    "native_unsupported_basis": "basis",
    "native_unsupported_activation": "activation",
    "native_unsupported_step": "step",
    "native_unsupported_initial_stop": "initial stop",
    "native_unsupported_take_profit": "take profit",
    "native_unsupported_atr_data": "ATR candles for the take profit",
}

_SYNTHETIC_BLOCKER_LABELS: dict[str, str] = {
    "synthetic_unsupported_protection": "position protection",
    "synthetic_unsupported_stop": "stop placement",
    "synthetic_unsupported_ticker": "ticker data",
    "synthetic_unsupported_take_profit": "take profit",
    "synthetic_unsupported_atr_data": "ATR candles",
}


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _new_id() -> str:
    return str(uuid7())


def _uses_atr(spec: TrailingSpec) -> bool:
    return (
        spec.offset_type is TrailingOffsetType.ATR
        or (spec.initial_stop is not None and spec.initial_stop.type is ProtectionLevelType.ATR)
        or (spec.take_profit is not None and spec.take_profit.type is ProtectionLevelType.ATR)
    )


def _native_blockers(
    spec: TrailingSpec,
    native: TrailingCapability,
    protection: PositionProtectionCapability,
    market_data: MarketDataCapability,
) -> list[str]:
    if not native.supported:
        return ["native_unsupported"]

    blockers: list[str] = []
    if spec.offset_type is TrailingOffsetType.ATR:
        blockers.append("native_unsupported_atr")
    elif spec.offset_type not in native.offset_types:
        blockers.append("native_unsupported_offset_type")
    if spec.basis is not None and spec.basis not in native.bases:
        blockers.append("native_unsupported_basis")
    activation = (
        TrailingActivation.PRICE
        if spec.activation is TrailingActivation.PROFIT_PERCENT
        else spec.activation
    )
    if activation not in native.activations:
        blockers.append("native_unsupported_activation")
    if spec.step is not None and not native.supports_step:
        blockers.append("native_unsupported_step")
    if spec.initial_stop is not None and (
        not native.supports_initial_stop or spec.initial_stop.type is ProtectionLevelType.ATR
    ):
        blockers.append("native_unsupported_initial_stop")
    if spec.take_profit is not None and not protection.can_set_take_profit:
        blockers.append("native_unsupported_take_profit")
    if (
        spec.take_profit is not None
        and spec.take_profit.type is ProtectionLevelType.ATR
        and spec.atr_timeframe not in market_data.candle_timeframes
    ):
        blockers.append("native_unsupported_atr_data")
    return blockers


def _synthetic_blockers(
    spec: TrailingSpec,
    protection: PositionProtectionCapability,
    market_data: MarketDataCapability,
) -> list[str]:
    blockers: list[str] = []
    if not protection.supported:
        blockers.append("synthetic_unsupported_protection")
    elif not protection.can_set_stop:
        blockers.append("synthetic_unsupported_stop")
    if not market_data.ticker_bases:
        blockers.append("synthetic_unsupported_ticker")
    if spec.take_profit is not None and not protection.can_set_take_profit:
        blockers.append("synthetic_unsupported_take_profit")
    if _uses_atr(spec) and spec.atr_timeframe not in market_data.candle_timeframes:
        blockers.append("synthetic_unsupported_atr_data")
    return blockers


def _unsupported_message(
    instrument: Instrument,
    mode: TrailingMode,
    blockers: list[str],
    labels: dict[str, str],
) -> str:
    named = ", ".join(labels.get(code, code) for code in blockers)
    return f"{mode.value} trailing cannot express this request on {instrument.symbol}: {named}"


def resolve_routing(
    request: TrailingRequest,
    *,
    protection: PositionProtectionCapability,
    native: TrailingCapability,
    market_data: MarketDataCapability,
) -> tuple[TrailingMode, str]:
    """Resolve the execution mode and the persisted routing reason.

    ``AUTO`` prefers an exact native match and otherwise falls back to
    synthetic, recording the first blocking native reason.  ``NATIVE`` and
    ``SYNTHETIC`` skip the fallback.  A named basis the adapter cannot sample
    is rejected before either mode is considered.
    """
    spec = request.spec
    if spec.basis is not None and spec.basis not in market_data.ticker_bases:
        supported = ", ".join(sorted(basis.value for basis in market_data.ticker_bases))
        raise TrailingNotSupportedError(
            f"basis {spec.basis.value!r} cannot be sampled for {request.instrument.symbol}; "
            f"supported: {supported or 'none'}"
        )

    native_blockers = _native_blockers(spec, native, protection, market_data)
    if request.mode is TrailingMode.NATIVE:
        if native_blockers:
            raise TrailingNotSupportedError(
                _unsupported_message(
                    request.instrument, request.mode, native_blockers, _NATIVE_BLOCKER_LABELS
                )
            )
        return TrailingMode.NATIVE, "native_exact"

    synthetic_blockers = _synthetic_blockers(spec, protection, market_data)
    if request.mode is TrailingMode.SYNTHETIC:
        if synthetic_blockers:
            raise TrailingNotSupportedError(
                _unsupported_message(
                    request.instrument,
                    request.mode,
                    synthetic_blockers,
                    _SYNTHETIC_BLOCKER_LABELS,
                )
            )
        return TrailingMode.SYNTHETIC, "synthetic_mode_requested"

    if not native_blockers:
        return TrailingMode.NATIVE, "native_exact"
    if not synthetic_blockers:
        return TrailingMode.SYNTHETIC, native_blockers[0]

    native_named = ", ".join(_NATIVE_BLOCKER_LABELS.get(code, code) for code in native_blockers)
    synthetic_named = ", ".join(
        _SYNTHETIC_BLOCKER_LABELS.get(code, code) for code in synthetic_blockers
    )
    raise TrailingNotSupportedError(
        f"no trailing mode can express this request on {request.instrument.symbol}: "
        f"native ({native_named}); synthetic ({synthetic_named})"
    )


def ensure_transition_allowed(current: TrailingPhase, target: TrailingPhase) -> None:
    """Raise :class:`TrailingStateError` when the lifecycle edge is invalid."""
    if target not in ALLOWED_TRAILING_TRANSITIONS[current]:
        raise TrailingStateError(f"invalid trailing transition {current.value} -> {target.value}")


def _describe_leg(position: Position) -> str:
    side = "long" if position.quantity > 0 else "short"
    opened = position.opened_at.isoformat() if position.opened_at is not None else "unknown"
    return (
        f"{position.position_id!r} ({side}, quantity {abs(position.quantity)}, "
        f"entry {position.average_entry_price}, opened {opened})"
    )


def resolve_position_leg(
    legs: list[Position],
    instrument: Instrument,
    position_id: str | None,
) -> tuple[Position, str]:
    """Pick the one open leg a trailing state targets.

    An explicit ``position_id`` must match an open leg.  Without one, exactly
    one open leg is required; several open legs are ambiguous and every
    candidate is listed.  A leg without a platform id cannot be addressed.
    """
    if position_id is not None:
        matches = [leg for leg in legs if leg.position_id == position_id]
        if not matches:
            open_ids = [leg.position_id for leg in legs if leg.quantity != 0]
            listed = ", ".join(repr(open_id) for open_id in open_ids) or "none"
            raise InvalidOrderError(
                f"no open position {position_id!r} on {instrument.symbol}; "
                f"open position ids: {listed}"
            )
        if len(matches) > 1:
            raise TrailingStateError(
                f"position id {position_id!r} matches {len(matches)} legs on "
                f"{instrument.symbol}; the mirror is defective"
            )
        leg = matches[0]
        if leg.quantity == 0:
            raise InvalidOrderError(f"position {position_id!r} on {instrument.symbol} is closed")
        return leg, position_id

    open_legs = [leg for leg in legs if leg.quantity != 0]
    if not open_legs:
        raise InvalidOrderError(
            f"no open position on {instrument.symbol}; the mirror can lag a fresh "
            "entry, retry after the position update"
        )
    if any(leg.position_id is None for leg in open_legs):
        raise TrailingStateError(
            f"the open positions on {instrument.symbol} include a leg without a "
            "platform position_id; the mirror is defective"
        )
    if len(open_legs) > 1:
        candidates = "; ".join(_describe_leg(leg) for leg in open_legs)
        raise TrailingStateError(
            f"{instrument.symbol} has {len(open_legs)} open positions; "
            f"pass position_id to choose one: {candidates}"
        )
    leg = open_legs[0]
    resolved_id = leg.position_id
    if resolved_id is None:  # pragma: no cover - guarded above, narrows the type
        raise TrailingStateError(
            f"the open position on {instrument.symbol} has no platform position_id"
        )
    return leg, resolved_id


def _resolve_bases(
    spec: TrailingSpec,
    mode: TrailingMode,
    protection: PositionProtectionCapability,
) -> tuple[TrailingBasis | None, TrailingBasis | None]:
    """Return the sampling and venue trigger bases recorded at attach.

    Native defaults to last.  Synthetic without a named basis leaves sampling
    to the first ticker read.  The trigger basis is only recorded when the
    adapter declares it; otherwise the venue default applies.
    """
    basis = spec.basis
    if basis is None and mode is TrailingMode.NATIVE:
        basis = TrailingBasis.LAST
    trigger = basis if basis is not None and basis in protection.trigger_bases else None
    return basis, trigger


class TrailingManager:
    """Lifecycle and persistence for trailing states.

    Construct one per connected engine.  The manager never starts tasks and
    never queries the venue to resolve a position leg; ``attach`` reads the
    state mirror.  Every public method requires a connected adapter and an
    initialized store.
    """

    def __init__(
        self,
        adapter: Adapter,
        state_store: StateStore,
        *,
        event_bus: EventBus | None = None,
        id_factory: Callable[[], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._adapter = adapter
        self._state_store = state_store
        self._event_bus = event_bus or EventBus()
        self._id_factory = id_factory or _new_id
        self._clock = clock or _utcnow
        self._lock = asyncio.Lock()

    async def attach(self, request: TrailingRequest) -> TrailingState:
        """Resolve a leg, route the request, and persist the new state."""
        self._require_ready()
        async with self._lock:
            legs = await self._state_store.get_positions(request.instrument)
            leg, position_id = resolve_position_leg(legs, request.instrument, request.position_id)

            active = await self._state_store.list_trailing_states(
                instrument=request.instrument,
                phases=tuple(ACTIVE_TRAILING_PHASES),
            )
            if any(state.position_id == position_id for state in active):
                raise TrailingStateError(
                    f"an active trailing state is already attached to position "
                    f"{position_id!r} on {request.instrument.symbol}"
                )

            protection = self._adapter.position_protection_capability(request.instrument)
            native = self._adapter.trailing_capability(request.instrument)
            market_data = self._adapter.market_data_capability(request.instrument)
            mode, reason = resolve_routing(
                request,
                protection=protection,
                native=native,
                market_data=market_data,
            )

            resolved_basis, trigger_basis = _resolve_bases(request.spec, mode, protection)
            now = self._clock()
            state = TrailingState(
                trailing_id=self._id_factory(),
                instrument=request.instrument,
                position_id=position_id,
                mode=mode,
                routing_reason=reason,
                phase=TrailingPhase.ARMING,
                spec=request.spec,
                resolved_basis=resolved_basis,
                protection_trigger_basis=trigger_basis,
                side=OrderSide.BUY if leg.quantity > 0 else OrderSide.SELL,
                entry_price=leg.average_entry_price,
                quantity=abs(leg.quantity),
                created_at=now,
                updated_at=now,
                position_opened_at=leg.opened_at,
            )
            await self._state_store.upsert_trailing_state(state)
            self._publish(state, previous=TrailingPhase.ARMING, reason="attached")
            await self._write_audit(
                "trailing.attached",
                state,
                previous_stop=None,
                current_stop=state.current_stop,
                reason="attached",
            )
            return state

    async def cancel(
        self,
        trailing_id: str,
        *,
        clear_protection: bool = False,
        reason: str | None = None,
    ) -> TrailingState:
        """Cancel an active state and persist the terminal phase.

        ``clear_protection`` is accepted for the public contract; attachment
        performs no venue write before protection is confirmed, so there is
        nothing owned to clear until execution advances the state.
        """
        self._require_ready()
        async with self._lock:
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None:
                raise TrailingStateError(f"unknown trailing state {trailing_id!r}")
            ensure_transition_allowed(state.phase, TrailingPhase.CANCELLED)

            now = self._clock()
            updated = replace(
                state,
                phase=TrailingPhase.CANCELLED,
                last_error=reason if reason is not None else state.last_error,
                updated_at=now,
            )
            await self._state_store.upsert_trailing_state(updated)
            self._publish(updated, previous=state.phase, reason=reason or "cancelled")
            await self._write_audit(
                "trailing.cancelled",
                updated,
                previous_stop=state.current_stop,
                current_stop=updated.current_stop,
                reason=reason or "cancelled",
            )
            return updated

    async def get(self, trailing_id: str) -> TrailingState | None:
        """Return one trailing state, including terminal history."""
        self._require_ready()
        return await self._state_store.get_trailing_state(trailing_id)

    async def list(
        self,
        *,
        instrument: Instrument | None = None,
        active_only: bool = False,
    ) -> list[TrailingState]:
        """Return trailing states, newest first, optionally active only."""
        self._require_ready()
        phases = tuple(ACTIVE_TRAILING_PHASES) if active_only else None
        return await self._state_store.list_trailing_states(instrument=instrument, phases=phases)

    def _require_ready(self) -> None:
        if not self._adapter.is_connected:
            raise PlatformConnectionError(
                "trailing manager requires a connected engine with an initialized store"
            )

    def _publish(
        self,
        state: TrailingState,
        *,
        previous: TrailingPhase,
        reason: str | None,
    ) -> None:
        self._event_bus.publish(
            TrailingStateEvent(
                event_id=self._id_factory(),
                timestamp=self._clock(),
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=state.trailing_id,
                trailing_id=state.trailing_id,
                instrument=state.instrument,
                position_id=state.position_id,
                previous_phase=previous,
                phase=state.phase,
                mode=state.mode,
                current_stop=state.current_stop,
                reason=reason,
            )
        )

    async def _write_audit(
        self,
        event_type: str,
        state: TrailingState,
        *,
        previous_stop: Decimal | None,
        current_stop: Decimal | None,
        reason: str | None,
    ) -> None:
        await self._state_store.write_audit_event(
            AuditEvent(
                event_id=self._id_factory(),
                timestamp=self._clock(),
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=state.trailing_id,
                event_type=event_type,
                payload={
                    "trailing_id": state.trailing_id,
                    "position_id": state.position_id,
                    "mode": state.mode.value,
                    "previous_stop": str(previous_stop) if previous_stop is not None else None,
                    "current_stop": str(current_stop) if current_stop is not None else None,
                    "reason": reason,
                },
            )
        )

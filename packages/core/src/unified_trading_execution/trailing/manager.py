"""Trailing-stop manager: attachment lifecycle, routing, and position targeting.

The manager owns one :class:`~unified_trading_execution.trailing.types.TrailingState`
per position leg.  It resolves the leg from the state mirror, chooses native or
synthetic execution from the adapter's declared capabilities, persists the
state, and publishes the audit record and lifecycle event.

Synthetic attachment persists ``ARMING`` before the venue call, writes the
initial stop and take profit, confirms them, and advances to ``ARMED``.
The polling worker samples tickers and calls :meth:`TrailingManager.on_ticker`;
ratcheting, confirmation, and failure persistence stay in this module.
Position updates close or resize states, and the integrity pass re-reads venue
protection to restore a lost or externally loosened stop.
"""

from __future__ import annotations

import asyncio
import logging
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
    InstrumentHaltedError,
    InvalidOrderError,
    PlatformConnectionError,
    RateLimitError,
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.events import (
    AuditEvent,
    EventBus,
    TickerEvent,
    TrailingStateEvent,
)
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.trailing.math import (
    candidate_stop,
    initial_stop_price,
    is_activation_reached,
    quantize_stop,
    quantize_take_profit,
    ratchet_accepts,
    resolve_activation_price,
    respects_min_stop_distance,
    take_profit_price,
    update_best_basis,
    validate_level_direction,
    wilder_atr,
)
from unified_trading_execution.trailing.types import (
    ACTIVE_TRAILING_PHASES,
    NativeTrailingRequest,
    NativeTrailingResult,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingConfig,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import OrderSide
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import TpSlAttachment
from unified_trading_execution.types.position import Position

logger = logging.getLogger(__name__)

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

_FETCH_ERROR_PREFIX = "fetch: "
_PROTECTION_ERROR_PREFIX = "protection: "
_ATR_ERROR_PREFIX = "atr_stale: "

_SYNTHETIC_BLOCKER_LABELS: dict[str, str] = {
    "synthetic_unsupported_protection": "position protection",
    "synthetic_unsupported_stop": "stop placement",
    "synthetic_unsupported_merge": "unmentioned protection side preservation",
    "synthetic_unsupported_ticker": "ticker data",
    "synthetic_unsupported_take_profit": "take profit",
    "synthetic_unsupported_atr_data": "ATR candles",
}


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _new_id() -> str:
    return str(uuid7())


def _uses_atr(spec: TrailingSpec) -> bool:
    return spec.uses_atr


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
    elif not protection.preserves_unmentioned_side:
        blockers.append("synthetic_unsupported_merge")
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
    market_data: MarketDataCapability,
    side: OrderSide,
) -> tuple[TrailingBasis | None, TrailingBasis | None]:
    """Return the sampling and venue trigger bases recorded at attach.

    Native defaults to last.  Synthetic without a named basis prefers a basis
    the venue can both sample and trigger on, then the side-specific quote
    (bid for a long stop, ask for a short stop).  The trigger basis is only
    recorded when the adapter declares it; otherwise the venue default applies.
    """
    if spec.basis is not None:
        basis = spec.basis
    elif mode is TrailingMode.NATIVE:
        basis = TrailingBasis.LAST
    else:
        candidates = market_data.ticker_bases
        preferred = [
            TrailingBasis.LAST,
            TrailingBasis.BID if side is OrderSide.BUY else TrailingBasis.ASK,
            TrailingBasis.ASK if side is OrderSide.BUY else TrailingBasis.BID,
            TrailingBasis.MARK,
            TrailingBasis.MID,
        ]
        triggerable = candidates & protection.trigger_bases
        pool = triggerable or candidates
        basis = next(
            (item for item in preferred if item in pool),
            min(pool, key=lambda item: item.value) if pool else None,
        )
    trigger = basis if basis is not None and basis in protection.trigger_bases else None
    return basis, trigger


def _basis_price(ticker: Ticker, basis: TrailingBasis) -> Decimal | None:
    if basis is TrailingBasis.LAST:
        return ticker.last
    if basis is TrailingBasis.BID:
        return ticker.bid
    if basis is TrailingBasis.ASK:
        return ticker.ask
    if basis is TrailingBasis.MARK:
        return ticker.mark
    if ticker.bid is not None and ticker.ask is not None:
        return (ticker.bid + ticker.ask) / Decimal("2")
    return None


def _attachment_matches(
    actual: TpSlAttachment | None,
    requested: Decimal | None,
    tick_size: Decimal,
) -> bool:
    if requested is None:
        return True
    return actual is not None and abs(actual.trigger_price - requested) <= tick_size


def _stop_is_tighter(side: OrderSide, candidate: Decimal, reference: Decimal) -> bool:
    if side is OrderSide.BUY:
        return candidate > reference
    return candidate < reference


def _open_leg(position: Position | None) -> Position | None:
    if position is None or position.quantity == 0:
        return None
    return position


def _recovery_identity(
    state: TrailingState,
    leg: Position,
    tick_size: Decimal,
) -> tuple[str | None, bool]:
    current_side = OrderSide.BUY if leg.quantity > 0 else OrderSide.SELL
    if current_side is not state.side:
        return "position_replaced", True

    persisted_opened = state.position_opened_at
    current_opened = leg.opened_at
    if persisted_opened is not None and current_opened is not None:
        age = abs((persisted_opened - current_opened).total_seconds())
        return ("position_replaced", False) if age > 1.0 else (None, False)

    if abs(leg.average_entry_price - state.entry_price) > tick_size:
        return "position_replaced", False
    return "position_identity_unverified", False


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
        halt_machine: HaltStateMachine,
        reserve_rate_limit: Callable[[], bool],
        event_bus: EventBus | None = None,
        config: TrailingConfig | None = None,
        id_factory: Callable[[], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._adapter = adapter
        self._state_store = state_store
        self._halt_machine = halt_machine
        self._reserve_rate_limit = reserve_rate_limit
        self._event_bus = event_bus or EventBus()
        self._config = config or TrailingConfig()
        self._id_factory = id_factory or _new_id
        self._clock = clock or _utcnow
        self._lock = asyncio.Lock()
        self._state_locks: dict[str, asyncio.Lock] = {}
        self._fetch_failures: dict[str, int] = {}
        self._atr_failures: dict[str, int] = {}
        self._instrument_specs: dict[Instrument, InstrumentSpec] = {}
        self._rehydrated: bool | None = None

    async def attach(self, request: TrailingRequest) -> TrailingState:
        """Resolve a leg, route the request, and persist the new state."""
        self._require_ready()
        async with self._lock:
            await self._require_rehydrated()
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

            if mode is TrailingMode.SYNTHETIC:
                final = await self._attach_synthetic(
                    request, leg, position_id, reason, protection, market_data
                )
            else:
                final = await self._attach_native(
                    request, leg, position_id, reason, protection, market_data
                )

            await self._write_audit(
                "trailing.routing",
                final,
                previous_stop=final.previous_stop_loss,
                current_stop=final.current_stop,
                reason=reason,
            )
            return final

    async def _attach_synthetic(
        self,
        request: TrailingRequest,
        leg: Position,
        position_id: str,
        routing_reason: str,
        protection: PositionProtectionCapability,
        market_data: MarketDataCapability,
    ) -> TrailingState:
        spec = request.spec
        instrument = request.instrument
        side = OrderSide.BUY if leg.quantity > 0 else OrderSide.SELL
        entry = leg.average_entry_price

        instrument_spec = await self._instrument_spec(instrument)
        atr = await self._load_atr(spec, instrument)
        resolved_basis, trigger_basis = _resolve_bases(
            spec, TrailingMode.SYNTHETIC, protection, market_data, side
        )
        if resolved_basis is None:
            raise TrailingNotSupportedError(
                f"no sampling basis is available for {instrument.symbol}"
            )

        ticker = await self._adapter.fetch_ticker(instrument)
        if ticker is None:
            raise TrailingNotSupportedError(f"no ticker available for {instrument.symbol}")
        basis = _basis_price(ticker, resolved_basis)
        if basis is None:
            raise TrailingNotSupportedError(
                f"ticker for {instrument.symbol} has no {resolved_basis.value} price"
            )

        activation_price = resolve_activation_price(spec, side, entry)
        activated = is_activation_reached(side, basis, activation_price)
        stop = self._first_stop(
            spec, side, entry, basis, atr, instrument_spec, activated=activated
        )
        take_profit = self._take_profit(spec, side, entry, atr, instrument_spec)

        existing = await self._adapter.get_position_tpsl(instrument, position_id)
        if existing is None:
            raise InvalidOrderError(
                f"no open position {position_id!r} on {instrument.symbol}"
            )
        previous_tp, previous_sl = existing
        if previous_sl is not None and not request.take_over_existing_protection:
            raise TrailingStateError(
                f"position {position_id!r} on {instrument.symbol} already has a stop "
                f"{previous_sl.trigger_price}; pass take_over_existing_protection=True"
            )
        if (
            previous_tp is not None
            and take_profit is not None
            and not request.take_over_existing_protection
        ):
            raise TrailingStateError(
                f"position {position_id!r} on {instrument.symbol} already has a take "
                f"profit {previous_tp.trigger_price}; pass take_over_existing_protection=True"
            )

        will_write = stop is not None or take_profit is not None
        if not self._halt_machine.can_place_order(instrument, reduce_only=True):
            raise InstrumentHaltedError(
                f"cannot attach trailing on {instrument.symbol}: instrument or account is halted"
            )
        if will_write and not self._reserve_rate_limit():
            raise RateLimitError(
                f"cannot attach trailing on {instrument.symbol}: rate limit budget exhausted"
            )

        now = self._clock()
        arming = TrailingState(
            trailing_id=self._id_factory(),
            instrument=instrument,
            position_id=position_id,
            mode=TrailingMode.SYNTHETIC,
            routing_reason=routing_reason,
            phase=TrailingPhase.ARMING,
            spec=spec,
            resolved_basis=resolved_basis,
            protection_trigger_basis=trigger_basis,
            side=side,
            entry_price=entry,
            quantity=abs(leg.quantity),
            created_at=now,
            updated_at=now,
            position_opened_at=leg.opened_at,
            previous_stop_loss=previous_sl.trigger_price if previous_sl is not None else None,
            previous_take_profit=previous_tp.trigger_price if previous_tp is not None else None,
            best_basis=basis,
            resolved_activation_price=activation_price,
            atr_value=atr,
            atr_as_of=now if atr is not None else None,
            stop_owned=stop is not None,
            take_profit_owned=take_profit is not None,
        )
        await self._state_store.upsert_trailing_state(arming)

        if will_write:
            try:
                await self._adapter.modify_position_tpsl(
                    instrument,
                    position_id,
                    take_profit=(
                        TpSlAttachment(trigger_price=take_profit)
                        if take_profit is not None
                        else None
                    ),
                    stop_loss=TpSlAttachment(trigger_price=stop) if stop is not None else None,
                )
            except Exception as exc:
                await self._attach_failed(arming, exc)
                raise

            await self._confirm_protection(
                arming, position_id, stop, take_profit, instrument_spec.tick_size
            )

        final = replace(
            arming,
            phase=TrailingPhase.ARMED,
            current_stop=stop,
            current_take_profit=take_profit,
            activated=activated,
            last_reprice_at=now if stop is not None else None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(final)
        self._publish(final, previous=TrailingPhase.ARMING, reason="attached")
        await self._write_audit(
            "trailing.attached",
            final,
            previous_stop=arming.previous_stop_loss,
            current_stop=final.current_stop,
            reason="attached",
        )
        if request.take_over_existing_protection and (
            arming.previous_stop_loss is not None or arming.previous_take_profit is not None
        ):
            await self._write_audit(
                "trailing.takeover",
                final,
                previous_stop=arming.previous_stop_loss,
                current_stop=final.current_stop,
                reason="takeover",
            )
        return final

    async def _attach_native(
        self,
        request: TrailingRequest,
        leg: Position,
        position_id: str,
        routing_reason: str,
        protection: PositionProtectionCapability,
        market_data: MarketDataCapability,
    ) -> TrailingState:
        spec = request.spec
        instrument = request.instrument
        side = OrderSide.BUY if leg.quantity > 0 else OrderSide.SELL
        entry = leg.average_entry_price

        instrument_spec = await self._instrument_spec(instrument)
        atr = await self._load_atr(spec, instrument)
        resolved_basis, trigger_basis = _resolve_bases(
            spec, TrailingMode.NATIVE, protection, market_data, side
        )
        activation_price = resolve_activation_price(spec, side, entry)
        take_profit = self._take_profit(spec, side, entry, atr, instrument_spec)

        existing = await self._adapter.get_position_tpsl(instrument, position_id)
        if existing is None:
            raise InvalidOrderError(
                f"no open position {position_id!r} on {instrument.symbol}"
            )
        previous_tp, previous_sl = existing
        if previous_sl is not None and not request.take_over_existing_protection:
            raise TrailingStateError(
                f"position {position_id!r} on {instrument.symbol} already has a stop "
                f"{previous_sl.trigger_price}; pass take_over_existing_protection=True"
            )
        if (
            previous_tp is not None
            and take_profit is not None
            and not request.take_over_existing_protection
        ):
            raise TrailingStateError(
                f"position {position_id!r} on {instrument.symbol} already has a take "
                f"profit {previous_tp.trigger_price}; pass take_over_existing_protection=True"
            )

        if not self._halt_machine.can_place_order(instrument, reduce_only=True):
            raise InstrumentHaltedError(
                f"cannot attach trailing on {instrument.symbol}: instrument or account is halted"
            )
        if not self._reserve_rate_limit():
            raise RateLimitError(
                f"cannot attach trailing on {instrument.symbol}: rate limit budget exhausted"
            )

        now = self._clock()
        arming = TrailingState(
            trailing_id=self._id_factory(),
            instrument=instrument,
            position_id=position_id,
            mode=TrailingMode.NATIVE,
            routing_reason=routing_reason,
            phase=TrailingPhase.ARMING,
            spec=spec,
            resolved_basis=resolved_basis,
            protection_trigger_basis=trigger_basis,
            side=side,
            entry_price=entry,
            quantity=abs(leg.quantity),
            created_at=now,
            updated_at=now,
            position_opened_at=leg.opened_at,
            previous_stop_loss=previous_sl.trigger_price if previous_sl is not None else None,
            previous_take_profit=previous_tp.trigger_price if previous_tp is not None else None,
            resolved_activation_price=activation_price,
            atr_value=atr,
            atr_as_of=now if atr is not None else None,
        )
        await self._state_store.upsert_trailing_state(arming)

        try:
            result = await self._adapter.place_trailing_stop(
                self._native_request(arming, leg)
            )
        except Exception as exc:
            await self._attach_failed(arming, exc)
            raise

        armed = replace(
            arming,
            native_id=result.native_id,
            current_stop=result.current_stop,
            activated=True,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(armed)

        try:
            confirmed = await self._adapter.get_trailing_stop(
                result.native_id, instrument
            )
        except Exception as exc:
            reason = str(exc) or type(exc).__name__
            await self._degrade_attach(
                armed, f"protection: native confirm: {reason}"
            )
            raise
        if confirmed is None:
            reason = "the venue did not confirm the native trailing stop"
            await self._degrade_attach(armed, f"protection: native confirm: {reason}")
            raise TrailingStateError(reason)

        armed = replace(
            armed,
            native_id=confirmed.native_id,
            current_stop=confirmed.current_stop,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(armed)

        take_profit_owned = False
        if take_profit is not None:
            if not self._halt_machine.can_place_order(
                instrument, reduce_only=True
            ):
                error = InstrumentHaltedError(
                    f"native trail is armed on {instrument.symbol} but the instrument "
                    "or account is halted"
                )
                await self._degrade_attach(
                    armed, f"protection: native take profit: {error}"
                )
                raise error
            if not self._reserve_rate_limit():
                error = RateLimitError(
                    f"native trail is armed on {instrument.symbol} but the rate limit "
                    "budget is exhausted"
                )
                await self._degrade_attach(
                    armed, f"protection: native take profit: {error}"
                )
                raise error
            try:
                await self._adapter.modify_position_tpsl(
                    instrument,
                    position_id,
                    take_profit=TpSlAttachment(trigger_price=take_profit),
                )
            except Exception as exc:
                reason = str(exc) or type(exc).__name__
                await self._degrade_attach(
                    armed, f"protection: native take profit: {reason}"
                )
                raise TrailingStateError(
                    f"native trail is armed but the take profit could not be written: "
                    f"{reason}"
                ) from exc
            await self._confirm_protection(
                armed, position_id, None, take_profit, instrument_spec.tick_size
            )
            take_profit_owned = True

        final = replace(
            armed,
            phase=TrailingPhase.ARMED,
            current_take_profit=take_profit,
            take_profit_owned=take_profit_owned,
            updated_at=self._clock(),
        )
        await self._state_store.upsert_trailing_state(final)
        self._publish(final, previous=TrailingPhase.ARMING, reason="attached")
        await self._write_audit(
            "trailing.attached",
            final,
            previous_stop=arming.previous_stop_loss,
            current_stop=final.current_stop,
            reason="attached",
        )
        if request.take_over_existing_protection and (
            arming.previous_stop_loss is not None or arming.previous_take_profit is not None
        ):
            await self._write_audit(
                "trailing.takeover",
                final,
                previous_stop=arming.previous_stop_loss,
                current_stop=final.current_stop,
                reason="takeover",
            )
        return final

    def _native_request(
        self, state: TrailingState, leg: Position
    ) -> NativeTrailingRequest:
        activation_price = state.resolved_activation_price
        if activation_price is None:
            activation_price = resolve_activation_price(
                state.spec, state.side, leg.average_entry_price
            )
        return NativeTrailingRequest(
            instrument=state.instrument,
            position_id=state.position_id,
            side=state.side,
            quantity=abs(leg.quantity),
            offset=state.spec.offset,
            offset_type=state.spec.offset_type,
            basis=state.resolved_basis,
            activation_price=activation_price,
            client_id=state.trailing_id,
        )

    async def _load_atr(self, spec: TrailingSpec, instrument: Instrument) -> Decimal | None:
        if not _uses_atr(spec):
            return None
        if spec.atr_period is None or spec.atr_timeframe is None:
            raise TrailingNotSupportedError("ATR values require a period and timeframe")
        candles = await self._adapter.fetch_candles(
            instrument, spec.atr_timeframe, limit=spec.atr_period + 1
        )
        return wilder_atr(candles, spec.atr_period)

    def _first_stop(
        self,
        spec: TrailingSpec,
        side: OrderSide,
        entry: Decimal,
        basis: Decimal,
        atr: Decimal | None,
        instrument_spec: InstrumentSpec,
        *,
        activated: bool,
    ) -> Decimal | None:
        level: ProtectionLevel | None = spec.initial_stop
        if level is not None:
            if level.type is ProtectionLevelType.PERCENT and level.value >= 100:
                raise TrailingNotSupportedError(
                    f"percent initial stop must be below 100, got {level.value}"
                )
            stop = initial_stop_price(level, side, entry, atr=atr)
        elif activated:
            stop = candidate_stop(side=side, best_basis=basis, spec=spec, atr=atr)
        else:
            return None

        stop = quantize_stop(stop, instrument_spec.tick_size, side)
        if stop <= 0:
            raise TrailingNotSupportedError(f"initial stop must be > 0, got {stop}")
        validate_level_direction(
            side=side, role="initial_stop", level_price=stop, reference_price=basis
        )
        if not respects_min_stop_distance(
            side=side,
            candidate=stop,
            basis=basis,
            min_stop_distance=instrument_spec.min_stop_distance,
        ):
            raise TrailingNotSupportedError(
                f"initial stop {stop} violates the minimum stop distance "
                f"{instrument_spec.min_stop_distance} from basis {basis}"
            )
        return stop

    def _take_profit(
        self,
        spec: TrailingSpec,
        side: OrderSide,
        entry: Decimal,
        atr: Decimal | None,
        instrument_spec: InstrumentSpec,
    ) -> Decimal | None:
        level: ProtectionLevel | None = spec.take_profit
        if level is None:
            return None
        price = quantize_take_profit(
            take_profit_price(level, side, entry, atr=atr), instrument_spec.tick_size, side
        )
        if price <= 0:
            raise TrailingNotSupportedError(f"take profit must be > 0, got {price}")
        validate_level_direction(
            side=side, role="take_profit", level_price=price, reference_price=entry
        )
        return price

    async def _confirm_protection(
        self,
        state: TrailingState,
        position_id: str,
        stop: Decimal | None,
        take_profit: Decimal | None,
        tick_size: Decimal,
    ) -> None:
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                confirmed = await self._adapter.get_position_tpsl(state.instrument, position_id)
            except Exception as exc:
                last_error = exc
            else:
                if confirmed is not None:
                    actual_tp, actual_sl = confirmed
                    if _attachment_matches(actual_sl, stop, tick_size) and _attachment_matches(
                        actual_tp, take_profit, tick_size
                    ):
                        return
                last_error = TrailingStateError(
                    "the venue did not confirm the requested protection"
                )
            if attempt == 0:
                continue

        reason = str(last_error) if last_error is not None else "confirmation mismatch"
        degraded = replace(
            state,
            phase=TrailingPhase.DEGRADED,
            last_error=reason,
            failure_count=state.failure_count + 1,
            updated_at=self._clock(),
        )
        await self._state_store.upsert_trailing_state(degraded)
        self._publish(degraded, previous=TrailingPhase.ARMING, reason=reason)
        await self._write_audit(
            "trailing.degraded",
            degraded,
            previous_stop=state.previous_stop_loss,
            current_stop=None,
            reason=reason,
        )
        raise TrailingStateError(f"could not confirm protection: {reason}")

    async def _attach_failed(self, state: TrailingState, exc: Exception) -> None:
        error = str(exc) or type(exc).__name__
        failed = replace(
            state,
            phase=TrailingPhase.FAILED,
            last_error=error,
            failure_count=state.failure_count + 1,
            updated_at=self._clock(),
        )
        await self._state_store.upsert_trailing_state(failed)
        self._publish(failed, previous=TrailingPhase.ARMING, reason=error)
        await self._write_audit(
            "trailing.failed",
            failed,
            previous_stop=state.previous_stop_loss,
            current_stop=None,
            reason=error,
        )

    async def _degrade_attach(self, state: TrailingState, reason: str) -> TrailingState:
        degraded = replace(
            state,
            phase=TrailingPhase.DEGRADED,
            last_error=reason,
            failure_count=state.failure_count + 1,
            updated_at=self._clock(),
        )
        await self._state_store.upsert_trailing_state(degraded)
        self._publish(degraded, previous=state.phase, reason=reason)
        await self._write_audit(
            "trailing.degraded",
            degraded,
            previous_stop=state.previous_stop_loss,
            current_stop=degraded.current_stop,
            reason=reason,
        )
        return degraded

    async def on_ticker(self, trailing_id: str, ticker: Ticker) -> TrailingState | None:
        """Apply one sampled ticker to a synthetic state.

        The worker fetches once per instrument and passes the same snapshot to
        every state on that instrument.  Fetch failures are counted separately
        from protection write failures.
        """
        self._require_ready()
        async with self._state_lock(trailing_id):
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None or state.mode is not TrailingMode.SYNTHETIC:
                return state
            if state.phase not in (TrailingPhase.ARMED, TrailingPhase.DEGRADED):
                return state

            basis = (
                _basis_price(ticker, state.resolved_basis)
                if state.resolved_basis is not None
                else None
            )
            if basis is None:
                field = (
                    state.resolved_basis.value
                    if state.resolved_basis is not None
                    else "resolved"
                )
                await self._record_fetch_failure(
                    state, f"ticker has no {field} price"
                )
                return await self._state_store.get_trailing_state(trailing_id)

            self._fetch_failures[trailing_id] = 0
            now = self._clock()
            state = await self._recover_fetch_degraded(state, now)

            best = update_best_basis(state.side, state.best_basis, basis)
            if best != state.best_basis:
                state = replace(state, best_basis=best, updated_at=now)
                await self._state_store.upsert_trailing_state(state)

            activation_price = resolve_activation_price(
                state.spec, state.side, state.entry_price
            )
            if activation_price != state.resolved_activation_price:
                state = replace(
                    state,
                    resolved_activation_price=activation_price,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(state)

            if not is_activation_reached(state.side, basis, activation_price):
                return state

            if not state.activated and state.current_stop is not None:
                activated_state = replace(
                    state,
                    activated=True,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(activated_state)
                self._publish(activated_state, previous=state.phase, reason="activated")
                await self._write_audit(
                    "trailing.activated",
                    activated_state,
                    previous_stop=activated_state.current_stop,
                    current_stop=activated_state.current_stop,
                    reason="activation_reached",
                )
                return activated_state

            instrument_spec = await self._instrument_spec(state.instrument)
            atr = state.atr_value
            try:
                candidate = quantize_stop(
                    candidate_stop(
                        side=state.side,
                        best_basis=best,
                        spec=state.spec,
                        atr=atr,
                    ),
                    instrument_spec.tick_size,
                    state.side,
                )
            except Exception as exc:
                return await self._record_protection_failure(
                    state, f"candidate: {str(exc) or type(exc).__name__}"
                )

            if candidate <= 0:
                return state
            if not respects_min_stop_distance(
                side=state.side,
                candidate=candidate,
                basis=basis,
                min_stop_distance=instrument_spec.min_stop_distance,
            ):
                return state
            if not ratchet_accepts(
                side=state.side,
                current_stop=state.current_stop,
                candidate=candidate,
                step=state.spec.step,
            ):
                return state

            capability = self._adapter.position_protection_capability(state.instrument)
            if state.last_reprice_at is not None:
                elapsed = (now - state.last_reprice_at).total_seconds()
                if elapsed < capability.min_reprice_interval_seconds:
                    return state

            if not self._halt_machine.can_place_order(state.instrument, reduce_only=True):
                return state
            if not self._reserve_rate_limit():
                return state

            attempt_state = replace(state, last_reprice_at=now, updated_at=now)
            if capability.reprice_gap:
                await self._write_audit(
                    "trailing.reprice_gap",
                    attempt_state,
                    previous_stop=state.current_stop,
                    current_stop=state.current_stop,
                    reason="gap_start",
                )
            try:
                await self._adapter.modify_position_tpsl(
                    state.instrument,
                    state.position_id,
                    stop_loss=TpSlAttachment(trigger_price=candidate),
                )
            except Exception as exc:
                if capability.reprice_gap:
                    await self._write_audit(
                        "trailing.reprice_gap",
                        attempt_state,
                        previous_stop=state.current_stop,
                        current_stop=state.current_stop,
                        reason="gap_end_failed",
                    )
                return await self._record_protection_failure(
                    attempt_state, f"write: {str(exc) or type(exc).__name__}"
                )

            confirmed, error = await self._confirm_stop(
                attempt_state, candidate, instrument_spec.tick_size
            )
            if confirmed is None:
                if capability.reprice_gap:
                    await self._write_audit(
                        "trailing.reprice_gap",
                        attempt_state,
                        previous_stop=attempt_state.current_stop,
                        current_stop=attempt_state.current_stop,
                        reason="gap_end_failed",
                    )
                return await self._record_protection_failure(
                    attempt_state, f"confirm: {error or 'mismatch'}"
                )

            phase = attempt_state.phase
            if (attempt_state.last_error or "").startswith(_PROTECTION_ERROR_PREFIX):
                phase = TrailingPhase.ARMED
            final = replace(
                attempt_state,
                phase=phase,
                current_stop=confirmed,
                resolved_activation_price=activation_price,
                activated=True,
                stop_owned=True,
                reprice_count=attempt_state.reprice_count + 1,
                failure_count=0,
                last_error=None,
                updated_at=now,
            )
            await self._state_store.upsert_trailing_state(final)
            self._publish(final, previous=state.phase, reason="repriced")
            if capability.reprice_gap:
                await self._write_audit(
                    "trailing.reprice_gap",
                    final,
                    previous_stop=state.current_stop,
                    current_stop=final.current_stop,
                    reason="gap_end",
                )
            await self._write_audit(
                "trailing.reprice",
                final,
                previous_stop=state.current_stop,
                current_stop=final.current_stop,
                reason="repriced",
            )
            return final

    async def on_ticker_failure(self, trailing_id: str, reason: str) -> TrailingState | None:
        """Count a ticker fetch failure for one synthetic state."""
        self._require_ready()
        async with self._state_lock(trailing_id):
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None or state.mode is not TrailingMode.SYNTHETIC:
                return state
            if state.phase not in (TrailingPhase.ARMED, TrailingPhase.DEGRADED):
                return state
            return await self._record_fetch_failure(state, reason)

    async def on_atr(
        self, trailing_id: str, atr: Decimal, *, as_of: datetime
    ) -> TrailingState | None:
        """Persist a refreshed ATR and recover an ATR-stale state."""
        self._require_ready()
        async with self._state_lock(trailing_id):
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None or state.mode is not TrailingMode.SYNTHETIC:
                return state
            if not state.spec.uses_atr:
                return state
            if state.phase not in (TrailingPhase.ARMED, TrailingPhase.DEGRADED):
                return state

            self._atr_failures[trailing_id] = 0
            recover = state.phase is TrailingPhase.DEGRADED and (
                state.last_error or ""
            ).startswith(_ATR_ERROR_PREFIX)
            updated = replace(
                state,
                phase=TrailingPhase.ARMED if recover else state.phase,
                atr_value=atr,
                atr_as_of=as_of,
                last_error=None if recover else state.last_error,
                updated_at=self._clock(),
            )
            await self._state_store.upsert_trailing_state(updated)
            if recover:
                self._publish(
                    updated,
                    previous=TrailingPhase.DEGRADED,
                    reason="atr_refreshed",
                )
                await self._write_audit(
                    "trailing.recovered",
                    updated,
                    previous_stop=updated.current_stop,
                    current_stop=updated.current_stop,
                    reason="atr_refreshed",
                )
            return updated

    async def on_atr_failure(
        self, trailing_id: str, reason: str
    ) -> TrailingState | None:
        """Count a candle refresh failure and degrade after the stale limit."""
        self._require_ready()
        async with self._state_lock(trailing_id):
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None or state.mode is not TrailingMode.SYNTHETIC:
                return state
            if not state.spec.uses_atr:
                return state
            if state.phase not in (TrailingPhase.ARMED, TrailingPhase.DEGRADED):
                return state

            count = self._atr_failures.get(trailing_id, 0) + 1
            self._atr_failures[trailing_id] = count
            if count < self._config.atr_stale_after_candles:
                return state

            error = f"{_ATR_ERROR_PREFIX}{reason}"
            if state.phase is TrailingPhase.DEGRADED:
                if not (state.last_error or "").startswith(_ATR_ERROR_PREFIX):
                    return state
                updated = replace(
                    state,
                    last_error=error,
                    updated_at=self._clock(),
                )
                await self._state_store.upsert_trailing_state(updated)
                return updated

            degraded = replace(
                state,
                phase=TrailingPhase.DEGRADED,
                last_error=error,
                updated_at=self._clock(),
            )
            await self._state_store.upsert_trailing_state(degraded)
            self._publish(degraded, previous=state.phase, reason="atr_stale")
            await self._write_audit(
                "trailing.degraded",
                degraded,
                previous_stop=state.current_stop,
                current_stop=state.current_stop,
                reason="atr_stale",
            )
            return degraded

    async def on_position_update(self, position: Position) -> TrailingState | None:
        """Apply a quantity change, close, or side flip to one state."""
        self._require_ready()
        if position.position_id is None:
            return None
        states = await self._state_store.list_trailing_states(
            instrument=position.instrument,
            phases=tuple(ACTIVE_TRAILING_PHASES),
        )
        match = next(
            (state for state in states if state.position_id == position.position_id),
            None,
        )
        if match is None:
            return None

        async with self._state_lock(match.trailing_id):
            state = await self._state_store.get_trailing_state(match.trailing_id)
            if state is None or state.phase not in ACTIVE_TRAILING_PHASES:
                return state

            now = self._clock()
            if position.quantity == 0:
                closed = replace(
                    state,
                    phase=TrailingPhase.DONE,
                    quantity=Decimal("0"),
                    last_error=None,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(closed)
                self._publish(closed, previous=state.phase, reason="position_closed")
                await self._write_audit(
                    "trailing.done",
                    closed,
                    previous_stop=state.current_stop,
                    current_stop=closed.current_stop,
                    reason="position_closed",
                )
                self._state_locks.pop(state.trailing_id, None)
                return closed

            current_side = OrderSide.BUY if position.quantity > 0 else OrderSide.SELL
            if current_side is not state.side:
                clear_note, cleared = await self._clear_protection_for_replacement(
                    state, static=True
                )
                cancelled = replace(
                    state,
                    phase=TrailingPhase.CANCELLED,
                    last_error=clear_note,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(cancelled)
                self._publish(
                    cancelled, previous=state.phase, reason="position_side_flipped"
                )
                await self._write_audit(
                    "trailing.cancelled",
                    cancelled,
                    previous_stop=state.current_stop,
                    current_stop=cancelled.current_stop,
                    reason="position_side_flipped",
                )
                if cleared:
                    await self._write_audit(
                        "trailing.clear",
                        cancelled,
                        previous_stop=state.current_stop,
                        current_stop=cancelled.current_stop,
                        reason=clear_note or "cleared",
                    )
                self._state_locks.pop(state.trailing_id, None)
                return cancelled

            quantity = abs(position.quantity)
            quantity_changed = quantity != state.quantity
            if quantity > state.quantity:
                entry_price = position.average_entry_price
                activation_price = resolve_activation_price(
                    state.spec, state.side, entry_price
                )
                updated = replace(
                    state,
                    quantity=quantity,
                    entry_price=entry_price,
                    resolved_activation_price=activation_price,
                    updated_at=now,
                )
            else:
                updated = replace(state, quantity=quantity, updated_at=now)
            await self._state_store.upsert_trailing_state(updated)

            if (
                quantity_changed
                and updated.mode is TrailingMode.SYNTHETIC
                and self._resize_required(updated)
            ):
                return await self._resize_protection(updated)
            return updated

    async def check_integrity(self, trailing_id: str) -> TrailingState | None:
        """Verify and restore protection for one synthetic state."""
        self._require_ready()
        async with self._state_lock(trailing_id):
            state = await self._state_store.get_trailing_state(trailing_id)
            if state is None:
                return state
            if state.mode is TrailingMode.NATIVE:
                return await self._check_native_integrity(state)
            if state.mode is not TrailingMode.SYNTHETIC:
                return state
            if state.phase not in (
                TrailingPhase.ARMED,
                TrailingPhase.DEGRADED,
                TrailingPhase.TRIGGERED,
            ):
                return state

            try:
                current = await self._adapter.get_position_tpsl(
                    state.instrument, state.position_id
                )
            except Exception as exc:
                return await self._record_protection_failure(
                    state, f"integrity read: {str(exc) or type(exc).__name__}"
                )

            now = self._clock()
            if current is None:
                closed = replace(
                    state,
                    phase=TrailingPhase.DONE,
                    quantity=Decimal("0"),
                    last_error=None,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(closed)
                self._publish(closed, previous=state.phase, reason="position_closed")
                await self._write_audit(
                    "trailing.done",
                    closed,
                    previous_stop=state.current_stop,
                    current_stop=closed.current_stop,
                    reason="position_closed",
                )
                self._state_locks.pop(trailing_id, None)
                return closed

            actual_tp, actual_sl = current
            instrument_spec = await self._instrument_spec(state.instrument)
            tick_size = instrument_spec.tick_size
            expected_stop = state.current_stop
            expected_take_profit = (
                state.current_take_profit if state.take_profit_owned else None
            )

            write_stop: Decimal | None = None
            write_take_profit: Decimal | None = None
            adopted_stop: Decimal | None = None
            reasons: list[str] = []
            protection_ok = True

            if expected_stop is not None:
                if actual_sl is None:
                    write_stop = expected_stop
                    reasons.append("missing_stop")
                    protection_ok = False
                elif abs(actual_sl.trigger_price - expected_stop) > tick_size:
                    if _stop_is_tighter(
                        state.side, actual_sl.trigger_price, expected_stop
                    ):
                        adopted_stop = actual_sl.trigger_price
                        reasons.append("adopted_tighter_stop")
                    else:
                        write_stop = expected_stop
                        reasons.append("protection_changed_externally")
                        protection_ok = False

            if expected_take_profit is not None and actual_tp is None:
                write_take_profit = expected_take_profit
                reasons.append("missing_take_profit")
                protection_ok = False

            if write_stop is not None or write_take_profit is not None:
                if not self._halt_machine.can_place_order(
                    state.instrument, reduce_only=True
                ):
                    return await self._record_protection_failure(
                        state, "integrity: halted"
                    )
                if not self._reserve_rate_limit():
                    return state
                try:
                    await self._adapter.modify_position_tpsl(
                        state.instrument,
                        state.position_id,
                        take_profit=(
                            TpSlAttachment(trigger_price=write_take_profit)
                            if write_take_profit is not None
                            else None
                        ),
                        stop_loss=(
                            TpSlAttachment(trigger_price=write_stop)
                            if write_stop is not None
                            else None
                        ),
                    )
                except Exception as exc:
                    return await self._record_protection_failure(
                        state, f"integrity: {str(exc) or type(exc).__name__}"
                    )

                confirmed_stop, confirmed_tp, detail = await self._confirm_levels(
                    state, write_stop, write_take_profit, tick_size
                )
                if detail is not None:
                    return await self._record_protection_failure(
                        state, f"integrity confirm: {detail}"
                    )
                final, recovered = self._protection_confirmed(
                    state,
                    stop=confirmed_stop if confirmed_stop is not None else adopted_stop,
                    take_profit=confirmed_tp,
                    now=now,
                )
                await self._state_store.upsert_trailing_state(final)
                if recovered:
                    self._publish(
                        final,
                        previous=TrailingPhase.DEGRADED,
                        reason="integrity_restore",
                    )
                await self._write_audit(
                    "trailing.integrity_restore",
                    final,
                    previous_stop=state.current_stop,
                    current_stop=final.current_stop,
                    reason=", ".join(reasons),
                )
                return final

            if adopted_stop is not None:
                final, recovered = self._protection_confirmed(
                    state, stop=adopted_stop, take_profit=None, now=now
                )
                await self._state_store.upsert_trailing_state(final)
                if recovered:
                    self._publish(
                        final,
                        previous=TrailingPhase.DEGRADED,
                        reason="integrity_restore",
                    )
                await self._write_audit(
                    "trailing.integrity_restore",
                    final,
                    previous_stop=state.current_stop,
                    current_stop=final.current_stop,
                    reason=", ".join(reasons),
                )
                return final

            if not protection_ok:
                return state
            if not (state.last_error or "").startswith(
                _PROTECTION_ERROR_PREFIX
            ) and state.failure_count == 0:
                return state

            final, recovered = self._protection_confirmed(
                state, stop=None, take_profit=None, now=now
            )
            await self._state_store.upsert_trailing_state(final)
            if recovered:
                self._publish(
                    final,
                    previous=TrailingPhase.DEGRADED,
                    reason="integrity_restore",
                )
                await self._write_audit(
                    "trailing.integrity_restore",
                    final,
                    previous_stop=state.current_stop,
                    current_stop=final.current_stop,
                    reason="protection_confirmed",
                )
            return final

    async def _check_native_integrity(
        self, state: TrailingState
    ) -> TrailingState | None:
        if state.phase not in (
            TrailingPhase.ARMED,
            TrailingPhase.DEGRADED,
            TrailingPhase.TRIGGERED,
        ):
            return state

        legs = await self._state_store.get_positions(state.instrument)
        leg = next(
            (item for item in legs if item.position_id == state.position_id),
            None,
        )
        now = self._clock()
        open_leg = _open_leg(leg)
        if open_leg is None:
            closed = replace(
                state,
                phase=TrailingPhase.DONE,
                quantity=Decimal("0"),
                last_error=None,
                updated_at=now,
            )
            await self._state_store.upsert_trailing_state(closed)
            self._publish(closed, previous=state.phase, reason="position_closed")
            await self._write_audit(
                "trailing.done",
                closed,
                previous_stop=state.current_stop,
                current_stop=closed.current_stop,
                reason="position_closed",
            )
            self._state_locks.pop(state.trailing_id, None)
            return closed

        try:
            instrument_spec = await self._instrument_spec(state.instrument)
        except Exception as exc:
            return await self._record_protection_failure(
                state,
                f"integrity: instrument spec: {str(exc) or type(exc).__name__}",
            )

        identity, _ = _recovery_identity(
            state, open_leg, instrument_spec.tick_size
        )
        if identity is not None:
            return await self._degrade_now(state, identity)

        result: NativeTrailingResult | None = None
        if state.native_id is not None:
            try:
                result = await self._adapter.get_trailing_stop(
                    state.native_id, state.instrument
                )
            except Exception as exc:
                return await self._record_protection_failure(
                    state,
                    f"integrity: native read: {str(exc) or type(exc).__name__}",
                )

        if result is not None:
            recovered = state.phase is TrailingPhase.DEGRADED and (
                state.last_error or ""
            ).startswith(_PROTECTION_ERROR_PREFIX)
            if not recovered and result.current_stop == state.current_stop:
                return state
            target_phase = (
                TrailingPhase.TRIGGERED
                if state.phase is TrailingPhase.TRIGGERED
                else TrailingPhase.ARMED
                if recovered
                else state.phase
            )
            final = replace(
                state,
                phase=target_phase,
                current_stop=result.current_stop,
                failure_count=0 if recovered else state.failure_count,
                last_error=None if recovered else state.last_error,
                updated_at=now,
            )
            await self._state_store.upsert_trailing_state(final)
            if recovered:
                self._publish(
                    final, previous=TrailingPhase.DEGRADED, reason="integrity_restore"
                )
                await self._write_audit(
                    "trailing.recovered",
                    final,
                    previous_stop=state.current_stop,
                    current_stop=final.current_stop,
                    reason="integrity_restore",
                )
            return final

        if not self._halt_machine.can_place_order(
            state.instrument, reduce_only=True
        ):
            return await self._degrade_now(state, "halted: native trail missing")
        if not self._reserve_rate_limit():
            return state
        try:
            result = await self._adapter.place_trailing_stop(
                self._native_request(state, open_leg)
            )
        except Exception as exc:
            return await self._degrade_now(
                state,
                f"integrity: native rearm: {str(exc) or type(exc).__name__}",
            )

        target_phase = (
            TrailingPhase.TRIGGERED
            if state.phase is TrailingPhase.TRIGGERED
            else TrailingPhase.ARMED
        )
        final = replace(
            state,
            phase=target_phase,
            native_id=result.native_id,
            current_stop=result.current_stop,
            activated=True,
            failure_count=0,
            last_error=None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(final)
        if state.phase is not target_phase:
            self._publish(final, previous=state.phase, reason="integrity_restore")
        await self._write_audit(
            "trailing.integrity_restore",
            final,
            previous_stop=state.current_stop,
            current_stop=final.current_stop,
            reason="native_rearmed",
        )
        return final

    async def integrity_pass(self) -> None:
        """Verify venue protection for every active state."""
        self._require_ready()
        for state in await self.list(active_only=True):
            try:
                await self.check_integrity(state.trailing_id)
            except Exception:
                logger.exception(
                    "trailing integrity check failed for %s", state.trailing_id
                )

    async def rehydrate(self, positions: list[Position] | None = None) -> None:
        """Restore active states from a direct venue position snapshot.

        A clean reconciliation does not prove the position dataset was fetched,
        so callers must pass the direct snapshot.  Omitting it fetches one here.
        Attach is refused until this method completes.  A failed snapshot fetch
        leaves rehydration pending and re-raises so the engine can retry.
        """
        self._require_ready()
        async with self._lock:
            self._rehydrated = False
            states = await self._state_store.list_trailing_states(
                phases=tuple(ACTIVE_TRAILING_PHASES)
            )

        try:
            snapshot = (
                positions
                if positions is not None
                else await self._adapter.fetch_positions()
            )
        except Exception:
            logger.exception("trailing rehydration deferred: position snapshot failed")
            raise

        mirror = await self._state_store.query_positions(limit=100_000)
        snapshot_by_leg = {
            (position.instrument, position.position_id): position
            for position in snapshot
            if position.position_id is not None
        }
        mirror_by_leg = {
            (position.instrument, position.position_id): position
            for position in mirror
            if position.position_id is not None
        }

        for state in states:
            key = (state.instrument, state.position_id)
            try:
                await self._rehydrate_state(
                    state,
                    snapshot_by_leg.get(key),
                    mirror_by_leg.get(key),
                )
            except Exception:
                logger.exception(
                    "trailing rehydration failed for %s", state.trailing_id
                )

        async with self._lock:
            self._rehydrated = True

    async def _require_rehydrated(self) -> None:
        if self._rehydrated is True:
            return
        active = await self._state_store.list_trailing_states(
            phases=tuple(ACTIVE_TRAILING_PHASES)
        )
        if active or self._rehydrated is False:
            raise TrailingStateError(
                "trailing rehydration has not completed; attach is not available"
            )
        self._rehydrated = True

    async def _rehydrate_state(
        self,
        row: TrailingState,
        snapshot_leg: Position | None,
        mirror_leg: Position | None,
    ) -> None:
        async with self._state_lock(row.trailing_id):
            state = await self._state_store.get_trailing_state(row.trailing_id)
            if state is None or state.phase not in ACTIVE_TRAILING_PHASES:
                return

            now = self._clock()
            snapshot_open = _open_leg(snapshot_leg)
            mirror_open = _open_leg(mirror_leg)
            if snapshot_open is None and mirror_open is None:
                await self._rehydrate_done(state, now)
                return
            if snapshot_open is None or mirror_open is None:
                await self._rehydrate_degraded(
                    state, "position_dataset_inconsistent", now=now
                )
                return
            if (
                abs(snapshot_open.quantity) != abs(mirror_open.quantity)
                or (snapshot_open.quantity > 0) != (mirror_open.quantity > 0)
            ):
                await self._rehydrate_degraded(
                    state, "position_dataset_inconsistent", now=now
                )
                return

            if snapshot_open != mirror_open:
                await self._state_store.upsert_position(snapshot_open)

            try:
                instrument_spec = await self._instrument_spec(state.instrument)
            except Exception as exc:
                await self._rehydrate_degraded(
                    state,
                    f"fetch: instrument spec: {str(exc) or type(exc).__name__}",
                    now=now,
                )
                return

            identity, side_changed = _recovery_identity(
                state, snapshot_open, instrument_spec.tick_size
            )
            if identity is not None:
                clear_note: str | None = None
                cleared = False
                if side_changed:
                    clear_note, cleared = await self._clear_protection_for_replacement(
                        state, static=True
                    )
                error = identity if clear_note is None else f"{identity}: {clear_note}"
                degraded = await self._rehydrate_degraded(state, error, now=now)
                if cleared:
                    await self._write_audit(
                        "trailing.clear",
                        degraded,
                        previous_stop=state.current_stop,
                        current_stop=degraded.current_stop,
                        reason=clear_note or "cleared",
                    )
                return

            if not self._config.rearm_on_reconnect:
                await self._rehydrate_degraded(state, "rearm_disabled", now=now)
                return

            activation_price = resolve_activation_price(
                state.spec, state.side, snapshot_open.average_entry_price
            )
            updated = replace(
                state,
                quantity=abs(snapshot_open.quantity),
                entry_price=snapshot_open.average_entry_price,
                position_opened_at=(
                    snapshot_open.opened_at or state.position_opened_at
                ),
                resolved_activation_price=activation_price,
                updated_at=now,
            )
            if updated.mode is TrailingMode.SYNTHETIC:
                await self._rehydrate_synthetic(
                    updated, snapshot_open, instrument_spec.tick_size, now
                )
            else:
                await self._rehydrate_native(updated, snapshot_open, now)

    async def _rehydrate_synthetic(
        self,
        state: TrailingState,
        leg: Position,
        tick_size: Decimal,
        now: datetime,
    ) -> TrailingState:
        try:
            current = await self._adapter.get_position_tpsl(
                state.instrument, state.position_id
            )
        except Exception as exc:
            return await self._rehydrate_degraded(
                state,
                f"protection: rehydrate read: {str(exc) or type(exc).__name__}",
                now=now,
            )

        actual_tp, actual_sl = current if current is not None else (None, None)
        expected_stop = state.current_stop
        expected_tp = (
            state.current_take_profit if state.take_profit_owned else None
        )
        stop_to_write: Decimal | None = None
        tp_to_write: Decimal | None = None
        reasons: list[str] = []

        if state.stop_owned:
            if expected_stop is None and actual_sl is not None:
                expected_stop = actual_sl.trigger_price
                reasons.append("adopted_venue_stop")
            elif expected_stop is not None:
                if actual_sl is None:
                    stop_to_write = expected_stop
                    reasons.append("missing_stop")
                elif abs(actual_sl.trigger_price - expected_stop) > tick_size:
                    if _stop_is_tighter(
                        state.side, actual_sl.trigger_price, expected_stop
                    ):
                        expected_stop = actual_sl.trigger_price
                        reasons.append("adopted_tighter_stop")
                    else:
                        stop_to_write = expected_stop
                        reasons.append("protection_changed_externally")

        if expected_tp is not None and actual_tp is None:
            tp_to_write = expected_tp
            reasons.append("missing_take_profit")

        if stop_to_write is not None or tp_to_write is not None:
            if not self._halt_machine.can_place_order(
                state.instrument, reduce_only=True
            ):
                return await self._rehydrate_degraded(
                    state, "halted: rehydrate protection", now=now
                )
            if not self._reserve_rate_limit():
                return await self._rehydrate_degraded(
                    state, "rate limit budget exhausted: rehydrate protection", now=now
                )
            try:
                await self._adapter.modify_position_tpsl(
                    state.instrument,
                    state.position_id,
                    take_profit=(
                        TpSlAttachment(trigger_price=tp_to_write)
                        if tp_to_write is not None
                        else None
                    ),
                    stop_loss=(
                        TpSlAttachment(trigger_price=stop_to_write)
                        if stop_to_write is not None
                        else None
                    ),
                )
            except Exception as exc:
                return await self._rehydrate_degraded(
                    state,
                    f"protection: rehydrate write: {str(exc) or type(exc).__name__}",
                    now=now,
                )
            confirmed_stop, confirmed_tp, detail = await self._confirm_levels(
                state, stop_to_write, tp_to_write, tick_size
            )
            if detail is not None:
                return await self._rehydrate_degraded(
                    state, f"protection: rehydrate confirm: {detail}", now=now
                )
            if confirmed_stop is not None:
                expected_stop = confirmed_stop
            if confirmed_tp is not None:
                expected_tp = confirmed_tp
        elif state.stop_owned and actual_sl is not None:
            expected_stop = actual_sl.trigger_price

        if expected_tp is None and state.take_profit_owned and actual_tp is not None:
            expected_tp = actual_tp.trigger_price

        base = replace(
            state,
            current_stop=expected_stop,
            current_take_profit=expected_tp,
            updated_at=now,
        )

        atr = base.atr_value
        atr_as_of = base.atr_as_of
        if base.spec.uses_atr:
            try:
                atr = await self._load_atr(base.spec, base.instrument)
            except Exception as exc:
                return await self._rehydrate_degraded(
                    base,
                    f"atr_stale: {str(exc) or type(exc).__name__}",
                    now=now,
                )
            atr_as_of = now

        if base.resolved_basis is None:
            return await self._rehydrate_degraded(
                base, "fetch: no sampling basis", now=now
            )
        try:
            ticker = await self._adapter.fetch_ticker(base.instrument)
        except Exception as exc:
            return await self._rehydrate_degraded(
                base,
                f"fetch: {str(exc) or type(exc).__name__}",
                now=now,
            )
        basis = (
            _basis_price(ticker, base.resolved_basis)
            if ticker is not None
            else None
        )
        if basis is None:
            return await self._rehydrate_degraded(
                base, "fetch: ticker has no resolved basis", now=now
            )

        best = update_best_basis(base.side, base.best_basis, basis)
        activation_price = resolve_activation_price(
            base.spec, base.side, base.entry_price
        )
        activated = base.activated or is_activation_reached(
            base.side, basis, activation_price
        )
        base = replace(
            base,
            best_basis=best,
            resolved_activation_price=activation_price,
            updated_at=now,
        )

        if base.current_stop is None and (
            activated or base.spec.initial_stop is not None
        ):
            try:
                first_stop = self._first_stop(
                    base.spec,
                    base.side,
                    base.entry_price,
                    basis,
                    atr,
                    await self._instrument_spec(base.instrument),
                    activated=activated,
                )
            except Exception as exc:
                return await self._rehydrate_degraded(
                    base,
                    f"protection: rehydrate stop: {str(exc) or type(exc).__name__}",
                    now=now,
                )
            if first_stop is not None:
                if not self._halt_machine.can_place_order(
                    base.instrument, reduce_only=True
                ):
                    return await self._rehydrate_degraded(
                        base, "halted: rehydrate first stop", now=now
                    )
                if not self._reserve_rate_limit():
                    return await self._rehydrate_degraded(
                        base,
                        "rate limit budget exhausted: rehydrate first stop",
                        now=now,
                    )
                try:
                    await self._adapter.modify_position_tpsl(
                        base.instrument,
                        base.position_id,
                        stop_loss=TpSlAttachment(trigger_price=first_stop),
                    )
                except Exception as exc:
                    return await self._rehydrate_degraded(
                        base,
                        f"protection: rehydrate first stop: "
                        f"{str(exc) or type(exc).__name__}",
                        now=now,
                    )
                confirmed_stop, detail = await self._confirm_stop(
                    base, first_stop, tick_size
                )
                if confirmed_stop is None:
                    return await self._rehydrate_degraded(
                        base,
                        f"protection: rehydrate first stop confirm: "
                        f"{detail or 'mismatch'}",
                        now=now,
                    )
                base = replace(
                    base,
                    current_stop=confirmed_stop,
                    stop_owned=True,
                    updated_at=now,
                )
                reasons.append("rehydrate_stop")

        if activated and base.stop_owned and base.current_stop is not None:
            try:
                candidate = quantize_stop(
                    candidate_stop(
                        side=base.side,
                        best_basis=best,
                        spec=base.spec,
                        atr=atr,
                    ),
                    tick_size,
                    base.side,
                )
            except Exception as exc:
                return await self._rehydrate_degraded(
                    base,
                    f"candidate: rehydrate: {str(exc) or type(exc).__name__}",
                    now=now,
                )

            instrument_spec = await self._instrument_spec(base.instrument)
            capability = self._adapter.position_protection_capability(
                base.instrument
            )
            reprice_interval_elapsed = (
                base.last_reprice_at is None
                or (now - base.last_reprice_at).total_seconds()
                >= capability.min_reprice_interval_seconds
            )
            if (
                candidate > 0
                and respects_min_stop_distance(
                    side=base.side,
                    candidate=candidate,
                    basis=basis,
                    min_stop_distance=instrument_spec.min_stop_distance,
                )
                and ratchet_accepts(
                    side=base.side,
                    current_stop=base.current_stop,
                    candidate=candidate,
                    step=base.spec.step,
                )
                and reprice_interval_elapsed
                and self._halt_machine.can_place_order(
                    base.instrument, reduce_only=True
                )
                and self._reserve_rate_limit()
            ):
                if capability.reprice_gap:
                    await self._write_audit(
                        "trailing.reprice_gap",
                        base,
                        previous_stop=base.current_stop,
                        current_stop=base.current_stop,
                        reason="gap_start",
                    )
                try:
                    await self._adapter.modify_position_tpsl(
                        base.instrument,
                        base.position_id,
                        stop_loss=TpSlAttachment(trigger_price=candidate),
                    )
                except Exception as exc:
                    if capability.reprice_gap:
                        await self._write_audit(
                            "trailing.reprice_gap",
                            base,
                            previous_stop=base.current_stop,
                            current_stop=base.current_stop,
                            reason="gap_end_failed",
                        )
                    return await self._rehydrate_degraded(
                        base,
                        f"protection: rehydrate reprice: "
                        f"{str(exc) or type(exc).__name__}",
                        now=now,
                    )
                confirmed_stop, detail = await self._confirm_stop(
                    base, candidate, tick_size
                )
                if confirmed_stop is None:
                    if capability.reprice_gap:
                        await self._write_audit(
                            "trailing.reprice_gap",
                            base,
                            previous_stop=base.current_stop,
                            current_stop=base.current_stop,
                            reason="gap_end_failed",
                        )
                    return await self._rehydrate_degraded(
                        base,
                        f"protection: rehydrate reprice confirm: "
                        f"{detail or 'mismatch'}",
                        now=now,
                    )
                base = replace(
                    base,
                    current_stop=confirmed_stop,
                    resolved_activation_price=activation_price,
                    activated=True,
                    stop_owned=True,
                    last_reprice_at=now,
                    reprice_count=base.reprice_count + 1,
                    updated_at=now,
                )
                reasons.append("rehydrate_reprice")
                if capability.reprice_gap:
                    await self._write_audit(
                        "trailing.reprice_gap",
                        base,
                        previous_stop=state.current_stop,
                        current_stop=base.current_stop,
                        reason="gap_end",
                    )

        target_phase = (
            TrailingPhase.TRIGGERED
            if state.phase is TrailingPhase.TRIGGERED
            else TrailingPhase.ARMED
        )
        final = replace(
            base,
            phase=target_phase,
            activated=activated or base.activated,
            atr_value=atr,
            atr_as_of=atr_as_of,
            failure_count=0,
            last_error=None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(final)
        if state.phase is not target_phase:
            self._publish(final, previous=state.phase, reason="rehydrated")
            await self._write_audit(
                "trailing.recovered",
                final,
                previous_stop=state.current_stop,
                current_stop=final.current_stop,
                reason="rehydrated",
            )
        if reasons:
            await self._write_audit(
                "trailing.integrity_restore",
                final,
                previous_stop=state.current_stop,
                current_stop=final.current_stop,
                reason=", ".join(dict.fromkeys(reasons)),
            )
        return final

    async def _rehydrate_native(
        self,
        state: TrailingState,
        leg: Position,
        now: datetime,
    ) -> TrailingState:
        result: NativeTrailingResult | None = None
        if state.native_id is not None:
            try:
                result = await self._adapter.get_trailing_stop(
                    state.native_id, state.instrument
                )
            except Exception as exc:
                return await self._rehydrate_degraded(
                    state,
                    f"fetch: native trail: {str(exc) or type(exc).__name__}",
                    now=now,
                )

        rearmed = False
        if result is None:
            if not self._halt_machine.can_place_order(
                state.instrument, reduce_only=True
            ):
                return await self._rehydrate_degraded(
                    state, "halted: native rearm", now=now
                )
            if not self._reserve_rate_limit():
                return await self._rehydrate_degraded(
                    state, "rate limit budget exhausted: native rearm", now=now
                )
            try:
                result = await self._adapter.place_trailing_stop(
                    self._native_request(state, leg)
                )
            except Exception as exc:
                return await self._rehydrate_degraded(
                    state,
                    f"protection: native rearm: {str(exc) or type(exc).__name__}",
                    now=now,
                )
            rearmed = True

        target_phase = (
            TrailingPhase.TRIGGERED
            if state.phase is TrailingPhase.TRIGGERED
            else TrailingPhase.ARMED
        )
        final = replace(
            state,
            phase=target_phase,
            native_id=result.native_id,
            current_stop=result.current_stop,
            activated=True,
            failure_count=0,
            last_error=None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(final)
        if state.phase is not target_phase:
            self._publish(final, previous=state.phase, reason="rehydrated")
            await self._write_audit(
                "trailing.recovered",
                final,
                previous_stop=state.current_stop,
                current_stop=final.current_stop,
                reason="rehydrated",
            )
        if rearmed:
            await self._write_audit(
                "trailing.integrity_restore",
                final,
                previous_stop=state.current_stop,
                current_stop=final.current_stop,
                reason="native_rearmed",
            )
        return final

    async def _rehydrate_done(
        self, state: TrailingState, now: datetime
    ) -> TrailingState:
        ensure_transition_allowed(state.phase, TrailingPhase.DONE)
        closed = replace(
            state,
            phase=TrailingPhase.DONE,
            quantity=Decimal("0"),
            last_error=None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(closed)
        self._publish(
            closed, previous=state.phase, reason="position_closed_while_offline"
        )
        await self._write_audit(
            "trailing.done",
            closed,
            previous_stop=state.current_stop,
            current_stop=closed.current_stop,
            reason="position_closed_while_offline",
        )
        self._state_locks.pop(state.trailing_id, None)
        return closed

    async def _rehydrate_degraded(
        self, state: TrailingState, reason: str, *, now: datetime
    ) -> TrailingState:
        transitioned = state.phase is not TrailingPhase.DEGRADED
        if transitioned:
            ensure_transition_allowed(state.phase, TrailingPhase.DEGRADED)
        degraded = replace(
            state,
            phase=TrailingPhase.DEGRADED,
            failure_count=state.failure_count + 1,
            last_error=reason,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(degraded)
        if transitioned:
            self._publish(degraded, previous=state.phase, reason=reason)
            await self._write_audit(
                "trailing.degraded",
                degraded,
                previous_stop=state.current_stop,
                current_stop=degraded.current_stop,
                reason=reason,
            )
        return degraded

    def _resize_required(self, state: TrailingState) -> bool:
        if not (state.stop_owned or state.take_profit_owned):
            return False
        capability = self._adapter.position_protection_capability(state.instrument)
        return capability.reprice_gap

    async def _resize_protection(self, state: TrailingState) -> TrailingState:
        stop = state.current_stop if state.stop_owned else None
        take_profit = state.current_take_profit if state.take_profit_owned else None
        if stop is None and take_profit is None:
            return state
        if not self._halt_machine.can_place_order(state.instrument, reduce_only=True):
            return await self._degrade_now(state, "halted: protection not resized")
        if not self._reserve_rate_limit():
            return await self._degrade_now(
                state, "rate limit budget exhausted: protection not resized"
            )
        try:
            await self._adapter.modify_position_tpsl(
                state.instrument,
                state.position_id,
                take_profit=(
                    TpSlAttachment(trigger_price=take_profit)
                    if take_profit is not None
                    else None
                ),
                stop_loss=(
                    TpSlAttachment(trigger_price=stop) if stop is not None else None
                ),
            )
        except Exception as exc:
            return await self._degrade_now(
                state, f"resize: {str(exc) or type(exc).__name__}"
            )

        instrument_spec = await self._instrument_spec(state.instrument)
        confirmed_stop, confirmed_tp, detail = await self._confirm_levels(
            state, stop, take_profit, instrument_spec.tick_size
        )
        if detail is not None:
            return await self._degrade_now(state, f"resize confirm: {detail}")
        final, recovered = self._protection_confirmed(
            state, stop=confirmed_stop, take_profit=confirmed_tp, now=self._clock()
        )
        await self._state_store.upsert_trailing_state(final)
        if recovered:
            self._publish(
                final, previous=TrailingPhase.DEGRADED, reason="quantity_change"
            )
        await self._write_audit(
            "trailing.reprice",
            final,
            previous_stop=state.current_stop,
            current_stop=final.current_stop,
            reason="quantity_change",
        )
        return final

    def publish_ticker_event(self, instrument: Instrument, ticker: Ticker) -> None:
        """Publish a sampled ticker when the worker is configured to do so."""
        self._event_bus.publish(
            TickerEvent(
                event_id=self._id_factory(),
                timestamp=self._clock(),
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=None,
                instrument=instrument,
                ticker=ticker,
            )
        )

    def _state_lock(self, trailing_id: str) -> asyncio.Lock:
        lock = self._state_locks.get(trailing_id)
        if lock is None:
            lock = asyncio.Lock()
            self._state_locks[trailing_id] = lock
        return lock

    async def _instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        spec = self._instrument_specs.get(instrument)
        if spec is None:
            spec = await self._adapter.fetch_instrument_spec(instrument)
            self._instrument_specs[instrument] = spec
        return spec

    async def _recover_fetch_degraded(
        self, state: TrailingState, now: datetime
    ) -> TrailingState:
        if state.phase is not TrailingPhase.DEGRADED:
            return state
        if not (state.last_error or "").startswith(_FETCH_ERROR_PREFIX):
            return state
        recovered = replace(
            state,
            phase=TrailingPhase.ARMED,
            last_error=None,
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(recovered)
        self._publish(recovered, previous=TrailingPhase.DEGRADED, reason="ticker_recovered")
        await self._write_audit(
            "trailing.recovered",
            recovered,
            previous_stop=recovered.current_stop,
            current_stop=recovered.current_stop,
            reason="ticker_recovered",
        )
        return recovered

    async def _record_fetch_failure(
        self, state: TrailingState, reason: str
    ) -> TrailingState:
        count = self._fetch_failures.get(state.trailing_id, 0) + 1
        self._fetch_failures[state.trailing_id] = count
        if count < self._config.fetch_failure_threshold:
            return state
        if state.phase is TrailingPhase.DEGRADED:
            return state

        now = self._clock()
        degraded = replace(
            state,
            phase=TrailingPhase.DEGRADED,
            last_error=f"{_FETCH_ERROR_PREFIX}{reason}",
            updated_at=now,
        )
        await self._state_store.upsert_trailing_state(degraded)
        self._publish(degraded, previous=state.phase, reason=reason)
        await self._write_audit(
            "trailing.degraded",
            degraded,
            previous_stop=state.current_stop,
            current_stop=state.current_stop,
            reason=reason,
        )
        return degraded

    async def _record_protection_failure(
        self, state: TrailingState, reason: str
    ) -> TrailingState:
        now = self._clock()
        count = state.failure_count + 1
        error = f"{_PROTECTION_ERROR_PREFIX}{reason}"
        target = replace(
            state,
            failure_count=count,
            last_error=error,
            updated_at=now,
        )
        if state.phase is not TrailingPhase.DEGRADED:
            if count < self._config.protection_failure_threshold:
                await self._state_store.upsert_trailing_state(target)
                return target
            ensure_transition_allowed(state.phase, TrailingPhase.DEGRADED)
            target = replace(target, phase=TrailingPhase.DEGRADED)

        await self._state_store.upsert_trailing_state(target)
        if state.phase is not TrailingPhase.DEGRADED:
            self._publish(target, previous=state.phase, reason=reason)
            await self._write_audit(
                "trailing.degraded",
                target,
                previous_stop=state.current_stop,
                current_stop=state.current_stop,
                reason=reason,
            )
        return target

    async def _degrade_now(
        self, state: TrailingState, reason: str
    ) -> TrailingState:
        if state.phase is not TrailingPhase.DEGRADED:
            ensure_transition_allowed(state.phase, TrailingPhase.DEGRADED)
        target = replace(
            state,
            phase=TrailingPhase.DEGRADED,
            failure_count=state.failure_count + 1,
            last_error=f"{_PROTECTION_ERROR_PREFIX}{reason}",
            updated_at=self._clock(),
        )
        await self._state_store.upsert_trailing_state(target)
        if state.phase is not TrailingPhase.DEGRADED:
            self._publish(target, previous=state.phase, reason=reason)
            await self._write_audit(
                "trailing.degraded",
                target,
                previous_stop=state.current_stop,
                current_stop=state.current_stop,
                reason=reason,
            )
        return target

    def _protection_confirmed(
        self,
        state: TrailingState,
        *,
        stop: Decimal | None,
        take_profit: Decimal | None,
        now: datetime,
    ) -> tuple[TrailingState, bool]:
        had_protection_error = (state.last_error or "").startswith(
            _PROTECTION_ERROR_PREFIX
        )
        recovered = (
            state.phase is TrailingPhase.DEGRADED and had_protection_error
        )
        final = replace(
            state,
            phase=TrailingPhase.ARMED if recovered else state.phase,
            current_stop=stop if stop is not None else state.current_stop,
            current_take_profit=(
                take_profit if take_profit is not None else state.current_take_profit
            ),
            failure_count=0,
            last_error=None if had_protection_error else state.last_error,
            updated_at=now,
        )
        return final, recovered

    async def _confirm_levels(
        self,
        state: TrailingState,
        stop: Decimal | None,
        take_profit: Decimal | None,
        tick_size: Decimal,
    ) -> tuple[Decimal | None, Decimal | None, str | None]:
        detail: str | None = None
        for attempt in range(2):
            try:
                current = await self._adapter.get_position_tpsl(
                    state.instrument, state.position_id
                )
            except Exception as exc:
                detail = str(exc) or type(exc).__name__
            else:
                if current is None:
                    detail = "position protection not found"
                else:
                    actual_tp, actual_sl = current
                    if _attachment_matches(
                        actual_sl, stop, tick_size
                    ) and _attachment_matches(actual_tp, take_profit, tick_size):
                        confirmed_stop = (
                            actual_sl.trigger_price
                            if stop is not None and actual_sl is not None
                            else None
                        )
                        confirmed_tp = (
                            actual_tp.trigger_price
                            if take_profit is not None and actual_tp is not None
                            else None
                        )
                        return confirmed_stop, confirmed_tp, None
                    actual_stop = (
                        actual_sl.trigger_price if actual_sl is not None else "none"
                    )
                    actual_take_profit = (
                        actual_tp.trigger_price if actual_tp is not None else "none"
                    )
                    detail = (
                        f"expected stop {stop if stop is not None else 'unchanged'}, "
                        "take profit "
                        f"{take_profit if take_profit is not None else 'unchanged'}; "
                        f"got stop {actual_stop}, take profit {actual_take_profit}"
                    )
            if attempt == 0:
                continue
        return None, None, detail

    async def _confirm_stop(
        self,
        state: TrailingState,
        expected: Decimal,
        tick_size: Decimal,
    ) -> tuple[Decimal | None, str | None]:
        detail: str | None = None
        for attempt in range(2):
            try:
                current = await self._adapter.get_position_tpsl(
                    state.instrument, state.position_id
                )
            except Exception as exc:
                detail = str(exc) or type(exc).__name__
            else:
                if current is None:
                    detail = "position protection not found"
                else:
                    actual = current[1]
                    if (
                        actual is not None
                        and abs(actual.trigger_price - expected) <= tick_size
                    ):
                        return actual.trigger_price, None
                    detail = (
                        f"expected {expected}, got "
                        f"{actual.trigger_price if actual is not None else 'none'}"
                    )
            if attempt == 0:
                continue
        return None, detail

    async def cancel(
        self,
        trailing_id: str,
        *,
        clear_protection: bool = False,
        reason: str | None = None,
    ) -> TrailingState:
        """Cancel an active state and persist the terminal phase.

        ``clear_protection`` removes only owned sides whose venue value still
        matches the last level this state wrote.  A failed or skipped clear is
        recorded in ``last_error``; the state still becomes ``CANCELLED``.
        """
        self._require_ready()
        async with self._lock:
            async with self._state_lock(trailing_id):
                state = await self._state_store.get_trailing_state(trailing_id)
                if state is None:
                    raise TrailingStateError(f"unknown trailing state {trailing_id!r}")
                ensure_transition_allowed(state.phase, TrailingPhase.CANCELLED)

                clear_note, cleared = await self._clear_protection_for_replacement(
                    state, static=clear_protection
                )

                last_error = clear_note if clear_note is not None else reason
                if last_error is None:
                    last_error = state.last_error
                now = self._clock()
                updated = replace(
                    state,
                    phase=TrailingPhase.CANCELLED,
                    last_error=last_error,
                    updated_at=now,
                )
                await self._state_store.upsert_trailing_state(updated)
                event_reason = clear_note or reason or "cancelled"
                self._publish(updated, previous=state.phase, reason=event_reason)
                await self._write_audit(
                    "trailing.cancelled",
                    updated,
                    previous_stop=state.current_stop,
                    current_stop=updated.current_stop,
                    reason=event_reason,
                )
                if cleared:
                    await self._write_audit(
                        "trailing.clear",
                        updated,
                        previous_stop=state.current_stop,
                        current_stop=updated.current_stop,
                        reason=clear_note or "cleared",
                    )
                self._state_locks.pop(trailing_id, None)
                return updated

    async def _clear_owned_protection(self, state: TrailingState) -> tuple[str | None, bool]:
        if not state.stop_owned and not state.take_profit_owned:
            return None, False
        if not self._halt_machine.can_place_order(state.instrument, reduce_only=True):
            return "halted: protection left in place", False

        try:
            current = await self._adapter.get_position_tpsl(
                state.instrument, state.position_id
            )
        except Exception as exc:
            return str(exc) or type(exc).__name__, False
        if current is None:
            return None, False

        actual_tp, actual_sl = current
        clear_tp = False
        clear_sl = False
        notes: list[str] = []
        if state.take_profit_owned:
            if actual_tp is not None:
                if (
                    state.current_take_profit is not None
                    and actual_tp.trigger_price == state.current_take_profit
                ):
                    clear_tp = True
                else:
                    notes.append("protection_changed_externally")
        if state.stop_owned:
            if actual_sl is not None:
                if (
                    state.current_stop is not None
                    and actual_sl.trigger_price == state.current_stop
                ):
                    clear_sl = True
                else:
                    notes.append("protection_changed_externally")

        note = ", ".join(dict.fromkeys(notes)) or None
        if not clear_tp and not clear_sl:
            return note, False
        if not self._reserve_rate_limit():
            return "rate limit budget exhausted; protection left in place", False

        try:
            await self._adapter.clear_position_tpsl(
                state.instrument,
                state.position_id,
                take_profit=clear_tp,
                stop_loss=clear_sl,
            )
        except Exception as exc:
            return str(exc) or type(exc).__name__, False
        return note, True

    async def _cancel_native_trail(
        self, state: TrailingState
    ) -> tuple[str | None, bool]:
        if state.mode is not TrailingMode.NATIVE or state.native_id is None:
            return None, False
        if not self._halt_machine.can_place_order(state.instrument, reduce_only=True):
            return "halted: native trail left in place", False
        if not self._reserve_rate_limit():
            return "rate limit budget exhausted; native trail left in place", False
        try:
            await self._adapter.cancel_trailing_stop(
                state.native_id, state.instrument
            )
            remaining = await self._adapter.get_trailing_stop(
                state.native_id, state.instrument
            )
        except Exception as exc:
            return str(exc) or type(exc).__name__, False
        if remaining is not None:
            return "native trailing stop still present after cancel", False
        return None, True

    async def _clear_protection_for_replacement(
        self, state: TrailingState, *, static: bool
    ) -> tuple[str | None, bool]:
        notes: list[str] = []
        cleared = False
        if state.mode is TrailingMode.NATIVE:
            note, native_cleared = await self._cancel_native_trail(state)
            if note is not None:
                notes.append(note)
            cleared = cleared or native_cleared
        if static and (state.stop_owned or state.take_profit_owned):
            note, static_cleared = await self._clear_owned_protection(state)
            if note is not None:
                notes.append(note)
            cleared = cleared or static_cleared
        return (", ".join(dict.fromkeys(notes)) or None), cleared

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

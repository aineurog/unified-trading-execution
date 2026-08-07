"""BybitAdapter — concrete Adapter ABC implementation for Bybit (Section 17.10).

Usage::

    from unified_trading_execution.bybit import BybitAdapter, BybitConfig
    from unified_trading_execution.events import EventBus

    config = BybitConfig(api_key="...", api_secret="...", testnet=True)
    adapter = BybitAdapter(config, event_bus=EventBus())
    await adapter.connect()
    # ...
    await adapter.disconnect()
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import OrderedDict, defaultdict
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from pybit.exceptions import FailedRequestError, InvalidRequestError
from pybit.unified_trading import HTTP
from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import (
    AsymmetricLeverageError,
    LeverageDriftError,
    LeverageExceedsMaxError,
    LeverageNotModifiedError,
    MarginModeNotModifiedError,
    map_bybit_error,
)
from unified_trading_execution.bybit.events import (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    LeverageDriftEvent,
    MarginModeChangedEvent,
)
from unified_trading_execution.bybit.margin import MarginMode
from unified_trading_execution.bybit.orders import (
    build_amend_payload,
    build_cancel_payload,
    build_place_order_payload,
    parse_order_result,
)
from unified_trading_execution.bybit.streams import (
    is_final_order_status,
    is_terminal_order_status,
    translate_fill,
    translate_order_entry,
    translate_position,
    translate_wallet_member,
)
from unified_trading_execution.bybit.symbols import from_bybit_symbol, to_bybit_symbol
from unified_trading_execution.bybit.websocket import BybitWebSocket
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformError,
    UteError,
)
from unified_trading_execution.events import (
    AuditEvent,
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    HaltClearedEvent,
    HaltEnteredEvent,
    HaltEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.enums import AssetClass, OrderStatus, OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

logger = logging.getLogger(__name__)

_DEFAULT_REQUESTS_PER_INTERVAL = 120
_DEFAULT_INTERVAL_SECONDS = 60
_CONNECTION_MONITOR_INTERVAL_SECONDS = 5.0
_ORDER_CATEGORIES: tuple[str, ...] = ("spot", "linear", "inverse")
# The wallet is a Bybit unified-account concept; v1 targets that single
# account type.  If real multi-account-type support is ever needed this is
# promoted to BybitConfig — mirroring the hardcoded-categories pattern above.
_ACCOUNT_TYPE = "UNIFIED"
_MAX_TRACKED_FINAL_ORDER_IDS = 10_000
_LEVERAGE_KIND_PREFIX = "leverage."


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _parse_stream_ms(raw: object) -> datetime:
    """Parse a Bybit stream millisecond timestamp into a tz-aware datetime."""
    ms = int(str(raw))
    seconds, millis = divmod(ms, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)


def _safe_header_int(headers: dict[str, str], key: str, default: int) -> int:
    raw = headers.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


def _parse_rate_limits(headers: dict[str, str]) -> RateLimits:
    raw_limit = _safe_header_int(headers, "X-Bapi-Limit", _DEFAULT_REQUESTS_PER_INTERVAL)
    limit = max(raw_limit, 1)

    raw_remaining = _safe_header_int(headers, "X-Bapi-Remaining", _DEFAULT_REQUESTS_PER_INTERVAL)
    remaining = max(raw_remaining, 0)

    raw_reset_ts = _safe_header_int(headers, "X-Bapi-Reset-Timestamp", 0)
    reset_at = (
        datetime.fromtimestamp(raw_reset_ts / 1000, tz=UTC)
        if raw_reset_ts > 0
        else datetime.now(UTC)
    )

    return RateLimits(
        requests_per_interval=limit,
        interval_seconds=_DEFAULT_INTERVAL_SECONDS,
        remaining=remaining,
        reset_at=reset_at,
    )


def _group_leverage_rows(rows: dict[str, str]) -> dict[str, dict[str, int | None]]:
    """Group ``adapter_config`` leverage rows into ``{symbol: {buy, sell}}``.

    Keys are ``leverage.buy:{symbol}`` / ``leverage.sell:{symbol}``.  Rows that
    do not match the two-side schema are ignored, and a missing side is left as
    None for the caller to skip.
    """
    grouped: dict[str, dict[str, int | None]] = defaultdict(lambda: {"buy": None, "sell": None})
    for full_key, value in rows.items():
        try:
            value_int = int(value)
        except ValueError:
            continue
        body = full_key.removeprefix(_LEVERAGE_KIND_PREFIX)
        if body.startswith("buy:"):
            side, symbol = "buy", body[len("buy:") :]
        elif body.startswith("sell:"):
            side, symbol = "sell", body[len("sell:") :]
        else:
            continue
        grouped[symbol][side] = value_int
    return grouped


class BybitAdapter(Adapter):
    """Concrete Adapter ABC implementation for Bybit.

    Construction follows the Adapter ABC convention (Section 17.10):
    configuration is supplied as a ``BybitConfig`` dataclass, not loose
    strings; the EventBus reference is required so the adapter can publish
    translated events from its internal WebSocket handlers.
    """

    def __init__(
        self,
        config: BybitConfig,
        *,
        event_bus: EventBus | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        # Optional handle to the shared StateStore, used only for the
        # adapter-owned ``adapter_config`` keyspace (leverage/margin-mode
        # intent, Section 2).  The Adapter ABC deliberately keeps the
        # state *mirror* out of the adapter; this is the one documented
        # exception, scoped to the adapter's own config table.
        self._state_store = state_store
        self._halt_machine: HaltStateMachine | None = None
        self._connected = False
        self._ws: BybitWebSocket | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._last_rate_limits = _parse_rate_limits({})
        self._order_refs: dict[str, tuple[str, str]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._instruments: dict[tuple[str, str], Instrument] = {}
        # Cache of fetched InstrumentSpecs, keyed by the canonical Instrument.
        # Each value carries the time.monotonic() wall-clock at fetch so the
        # optional TTL (config.instrument_spec_cache_ttl) can expire it.  Lives
        # for the adapter instance lifetime (Section 17.3).
        self._instrument_specs: dict[Instrument, tuple[InstrumentSpec, float]] = {}
        self._instrument_spec_cache_ttl: float | None = config.instrument_spec_cache_ttl
        self._open_order_ids: set[str] = set()
        self._final_order_ids: OrderedDict[str, None] = OrderedDict()
        # Protects _open_order_ids and _final_order_ids which are read/written
        # from both the event-loop thread and pybit's background WS thread.
        self._order_ids_lock = threading.Lock()

        self._session = HTTP(
            testnet=config.testnet,
            demo=config.demo,
            api_key=config.api_key,
            api_secret=config.api_secret,
            return_response_headers=True,
        )

    def _update_rate_limits(self, headers: dict[str, str]) -> None:
        self._last_rate_limits = _parse_rate_limits(headers)

    @staticmethod
    def _instrument_to_category(instrument: Instrument) -> str:
        if instrument.asset_class == AssetClass.SPOT:
            return "spot"
        if instrument.asset_class == AssetClass.FUTURES:
            if instrument.currency == instrument.quote_currency:
                return "linear"
            return "inverse"
        raise InvalidSymbolError(
            f"Asset class {instrument.asset_class} is not supported by Bybit",
        )

    # ---- Leverage and margin mode (Section 4, Phase 3) ----

    @staticmethod
    def _leverage_buy_key(instrument: Instrument) -> str:
        """adapter_config key for stored buy-side leverage intent."""
        return f"leverage.buy:{to_bybit_symbol(instrument)}"

    @staticmethod
    def _leverage_sell_key(instrument: Instrument) -> str:
        """adapter_config key for stored sell-side leverage intent."""
        return f"leverage.sell:{to_bybit_symbol(instrument)}"

    @staticmethod
    def _margin_mode_key() -> str:
        """adapter_config key for stored margin mode intent.

        Margin mode is account-wide on Bybit UTA — a single key, no symbol.
        """
        return "margin_mode"

    async def _require_store(self) -> StateStore:
        """Return the state store or raise if leverage persistence is unavailable.

        Leverage intent persistence needs the shared StateStore, which is an
        optional constructor argument (the Adapter ABC keeps the state mirror
        out of the adapter).  A missing store is a hard error here — silently
        skipping persistence would silently defeat user intent.
        """
        if self._state_store is None:
            raise PlatformError(
                "BybitAdapter was constructed without a state_store — "
                "leverage/margin-mode persistence is unavailable"
            )
        return self._state_store

    async def _validate_leverage(self, instrument: Instrument, leverage: int) -> None:
        """Reject leverage requests that the platform cannot honour.

        Per Section 8, validation is eager at the ``set_leverage`` call, not
        lazy on platform rejection: a spot instrument is invalid, and a value
        above ``max_leverage`` raises ``LeverageExceedsMaxError`` with the
        platform's cap in the message.
        """
        category = self._instrument_to_category(instrument)
        if category == "spot":
            raise InvalidSymbolError(
                f"Spot instrument {to_bybit_symbol(instrument)} has no leverage"
            )
        spec = await self.fetch_instrument_spec(instrument)
        if spec.max_leverage is not None and leverage > int(spec.max_leverage):
            raise LeverageExceedsMaxError(
                f"Leverage {leverage} exceeds max {spec.max_leverage} "
                f"for {to_bybit_symbol(instrument)}"
            )

    async def _block_on_open_position(
        self,
        instrument: Instrument,
        *,
        action: str = "change leverage",
    ) -> None:
        """Raise if the instrument has an open position and the guard is enabled.

        Section 5.6 — ``block_on_open_position`` defaults to True.  Changing
        leverage or margin mode with an open position recalculates margin
        immediately and can cause liquidation, so the safe default is to
        refuse.  ``action`` names the operation in the raised error.
        """
        if not self._config.leverage.block_on_open_position:
            return
        category = self._instrument_to_category(instrument)
        if category == "spot":
            return
        data, _ = await self._run_request(
            self._session.get_positions,
            category=category,
            symbol=to_bybit_symbol(instrument),
        )
        entries = (data.get("result") or {}).get("list") or []
        for entry in entries:
            if Decimal(str(entry.get("size") or "0")) != 0:
                raise PlatformError(
                    f"Cannot {action} with open position for {to_bybit_symbol(instrument)}"
                )

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        buy_leverage: int,
        sell_leverage: int | None = None,
    ) -> None:
        """Set leverage for *instrument* on the platform and persist the intent.

        ``buy_leverage`` and ``sell_leverage`` are stored and applied
        independently (keys ``leverage.buy:{symbol}`` / ``leverage.sell:{symbol}``)
        so hedge-mode asymmetric leverage can be enabled without a schema
        change.  v1 runs one-way mode, where Bybit requires
        ``buyLeverage == sellLeverage``, so any buy != sell request is rejected
        upfront with ``AsymmetricLeverageError``.  ``sell_leverage`` defaults
        to ``buy_leverage``.

        Raises:
            InvalidSymbolError: instrument not supported / not derivatives.
            LeverageExceedsMaxError: leverage exceeds the platform max.
            AsymmetricLeverageError: buy != sell requested (hedge mode not
                supported in v1).
            PlatformError: platform rejected the request, or an open position
                blocked the change while ``block_on_open_position`` is enabled.
        """
        if sell_leverage is None:
            sell_leverage = buy_leverage
        symbol = to_bybit_symbol(instrument)
        if buy_leverage != sell_leverage:
            raise AsymmetricLeverageError(
                f"Asymmetric leverage (buy={buy_leverage} sell={sell_leverage}) "
                f"for {symbol} requires hedge mode, which is not supported in v1"
            )
        await self._validate_leverage(instrument, buy_leverage)
        await self._block_on_open_position(instrument)

        category = self._instrument_to_category(instrument)
        try:
            await self._run_request(
                self._session.set_leverage,
                category=category,
                symbol=symbol,
                buyLeverage=str(buy_leverage),
                sellLeverage=str(sell_leverage),
            )
        except LeverageNotModifiedError:
            logger.info(
                "Leverage already %s for %s — treating as applied",
                buy_leverage,
                symbol,
            )
        store = await self._require_store()
        await store.set_adapter_config(self._leverage_buy_key(instrument), str(buy_leverage))
        await store.set_adapter_config(self._leverage_sell_key(instrument), str(sell_leverage))

    async def get_leverage(self, instrument: Instrument) -> tuple[int, int] | None:
        """Query current leverage from the platform for *instrument*.

        Returns ``(buy, sell)`` — in one-way mode both sides are equal.  Returns
        None if the instrument has no leverage setting (spot, delisted, etc.).
        """
        category = self._instrument_to_category(instrument)
        if category == "spot":
            return None
        data, _ = await self._run_request(
            self._session.get_positions,
            category=category,
            symbol=to_bybit_symbol(instrument),
        )
        entries = (data.get("result") or {}).get("list") or []
        if not entries:
            return None
        buy: int | None = None
        sell: int | None = None
        for entry in entries:
            raw = entry.get("leverage")
            if raw is None:
                continue
            lev = int(str(raw))
            position_idx = int(str(entry.get("positionIdx") or "0"))
            if position_idx == 0:
                buy = sell = lev
            elif position_idx == 1:
                buy = lev
            elif position_idx == 2:
                sell = lev
        if buy is None and sell is None:
            return None
        if buy is None:
            assert sell is not None
            return (sell, sell)
        if sell is None:
            return (buy, buy)
        return (buy, sell)

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Remove stored leverage intent for *instrument*.

        Does NOT change leverage on the platform — only drops the stored intent
        so the engine stops managing it.  ``margin_mode.{symbol}`` is left
        untouched; margin mode is independently managed (Section 5.7).
        """
        store = await self._require_store()
        await store.delete_adapter_config(self._leverage_buy_key(instrument))
        await store.delete_adapter_config(self._leverage_sell_key(instrument))

    async def remove_margin_mode(self) -> None:
        """Remove stored margin-mode intent for the account.

        Does NOT change margin mode on the platform — only drops the stored
        intent so the engine stops managing it.  Per-symbol leverage intent
        is left untouched; leverage is independently managed (Section 5.7).
        """
        store = await self._require_store()
        await store.delete_adapter_config(self._margin_mode_key())

    async def set_margin_mode(self, mode: MarginMode) -> None:
        """Set the account-wide margin mode on the platform and persist intent.

        Bybit UTA margin mode is account-wide — ``POST /v5/account/set-margin-mode``
        takes only ``setMarginMode``, no symbol or leverage parameter.  Leverage
        is set independently per symbol via :meth:`set_leverage`.

        Raises:
            PlatformError: platform rejected the switch.
        """
        target = "ISOLATED_MARGIN" if mode is MarginMode.ISOLATED else "REGULAR_MARGIN"
        try:
            await self._run_request(
                self._session.set_margin_mode,
                setMarginMode=target,
            )
        except MarginModeNotModifiedError:
            logger.info("Margin mode already %s — treating as applied", target)
        store = await self._require_store()
        await store.set_adapter_config(self._margin_mode_key(), mode.value)

    async def get_margin_mode(self) -> MarginMode | None:
        """Query the current account-wide margin mode from the platform.

        Reads ``marginMode`` from ``GET /v5/account/info``.
        ``REGULAR_MARGIN`` → cross, ``ISOLATED_MARGIN`` → isolated.
        ``PORTFOLIO_MARGIN`` is not mapped and returns None.
        """
        data, _ = await self._run_request(self._session.get_account_info)
        margin_mode = (data.get("result") or {}).get("marginMode")
        if margin_mode == "ISOLATED_MARGIN":
            return MarginMode.ISOLATED
        if margin_mode == "REGULAR_MARGIN":
            return MarginMode.CROSS
        return None

    # ---- Adapter-owned user intent (Phase 5, Phase 6) ----

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Store core's shared halt state machine so drift can enter halts."""
        self._halt_machine = halt_machine

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so WS handlers can publish."""
        self._event_bus = event_bus

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via Engine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

    async def _stored_leverage(self, instrument: Instrument) -> tuple[int, int] | None:
        """Read the stored leverage intent for *instrument* as ``(buy, sell)``.

        If only one side is stored (e.g. written before the schema settled) the
        missing side is filled from the present one.  Returns None when no
        leverage intent exists.
        """
        if self._state_store is None:
            return None
        try:
            buy_raw = await self._state_store.get_adapter_config(self._leverage_buy_key(instrument))
            sell_raw = await self._state_store.get_adapter_config(
                self._leverage_sell_key(instrument)
            )
        except Exception:
            logger.exception("Failed to read stored leverage for %s", instrument)
            return None
        if buy_raw is None and sell_raw is None:
            return None
        try:
            if buy_raw is None or sell_raw is None:
                # Only one side stored (written before the schema settled):
                # v1 is symmetric, so both sides take the present value.
                present = buy_raw if buy_raw is not None else sell_raw
                assert present is not None
                return (int(present), int(present))
            return (int(buy_raw), int(sell_raw))
        except ValueError:
            return None

    async def _handle_leverage_drift(
        self,
        instrument: Instrument,
        stored: tuple[int, int],
        platform: tuple[int, int],
        *,
        context: str,
    ) -> None:
        """Execute the configured ``on_drift`` behavior for a leverage mismatch."""
        on_drift = self._config.leverage.on_drift

        if on_drift == "reapply":
            await self.set_leverage(
                instrument,
                buy_leverage=stored[0],
                sell_leverage=stored[1],
            )
            action: Literal["reapplied", "notified", "halted"] = "reapplied"
            await self._emit_drift_event(instrument, stored, platform, action)
            return

        if on_drift == "notify":
            await self._emit_drift_event(instrument, stored, platform, "notified")
            return

        # on_drift == "halt"
        await self._emit_drift_event(instrument, stored, platform, "halted")
        await self._enter_instrument_halt(
            instrument,
            reason="leverage_drift",
            detail=f"{context}: stored_buy={stored[0]} stored_sell={stored[1]} "
            f"platform_buy={platform[0]} platform_sell={platform[1]}",
        )

    async def _emit_drift_event(
        self,
        instrument: Instrument,
        stored: tuple[int, int],
        platform: tuple[int, int],
        action: Literal["reapplied", "notified", "halted"],
    ) -> None:
        self._publish(
            LeverageDriftEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                instrument=instrument,
                stored_buy=stored[0],
                stored_sell=stored[1],
                platform_buy=platform[0],
                platform_sell=platform[1],
                action_taken=action,
            )
        )
        await self._write_leverage_audit(
            event_type="bybit.leverage.drift",
            instrument=instrument,
            payload={
                "stored_buy": stored[0],
                "stored_sell": stored[1],
                "platform_buy": platform[0],
                "platform_sell": platform[1],
                "action_taken": action,
            },
        )

    async def _enter_instrument_halt(
        self,
        instrument: Instrument,
        *,
        reason: str,
        detail: str,
    ) -> None:
        """Enter an instrument-scoped halt and persist its audit record.

        A missing halt machine (adapter constructed standalone, or core not
        sharing one) degrades to logging — drift is still reported via
        ``LeverageDriftEvent`` so users are not silently left in the dark.
        """
        halt_machine = self._halt_machine
        if halt_machine is None:
            logger.warning(
                "Cannot enter %s halt for %s — no halt machine attached",
                reason,
                to_bybit_symbol(instrument),
            )
            return
        if not halt_machine.enter_halt(
            scope="instrument",
            instrument=instrument,
            reason=reason,
            detail=detail,
        ):
            return
        self._publish(
            HaltEnteredEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                scope="instrument",
                instrument=instrument,
                reason=reason,
                detail=detail,
            )
        )
        await self._write_halt_audit(
            action="entered",
            instrument=instrument,
            reason=reason,
            detail=detail,
        )

    async def _write_halt_audit(
        self,
        *,
        action: Literal["entered", "cleared"],
        instrument: Instrument,
        reason: str,
        detail: str,
    ) -> None:
        store = self._state_store
        if store is None:
            return
        try:
            await store.write_halt_event(
                HaltEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    action=action,
                    scope="instrument",
                    instrument=instrument,
                    reason=reason,
                    detail=detail,
                    cleared_by="automatic" if action == "cleared" else None,
                )
            )
        except Exception:
            logger.exception("Failed to write halt audit for %s", instrument)

    async def _write_leverage_audit(
        self,
        *,
        event_type: str,
        instrument: Instrument | None,
        payload: dict[str, object],
    ) -> None:
        """Append a leverage/margin-mode ``AuditEvent`` to the state store.

        ``instrument`` is None for account-wide events (e.g. margin mode change)
        where no specific symbol applies.  Adapter-owned events never pass
        through core's dispatch pipeline — the adapter writes the audit record
        itself in the same coroutine as the bus publish, so no emit is ever
        missing from the trail.  A failure to write is logged, not raised.
        """
        store = self._state_store
        if store is None:
            return
        symbol_entry: dict[str, object] = (
            {"symbol": to_bybit_symbol(instrument)} if instrument is not None else {}
        )
        try:
            await store.write_audit_event(
                AuditEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id="",
                    event_type=event_type,
                    payload={**symbol_entry, **payload},
                )
            )
        except Exception:
            logger.exception(
                "Failed to write %s audit%s",
                event_type,
                f" for {instrument}" if instrument is not None else "",
            )

    async def _strict_check_leverage(self, instrument: Instrument) -> None:
        """Pre-order leverage verification (Phase 5, §5.2).

        Runs when ``strict_check`` is enabled, before every order dispatch:
        if a stored leverage intent exists and the platform leverage differs,
        the configured ``on_drift`` behavior is executed — reapply restores the
        stored value and the order proceeds; notify/halt raise
        ``LeverageDriftError`` to reject the order.
        """
        if not self._config.leverage.strict_check:
            return
        stored = await self._stored_leverage(instrument)
        if stored is None:
            return
        platform = await self.get_leverage(instrument)
        if platform is None or platform == stored:
            return
        await self._handle_leverage_drift(
            instrument,
            stored,
            platform,
            context="pre-order strict check",
        )
        if self._config.leverage.on_drift != "reapply":
            raise LeverageDriftError(
                f"Platform leverage {platform} differs from stored intent {stored} "
                f"for {to_bybit_symbol(instrument)}"
            )

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored leverage/margin-mode intent against the platform (§5.3).

        Called by core during ``engine.reconcile()``.  For each instrument with
        a stored intent, the platform's current value is queried and compared;
        a mismatch executes the configured ``on_drift`` behavior exactly as the
        strict check does, minus the per-order rejection (reconcile does not
        raise — it re-applies, notifies, or halts).  When the platform has
        recovered to the stored intent, any residual drift halt is cleared.
        """
        if self._state_store is None:
            return
        leverage_rows = await self._state_store.list_adapter_config(_LEVERAGE_KIND_PREFIX)
        stored_margin_mode = await self._state_store.get_adapter_config(self._margin_mode_key())
        if not leverage_rows and stored_margin_mode is None:
            return
        instruments = {to_bybit_symbol(i): i for i in self._instruments.values()}

        leverage_by_symbol = _group_leverage_rows(leverage_rows)

        for symbol, sides in leverage_by_symbol.items():
            instrument = instruments.get(symbol)
            if instrument is None:
                continue
            if sides["buy"] is None or sides["sell"] is None:
                continue
            stored = (int(sides["buy"]), int(sides["sell"]))
            platform = await self.get_leverage(instrument)
            if platform is None:
                continue
            if platform == stored:
                await self._try_clear_recovered_halt(instrument)
                continue
            try:
                await self._handle_leverage_drift(
                    instrument,
                    stored,
                    platform,
                    context="reconciliation",
                )
            except Exception:
                logger.exception(
                    "Leverage drift handling failed for %s during reconcile",
                    symbol,
                )

        # Margin mode is account-wide — single key, no per-symbol loop.
        stored_mode_value = stored_margin_mode
        if stored_mode_value is not None:
            try:
                stored_mode = MarginMode(stored_mode_value)
            except ValueError:
                logger.warning("Stored margin mode value %r is unrecognised — skipping", stored_mode_value)
                stored_mode = None
            if stored_mode is not None:
                platform_mode = await self.get_margin_mode()
                if platform_mode is not None and platform_mode is not stored_mode:
                    try:
                        await self._handle_margin_mode_drift(stored_mode, platform_mode)
                    except Exception:
                        logger.exception("Margin-mode drift handling failed during reconcile")

    async def _try_clear_recovered_halt(self, instrument: Instrument) -> None:
        """Clear an instrument halt once the platform matches stored intent.

        §5.3 — a drift halt blocks new orders until cleared; when a later
        reconcile pass finds the platform back in line with stored intent the
        halt is cleared automatically and a ``HaltClearedEvent`` published.
        """
        halt_machine = self._halt_machine
        if halt_machine is None:
            return
        if not halt_machine.try_clear_halt(
            "instrument",
            instrument=instrument,
            reconciliation_is_clean=True,
        ):
            return
        self._publish(
            HaltClearedEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                scope="instrument",
                instrument=instrument,
                cleared_by="automatic",
            )
        )
        await self._write_halt_audit(
            action="cleared",
            instrument=instrument,
            reason="leverage_drift_recovered",
            detail="Platform leverage matches stored intent",
        )

    async def _handle_margin_mode_drift(
        self,
        stored: MarginMode,
        platform: MarginMode,
    ) -> None:
        """Execute the configured ``on_drift`` behavior for an account-level margin-mode mismatch.

        Margin mode is account-wide — no instrument parameter.  Reapply
        restores the stored mode, notify only reports, halt is not applicable
        at the account margin-mode level (no per-instrument halt for this).
        """
        on_drift = self._config.leverage.on_drift

        if on_drift == "reapply":
            previous = platform
            await self.set_margin_mode(stored)
            self._publish(
                MarginModeChangedEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    previous=previous,
                    current=stored,
                )
            )
            await self._write_leverage_audit(
                event_type="bybit.margin_mode.changed",
                instrument=None,
                payload={
                    "previous": previous.value if previous else None,
                    "current": stored.value,
                    "context": "reconciliation",
                },
            )
            return

        if on_drift == "notify":
            logger.warning(
                "Margin mode drift: stored=%s platform=%s",
                stored.value,
                platform.value,
            )
            return

        # on_drift == "halt": margin mode is account-wide, no instrument to halt.
        # Log clearly and notify — halting a specific instrument for an account-wide
        # setting would be misleading.
        logger.error(
            "Margin mode drift (stored=%s platform=%s) — on_drift=halt is not applicable "
            "for account-wide margin mode; treating as notify.",
            stored.value,
            platform.value,
        )

    # ---- Identification (Section 17.10) ----

    @property
    def platform_name(self) -> str:
        return self._config.platform_name

    @property
    def account_id(self) -> str:
        return self._config.account_id

    # ---- Connection lifecycle ----

    async def connect(self) -> None:
        """Open persistent connections — REST session + WebSocket streams.

        Must publish ``ConnectionStateEvent(connected=True)`` on success.
        See Section 17.10, "Connection lifecycle."
        """
        if self._connected:
            return
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None

        self._loop = asyncio.get_running_loop()
        await self._refresh_instrument_registry()

        ws = BybitWebSocket(self._config)
        await asyncio.to_thread(ws.connect)
        self._ws = ws
        await asyncio.to_thread(ws.subscribe_order, self._on_order_message)
        await asyncio.to_thread(ws.subscribe_execution, self._on_execution_message)
        await asyncio.to_thread(ws.subscribe_position, self._on_position_message)
        await asyncio.to_thread(ws.subscribe_wallet, self._on_wallet_message)
        self._connected = True
        self._publish_connection_state(True)
        self._monitor_task = asyncio.create_task(self._monitor_connection())
        await self._reapply_stored_intent()

    async def _reapply_stored_intent(self) -> None:
        """Reapply stored leverage / margin-mode intent to the platform.

        Runs after a successful connect.  For each instrument with stored intent:

        - if margin mode is stored, ``switch_margin_mode`` is called with the
          stored margin mode and the stored leverage value — a single call that
          sets both.
        - if only leverage is stored, ``set_leverage`` restores it.
        - a platform rejection emits ``LeverageApplyFailedEvent`` and never
          crashes the connection; successful applies emit
          ``LeverageAppliedEvent`` (and ``MarginModeChangedEvent`` when a margin
          mode was applied).

        Instruments with no stored intent are not touched — their platform
        leverage/margin mode is whatever it is.
        """
        if not self._config.leverage.auto_apply_on_connect:
            return
        if self._state_store is None:
            return

        leverage_rows = await self._state_store.list_adapter_config(_LEVERAGE_KIND_PREFIX)
        margin_mode_value = await self._state_store.get_adapter_config(self._margin_mode_key())
        if not leverage_rows and margin_mode_value is None:
            return

        # ---- Account-wide margin mode (applied once, before per-symbol leverage) ----
        if margin_mode_value is not None:
            try:
                mode = MarginMode(margin_mode_value)
                previous = await self.get_margin_mode()
                await self.set_margin_mode(mode)
                self._publish(
                    MarginModeChangedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        previous=previous,
                        current=mode,
                    )
                )
                await self._write_leverage_audit(
                    event_type="bybit.margin_mode.changed",
                    instrument=None,
                    payload={
                        "previous": previous.value if previous else None,
                        "current": mode.value,
                        "context": "connect_reapply",
                    },
                )
            except Exception as exc:
                logger.exception(
                    "Failed to reapply stored margin mode %r on connect: %s",
                    margin_mode_value,
                    exc,
                )

        if not leverage_rows:
            return

        instruments = {to_bybit_symbol(i): i for i in self._instruments.values()}
        leverage_by_symbol = _group_leverage_rows(leverage_rows)

        for symbol, sides in leverage_by_symbol.items():
            if sides["buy"] is None or sides["sell"] is None:
                continue
            buy_leverage = int(sides["buy"])
            sell_leverage = int(sides["sell"])
            instrument = instruments.get(symbol)
            if instrument is None:
                # Not listed / delisted — skip.  The stored intent is left in
                # place so the symbol is re-managed if it is re-listed
                # (Section 9.1); there is no Instrument to emit an event with.
                logger.warning(
                    "Skipping stored leverage reapply for unknown/delisted symbol %s",
                    symbol,
                )
                continue
            try:
                await self.set_leverage(
                    instrument,
                    buy_leverage=buy_leverage,
                    sell_leverage=sell_leverage,
                )
                self._publish(
                    LeverageAppliedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        buy_leverage=buy_leverage,
                        sell_leverage=sell_leverage,
                    )
                )
                await self._write_leverage_audit(
                    event_type="bybit.leverage.applied",
                    instrument=instrument,
                    payload={
                        "buy_leverage": buy_leverage,
                        "sell_leverage": sell_leverage,
                    },
                )
            except Exception as exc:
                self._publish(
                    LeverageApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        buy_leverage=buy_leverage,
                        sell_leverage=sell_leverage,
                        reason=str(exc),
                    )
                )
                await self._write_leverage_audit(
                    event_type="bybit.leverage.apply_failed",
                    instrument=instrument,
                    payload={
                        "buy_leverage": buy_leverage,
                        "sell_leverage": sell_leverage,
                        "reason": str(exc),
                    },
                )

    async def disconnect(self) -> None:
        """Close all connections gracefully.

        Must publish ``ConnectionStateEvent(connected=False)`` on disconnect.
        See Section 17.10, "Connection lifecycle."
        """
        if self._ws is None and not self._connected:
            return
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None
        if self._ws is not None:
            await asyncio.to_thread(self._ws.disconnect)
            self._ws = None
        self._connected = False
        self._order_refs.clear()
        self._publish_connection_state(False)

    @property
    def is_connected(self) -> bool:
        """Return True if connections are currently established."""
        return self._connected

    def _publish_connection_state(self, connected: bool) -> None:
        self._publish(
            ConnectionStateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                connected=connected,
            )
        )

    async def _monitor_connection(self) -> None:
        """Detect platform-initiated drops/reconnects and publish state changes.

        On a reconnect (False -> True) the instrument registry is re-refreshed
        so cached ``InstrumentSpec`` entries invalidated by a mid-session
        platform change (status leaving ``Trading``) are dropped, forcing a
        re-fetch on the next access (Section 17.3).
        """
        while True:
            await asyncio.sleep(_CONNECTION_MONITOR_INTERVAL_SECONDS)
            connected = self._ws is not None and self._ws.is_connected()
            if connected != self._connected:
                was_connected = self._connected
                self._connected = connected
                if connected and not was_connected:
                    try:
                        await self._refresh_instrument_registry()
                    except Exception:
                        logger.exception("Bybit instrument registry refresh after reconnect failed")
                self._publish_connection_state(connected)

    # ---- WebSocket event streams (Section 6.1, Section 17.12) ----

    async def _refresh_instrument_registry(self) -> None:
        """Populate the ``(category, symbol) -> Instrument`` reverse registry.

        Seeded from the platform's instrument list on connect so inbound
        stream messages carrying only a symbol string can be resolved back to
        a canonical ``Instrument`` (Section 6.4).  The registry lives for the
        lifetime of the adapter instance.

        Paginates through all pages using ``nextPageCursor`` so the registry
        is complete — a missing instrument would cause stream messages to be
        silently dropped.

        Also refreshes cached ``InstrumentSpec`` entries: a refreshed listing
        whose ``status`` has left ``Trading`` (halted/delisted) invalidates the
        cached spec so the next ``fetch_instrument_spec`` re-fetches.  This is
        the reconnect-time invalidation trigger (Section 17.3): cached specs on
        initial connect are empty, so the check is a no-op then.
        """
        for category in _ORDER_CATEGORIES:
            cursor: str | None = None
            while True:
                kwargs: dict[str, Any] = {"category": category}
                if cursor:
                    kwargs["cursor"] = cursor
                data, _ = await self._run_request(
                    self._session.get_instruments_info,
                    **kwargs,
                )
                result = data.get("result") or {}
                listings = result.get("list") or []
                for listing in listings:
                    symbol = listing.get("symbol")
                    base = listing.get("baseCoin")
                    quote = listing.get("quoteCoin")
                    if not symbol or not base or not quote:
                        continue
                    try:
                        instrument = from_bybit_symbol(symbol, base, quote, category)
                    except InvalidSymbolError:
                        continue
                    self._instruments[(category, symbol)] = instrument
                    if listing.get("status") != "Trading":
                        self._invalidate_instrument_spec(instrument)
                cursor = result.get("nextPageCursor") or None
                if not cursor:
                    break

    def _resolve_instrument(self, symbol: str, category: str) -> Instrument:
        """Look up the canonical ``Instrument`` for a stream ``(category, symbol)``.

        Raises ``PlatformError`` when the symbol is unknown — an unrecognised
        instrument must never be silently mapped (fail loud, not silent).
        """
        instrument = self._instruments.get((category, symbol))
        if instrument is None:
            raise PlatformError(f"Unknown Bybit instrument {category}:{symbol} in stream update")
        return instrument

    def _publish_from_ws(self, event: Event) -> None:
        """Publish an event from pybit's WS thread by scheduling it on the loop."""
        loop = self._loop
        if loop is None:
            raise PlatformError("Bybit adapter is not connected to an event loop")
        loop.call_soon_threadsafe(self._publish, event)

    def _move_to_final(self, platform_id: str) -> None:
        """Retire an order id from the live set into the bounded LRU.

        Removes the id from ``_open_order_ids`` and records it in
        ``_final_order_ids`` (bounded) so duplicate terminal echoes are
        suppressed without unbounded memory growth.

        Must be called with ``_order_ids_lock`` held.
        """
        self._open_order_ids.discard(platform_id)
        self._final_order_ids[platform_id] = None
        self._final_order_ids.move_to_end(platform_id)
        while len(self._final_order_ids) > _MAX_TRACKED_FINAL_ORDER_IDS:
            self._final_order_ids.popitem(last=False)

    def _on_order_message(self, message: dict[str, Any]) -> None:
        """Translate ``order`` stream entries into reconcile-safe order events.

        Emits ``OrderPlacedEvent`` for a newly-seen order and
        ``OrderCancelledEvent`` for a previously-seen order that reaches a
        terminal cancelled state.  ``OrderModifiedEvent`` is deliberately not
        emitted — the stream carries no ``previous`` state, so core's mirror
        diffs updates instead (Section 6.1).

        Seen-order bookkeeping is bounded: ``_open_order_ids`` holds only live
        (non-final) orders and is pruned as they finalise, while
        ``_final_order_ids`` is a bounded LRU purely to suppress duplicate
        terminal echoes (Bybit can repeat a ``Filled`` and redeliver terminal
        states) so an echo is never misclassified as a brand-new placement.
        """
        for entry in message.get("data") or []:
            try:
                instrument = self._resolve_instrument(
                    entry.get("symbol") or "", entry.get("category") or ""
                )
                order = translate_order_entry(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed Bybit order stream entry: %s", entry)
                continue

            # A rejected order is a symptom that the platform's rules for this
            # instrument differ from the cached spec (Section 17.3) — e.g. a
            # changed tick/lot size or a halted contract.  Invalidating is
            # idempotent, so an instrument whose spec was never cached is a
            # no-op; the next fetch_instrument_spec re-queries fresh rules.
            if order.status == OrderStatus.REJECTED:
                self._invalidate_spec_from_ws(instrument)

            platform_id = order.platform_order_id or ""
            if not platform_id:
                logger.error("Bybit order stream entry has no orderId: %s", entry)
                continue

            with self._order_ids_lock:
                if platform_id in self._final_order_ids:
                    # Duplicate echo of an already-final order — ignore.
                    continue

                if platform_id in self._open_order_ids:
                    # Previously-seen live order.  Emit a cancel only when it now
                    # reaches a terminal cancelled state; a fill is final without
                    # being a cancellation, so it emits no event of its own.
                    if is_terminal_order_status(order.status):
                        self._move_to_final(platform_id)
                        self._publish_from_ws(
                            OrderCancelledEvent(
                                event_id=_new_id(),
                                timestamp=_utcnow(),
                                adapter_name=self.platform_name,
                                account_id=self.account_id,
                                correlation_id=order.client_order_id or None,
                                client_order_id=order.client_order_id,
                                instrument=instrument,
                            )
                        )
                    elif is_final_order_status(order.status):
                        self._move_to_final(platform_id)
                    continue

                # Brand-new order — first sighting.
                self._publish_from_ws(
                    OrderPlacedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=order.client_order_id or None,
                        order=order,
                    )
                )
                if is_final_order_status(order.status):
                    self._move_to_final(platform_id)
                else:
                    self._open_order_ids.add(platform_id)

    def _on_execution_message(self, message: dict[str, Any]) -> None:
        """Translate ``execution`` stream updates into ``FillEvent``.

        Only ``Trade`` executions are emitted — the WebSocket ``execution``
        stream reports real trades but can also carry Funding/AdlTrade/BustTrade
        events for non-trade balance changes.  Filtering here matches
        ``fetch_fills`` so the REST snapshot and the live mirror stay strictly
        comparable.
        """
        for entry in message.get("data") or []:
            if entry.get("execType") != "Trade":
                continue
            try:
                instrument = self._resolve_instrument(
                    entry.get("symbol") or "", entry.get("category") or ""
                )
                client_order_id = entry.get("orderLinkId") or ""
                fill = translate_fill(entry, instrument=instrument, client_order_id=client_order_id)
            except Exception:
                logger.exception("Skipping malformed Bybit execution stream entry: %s", entry)
                continue
            self._publish_from_ws(
                FillEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=client_order_id or None,
                    fill=fill,
                )
            )

    def _on_position_message(self, message: dict[str, Any]) -> None:
        """Translate ``position`` stream updates into ``PositionUpdateEvent``."""
        for entry in message.get("data") or []:
            try:
                instrument = self._resolve_instrument(
                    entry.get("symbol") or "", entry.get("category") or ""
                )
                position = translate_position(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed Bybit position stream entry: %s", entry)
                continue
            self._publish_from_ws(
                PositionUpdateEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    position=position,
                )
            )

    def _on_wallet_message(self, message: dict[str, Any]) -> None:
        """Translate ``wallet`` stream updates into one ``BalanceUpdateEvent`` per coin."""
        try:
            timestamp = _utcnow()
            creation_time = message.get("creationTime")
            if creation_time:
                timestamp = _parse_stream_ms(creation_time)
            members = message.get("data") or []
        except Exception:
            logger.exception("Skipping malformed Bybit wallet stream message: %s", message)
            return
        for member in members:
            for balance in translate_wallet_member(member, timestamp=timestamp):
                self._publish_from_ws(
                    BalanceUpdateEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        balance=balance,
                    )
                )

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to Bybit.

        Receives a ``UnifiedOrder`` that has already passed all risk checks.
        Bybit's place-order ack carries no order state, so the adapter
        re-queries the order to build an accurate ``OrderResult``.
        If Bybit supports native TP/SL attachment, use it; otherwise raise
        ``UnsupportedOrderTypeError`` — never approximate.
        See Section 17.10, "Order operations."
        """
        await self._strict_check_leverage(order.instrument)
        category = self._instrument_to_category(order.instrument)
        symbol = to_bybit_symbol(order.instrument)
        client_order_id = order.client_order_id or _new_id()
        payload = build_place_order_payload(
            order,
            category=category,
            symbol=symbol,
            client_order_id=client_order_id,
        )
        try:
            await self._run_request(self._session.place_order, **payload)
        except UteError:
            # A platform rejection is a symptom that the cached rules for this
            # instrument may differ from reality (Section 17.3) — e.g. a changed
            # tick/lot size or min-notional.  Invalidate so the next
            # fetch_instrument_spec re-queries fresh rules, then re-raise the
            # mapped error to the caller (invalidation is a side-effect, never
            # a swallow).
            self._invalidate_instrument_spec(order.instrument)
            raise
        self._order_refs[client_order_id] = (category, symbol)
        return await self._require_order_result(client_order_id, "placed on Bybit")

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification to Bybit.

        Core runs risk checks against the resulting order before calling.
        See Section 17.10, "Order operations."
        """
        category, symbol = await self._resolve_order_ref(modification.client_order_id)
        payload = build_amend_payload(
            modification,
            category=category,
            symbol=symbol,
        )
        try:
            await self._run_request(self._session.amend_order, **payload)
        except UteError:
            instrument = self._instruments.get((category, symbol))
            if instrument is not None:
                self._invalidate_instrument_spec(instrument)
            raise
        return await self._require_order_result(modification.client_order_id, "amended on Bybit")

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by client_order_id.

        Raises ``OrderNotFoundError`` if Bybit does not know the order.
        See Section 17.10, "Order operations."
        """
        category, symbol = await self._resolve_order_ref(client_order_id)
        payload = build_cancel_payload(
            client_order_id,
            category=category,
            symbol=symbol,
        )
        await self._run_request(self._session.cancel_order, **payload)
        return await self._require_order_result(client_order_id, "cancelled on Bybit")

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by client_order_id. Returns None if not found.
        See Section 17.10, "Order operations."
        """
        found = await self._find_order(client_order_id)
        if found is None:
            return None
        category, symbol, entry = found
        self._order_refs[client_order_id] = (category, symbol)
        return parse_order_result(entry, client_order_id)

    async def _require_order_result(self, client_order_id: str, context: str) -> OrderResult:
        result = await self.get_order_by_client_id(client_order_id)
        if result is None:
            raise OrderNotFoundError(
                f"Order {client_order_id} was {context} but could not be re-queried"
            )
        return result

    async def _run_request(
        self,
        method: Callable[..., Any],
        **kwargs: Any,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Invoke a pybit HTTP method, translating native errors and rate limits."""
        try:
            result = await asyncio.to_thread(method, **kwargs)
        except FailedRequestError as exc:
            raise map_bybit_error(http_status=exc.status_code, ret_msg=exc.message) from exc
        except InvalidRequestError as exc:
            raise map_bybit_error(ret_code=exc.status_code, ret_msg=exc.message) from exc
        data, _, response_headers = result
        self._update_rate_limits(response_headers or {})
        return data, response_headers

    async def _query_order_entry(
        self,
        client_order_id: str,
        category: str,
        *,
        symbol: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the raw Bybit order object for a client order id, or None.

        Queries open/closed orders (realtime) first, then falls back to the
        two-year order history so closed orders survive server restarts.
        """
        query: dict[str, Any] = {"category": category, "orderLinkId": client_order_id}
        if symbol is not None:
            query["symbol"] = symbol

        data = await self._query_realtime(client_order_id, query)
        if data is None:
            data = await self._query_history(client_order_id, query)
        return data

    async def _query_realtime(
        self,
        client_order_id: str,
        query: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            data, _ = await self._run_request(self._session.get_open_orders, **query)
        except OrderNotFoundError:
            return None
        return self._find_entry_in(data, client_order_id)

    async def _query_history(
        self,
        client_order_id: str,
        query: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            data, _ = await self._run_request(self._session.get_order_history, **query)
        except OrderNotFoundError:
            return None
        return self._find_entry_in(data, client_order_id)

    @staticmethod
    def _find_entry_in(data: dict[str, Any], client_order_id: str) -> dict[str, Any] | None:
        entries: list[dict[str, Any]] = (data.get("result") or {}).get("list") or []
        for entry in entries:
            if entry.get("orderLinkId") == client_order_id:
                return entry
        return None

    async def _find_order(
        self,
        client_order_id: str,
    ) -> tuple[str, str, dict[str, Any]] | None:
        """Locate an order's (category, symbol, entry) via the ref cache or a scan."""
        ref = self._order_refs.get(client_order_id)
        if ref is not None:
            entry = await self._query_order_entry(client_order_id, ref[0], symbol=ref[1])
            if entry is not None:
                return ref[0], ref[1], entry
            return None
        for category in _ORDER_CATEGORIES:
            entry = await self._query_order_entry(client_order_id, category)
            if entry is not None:
                return category, entry["symbol"], entry
        return None

    async def _resolve_order_ref(self, client_order_id: str) -> tuple[str, str]:
        found = await self._find_order(client_order_id)
        if found is None:
            raise OrderNotFoundError(f"Order {client_order_id} not found on Bybit")
        category, symbol, _ = found
        self._order_refs[client_order_id] = (category, symbol)
        return category, symbol

    # ---- Instrument metadata ----

    def _invalidate_instrument_spec(self, instrument: Instrument) -> None:
        """Drop a cached ``InstrumentSpec`` so the next access re-fetches.

        Idempotent: ``dict.pop`` never raises for a missing key, so unknown
        instruments simply leave the cache untouched (no thrash for
        genuinely-misspelled symbols).  Event-loop thread only.
        """
        self._instrument_specs.pop(instrument, None)

    def _invalidate_spec_from_ws(self, instrument: Instrument) -> None:
        """Schedule spec invalidation from pybit's WS thread onto the loop.

        Mirrors ``_publish_from_ws``: pybit invokes stream callbacks on its
        background thread, and ``_instrument_specs`` is only ever mutated on
        the event loop.
        """
        loop = self._loop
        if loop is None:
            raise PlatformError("Bybit adapter is not connected to an event loop")
        loop.call_soon_threadsafe(self._invalidate_instrument_spec, instrument)

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch (or return a cached) ``InstrumentSpec`` for ``instrument``.

        Each entry re-fetches after an expiry set by
        ``instrument_spec_cache_ttl`` (defaults to one day) or on adapter-internal
        invalidation.  ``None`` caches indefinitely.  Invalidation is internally
        visible only — core never sees the cache, only the ``InstrumentSpec`` value.
        """
        cached = self._instrument_specs.get(instrument)
        if cached is not None:
            spec, fetched_at = cached
            ttl = self._instrument_spec_cache_ttl
            if ttl is None or time.monotonic() - fetched_at < ttl:
                return spec
            self._instrument_specs.pop(instrument, None)

        bybit_symbol = to_bybit_symbol(instrument)
        category = self._instrument_to_category(instrument)

        data, _ = await self._run_request(
            self._session.get_instruments_info,
            category=category,
            symbol=bybit_symbol,
        )

        listings = (data.get("result", {}) or {}).get("list", [])
        if not listings:
            raise map_bybit_error(
                ret_msg=f"No instrument spec found for {bybit_symbol}",
            )

        entry = listings[0]
        status = entry.get("status", "")
        if status != "Trading":
            raise map_bybit_error(
                ret_msg=f"Instrument {bybit_symbol} is not tradable (status: {status})",
            )

        lot_filter = entry.get("lotSizeFilter", {})
        price_filter = entry.get("priceFilter", {})

        tick_size = Decimal(str(price_filter.get("tickSize", "1")))

        # Only derivatives carry ``leverageFilter``; spot has none, so
        # ``max_leverage`` stays None there.
        leverage_filter = entry.get("leverageFilter") or {}
        max_leverage_raw = leverage_filter.get("maxLeverage")
        max_leverage = Decimal(str(max_leverage_raw)) if max_leverage_raw else None

        if category == "spot":
            lot_size = Decimal(str(lot_filter.get("basePrecision", "1")))
        else:
            lot_size = Decimal(str(lot_filter.get("qtyStep", "1")))

        # Spot uses ``minOrderAmt`` (minimum quote-currency order value, e.g. $5
        # for BTCUSDT spot) — there is no ``minNotionalValue`` field on spot.
        # Linear and inverse both carry ``minNotionalValue``.
        if category == "spot":
            min_notional_raw = lot_filter.get("minOrderAmt", "0")
        else:
            min_notional_raw = lot_filter.get("minNotionalValue", "0")

        raw_min_qty = Decimal(str(lot_filter.get("minOrderQty", "0")))

        # Inverse perpetuals/futures: each contract = $1 USD (Bybit design
        # constant — all 25 inverse symbols share quote=USD, qtyStep=1,
        # contract_size=$1).  ``minNotionalValue`` is expressed in USD, so it
        # equals the minimum contract count directly.  We raise ``min_qty`` to
        # ``minNotionalValue`` so callers never need to reason about the implicit
        # $1/contract conversion — ``min_qty`` already encodes the floor.
        min_qty: Decimal
        if category == "inverse":
            min_notional_dec = Decimal(str(min_notional_raw))
            min_qty = max(raw_min_qty, min_notional_dec)
        else:
            min_qty = raw_min_qty

        spec = InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=min_qty,
            max_qty=Decimal(str(lot_filter.get("maxOrderQty", "0"))),
            min_notional=Decimal(str(min_notional_raw)),
            price_precision=-int(tick_size.as_tuple().exponent),
            qty_precision=-int(lot_size.as_tuple().exponent),
            max_leverage=max_leverage,
        )
        self._instrument_specs[instrument] = (spec, time.monotonic())
        return spec

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        return frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        return self._last_rate_limits

    # ---- Reconciliation data (Section 6.1, Section 6.3) ----

    async def _paged_results(
        self,
        method: Callable[..., Any],
        category: str,
        **extra: Any,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield every entry of a cursor-paginated ``list`` endpoint.

        Iterates the Bybit ``nextPageCursor`` loop so pagination never leaks
        into the fetch methods below.  Termination is guaranteed: the cursor
        only continues from a response, and an absent/empty cursor ends the
        loop (no unbounded growth).

        ``extra`` kwargs (e.g. ``settleCoin``) are forwarded on every page
        request so callers can scope queries without duplicating the loop.
        """
        cursor: str | None = None
        while True:
            kwargs: dict[str, Any] = {"category": category, **extra}
            if cursor:
                kwargs["cursor"] = cursor
            data, _ = await self._run_request(method, **kwargs)
            result = data.get("result") or {}
            for entry in result.get("list") or []:
                yield entry
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break

    async def fetch_positions(self) -> dict[Instrument, Position]:
        """Fetch all Bybit positions across every applicable category, keyed by Instrument.

        Returns rows for both open and flat (size-0) positions — the same
        ``Position`` shape the WebSocket position stream emits — so the REST
        snapshot and the live mirror stay strictly comparable.  An entry that
        cannot be translated (unknown/de-listed symbol) is skipped with a
        logged error rather than aborting the whole snapshot, consistent with
        the WebSocket handlers.

        Category coverage:
        - ``linear``: queried twice — once scoped to ``settleCoin=USDT``
          (USDT-margined perps, e.g. BTCUSDT) and once to ``settleCoin=USDC``
          (USDC-margined perps, e.g. BTCPERP).  Both are required: the V5
          ``/position/list`` endpoint requires either ``symbol`` or
          ``settleCoin`` when ``category=linear``, and omitting ``settleCoin``
          returns an error or empty result.
        - ``inverse``: queried without ``settleCoin`` — the endpoint accepts
          ``category=inverse`` alone and returns all inverse positions.
        - ``spot``: excluded — spot holdings have no position concept on Bybit
          (no entry price, no liquidation price, no PnL tracking).  Spot
          balances are reconciled via ``fetch_balances`` instead.
        """
        result: dict[Instrument, Position] = {}

        # linear — must be split by settleCoin to cover both USDT and USDC perps.
        for settle_coin in ("USDT", "USDC"):
            async for entry in self._paged_results(
                self._session.get_positions, "linear", settleCoin=settle_coin
            ):
                try:
                    instrument = self._resolve_instrument(entry.get("symbol") or "", "linear")
                    position = translate_position(entry, instrument=instrument)
                except Exception:
                    logger.exception("Skipping malformed Bybit linear position entry: %s", entry)
                    continue
                result[position.instrument] = position

        # inverse — category alone is sufficient; no settleCoin required.
        async for entry in self._paged_results(self._session.get_positions, "inverse"):
            try:
                instrument = self._resolve_instrument(entry.get("symbol") or "", "inverse")
                position = translate_position(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed Bybit inverse position entry: %s", entry)
                continue
            result[position.instrument] = position

        return result

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch the account's per-coin balance, keyed by currency.

        Reuses ``translate_wallet_member`` so the REST snapshot and the
        WebSocket wallet stream produce identical ``Balance`` records.  The
        account does not support cursor pagination and returns a single
        per-coin member at ``result.list[0]``.
        """
        data, _ = await self._run_request(
            self._session.get_wallet_balance,
            accountType=_ACCOUNT_TYPE,
        )
        members = (data.get("result") or {}).get("list") or []
        if not members:
            return {}
        result: dict[str, Balance] = {}
        for balance in translate_wallet_member(members[0], timestamp=_utcnow()):
            result[balance.currency] = balance
        return result

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch every open order, keyed by client order id.

        Engine-placed orders always carry ``orderLinkId`` and key normally;
        an orphan order placed outside the engine may lack it, in which case
        the platform order id is used as a stable non-colliding key so it can
        still be reconciled (auto-imported) by core.  An entry with neither id
        is skipped with a log — never silently collapsed onto an empty key.

        Category coverage:
        - ``spot`` and ``inverse``: queried with category alone — both endpoints
          accept no additional scoping parameter.
        - ``linear``: queried twice (``settleCoin=USDT`` then ``settleCoin=USDC``)
          because Bybit's ``get_open_orders`` requires either ``symbol`` or
          ``settleCoin`` for ``category=linear`` and omitting both returns an
          API error (ErrCode 10001).
        """
        result: dict[str, OrderRecord] = {}

        def _collect(entry: dict[str, Any], category: str) -> None:
            try:
                instrument = self._resolve_instrument(entry.get("symbol") or "", category)
                order = translate_order_entry(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed Bybit order entry: %s", entry)
                return
            key = order.client_order_id or order.platform_order_id
            if not key:
                logger.error("Bybit open order entry has no order id: %s", entry)
                return
            result[key] = order

        # spot and inverse — category alone is accepted.
        for category in ("spot", "inverse"):
            async for entry in self._paged_results(self._session.get_open_orders, category):
                _collect(entry, category)

        # linear — must be split by settleCoin (USDT and USDC perps).
        for settle_coin in ("USDT", "USDC"):
            async for entry in self._paged_results(
                self._session.get_open_orders, "linear", settleCoin=settle_coin
            ):
                _collect(entry, "linear")

        return result

    async def fetch_fills(self) -> dict[str, list[FillRecord]]:
        """Fetch recent fills, grouped by client order id.

        Only ``Trade`` executions are returned — the WebSocket ``execution``
        stream reports real trades and excludes funding/adl/bust events, so
        filtering here keeps REST and WS views identical.  Executions without
        an ``orderLinkId`` cannot be attributed in core and are skipped with
        a log.
        """
        result: dict[str, list[FillRecord]] = {}
        for category in _ORDER_CATEGORIES:
            async for entry in self._paged_results(self._session.get_executions, category):
                if entry.get("execType") != "Trade":
                    continue
                client_order_id = entry.get("orderLinkId") or ""
                if not client_order_id:
                    logger.error("Skipping Bybit execution without orderLinkId: %s", entry)
                    continue
                try:
                    instrument = self._resolve_instrument(entry.get("symbol") or "", category)
                    fill = translate_fill(
                        entry, instrument=instrument, client_order_id=client_order_id
                    )
                except Exception:
                    logger.exception("Skipping malformed Bybit execution entry: %s", entry)
                    continue
                result.setdefault(client_order_id, []).append(fill)
        return result

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
from unified_trading_execution.bybit.enums import MarginMode, PositionMode
from unified_trading_execution.bybit.errors import (
    AsymmetricLeverageError,
    LeverageDriftError,
    LeverageExceedsMaxError,
    LeverageNotModifiedError,
    MarginModeNotModifiedError,
    PositionModeNotModifiedError,
    map_bybit_error,
)
from unified_trading_execution.bybit.events import (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    LeverageDriftEvent,
    MarginModeChangedEvent,
    PositionModeAppliedEvent,
    PositionModeApplyFailedEvent,
    PositionModeDriftEvent,
)
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
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
)
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
_POSITION_MODE_KIND_PREFIX = "position_mode."
# Per-instrument leverage behavior knobs, stored flat (one key per knob under
# ``leverage.policy.{knob}:{symbol}``) so strict checks / reconciliation /
# reapply can read just the knob they need without any serialization.
_POLICY_KNOB_ON_DRIFT = "on_drift"
_POLICY_KNOB_STRICT_CHECK = "strict_check"
_POLICY_KNOB_BLOCK_ON_OPEN = "block_on_open"
_POLICY_KNOB_AUTO_APPLY = "auto_apply"
# Position mode is a single value per symbol (one-way / hedge) — unlike
# leverage there is no per-side schema, so no grouping helper is needed.
# ``get_position_mode`` maps ``positionIdx`` directly (0 = one-way, 1/2 =
# hedge), so no reverse map is needed here.
_POSITION_MODE_TO_INT: dict[PositionMode, int] = {
    PositionMode.ONE_WAY: 0,
    PositionMode.HEDGE: 3,
}
# Defaults for leverage behavior.  They live here (and in set_leverage's
# signature) rather than in BybitConfig because they are per-instrument: the
# values chosen at set_leverage time are persisted per symbol and only fall
# back to these module defaults for symbols that were never configured.
DEFAULT_LEVERAGE = 1
DEFAULT_ON_DRIFT: Literal["reapply", "notify", "halt"] = "reapply"
DEFAULT_STRICT_CHECK = True
DEFAULT_BLOCK_ON_OPEN_POSITION = True
DEFAULT_AUTO_APPLY_ON_CONNECT = True


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _coerce_position_mode(mode: str | PositionMode) -> PositionMode:
    """Coerce a user-supplied mode to the ``PositionMode`` enum.

    Accepts the enum member directly or the raw string values ``"one_way"`` /
    ``"hedge"`` so callers need not import the enum.
    """
    if isinstance(mode, PositionMode):
        return mode
    try:
        return PositionMode(mode)
    except ValueError:
        raise ValueError(
            f"Invalid position mode {mode!r} — expected 'one_way' or 'hedge'"
        ) from None


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
    def _leverage_policy_key(instrument: Instrument, knob: str) -> str:
        """adapter_config key for one per-instrument leverage behavior knob."""
        return f"leverage.policy.{knob}:{to_bybit_symbol(instrument)}"

    @staticmethod
    def _position_mode_key(instrument: Instrument) -> str:
        """adapter_config key for stored position mode intent."""
        return f"position_mode.{to_bybit_symbol(instrument)}"

    @staticmethod
    def _position_mode_policy_key(instrument: Instrument, knob: str) -> str:
        """adapter_config key for one per-instrument position mode behavior knob."""
        return f"position_mode.policy.{knob}:{to_bybit_symbol(instrument)}"

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

        The guard reads the instrument's per-symbol ``block_on_open`` knob (set
        at ``set_leverage`` time); unconfigured instruments use the default.  An
        open position blocks leverage changes because they recalculate margin
        immediately and can cause liquidation.  ``action`` names the operation
        in the raised error.
        """
        raw = await self._policy_knob(instrument, _POLICY_KNOB_BLOCK_ON_OPEN)
        enabled = DEFAULT_BLOCK_ON_OPEN_POSITION if raw is None else raw == "1"
        if not enabled:
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
        buy_leverage: int = DEFAULT_LEVERAGE,
        sell_leverage: int | None = None,
        on_drift: Literal["reapply", "notify", "halt"] = DEFAULT_ON_DRIFT,
        strict_check: bool = DEFAULT_STRICT_CHECK,
        block_on_open_position: bool = DEFAULT_BLOCK_ON_OPEN_POSITION,
        auto_apply_on_connect: bool = DEFAULT_AUTO_APPLY_ON_CONNECT,
    ) -> None:
        """Set leverage for *instrument* on the platform and persist the intent.

        ``buy_leverage`` and ``sell_leverage`` are stored and applied
        independently (keys ``leverage.buy:{symbol}`` / ``leverage.sell:{symbol}``)
        so hedge-mode asymmetric leverage can be enabled without a schema
        change.  v1 runs one-way mode, where Bybit requires
        ``buyLeverage == sellLeverage``, so any buy != sell request is rejected
        upfront with ``AsymmetricLeverageError``.  ``sell_leverage`` defaults
        to ``buy_leverage``.

        The behavioral knobs (``on_drift``, ``strict_check``,
        ``block_on_open_position``, ``auto_apply_on_connect``) default inside
        this method and are persisted per instrument as flat
        ``leverage.policy.{knob}:{symbol}`` rows so later strict checks /
        reconciliation resolve each symbol's behavior from its own stored rows.
        Symbols never configured fall back to these defaults, with a leverage
        value of ``DEFAULT_LEVERAGE`` (1).

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
        await self._apply_leverage_values(instrument, buy_leverage, sell_leverage)

        store = await self._require_store()
        await store.set_adapter_config(self._leverage_buy_key(instrument), str(buy_leverage))
        await store.set_adapter_config(self._leverage_sell_key(instrument), str(sell_leverage))
        await store.set_adapter_config(
            self._leverage_policy_key(instrument, _POLICY_KNOB_ON_DRIFT),
            on_drift,
        )
        await store.set_adapter_config(
            self._leverage_policy_key(instrument, _POLICY_KNOB_STRICT_CHECK),
            "1" if strict_check else "0",
        )
        await store.set_adapter_config(
            self._leverage_policy_key(instrument, _POLICY_KNOB_BLOCK_ON_OPEN),
            "1" if block_on_open_position else "0",
        )
        await store.set_adapter_config(
            self._leverage_policy_key(instrument, _POLICY_KNOB_AUTO_APPLY),
            "1" if auto_apply_on_connect else "0",
        )

    async def _apply_leverage_values(
        self,
        instrument: Instrument,
        buy_leverage: int,
        sell_leverage: int,
    ) -> None:
        """Apply leverage values on the platform without touching stored policy.

        Used by ``set_leverage`` and by drift-reapply; does not persist any
        policy knob, so reapply cannot clobber the stored behavior.
        """
        symbol = to_bybit_symbol(instrument)
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

    async def set_position_mode(
        self,
        instrument: Instrument,
        mode: str | PositionMode,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = DEFAULT_ON_DRIFT,
        auto_apply_on_connect: bool = DEFAULT_AUTO_APPLY_ON_CONNECT,
    ) -> None:
        """Set position mode for *instrument* on the platform and persist intent.

        ``mode`` is the ``PositionMode`` enum or the raw strings ``"one_way"``
        (Bybit mode=0) / ``"hedge"`` (Bybit mode=3) — no enum import needed.
        The platform enforces that no open position or open order exists
        before the switch — if that condition is not met, Bybit returns 110030
        or 110031, translated to ``PlatformError``.

        The behavior knobs (``on_drift``, ``auto_apply_on_connect``) are
        persisted per instrument as flat ``position_mode.policy.{knob}:{symbol}``
        rows so later reapply / reconciliation resolve each symbol's behavior
        from its own stored rows.

        Raises:
            InvalidSymbolError: instrument is spot or unsupported category.
            ValueError: ``mode`` is not a valid position mode string.
            PlatformError: platform rejected the switch (110030: open position,
                110031: open orders, or other platform error).
        """
        mode = _coerce_position_mode(mode)
        symbol = to_bybit_symbol(instrument)
        category = self._instrument_to_category(instrument)
        if category == "spot":
            raise InvalidSymbolError(
                f"Position mode is not supported for spot symbol {symbol}"
            )
        await self._apply_position_mode(instrument, mode)

        store = await self._require_store()
        await store.set_adapter_config(self._position_mode_key(instrument), mode.value)
        await store.set_adapter_config(
            self._position_mode_policy_key(instrument, _POLICY_KNOB_ON_DRIFT),
            on_drift,
        )
        await store.set_adapter_config(
            self._position_mode_policy_key(instrument, _POLICY_KNOB_AUTO_APPLY),
            "1" if auto_apply_on_connect else "0",
        )

    async def _apply_position_mode(
        self,
        instrument: Instrument,
        mode: PositionMode,
    ) -> None:
        """Apply position mode on the platform without touching stored intent.

        Used by ``set_position_mode`` and by drift-reapply; does not persist
        anything, so reapply cannot clobber the stored behavior.  A 110025
        "not modified" is treated as already-applied and suppressed.
        """
        symbol = to_bybit_symbol(instrument)
        category = self._instrument_to_category(instrument)
        try:
            await self._run_request(
                self._session.switch_position_mode,
                category=category,
                symbol=symbol,
                mode=_POSITION_MODE_TO_INT[mode],
            )
        except PositionModeNotModifiedError:
            logger.info(
                "Position mode already %s for %s — treating as applied",
                mode.value,
                symbol,
            )

    async def get_position_mode(self, instrument: Instrument) -> PositionMode | None:
        """Query the current position mode from the platform for *instrument*.

        Reads ``positionIdx`` from ``get_positions`` (0 = one-way, 1 = hedge
        long, 2 = hedge short).  Returns None if the instrument has no open
        position (mode unreadable from the platform) or if the instrument is
        spot.
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
        position_idx = int(str(entries[0].get("positionIdx") or "0"))
        if position_idx == 0:
            return PositionMode.ONE_WAY
        if position_idx in (1, 2):
            return PositionMode.HEDGE
        return None

    async def remove_position_mode(self, instrument: Instrument) -> None:
        """Remove stored position mode intent for *instrument*.

        Does NOT change the mode on the platform — only drops the stored
        intent so the engine stops managing it.  Leverage intent is untouched.
        """
        store = await self._require_store()
        await store.delete_adapter_config(self._position_mode_key(instrument))
        await store.delete_adapter_config(
            self._position_mode_policy_key(instrument, _POLICY_KNOB_ON_DRIFT)
        )
        await store.delete_adapter_config(
            self._position_mode_policy_key(instrument, _POLICY_KNOB_AUTO_APPLY)
        )

    async def set_position_mode_for_coin(
        self,
        coin: str,
        category: str,
        mode: str | PositionMode,
    ) -> None:
        """Batch-switch position mode for all symbols of *coin* with no open positions.

        Passes ``coin`` to ``switch_position_mode`` — Bybit applies the switch
        to every symbol of the settle coin that has no open positions or
        orders; newly listed symbols of that coin inherit the mode.

        ``mode`` is the ``PositionMode`` enum or the raw strings ``"one_way"`` /
        ``"hedge"``.

        This does NOT persist per-symbol intent, so drift detection will not
        apply to symbols switched this way.  Call ``set_position_mode`` per
        symbol afterward if you want drift management.

        Raises:
            ValueError: ``mode`` is not a valid position mode string.
            PlatformError: if the batch switch is rejected by Bybit.
        """
        mode = _coerce_position_mode(mode)
        await self._run_request(
            self._session.switch_position_mode,
            category=category,
            coin=coin,
            mode=_POSITION_MODE_TO_INT[mode],
        )

    async def _resolve_position_idx(self, instrument: Instrument, side: OrderSide) -> int | None:
        """Resolve the Bybit ``positionIdx`` for an order on *instrument*.

        The stored position-mode intent is the source of truth (Section 6 /
        PLAN_feat_bybit-position-mode): the mode this adapter put the platform
        in is the mode orders must address.  Under hedge mode the order's
        side picks the leg — Buy opens the long side (1), Sell the short (2);
        under one-way mode every order carries 0.  ``None`` is returned when
        the instrument is spot (no position mode), so no ``positionIdx`` is
        attached at all.
        """
        category = self._instrument_to_category(instrument)
        if category == "spot":
            return None
        if self._state_store is None:
            return 0
        raw = await self._state_store.get_adapter_config(
            self._position_mode_key(instrument)
        )
        try:
            mode = PositionMode(raw) if raw is not None else PositionMode.ONE_WAY
        except ValueError:
            logger.warning(
                "Unrecognised stored position mode %r for %s — defaulting to one-way",
                raw,
                to_bybit_symbol(instrument),
            )
            mode = PositionMode.ONE_WAY
        if mode is PositionMode.ONE_WAY:
            return 0
        # Hedge mode: the leg follows the order side.
        return 1 if side == OrderSide.BUY else 2

    async def set_margin_mode(self, mode: MarginMode) -> None:
        """Set the account-wide margin mode on the platform.

        Margin mode is now a static ``BybitConfig`` value applied on connect —
        this is a low-level platform call used by :meth:`connect` to enforce
        the configured mode.  It is intentionally not a persisted per-symbol
        intent.

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

    def attach_state_store(self, state_store: StateStore) -> None:
        """Receive the engine-managed StateStore so the adapter can persist intent."""
        self._state_store = state_store

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via Engine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

    async def _policy_knob(self, instrument: Instrument, knob: str) -> str | None:
        """Read one persisted behavior knob for *instrument* (None if unset).

        Unconfigured symbols return None so callers fall back to the module
        default for that knob.
        """
        if self._state_store is None:
            return None
        try:
            return await self._state_store.get_adapter_config(
                self._leverage_policy_key(instrument, knob)
            )
        except Exception:
            logger.exception("Failed to read leverage policy for %s", instrument)
            return None

    async def _position_mode_policy_knob(self, instrument: Instrument, knob: str) -> str | None:
        """Read one persisted position-mode behavior knob for *instrument*.

        Unconfigured symbols return None so callers fall back to the module
        default for that knob.
        """
        if self._state_store is None:
            return None
        try:
            return await self._state_store.get_adapter_config(
                self._position_mode_policy_key(instrument, knob)
            )
        except Exception:
            logger.exception("Failed to read position mode policy for %s", instrument)
            return None

    async def _intent_leverage(self, instrument: Instrument) -> tuple[int, int]:
        """Return the effective per-side leverage intent for *instrument*.

        Uses the stored value if present, otherwise the default (1x).
        """
        stored = await self._stored_leverage(instrument)
        if stored is not None:
            return stored
        return (DEFAULT_LEVERAGE, DEFAULT_LEVERAGE)

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
        raw = await self._policy_knob(instrument, _POLICY_KNOB_ON_DRIFT)
        on_drift = raw if raw is not None else DEFAULT_ON_DRIFT

        if on_drift == "reapply":
            await self._apply_leverage_values(instrument, stored[0], stored[1])
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

    async def _handle_position_mode_drift(
        self,
        instrument: Instrument,
        stored: PositionMode,
        platform: PositionMode,
        *,
        context: str,
    ) -> None:
        """Execute the configured ``on_drift`` behavior for a position mode mismatch."""
        raw = await self._position_mode_policy_knob(instrument, _POLICY_KNOB_ON_DRIFT)
        on_drift = raw if raw is not None else DEFAULT_ON_DRIFT

        if on_drift == "reapply":
            await self._apply_position_mode(instrument, stored)
            await self._emit_position_mode_drift_event(instrument, stored, platform, "reapplied")
            return

        if on_drift == "notify":
            await self._emit_position_mode_drift_event(instrument, stored, platform, "notified")
            return

        # on_drift == "halt"
        await self._emit_position_mode_drift_event(instrument, stored, platform, "halted")
        await self._enter_instrument_halt(
            instrument,
            reason="position_mode_drift",
            detail=f"{context}: stored={stored.value} platform={platform.value}",
        )

    async def _emit_position_mode_drift_event(
        self,
        instrument: Instrument,
        stored: PositionMode,
        platform: PositionMode,
        action: Literal["reapplied", "notified", "halted"],
    ) -> None:
        self._publish(
            PositionModeDriftEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                instrument=instrument,
                stored=stored,
                platform=platform,
                action_taken=action,
            )
        )
        await self._write_leverage_audit(
            event_type="bybit.position_mode.drift",
            instrument=instrument,
            payload={
                "stored": stored.value,
                "platform": platform.value,
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

        Runs before every order dispatch.  The per-symbol policy decides whether
        the check runs at all (persisted via ``set_leverage``; unconfigured
        symbols use the default, which enables it).  The effective intent is the
        stored value, or the default (1x) for symbols never configured.  If the
        platform leverage differs, the per-symbol ``on_drift`` behavior is
        executed — reapply restores the intent and the order proceeds;
        notify/halt raise ``LeverageDriftError`` to reject the order.
        """
        strict_raw = await self._policy_knob(instrument, _POLICY_KNOB_STRICT_CHECK)
        if strict_raw is not None and strict_raw == "0":
            return
        if strict_raw is None and not DEFAULT_STRICT_CHECK:
            return
        intent = await self._intent_leverage(instrument)
        platform = await self.get_leverage(instrument)
        if platform is None or platform == intent:
            return
        await self._handle_leverage_drift(
            instrument,
            intent,
            platform,
            context="pre-order strict check",
        )
        on_drift_raw = await self._policy_knob(instrument, _POLICY_KNOB_ON_DRIFT)
        on_drift = on_drift_raw if on_drift_raw is not None else DEFAULT_ON_DRIFT
        if on_drift != "reapply":
            raise LeverageDriftError(
                f"Platform leverage {platform} differs from intent {intent} "
                f"for {to_bybit_symbol(instrument)}"
            )

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored leverage / position-mode intent (§5.3).

        Called by core during ``engine.reconcile()``.  For each instrument with
        a stored intent, the platform's current value is queried and compared;
        a mismatch executes the configured ``on_drift`` behavior exactly as the
        strict check does, minus the per-order rejection (reconcile does not
        raise — it re-applies, notifies, or halts).  When the platform has
        recovered to the stored intent, any residual drift halt is cleared.

        Position mode is only reconcilable while the symbol has an open
        position (``positionIdx`` is unreadable otherwise); symbols without
        one are skipped.  Margin mode is not reconciled — it is a static
        ``BybitConfig`` value applied on connect, not per-symbol intent.
        """
        if self._state_store is None:
            return
        instruments = {to_bybit_symbol(i): i for i in self._instruments.values()}

        # ---- Per-symbol leverage drift ----
        leverage_rows = await self._state_store.list_adapter_config(_LEVERAGE_KIND_PREFIX)
        if leverage_rows:
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

        # ---- Per-symbol position mode drift ----
        position_mode_rows = await self._state_store.list_adapter_config(
            _POSITION_MODE_KIND_PREFIX
        )
        for full_key, value in position_mode_rows.items():
            symbol = full_key.removeprefix(_POSITION_MODE_KIND_PREFIX)
            if "." in symbol:
                continue  # policy knob rows (position_mode.policy.*)
            instrument = instruments.get(symbol)
            if instrument is None:
                continue
            try:
                stored_mode = PositionMode(value)
            except ValueError:
                continue
            platform_mode = await self.get_position_mode(instrument)
            if platform_mode is None:
                continue  # no open position — mode unreadable, skip
            if platform_mode is stored_mode:
                await self._try_clear_recovered_halt(instrument)
                continue
            try:
                await self._handle_position_mode_drift(
                    instrument,
                    stored_mode,
                    platform_mode,
                    context="reconciliation",
                )
            except Exception:
                logger.exception(
                    "Position mode drift handling failed for %s during reconcile",
                    symbol,
                )

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
        await self._apply_configured_margin_mode()
        await self._reapply_stored_intent()

    async def _apply_configured_margin_mode(self) -> None:
        """Apply the ``BybitConfig`` margin mode to the platform at connect.

        Margin mode is a static configuration value, not runtime intent.  When
        the platform already matches the configured mode, this is a no-op.
        Failures are logged but never crash the connection — margin mode does
        not gate order dispatch.
        """
        mode = self._config.margin_mode
        if isinstance(mode, str):
            mode = MarginMode(mode)
        try:
            previous = await self.get_margin_mode()
            await self.set_margin_mode(mode)
        except Exception:
            logger.exception(
                "Failed to apply configured margin mode %s on connect",
                mode.value,
            )
            return
        if previous is mode:
            return
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
                "context": "connect",
            },
        )

    async def _reapply_stored_intent(self) -> None:
        """Reapply stored leverage intent to the platform.

        Runs after a successful connect.  For each instrument with stored
        leverage intent:

        - ``set_leverage`` restores the stored value.
        - a platform rejection emits ``LeverageApplyFailedEvent`` and never
          crashes the connection; successful applies emit
          ``LeverageAppliedEvent``.

        Margin mode is not persisted intent — it is a static ``BybitConfig``
        value applied separately at connect.

        Instruments with no stored intent are not touched.  For each symbol
        with stored leverage/position-mode intent, its per-symbol
        ``auto_apply_on_connect`` policy decides whether it is re-applied
        (default on).
        """
        if self._state_store is None:
            return

        instruments = {to_bybit_symbol(i): i for i in self._instruments.values()}

        # ---- Per-symbol leverage reapply ----
        leverage_rows = await self._state_store.list_adapter_config(_LEVERAGE_KIND_PREFIX)
        if leverage_rows:
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
                auto_apply_raw = await self._policy_knob(instrument, _POLICY_KNOB_AUTO_APPLY)
                if auto_apply_raw is None:
                    auto_apply = DEFAULT_AUTO_APPLY_ON_CONNECT
                else:
                    auto_apply = auto_apply_raw == "1"
                if not auto_apply:
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

        # ---- Per-symbol position mode reapply ----
        position_mode_rows = await self._state_store.list_adapter_config(
            _POSITION_MODE_KIND_PREFIX
        )
        for full_key, value in position_mode_rows.items():
            symbol = full_key.removeprefix(_POSITION_MODE_KIND_PREFIX)
            if "." in symbol:
                continue  # policy knob rows (position_mode.policy.*)
            instrument = instruments.get(symbol)
            if instrument is None:
                logger.warning(
                    "Skipping stored position mode reapply for unknown/delisted symbol %s",
                    symbol,
                )
                continue
            try:
                mode = PositionMode(value)
            except ValueError:
                logger.warning(
                    "Unrecognised stored position mode %r for %s — skipping",
                    value,
                    symbol,
                )
                continue
            auto_apply_raw = position_mode_rows.get(
                f"position_mode.policy.{_POLICY_KNOB_AUTO_APPLY}:{symbol}"
            )
            auto_apply = (
                DEFAULT_AUTO_APPLY_ON_CONNECT
                if auto_apply_raw is None
                else auto_apply_raw == "1"
            )
            if not auto_apply:
                continue
            try:
                await self._apply_position_mode(instrument, mode)
                self._publish(
                    PositionModeAppliedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        mode=mode,
                    )
                )
                await self._write_leverage_audit(
                    event_type="bybit.position_mode.applied",
                    instrument=instrument,
                    payload={"mode": mode.value},
                )
            except Exception as exc:
                self._publish(
                    PositionModeApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        mode=mode,
                        reason=str(exc),
                    )
                )
                await self._write_leverage_audit(
                    event_type="bybit.position_mode.apply_failed",
                    instrument=instrument,
                    payload={"mode": mode.value, "reason": str(exc)},
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
        position_idx = await self._resolve_position_idx(order.instrument, order.side)
        payload = build_place_order_payload(
            order,
            category=category,
            symbol=symbol,
            client_order_id=client_order_id,
            position_idx=position_idx,
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

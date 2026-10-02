"""HyperliquidAdapter — concrete Adapter ABC implementation for Hyperliquid.

Covers Hyperliquid spot and perpetual markets.  One ``Exchange`` instance
per adapter (one account per adapter; the SDK call path is blocking, so
every call goes through ``asyncio.to_thread`` with the configured request
timeout and is never awaited on the loop thread).  Identity is the wallet
address itself — no separate uid-resolve step.  One-way positions only —
no hedge routing.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

import requests
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError
from hyperliquid.utils.types import Cloid
from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidOrderError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderStatusEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.errors import (
    LeverageDriftError,
    LeverageExceedsMaxError,
    map_hyperliquid_error,
)
from unified_trading_execution.hyperliquid.events import (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    LeverageDriftEvent,
    MarginModeApplyFailedEvent,
    MarginModeChangedEvent,
    MarginModeDriftEvent,
)
from unified_trading_execution.hyperliquid.orders import (
    MAX_DECIMALS_PERPS,
    MAX_DECIMALS_SPOT,
    SL_CLOID_SUFFIX,
    TP_CLOID_SUFFIX,
    build_cancel_action,
    build_modify_action,
    build_place_order_action,
    build_position_tpsl_action,
    client_order_id_to_cloid,
    map_order_status,
    max_limit_notional,
    max_market_notional,
    parse_order_result,
    position_tpsl_cloid,
    quantize_price,
    raise_on_status_errors,
    round_price_to_tick,
    validate_size,
)
from unified_trading_execution.hyperliquid.rates import (
    CONNECT_WEIGHT,
    IP_WEIGHT_BUDGET_PER_MINUTE,
    IP_WEIGHT_WINDOW_SECONDS,
    RateBudget,
    describe_call,
    request_weight,
    surcharge_weight,
)
from unified_trading_execution.hyperliquid.signing import (
    assert_user_role_for_signing,
    build_wallet,
)
from unified_trading_execution.hyperliquid.streams import (
    translate_balance,
    translate_fill,
    translate_order_entry,
    translate_position,
    translate_ticker,
)
from unified_trading_execution.hyperliquid.symbols import (
    from_hyperliquid_coin,
    to_hyperliquid_coin,
)
from unified_trading_execution.hyperliquid.websocket import HyperliquidWebSocket
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.enums import (
    AssetClass,
    FillEntry,
    FillReason,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

logger = logging.getLogger(__name__)

_POLICY_KNOB_STRICT_CHECK = "strict_check"
_POLICY_KNOB_BLOCK_ON_OPEN = "block_on_open"
DEFAULT_STRICT_CHECK = True
DEFAULT_BLOCK_ON_OPEN_POSITION = True

#: Seconds between push-channel liveness polls (see ``_monitor_streams``).
_WS_MONITOR_INTERVAL_SECONDS = 15.0
#: Fill ids remembered for stream dedupe (matches the venue recent window).
_WS_SEEN_FILL_IDS = 10000


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


class HyperliquidAdapter(Adapter):
    """Concrete Adapter ABC implementation for Hyperliquid."""

    def __init__(
        self,
        config: HyperliquidConfig,
        *,
        event_bus: EventBus | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._state_store = state_store
        self._halt_machine: HaltStateMachine | None = None
        self._connected = False
        self._exchange: Exchange | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # client_order_id -> (coin, is_spot), populated at place time so
        # modify/cancel can address orders without a venue scan.  Unknown
        # ids fall back to scanning open orders for the cloid.
        self._client_coins: dict[str, tuple[str, bool]] = {}
        # platform oid -> (client_order_id, reason, entry) for fill
        # attribution; child-cloid raw -> (parent id, reason, entry).
        self._oid_clients: dict[str, tuple[str, FillReason | None, FillEntry | None]] = {}
        self._child_parents: dict[str, tuple[str, FillReason, FillEntry]] = {}
        # InstrumentSpec cache with monotonic fetch times (TTL-governed).
        self._instrument_specs: dict[Instrument, tuple[InstrumentSpec, float]] = {}
        self._spec_ttl: float | None = config.instrument_spec_cache_ttl
        # Local IP weight accounting (see ``rates``) — every SDK call flows
        # through ``_run_exchange``; ``connect`` records construction directly.
        self._rate_budget = RateBudget()
        # Push-channel state (see "WebSocket event streams" below).  The
        # socket is owned here but started explicitly via ``start_streams``.
        self._ws: HyperliquidWebSocket | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_pending: set[asyncio.Task[Any]] = set()
        # Fill ids ("<hash>:<tid>", the ``platform_fill_id`` shape) already
        # reported — shared by both fill channels so dual-subscribed fills
        # publish exactly once.  Bounded: matches the venue recent window.
        self._seen_fill_ids: deque[str] = deque(maxlen=_WS_SEEN_FILL_IDS)
        # Push-channel link state.  ``_ws_task`` polls it (see below).
        self._streams_up = False
        # Account-state baselines for the ``clearinghouseState`` / ``spotState``
        # diff (see handlers below).  Keyed by position id / currency; the
        # venue pushes full snapshots on a heartbeat, so publishing requires
        # a last-published value to compare against.
        self._position_baseline: dict[str, Position] = {}
        self._balance_baseline: dict[str, Balance] = {}
        # Channels whose first push has been swallowed as the baseline seed.
        # Seeding happens once per baseline lifetime (fresh adapter or after
        # ``stop_streams`` clears); reconnects keep baselines and diff across
        # the outage instead of re-seeding.
        self._account_seeded: set[str] = set()

    # ---- Identification ----

    @property
    def platform_name(self) -> str:
        return self._config.platform_name

    @property
    def account_id(self) -> str:
        """The wallet address is the canonical account identity."""
        return self._config.wallet_address

    async def resolve_account_id(self) -> str:
        """Return the canonical identity — the wallet address itself."""
        return self._config.wallet_address

    # ---- Adapter-owned wiring ----

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Store core's shared halt state machine so drift can enter halts."""
        self._halt_machine = halt_machine

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so WS handlers can publish."""
        self._event_bus = event_bus

    def attach_state_store(self, state_store: StateStore) -> None:
        """Receive the engine-managed StateStore for per-asset intent."""
        self._state_store = state_store

    # ---- Connection lifecycle ----

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via Engine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

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

    def _require_exchange(self) -> Exchange:
        if self._exchange is None:
            raise PlatformConnectionError("Hyperliquid adapter is not connected")
        return self._exchange

    async def connect(self) -> None:
        """Open the transport and verify signing identity.

        Builds the SDK ``Exchange`` (which fetches ``meta``/``spotMeta``
        over the network at construction, hence off-loop) with the
        configured timeout, then asserts the signing key's ``userRole``
        (``approveAgent`` one-time setup) and the account's
        ``userAbstraction`` (unified only).  Publishes
        ``ConnectionStateEvent(connected=True)`` on success.  WebSocket
        subscriptions attach in a later step; this only opens REST.
        """
        if self._connected:
            return
        self._loop = asyncio.get_running_loop()
        wallet = build_wallet(self._config.private_key)
        base_url = TESTNET_API_URL if self._config.testnet else MAINNET_API_URL
        try:
            exchange = await asyncio.to_thread(
                Exchange,
                wallet,
                base_url,
                None,
                None,
                self._config.wallet_address,
                None,
                None,
                self._config.request_timeout_seconds,
            )
        except ClientError as exc:
            raise PlatformError(f"Hyperliquid transport rejected the connection: {exc}") from exc
        except (ServerError, requests.exceptions.RequestException) as exc:
            raise PlatformConnectionError(f"Hyperliquid connection failed: {exc}") from exc

        try:
            role = await asyncio.to_thread(exchange.info.user_role, wallet.address)
            assert_user_role_for_signing((role or {}).get("role"))
            abstraction = await asyncio.to_thread(
                exchange.info.query_user_abstraction_state, self._config.wallet_address
            )
            if abstraction != "unifiedAccount":
                raise PlatformError(
                    f"Hyperliquid account abstraction {abstraction!r} is not supported — "
                    "unified account only"
                )
            self._rate_budget.record(CONNECT_WEIGHT)
        except (PlatformError, PlatformConnectionError):
            raise
        except ClientError as exc:
            raise PlatformError(f"Hyperliquid identity check rejected: {exc}") from exc
        except (ServerError, requests.exceptions.RequestException) as exc:
            raise PlatformConnectionError(f"Hyperliquid identity check failed: {exc}") from exc

        self._exchange = exchange
        self._connected = True
        await self._reapply_stored_intent()
        self._publish_connection_state(True)

    async def disconnect(self) -> None:
        """Close the transport gracefully.

        Stops push streams first, then drops REST.  Publishes
        ``ConnectionStateEvent(connected=False)``.  Clears the loop handle
        so manager-thread stragglers drop silently instead of scheduling
        onto a possibly closed loop.
        """
        await self.stop_streams()
        self._loop = None
        if self._exchange is None and not self._connected:
            return
        self._exchange = None
        self._connected = False
        self._client_coins.clear()
        self._oid_clients.clear()
        self._child_parents.clear()
        self._publish_connection_state(False)

    @property
    def is_connected(self) -> bool:
        """Return True if the transport is currently established."""
        return self._connected

    @property
    def streams_connected(self) -> bool:
        """Return True if the push channel is currently established."""
        socket = self._ws
        return socket is not None and socket.is_connected()

    # ---- WebSocket event streams ----

    async def start_streams(self) -> None:
        """Open the push channel and subscribe the account streams.

        Subscribes ``userEvents``, ``orderUpdates``, ``userFills``,
        ``clearinghouseState`` and ``spotState``, then seeds the fill
        seen-set from a REST read so pre-subscription history is never
        re-published (REST owns the past; the streams own everything
        after).  Position/balance baselines start empty, so each channel's
        first push only seeds (no connect burst).  Idempotent while the
        socket lives.  Requires a connected exchange and a wired event bus.
        """
        self._require_exchange()
        if self._ws is not None and self._ws.is_connected():
            return
        await self.stop_streams()
        socket = await asyncio.to_thread(self._build_connected_socket)
        self._ws = socket
        try:
            socket.subscribe_user_events(self._on_ws_message)
            socket.subscribe_order_updates(self._on_ws_message)
            socket.subscribe_user_fills(self._on_ws_message)
            socket.subscribe_clearinghouse_state(self._on_ws_message)
            socket.subscribe_spot_state(self._on_ws_message)
        except Exception:
            await asyncio.to_thread(socket.disconnect)
            self._ws = None
            raise
        await self._seed_seen_fills()
        self._streams_up = True
        self._ws_task = asyncio.ensure_future(self._monitor_streams())

    def _build_connected_socket(self) -> HyperliquidWebSocket:
        """Construct and connect one socket (blocking — always called off-loop)."""
        socket = HyperliquidWebSocket(self._config)
        socket.connect()
        return socket

    async def stop_streams(self) -> None:
        """Stop the monitor and tear the socket down (idempotent, never raises).

        The socket teardown is time-bounded: ``unsubscribe`` sends on a
        potentially wedged connection, and an unbounded teardown hangs the
        caller the same way a dropped socket hangs a reader. On timeout the
        reference is dropped and the (daemonized) worker is left to finish —
        teardown completeness is best-effort by contract.
        """
        self._streams_up = False
        # Baselines die with the streams: the next start re-seeds instead of
        # diffing against stale pre-stop truth (which would burst-publish).
        # Reconnects deliberately keep baselines (see _rebuild_streams).
        self._position_baseline = {}
        self._balance_baseline = {}
        self._account_seeded = set()
        task, self._ws_task = self._ws_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Hyperliquid streams monitor failed on stop")
        pending, self._ws_pending = set(self._ws_pending), set()
        for dispatch in pending:
            dispatch.cancel()
        if pending:
            try:
                await asyncio.gather(*pending, return_exceptions=True)
            except Exception:
                logger.exception("Hyperliquid streams dispatch drain failed on stop")
        socket, self._ws = self._ws, None
        if socket is not None:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(socket.disconnect),
                    timeout=self._config.request_timeout_seconds,
                )
            except TimeoutError:
                logger.warning("Hyperliquid streams disconnect timed out — reference dropped")
            except Exception:
                logger.exception("Hyperliquid streams disconnect failed")

    def _on_ws_message(self, message: dict[str, Any]) -> None:
        """Hand a manager-thread message to the event loop (never raises).

        No staleness gate: callbacks execute synchronously on the manager
        thread, so a dead manager cannot deliver — rebuilds and stops only
        ever silence live threads, never resurrect dead ones.
        """
        loop = self._loop
        if loop is None:
            return

        def _schedule() -> None:
            try:
                task = asyncio.ensure_future(self._dispatch_ws_message(message))
            except Exception:
                logger.exception("Hyperliquid WS message dropped before dispatch")
                return
            self._ws_pending.add(task)
            task.add_done_callback(self._ws_task_done)

        try:
            loop.call_soon_threadsafe(_schedule)
        except RuntimeError:
            logger.exception("Hyperliquid WS message dropped — loop closed")

    def _ws_task_done(self, task: asyncio.Task[Any]) -> None:
        """Retire a dispatch task, surfacing failures without warnings."""
        self._ws_pending.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.exception("Hyperliquid WS dispatch failed: %r", exc)

    async def _dispatch_ws_message(self, message: dict[str, Any]) -> None:
        """Route one push message to its channel handler (never raises)."""
        try:
            if not isinstance(message, dict):
                raise PlatformError(f"Unexpected WS message shape {message!r}")
            channel = message.get("channel")
            data = message.get("data")
            if channel == "userFills":
                await self._handle_user_fills_message(data)
            elif channel == "user":
                await self._handle_user_message(data)
            elif channel == "orderUpdates":
                await self._handle_order_updates_message(data)
            elif channel == "clearinghouseState":
                await self._handle_clearinghouse_message(data)
            elif channel == "spotState":
                await self._handle_spot_message(data)
            else:
                logger.warning("Ignoring WS message on unknown channel %r", channel)
        except Exception:
            logger.exception("Hyperliquid WS message handling failed")

    @staticmethod
    def _fill_key(entry: dict[str, Any]) -> str | None:
        """The dedupe key for a fill entry (None when the entry has no id)."""
        raw_hash, raw_tid = entry.get("hash"), entry.get("tid")
        if raw_hash in (None, "") or raw_tid in (None, ""):
            return None
        return f"{raw_hash}:{raw_tid}"

    def _fill_seen(self, entry: dict[str, Any]) -> bool:
        """True when this fill id already reported (records it when new).

        Id-less entries always report False — they still translate (or skip
        loudly) downstream; dedupe needs an id to key on.
        """
        key = self._fill_key(entry)
        if key is None:
            return False
        if key in self._seen_fill_ids:
            return True
        self._seen_fill_ids.append(key)
        return False

    def _publish_fill_record(self, record: FillRecord) -> None:
        """Publish one fill record on the bus."""
        self._publish(
            FillEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                fill=record,
            )
        )

    async def _absorb_fills(self, *, publish_unseen: bool) -> None:
        """Fold a REST fills snapshot into the seen-set.

        With ``publish_unseen=False`` (startup seed) history is only marked —
        REST owns the past.  With True (post-reconnect gap cover) unseen
        fills publish — the live channel missed them.
        """
        for records in (await self.fetch_fills()).values():
            for record in records:
                key = record.platform_fill_id
                if key in self._seen_fill_ids:
                    continue
                self._seen_fill_ids.append(key)
                if publish_unseen:
                    self._publish_fill_record(record)

    async def _seed_seen_fills(self) -> None:
        """Mark current REST history seen so streams never re-publish the past."""
        await self._absorb_fills(publish_unseen=False)

    async def _handle_user_fills_message(self, data: Any) -> None:
        """Publish streaming fills; snapshots only seed the seen-set."""
        if not isinstance(data, dict):
            raise PlatformError(f"Unexpected userFills shape {data!r}")
        self._require_user(data)
        for entry in data.get("fills") or []:
            if not isinstance(entry, dict):
                logger.warning("Skipping malformed userFills entry: %r", entry)
                continue
            if data.get("isSnapshot"):
                self._fill_seen(entry)
                continue
            await self._publish_fill_entry(entry)

    async def _handle_user_message(self, data: Any) -> None:
        """Dispatch a ``user``-channel event (fills publish; rest is logged)."""
        if not isinstance(data, dict):
            raise PlatformError(f"Unexpected user-channel shape {data!r}")
        if "fills" in data:
            for entry in data.get("fills") or []:
                if not isinstance(entry, dict):
                    logger.warning("Skipping malformed user fill entry: %r", entry)
                    continue
                await self._publish_fill_entry(entry)
            return
        if "funding" in data:
            logger.debug("Hyperliquid funding update: %r", data.get("funding"))
            return
        if "liquidation" in data:
            logger.warning("Hyperliquid liquidation update: %r", data.get("liquidation"))
            return
        if "nonUserCancel" in data:
            logger.debug("Hyperliquid non-user cancels: %r", data.get("nonUserCancel"))
            return
        logger.warning("Ignoring unknown user-channel update: %r", sorted(data))

    async def _handle_order_updates_message(self, data: Any) -> None:
        """Publish an ``OrderStatusEvent`` per order update (+ cancel on terminal)."""
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            if not isinstance(entry, dict):
                logger.warning("Skipping malformed order update: %r", entry)
                continue
            try:
                record = await self._translate_order_update(entry)
            except Exception:
                logger.exception("Skipping untranslatable order update: %r", entry)
                continue
            self._publish(
                OrderStatusEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    order=record,
                )
            )
            if record.status is OrderStatus.CANCELLED:
                self._publish(
                    OrderCancelledEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        client_order_id=record.client_order_id,
                        instrument=record.instrument,
                    )
                )

    def _known_client_id(self, raw_cloid: str) -> str | None:
        """Map a venue cloid back to the caller client id, if placed this session.

        Same derivation as ``fetch_open_orders``: non-hex cids hash on the
        wire, so the venue echo never matches. Session-scoped by construction —
        the map only holds cids placed since connect, so cloids from prior
        sessions stay unknown and pass through untouched.
        """
        if not raw_cloid:
            return None
        for client_order_id in list(self._client_coins):
            if raw_cloid == client_order_id_to_cloid(client_order_id):
                return client_order_id
        return None

    async def _translate_order_update(self, entry: dict[str, Any]) -> OrderRecord:
        """Translate one ``WsOrder`` update.

        ``status`` and ``statusTimestamp`` are siblings of the ``order``
        object on the wire, so both are folded in — without the timestamp
        the record's ``updated_at`` would silently fall back to creation.
        The record's client id is restored to the caller id (same rule as
        ``fetch_open_orders``) — consumers match on values, not dict keys.
        """
        payload = entry.get("order")
        if not isinstance(payload, dict):
            raise PlatformError(f"Order update is missing its order object: {entry!r}")
        coin = str(payload.get("coin") or "")
        if not coin:
            raise PlatformError(f"Order update is missing coin: {entry!r}")
        instrument = from_hyperliquid_coin(
            coin, is_spot=self._is_spot_coin(coin), spot_pair_coins=self._reverse_pair_coins()
        )
        merged = dict(payload)
        for key in ("status", "statusTimestamp"):
            if entry.get(key) is not None:
                merged[key] = entry[key]
        record = translate_order_entry(merged, instrument=instrument)
        resolved = self._known_client_id(record.client_order_id)
        if resolved is not None and resolved != record.client_order_id:
            record = dataclasses.replace(record, client_order_id=resolved)
        return record

    async def _publish_fill_entry(self, entry: dict[str, Any]) -> None:
        """Translate one fill and publish it unless already seen (never raises)."""
        try:
            if self._fill_seen(entry):
                return
            coin = str(entry.get("coin") or "")
            if not coin:
                raise PlatformError(f"Fill entry is missing coin: {entry!r}")
            instrument = from_hyperliquid_coin(
                coin, is_spot=self._is_spot_coin(coin), spot_pair_coins=self._reverse_pair_coins()
            )
            oid = str(entry.get("oid") or "")
            attributed = self._oid_clients.get(oid)
            if attributed is not None:
                client_order_id, reason, fill_entry = attributed
            else:
                # Match ``fetch_fills``: an unattributed fill keys by its L1
                # hash.  A constant placeholder would collide across fills.
                client_order_id, reason, fill_entry = (
                    (oid or str(entry.get("hash") or "")),
                    None,
                    None,
                )
            record = translate_fill(
                entry,
                instrument=instrument,
                client_order_id=client_order_id,
                reason=reason,
                fill_entry=fill_entry,
            )
            self._publish_fill_record(record)
        except Exception:
            logger.exception("Skipping untranslatable fill entry: %r", entry)

    async def _handle_clearinghouse_message(self, data: Any) -> None:
        """Diff a ``clearinghouseState`` push into ``PositionUpdateEvent``s (never raises).

        The venue pushes full snapshots on a ~5s heartbeat regardless of
        activity, so only transitions publish: new legs, legs whose quantity
        or entry price moved, and disappearances (a zero-quantity update
        carrying the position id — the core close signal). Mark-driven
        floats (``unrealizedPnl``, ``positionValue``) are heartbeat, never
        news. The first push per baseline lifetime only seeds the baseline.
        """
        try:
            if not isinstance(data, dict):
                raise PlatformError(f"Unexpected clearinghouseState shape {data!r}")
            inner = data.get("clearinghouseState")
            if not isinstance(inner, dict):
                raise PlatformError(f"Unexpected clearinghouseState shape {data!r}")
            timestamp = _utcnow()
            current: dict[str, Position] = {}
            for entry in inner.get("assetPositions") or []:
                if not isinstance(entry, dict):
                    continue
                try:
                    coin = str((entry.get("position") or {}).get("coin") or "")
                    if not coin:
                        continue
                    position = translate_position(
                        entry,
                        instrument=from_hyperliquid_coin(coin, is_spot=False),
                        timestamp=timestamp,
                    )
                except Exception:
                    logger.exception("Skipping malformed position leg: %s", entry)
                    continue
                if position.position_id is None or position.quantity == 0:
                    continue
                current[position.position_id] = position
            if "clearinghouseState" not in self._account_seeded:
                self._position_baseline = current
                self._account_seeded.add("clearinghouseState")
                return
            for position_id, position in current.items():
                baseline = self._position_baseline.get(position_id)
                if (
                    baseline is None
                    or baseline.quantity != position.quantity
                    or baseline.average_entry_price != position.average_entry_price
                ):
                    self._publish(
                        PositionUpdateEvent(
                            event_id=_new_id(),
                            timestamp=_utcnow(),
                            adapter_name=self.platform_name,
                            account_id=self.account_id,
                            correlation_id=None,
                            position=position,
                        )
                    )
                    self._position_baseline[position_id] = position
            for position_id in set(self._position_baseline) - set(current):
                baseline = self._position_baseline.pop(position_id)
                self._publish(
                    PositionUpdateEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        position=dataclasses.replace(
                            baseline, quantity=Decimal("0"), updated_at=timestamp
                        ),
                    )
                )
        except Exception:
            logger.exception("Hyperliquid clearinghouseState handling failed")

    async def _handle_spot_message(self, data: Any) -> None:
        """Diff a ``spotState`` push into ``BalanceUpdateEvent``s (never raises).

        Same heartbeat discipline as positions: new coins and rows whose
        total/free/used moved publish; zero-total dust slots are excluded
        from the baseline and never publish, except a baselined coin
        transitioning to zero publishes its final zero row once (so the
        DB mirror clears) then goes silent. Vanished rows otherwise are
        ignored (token slots persist — a disappearance is not a balance
        event). ``tokenToAvailableAfterMaintenance`` is ignored: ``free``
        already derives as ``total - hold`` in ``translate_balance``.
        """
        try:
            if not isinstance(data, dict):
                raise PlatformError(f"Unexpected spotState shape {data!r}")
            rows = (
                (data.get("spotState") or {}).get("balances")
                if isinstance(data.get("spotState"), dict)
                else None
            )
            if rows is None:
                raise PlatformError(f"Unexpected spotState shape {data!r}")
            timestamp = _utcnow()
            current: dict[str, Balance] = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    currency = str(row.get("coin") or "")
                    if not currency:
                        continue
                    balance = translate_balance(row, currency=currency, timestamp=timestamp)
                    if balance.total == 0:
                        continue
                    current[currency] = balance
                except Exception:
                    logger.exception("Skipping malformed balance row: %s", row)
                    continue
            if "spotState" not in self._account_seeded:
                self._balance_baseline = current
                self._account_seeded.add("spotState")
                return
            for currency, balance in current.items():
                baseline = self._balance_baseline.get(currency)
                if (
                    baseline is None
                    or baseline.total != balance.total
                    or baseline.free != balance.free
                    or baseline.used != balance.used
                ):
                    self._publish(
                        BalanceUpdateEvent(
                            event_id=_new_id(),
                            timestamp=_utcnow(),
                            adapter_name=self.platform_name,
                            account_id=self.account_id,
                            correlation_id=None,
                            balance=balance,
                        )
                    )
                    self._balance_baseline[currency] = balance
            for currency in set(self._balance_baseline) - set(current):
                baseline = self._balance_baseline.pop(currency)
                if baseline.total == 0:
                    continue
                self._publish(
                    BalanceUpdateEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        balance=dataclasses.replace(
                            baseline,
                            total=Decimal("0"),
                            free=Decimal("0"),
                            used=Decimal("0"),
                            updated_at=timestamp,
                        ),
                    )
                )
        except Exception:
            logger.exception("Hyperliquid spotState handling failed")

    def _require_user(self, data: dict[str, Any]) -> None:
        """Drop messages routed to a different user than configured."""
        user = data.get("user")
        if user is not None and str(user).lower() != self._config.wallet_address.lower():
            raise PlatformError(f"WS message for unexpected user {user!r}")

    async def _monitor_streams(self) -> None:
        """Own the push channel: rebuild drops, gap-fill, announce (never raises).

        The SDK manager dies silently on drop (no reconnect, no callback),
        so each tick compares socket liveness against ``_streams_up``.  On a
        drop the state flips and waiters are told; on rebuild the fresh
        socket resubscribes, unseen gap fills publish, and the state flips
        back.  A failed rebuild only logs — the next tick retries.
        """
        while True:
            try:
                await asyncio.sleep(_WS_MONITOR_INTERVAL_SECONDS)
                socket = self._ws
                if socket is None:
                    return
                try:
                    alive = socket.is_connected()
                except Exception:
                    logger.exception("Hyperliquid streams liveness check failed")
                    alive = False
                if alive:
                    if not self._streams_up:
                        self._streams_up = True
                        self._publish_connection_state(True)
                    continue
                if self._streams_up:
                    self._streams_up = False
                    self._publish_connection_state(False)
                await self._rebuild_streams(socket)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Hyperliquid streams monitor tick failed")

    async def _rebuild_streams(self, dead: HyperliquidWebSocket) -> None:
        """Swap a dead socket for a fresh subscribed one and cover the gap.

        Only replaces ``self._ws`` when the dead instance is still current
        (a concurrent ``stop_streams`` wins the race by clearing first).
        Gap fills missed between drop and resubscribe publish when unseen;
        the resubscribe snapshot itself only seeds.
        """
        try:
            replacement = await asyncio.to_thread(self._build_connected_socket)
        except Exception:
            logger.exception("Hyperliquid streams rebuild failed — retrying next tick")
            return
        if self._ws is not dead:
            try:
                await asyncio.to_thread(replacement.disconnect)
            except Exception:
                logger.exception("Hyperliquid replacement socket teardown failed")
            return
        self._ws = replacement
        try:
            await asyncio.to_thread(dead.disconnect)
        except Exception:
            logger.exception("Hyperliquid dead socket teardown failed")
        try:
            replacement.subscribe_user_events(self._on_ws_message)
            replacement.subscribe_order_updates(self._on_ws_message)
            replacement.subscribe_user_fills(self._on_ws_message)
            replacement.subscribe_clearinghouse_state(self._on_ws_message)
            replacement.subscribe_spot_state(self._on_ws_message)
        except Exception:
            # A connected-but-unsubscribed socket would look alive to the
            # monitor and never be rebuilt — tear it down so the next tick
            # retries instead of silently declaring the streams up.
            logger.exception("Hyperliquid resubscribe after reconnect failed")
            try:
                await asyncio.to_thread(replacement.disconnect)
            except Exception:
                logger.exception("Hyperliquid unsubscribed replacement teardown failed")
            return
        try:
            await self._absorb_fills(publish_unseen=True)
        except Exception:
            logger.exception("Hyperliquid gap re-query after reconnect failed")
            return
        self._streams_up = True
        self._publish_connection_state(True)

    # ---- Transport helper ----

    async def _run_exchange(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking SDK call off-loop with unified error mapping.

        Transport failures (HTTP 5xx, timeouts, drops) become
        ``PlatformConnectionError``; client errors (4xx) become
        ``PlatformError`` — except HTTP 429, which becomes
        ``RateLimitError``.  Venue ``{"status": "err"}`` envelopes do NOT
        raise here — the caller extracts and maps them via
        :meth:`_check_action_ok`, since only the caller knows the context.

        Every completed or venue-rejected call records its IP weight
        (base upfront, response surcharge after) — a 5xx is a venue
        response and bills too; only requests that never reached the venue
        (timeouts, drops) record nothing.
        """
        self._require_exchange()
        name, batch_length = describe_call(func, args)
        base = request_weight(name, batch_length=batch_length)
        try:
            result = await asyncio.to_thread(func, *args, **kwargs)
        except ClientError as exc:
            self._rate_budget.record(base)
            if getattr(exc, "status_code", None) == 429:
                raise RateLimitError(f"Hyperliquid rate limit exceeded: {exc}") from exc
            raise PlatformError(f"Hyperliquid request rejected: {exc}") from exc
        except ServerError as exc:
            self._rate_budget.record(base)
            raise PlatformConnectionError(f"Hyperliquid request failed: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise PlatformConnectionError(f"Hyperliquid request failed: {exc}") from exc
        self._rate_budget.record(base + surcharge_weight(name, result))
        return result

    @staticmethod
    def _check_action_ok(response: Any, *, context: str) -> Any:
        """Unwrap an action response or raise its mapped error.

        A top-level ``{"status": "err", "response": "<string>"}`` envelope
        (observed live on cancels and auth failures) maps through the error
        table; anything not shaped like a response raises generic
        ``PlatformError`` with context.
        """
        if not isinstance(response, dict):
            raise PlatformError(f"Unexpected Hyperliquid response {response!r} ({context})")
        if response.get("status") == "err":
            data = response.get("response")
            message = data if isinstance(data, str) else context
            raise map_hyperliquid_error(message=message)
        return response

    @staticmethod
    def _is_spot_coin(coin: str) -> bool:
        """Classify a coin by name encoding (aliases/pairs are spot, bare names perps).

        Biconditional with the SDK's own ``asset >= 10_000`` rule for every
        in-scope coin (verified across listings); used where the coin is
        known but the id is not yet resolved.
        """
        return coin.startswith("@") or "/" in coin

    def _reverse_pair_coins(self) -> dict[str, str]:
        """Pair table for alias decoding, viewed off the SDK index.

        A per-call copy rather than a stored second registry — nothing to
        sync, test, or diverge, since the SDK builds its index once.
        """
        exchange = self._require_exchange()
        return dict(exchange.info.name_to_coin)

    # ---- Order operations ----

    async def _find_open_entry(
        self, client_order_id: str
    ) -> tuple[dict[str, Any], str, bool] | None:
        """Find the newest open entry carrying the cloid, with its coin.

        Timestamp-max over matches so echo duplicates resolve to the live
        leg, never first-match.  Returns ``(entry, coin, is_spot)`` or None.
        """
        want = client_order_id_to_cloid(client_order_id)
        best: dict[str, Any] | None = None
        best_timestamp = -1
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("cloid") or "") != want:
                continue
            coin = str(entry.get("coin") or "")
            if not coin:
                continue
            try:
                timestamp = int(str(entry.get("timestamp") or "0"))
            except ValueError:
                timestamp = 0
            if timestamp >= best_timestamp:
                best, best_timestamp = entry, timestamp
        if best is None:
            return None
        coin = str(best.get("coin") or "")
        return best, coin, self._is_spot_coin(coin)

    async def _resolve_coin(self, client_order_id: str) -> tuple[str, bool]:
        """Resolve the ``(coin, is_spot)`` for a client order id.

        Placement cache first, then the open-orders scan (covers restarts
        and orders placed outside this session), then ``orderStatus``-by-
        cloid as a last resort: the scan is eventually consistent and can
        miss a leg briefly after cancel/replace churn while ``orderStatus``
        stays strongly consistent.  Truly unknown cloids still raise.
        """
        cached = self._client_coins.get(client_order_id)
        if cached is not None:
            return cached
        found = await self._find_open_entry(client_order_id)
        if found is not None:
            _, coin, is_spot = found
            resolved = (coin, is_spot)
            self._client_coins[client_order_id] = resolved
            return resolved
        exchange = self._require_exchange()
        response = await self._run_exchange(
            exchange.info.query_order_by_cloid,
            self._config.wallet_address,
            Cloid(client_order_id_to_cloid(client_order_id)),
        )
        if isinstance(response, dict) and response.get("status") != "unknownOid":
            try:
                coin = str(response["order"]["order"].get("coin") or "")
            except (KeyError, TypeError):
                coin = ""
            if coin:
                resolved = (coin, self._is_spot_coin(coin))
                self._client_coins[client_order_id] = resolved
                return resolved
        raise OrderNotFoundError(f"Order {client_order_id} not found")

    async def _open_order_entries(self) -> list[Any]:
        """Fetch raw open-order entries across both order shapes."""
        exchange = self._require_exchange()
        address = self._config.wallet_address
        basic = await self._run_exchange(exchange.info.open_orders, address)
        frontend = await self._run_exchange(exchange.info.frontend_open_orders, address)
        entries: list[Any] = []
        if isinstance(basic, list):
            entries.extend(basic)
        if isinstance(frontend, list):
            entries.extend(frontend)
        return entries

    async def _current_order_record(self, client_order_id: str) -> OrderRecord:
        """Fetch the live ``OrderRecord`` for modify-merge; missing becomes not-found."""
        found = await self._find_open_entry(client_order_id)
        if found is None:
            raise OrderNotFoundError(f"Order {client_order_id} is not open")
        entry, coin, is_spot = found
        instrument = from_hyperliquid_coin(
            coin, is_spot=is_spot, spot_pair_coins=self._reverse_pair_coins()
        )
        return translate_order_entry(entry, instrument=instrument)

    async def _market_band_price(
        self, coin: str, side: OrderSide, *, is_spot: bool, sz_decimals: int
    ) -> Decimal:
        """Touch-derived aggressive price for MARKET orders (up buys, down sells)."""
        exchange = self._require_exchange()
        book = await self._run_exchange(exchange.info.l2_snapshot, coin)
        try:
            levels = book["levels"]
            touch = levels[1][0] if side == OrderSide.BUY else levels[0][0]
            touch_px = Decimal(str(touch["px"]))
        except (KeyError, IndexError, TypeError) as exc:
            raise map_hyperliquid_error(message="No liquidity available for market order.") from exc
        band = touch_px * (Decimal("1.01") if side == OrderSide.BUY else Decimal("0.99"))
        return round_price_to_tick(
            band,
            sz_decimals,
            is_spot=is_spot,
            direction="up" if side == OrderSide.BUY else "down",
        )

    async def _refresh_oid_index(self) -> None:
        """Rebuild oid→client attribution from open orders carrying our cloids.

        Covers legs whose oids were never acked (``waitingForTrigger``
        children) and sessions restarted after placement.  Position TP/SL
        legs recover too: their cloids derive deterministically from the
        position id, so open positions re-anchor them with no stored state.
        Unknown oids stay unattributed — fetch_fills keys those by raw oid.
        Costs one ``userState`` read plus the open-orders scan.
        """
        position_cloids: dict[str, tuple[str, FillReason | None, FillEntry | None]] = {}
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        for leg in legs or []:
            position = leg.get("position") if isinstance(leg, dict) else None
            if not isinstance(position, dict) or not position.get("coin"):
                continue
            # Same id convention as ``translate_position``: f"{coin}:oneWay".
            position_id = f"{position['coin']}:oneWay"
            for suffix, reason in (
                (TP_CLOID_SUFFIX, FillReason.TAKE_PROFIT),
                (SL_CLOID_SUFFIX, FillReason.STOP_LOSS),
            ):
                raw = position_tpsl_cloid(position_id, suffix)
                position_cloids[raw] = (raw, reason, FillEntry.OUT)
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            raw = str(entry.get("cloid") or "")
            oid = entry.get("oid")
            if not raw or oid is None or oid == "":
                continue
            if raw in self._child_parents:
                parent, reason, entry_side = self._child_parents[raw]
                self._oid_clients[str(oid)] = (parent, reason, entry_side)
                continue
            if raw in position_cloids:
                self._oid_clients[str(oid)] = position_cloids[raw]
                continue
            for client_order_id in list(self._client_coins):
                if raw == client_order_id_to_cloid(client_order_id):
                    self._oid_clients[str(oid)] = (client_order_id, None, None)
                    break

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order.

        Receives a ``UnifiedOrder`` that has already passed all risk checks.
        Sizes/prices validate against live ``szDecimals`` (strict — never
        reshaped); MARKET pricing is touch-derived automatically; notionals
        check against the live max-leverage tier.  Returns the parent
        result; TP/SL legs ride the same action.  ``cloid``-addressed
        end-to-end for idempotent retry.  The coin's ``strict_check`` knob
        (default on) verifies leverage against intent first, rejecting the
        order on unrepaired drift.
        """
        await self._strict_check_leverage(order.instrument)
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(order.instrument)
        is_spot = order.instrument.asset_class == AssetClass.SPOT
        spec = await self.fetch_instrument_spec(order.instrument)
        lot_exponent = spec.lot_size.as_tuple().exponent
        if not isinstance(lot_exponent, int):
            raise PlatformError(f"InstrumentSpec has no usable lot_size for {coin}")
        sz_decimals = -lot_exponent

        client_order_id = order.client_order_id or _new_id()
        validate_size(order.quantity, sz_decimals)
        if order.price is not None:
            quantize_price(order.price, sz_decimals, is_spot=is_spot)
        if order.stop_price is not None:
            quantize_price(order.stop_price, sz_decimals, is_spot=is_spot)
        for attachment in (order.take_profit, order.stop_loss):
            if attachment is None:
                continue
            quantize_price(attachment.trigger_price, sz_decimals, is_spot=is_spot)
            if attachment.limit_price is not None:
                quantize_price(attachment.limit_price, sz_decimals, is_spot=is_spot)

        market_limit_price: Decimal | None = None
        if order.order_type == OrderType.MARKET:
            market_limit_price = await self._market_band_price(
                coin, order.side, is_spot=is_spot, sz_decimals=sz_decimals
            )
            if not is_spot and spec.max_leverage is not None:
                cap = max_market_notional(int(spec.max_leverage))
                if market_limit_price * order.quantity > cap:
                    raise InvalidOrderError(f"Market notional exceeds tier cap {cap} for {coin}")
        elif (
            order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)
            and not is_spot
            and spec.max_leverage is not None
        ):
            assert order.price is not None
            cap = max_limit_notional(int(spec.max_leverage))
            if order.price * order.quantity > cap:
                raise InvalidOrderError(f"Limit notional exceeds tier cap {cap} for {coin}")

        # Record the cid→coin and child-cloid maps BEFORE submitting: the
        # venue's WS push routinely beats the REST ack back, and any push
        # dispatched in between must already resolve (a rejected order leaves
        # a harmless entry — its cloid can never produce pushes).
        self._client_coins[client_order_id] = (coin, is_spot)
        for suffix, reason in (
            (TP_CLOID_SUFFIX, FillReason.TAKE_PROFIT),
            (SL_CLOID_SUFFIX, FillReason.STOP_LOSS),
        ):
            self._child_parents[client_order_id_to_cloid(f"{client_order_id}:{suffix}")] = (
                client_order_id,
                reason,
                FillEntry.OUT,
            )
        requests, grouping = build_place_order_action(
            order,
            coin=coin,
            client_order_id=client_order_id,
            market_limit_price=market_limit_price,
        )
        for request in requests:
            request["cloid"] = Cloid(request["cloid"])
        response = await self._run_exchange(exchange.bulk_orders, requests, grouping=grouping)
        data = self._check_action_ok(response, context=f"placing order {client_order_id}")
        try:
            statuses = data["response"]["data"]["statuses"]
        except (KeyError, TypeError) as exc:
            raise PlatformError(
                f"Unexpected order response shape {data!r} for {client_order_id}"
            ) from exc
        if not isinstance(statuses, list) or not statuses:
            raise PlatformError(f"Empty order statuses for {client_order_id}")
        raise_on_status_errors(statuses)
        parent = parse_order_result(statuses[0], client_order_id, requested_quantity=order.quantity)
        for status in statuses:
            if not isinstance(status, dict):
                continue
            detail = status.get("resting") or status.get("filled") or {}
            oid = detail.get("oid") if isinstance(detail, dict) else None
            if oid is None or oid == "":
                continue
            self._oid_clients[str(oid)] = (client_order_id, None, None)
        return parent

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification, preferring cloid.

        Merges over the live record, submits ``modify_order`` by cloid,
        then re-queries ``orderStatus`` for the authoritative result.
        """
        exchange = self._require_exchange()
        current = await self._current_order_record(modification.client_order_id)
        coin, _ = await self._resolve_coin(modification.client_order_id)
        kwargs = build_modify_action(modification, coin=coin, current=current)
        kwargs["oid"] = Cloid(kwargs["oid"])
        kwargs["cloid"] = Cloid(kwargs["cloid"])
        response = await self._run_exchange(exchange.modify_order, **kwargs)
        self._check_action_ok(response, context=f"amending order {modification.client_order_id}")
        result = await self.get_order_by_client_id(modification.client_order_id)
        if result is None:
            raise OrderNotFoundError(
                f"Order {modification.client_order_id} was amended but could not be re-queried"
            )
        if result.platform_order_id is not None:
            self._oid_clients[result.platform_order_id] = (
                modification.client_order_id,
                None,
                None,
            )
        return result

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order via ``cancelByCloid``.

        Raises ``OrderNotFoundError`` if the venue reports the order was
        never placed.  A cancel that removes the order from the book reads
        back as ``unknownOid`` — reported as CANCELLED, since absence after
        a cancel ack means gone.  Cloids are not venue-unique: a reused
        client id leaves sibling orders working, and the re-query then
        truthfully reports the remainder as OPEN.
        """
        exchange = self._require_exchange()
        coin, _ = await self._resolve_coin(client_order_id)
        cancel_coin, raw_cloid = build_cancel_action(client_order_id, coin=coin)
        response = await self._run_exchange(exchange.cancel_by_cloid, cancel_coin, Cloid(raw_cloid))
        self._check_action_ok(response, context=f"cancelling order {client_order_id}")
        result = await self.get_order_by_client_id(client_order_id)
        if result is None:
            now = _utcnow()
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=None,
                status=OrderStatus.CANCELLED,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=now,
                updated_at=now,
            )
        return result

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by cloid: open scan first, ``orderStatus`` second.

        Open orders are the live truth — ``orderStatus``-by-cloid resolves
        to the *original* leg after a modify (observed live: replacement
        live under a new oid while the query returns the cancelled
        original), so it is consulted only when no open entry carries the
        cloid.  Returns None on ``unknownOid``.  Average fill price is not
        reported by either endpoint and reads back as None — fills carry
        prices via fill events.
        """
        want = client_order_id_to_cloid(client_order_id)
        try:
            record = await self._current_order_record(client_order_id)
        except OrderNotFoundError:
            record = None
        if record is not None:
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=record.platform_order_id,
                status=record.status,
                filled_quantity=record.filled_quantity,
                average_fill_price=record.average_fill_price,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        exchange = self._require_exchange()
        response = await self._run_exchange(
            exchange.info.query_order_by_cloid, self._config.wallet_address, Cloid(want)
        )
        if not isinstance(response, dict):
            raise PlatformError(f"Unexpected orderStatus shape {response!r}")
        if response.get("status") == "unknownOid":
            return None
        try:
            payload = response["order"]
            order_object = payload["order"]
        except (KeyError, TypeError) as exc:
            raise PlatformError(f"Unexpected orderStatus shape {response!r}") from exc
        coin = str(order_object.get("coin") or "")
        if not coin:
            raise PlatformError(f"orderStatus entry is missing coin: {response!r}")
        instrument = from_hyperliquid_coin(
            coin, is_spot=self._is_spot_coin(coin), spot_pair_coins=self._reverse_pair_coins()
        )
        record = translate_order_entry(
            {
                **order_object,
                "status": payload.get("status"),
                "statusTimestamp": payload.get("statusTimestamp"),
            },
            instrument=instrument,
        )
        return OrderResult(
            client_order_id=client_order_id,
            platform_order_id=record.platform_order_id,
            status=record.status,
            filled_quantity=record.filled_quantity,
            average_fill_price=record.average_fill_price,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    # ---- Instrument metadata ----

    def _cached_spec(self, instrument: Instrument) -> InstrumentSpec | None:
        """Return the cached spec when fresh, else None (TTL-governed)."""
        cached = self._instrument_specs.get(instrument)
        if cached is None:
            return None
        spec, fetched_at = cached
        ttl = self._spec_ttl
        if ttl is None or time.monotonic() - fetched_at < ttl:
            return spec
        self._instrument_specs.pop(instrument, None)
        return None

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch (or return a cached) ``InstrumentSpec`` for ``instrument``.

        Perps read ``meta.universe`` (``szDecimals``/``maxLeverage``);
        spot reads ``spotMeta`` (base-token ``szDecimals``).  Tick is
        ``10^-(MAX_DECIMALS - szDecimals)`` with the 5-sig-fig rule enforced
        at placement, not here; lot is ``10^-szDecimals``; minimums are the
        venue $10 floors; maximum quantity is the tier-implied non-binding
        upper bound (real caps enforce at placement); delisted entries
        raise ``InvalidSymbolError``.
        """
        cached = self._cached_spec(instrument)
        if cached is not None:
            return cached
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        is_spot = instrument.asset_class == AssetClass.SPOT
        if not is_spot:
            meta = await self._run_exchange(exchange.info.meta)
            entry = next(
                (
                    e
                    for e in meta.get("universe") or []
                    if isinstance(e, dict) and e.get("name") == coin
                ),
                None,
            )
            if entry is None or entry.get("isDelisted"):
                raise InvalidSymbolError(f"Instrument {coin!r} is not tradable on Hyperliquid")
            sz_decimals = int(entry.get("szDecimals", 0))
            max_leverage = entry.get("maxLeverage")
            decimals_cap = MAX_DECIMALS_PERPS
        else:
            spot_meta = await self._run_exchange(exchange.info.spot_meta)
            alias = exchange.info.name_to_coin.get(coin, coin)
            entry = next(
                (
                    e
                    for e in spot_meta.get("universe") or []
                    if isinstance(e, dict) and e.get("name") == alias
                ),
                None,
            )
            if entry is None:
                raise InvalidSymbolError(f"Instrument {coin!r} is not tradable on Hyperliquid")
            token_ids = entry.get("tokens") or []
            token_rows = {
                t.get("index"): t for t in spot_meta.get("tokens") or [] if isinstance(t, dict)
            }
            base_row = token_rows.get(token_ids[0]) if len(token_ids) == 2 else None
            if base_row is None or base_row.get("szDecimals") is None:
                raise InvalidSymbolError(f"Spot pair {coin!r} has no usable token metadata")
            sz_decimals = int(base_row["szDecimals"])
            max_leverage = None
            decimals_cap = MAX_DECIMALS_SPOT
        tick_size = Decimal(1).scaleb(-(decimals_cap - sz_decimals))
        lot_size = Decimal(1).scaleb(-sz_decimals)
        tier = max_limit_notional(int(max_leverage) if max_leverage is not None else 1)
        spec = InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=lot_size,
            max_qty=tier / tick_size,
            min_notional=Decimal("10"),
            price_precision=decimals_cap - sz_decimals,
            qty_precision=sz_decimals,
            max_leverage=Decimal(str(max_leverage)) if max_leverage is not None else None,
        )
        self._instrument_specs[instrument] = (spec, time.monotonic())
        return spec

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the supported order types — all four guaranteed types."""
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
        """Return the live IP weight-budget state (locally tracked, no venue call).

        WebSocket and address-action budgets are separate limiter regimes
        this does not cover — see ``rates``.
        """
        now = _utcnow()
        return RateLimits(
            requests_per_interval=IP_WEIGHT_BUDGET_PER_MINUTE,
            interval_seconds=IP_WEIGHT_WINDOW_SECONDS,
            remaining=self._rate_budget.remaining(),
            reset_at=now + timedelta(seconds=self._rate_budget.resets_in()),
        )

    # ---- Market data ----

    async def fetch_ticker(self, instrument: Instrument) -> Ticker | None:
        """Snapshot best bid/ask via ``l2Book`` plus mark from asset ctx.

        Returns None when the book is empty (no live quote); a malformed
        book raises.  ``last`` is not reported by these endpoints and stays
        None — mid derives via ``Ticker.mid``.
        """
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        is_spot = instrument.asset_class == AssetClass.SPOT
        book = await self._run_exchange(exchange.info.l2_snapshot, coin)
        try:
            levels = book["levels"]
            bids, asks = levels[0], levels[1]
            if not bids or not asks:
                return None
            best_bid = str(bids[0]["px"])
            best_ask = str(asks[0]["px"])
        except (KeyError, IndexError, TypeError) as exc:
            raise PlatformError(f"Unexpected l2Book shape for {coin}") from exc
        mark: str | None = None
        if not is_spot:
            meta_ctx = await self._run_exchange(exchange.info.meta_and_asset_ctxs)
            try:
                universe, ctxs = meta_ctx[0]["universe"], meta_ctx[1]
                index = next(
                    i
                    for i, e in enumerate(universe)
                    if isinstance(e, dict) and e.get("name") == coin
                )
                mark = str(ctxs[index].get("markPx"))
            except (StopIteration, KeyError, IndexError, TypeError) as exc:
                raise PlatformError(f"Unexpected asset ctx shape for {coin}") from exc
        else:
            # Spot ctxs key by the venue alias (``@107``), not the pair
            # spelling the canonical instrument carries.
            alias = exchange.info.name_to_coin.get(coin, coin)
            spot_ctx = await self._run_exchange(exchange.info.spot_meta_and_asset_ctxs)
            try:
                ctxs = spot_ctx[1]
                ctx = next(c for c in ctxs if isinstance(c, dict) and c.get("coin") == alias)
                mark = str(ctx.get("markPx"))
            except (StopIteration, KeyError, IndexError, TypeError) as exc:
                raise PlatformError(f"Unexpected spot ctx shape for {coin}") from exc
        return translate_ticker(None, best_bid=best_bid, best_ask=best_ask, mark=mark)

    # ---- Position TP/SL modification ----

    async def _open_leg(self, instrument: Instrument, position_id: str) -> Position | None:
        """Return the open leg for ``position_id`` on ``instrument``'s coin, else None."""
        coin = to_hyperliquid_coin(instrument)
        for position in await self.fetch_positions():
            if position.position_id == position_id and position.instrument.symbol == coin:
                return position
        return None

    async def _position_tpsl_entries(self, position_id: str) -> dict[str, list[dict[str, Any]]]:
        """Open entries carrying our position cloids, keyed ``take_profit``/``stop_loss``.

        Lists, not single entries: the venue allows duplicate cloids, so a
        cloid can address several live legs (prior replaces, cross-run
        reuse).  Callers cancel every oid — cancelling one leaves siblings.
        """
        want = {
            position_tpsl_cloid(position_id, TP_CLOID_SUFFIX): "take_profit",
            position_tpsl_cloid(position_id, SL_CLOID_SUFFIX): "stop_loss",
        }
        found: dict[str, list[dict[str, Any]]] = {}
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            side = want.get(str(entry.get("cloid") or ""))
            if side is not None:
                found.setdefault(side, []).append(entry)
        return found

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Attach or replace TP/SL on an open position via ``positionTpsl``.

        Merge semantics: only mentioned sides are touched, unmentioned legs
        stay working.  Legs are full-size at attach time (fixed-size — the
        venue does not auto-resize API-placed legs, verified live), so
        re-attach after resizing the leg.  Detach one side by cancelling its
        leg (visible in ``fetch_open_orders`` keyed by cloid); both None
        raises ``ValueError`` per the ABC contract.  No open leg reads back
        as ``OrderNotFoundError``; spot raises ``InvalidSymbolError``.
        """
        if take_profit is None and stop_loss is None:
            raise ValueError("at least one of take_profit or stop_loss must be provided")
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no position TP/SL")
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        leg = await self._open_leg(instrument, position_id)
        if leg is None or leg.quantity == 0:
            raise OrderNotFoundError(f"No open position {position_id!r} for {coin}")
        spec = await self.fetch_instrument_spec(instrument)
        lot_exponent = spec.lot_size.as_tuple().exponent
        if not isinstance(lot_exponent, int):
            raise PlatformError(f"InstrumentSpec has no usable lot_size for {coin}")
        sz_decimals = -lot_exponent
        for attachment in (take_profit, stop_loss):
            if attachment is None:
                continue
            quantize_price(attachment.trigger_price, sz_decimals, is_spot=False)
            if attachment.limit_price is not None:
                quantize_price(attachment.limit_price, sz_decimals, is_spot=False)
        existing = await self._position_tpsl_entries(position_id)
        for side, attachment in (("take_profit", take_profit), ("stop_loss", stop_loss)):
            if attachment is None:
                continue
            cancelled: list[str] = []
            for entry in existing.get(side, []):
                oid = entry.get("oid")
                if oid is None or oid == "":
                    continue
                try:
                    response = await self._run_exchange(exchange.cancel, coin, int(oid))
                    self._check_action_ok(response, context=f"replacing position {side} for {coin}")
                except OrderNotFoundError:
                    continue  # leg vanished concurrently — the replace below still applies
                cancelled.append(str(oid))
            # Serialize on disappearance: the replacement reuses the same
            # cloid, and cancel_by_cloid only retires one leg per call, so
            # every cancelled oid must be gone before placing or siblings
            # survive under the shared cloid (observed live).  Bounded wait;
            # the replace proceeds regardless so a stuck listing can't wedge
            # the modify.
            for _ in range(5):
                if not cancelled:
                    break
                live = await self._open_order_entries()
                live_oids = {str(e.get("oid") or "") for e in live if isinstance(e, dict)}
                if not any(oid in live_oids for oid in cancelled):
                    break
                await asyncio.sleep(2)
        requests, grouping = build_position_tpsl_action(
            coin=coin,
            position_id=position_id,
            close_buy=leg.quantity < 0,
            quantity=abs(leg.quantity),
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        for request in requests:
            request["cloid"] = Cloid(request["cloid"])
        response = await self._run_exchange(exchange.bulk_orders, requests, grouping=grouping)
        data = self._check_action_ok(response, context=f"setting position TP/SL for {coin}")
        try:
            statuses = data["response"]["data"]["statuses"]
        except (KeyError, TypeError) as exc:
            raise PlatformError(
                f"Unexpected order response shape {data!r} for position {position_id}"
            ) from exc
        if not isinstance(statuses, list) or not statuses:
            raise PlatformError(f"Empty order statuses for position {position_id}")
        raise_on_status_errors(statuses)
        # Attribute each acked oid by the cloid it was actually sent with, not a
        # fixed side order: a merge that replaces only one side sends one request
        # and gets one status, so zipping a (TP, SL) tuple would tag an SL leg as
        # TAKE_PROFIT (and key its fills under the TP cloid).
        reason_for_cloid = {
            position_tpsl_cloid(position_id, TP_CLOID_SUFFIX): FillReason.TAKE_PROFIT,
            position_tpsl_cloid(position_id, SL_CLOID_SUFFIX): FillReason.STOP_LOSS,
        }
        for request, status in zip(requests, statuses, strict=False):
            if not isinstance(status, dict):
                continue
            raw = request["cloid"].to_raw()
            reason = reason_for_cloid.get(raw)
            if reason is None:
                continue
            detail = status.get("resting") or status.get("filled") or {}
            oid = detail.get("oid") if isinstance(detail, dict) else None
            if oid is None or oid == "":
                continue
            self._oid_clients[str(oid)] = (raw, reason, FillEntry.OUT)
        # ``positionTpsl`` acks usually carry no oid (bare
        # ``waitingForTrigger`` strings), so re-read the rested legs and
        # index those too — otherwise their fills key by raw oid with no
        # reason until a fills refresh happens to rebuild them.
        placed = {request["cloid"].to_raw() for request in requests}
        wanted = [
            ("take_profit", TP_CLOID_SUFFIX, FillReason.TAKE_PROFIT),
            ("stop_loss", SL_CLOID_SUFFIX, FillReason.STOP_LOSS),
        ]
        if any(position_tpsl_cloid(position_id, suffix) in placed for _, suffix, _ in wanted):
            entries = await self._position_tpsl_entries(position_id)
            for side, suffix, reason in wanted:
                raw = position_tpsl_cloid(position_id, suffix)
                if raw not in placed:
                    continue
                for entry in entries.get(side, []):
                    oid = entry.get("oid")
                    if oid is None or oid == "":
                        continue
                    self._oid_clients[str(oid)] = (raw, reason, FillEntry.OUT)

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an open position via ``orderStatus``-by-cloid.

        Returns ``(take_profit, stop_loss)`` with None per missing side, or
        None when no leg is open at ``position_id``.  Reads are authoritative
        per-leg queries (not the open-orders scan), so terminal legs read
        back as missing rather than stale.
        """
        if instrument.asset_class == AssetClass.SPOT:
            return None
        if await self._open_leg(instrument, position_id) is None:
            return None
        exchange = self._require_exchange()
        found: dict[str, TpSlAttachment] = {}
        for suffix in (TP_CLOID_SUFFIX, SL_CLOID_SUFFIX):
            raw = position_tpsl_cloid(position_id, suffix)
            response = await self._run_exchange(
                exchange.info.query_order_by_cloid, self._config.wallet_address, Cloid(raw)
            )
            if not isinstance(response, dict):
                raise PlatformError(f"Unexpected orderStatus shape {response!r}")
            if response.get("status") == "unknownOid":
                continue
            try:
                payload = response["order"]
                order_object = payload["order"]
                status = payload.get("status")
            except (KeyError, TypeError) as exc:
                raise PlatformError(f"Unexpected orderStatus shape {response!r}") from exc
            if not isinstance(status, str) or map_order_status(status) != OrderStatus.OPEN:
                # Only a working leg can be a live stop.  Every terminal status
                # must read back as missing — not just filled/canceled but the
                # whole cancel family (a TP/SL is OCO, so when one side fills
                # the other reports ``siblingFilledCanceled``) and every
                # ``*Rejected`` variant.  Reporting a dead leg as live would
                # tell the caller a position is protected when it is not.
                continue
            try:
                order_type = order_object.get("orderType")
                if isinstance(order_type, dict):
                    # Open-order entry shape: trigger descriptor is nested.
                    trigger = order_type.get("trigger") or {}
                    trigger_raw = trigger.get("triggerPx", order_object.get("triggerPx"))
                    is_market = bool(trigger.get("isMarket", True))
                else:
                    # orderStatus shape: flat fields, market-ness in the display
                    # string.  "Limit" is the discriminator, matching
                    # ``_translate_order_type``: a bare "Stop"/"Take Profit" is
                    # market-on-trigger (its ``limitPx`` carries the trigger).
                    trigger_raw = order_object.get("triggerPx")
                    is_market = not str(order_type or "").endswith("Limit")
                trigger_price = Decimal(str(trigger_raw))
                limit_price = None if is_market else Decimal(str(order_object.get("limitPx")))
            except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
                raise PlatformError(f"orderStatus leg is not a trigger: {response!r}") from exc
            side = "take_profit" if suffix == TP_CLOID_SUFFIX else "stop_loss"
            found[side] = TpSlAttachment(trigger_price=trigger_price, limit_price=limit_price)
        return found.get("take_profit"), found.get("stop_loss")

    # ---- Reconciliation data ----

    @staticmethod
    def _state_time(state: Any, field: str = "time") -> datetime:
        """Decode a state millisecond timestamp, defaulting to now."""
        try:
            ms = int(str(state.get(field)))
        except (AttributeError, TypeError, ValueError):
            return _utcnow()
        seconds, millis = divmod(ms, 1000)
        return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)

    async def fetch_positions(self) -> list[Position]:
        """Fetch open legs from ``clearinghouseState`` (spot has no legs).

        Flat (zero-size) legs are skipped; malformed legs are logged and
        skipped, never aborting the snapshot.
        """
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        timestamp = self._state_time(state)
        result: list[Position] = []
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        for leg in legs or []:
            try:
                coin = str((leg.get("position") or {}).get("coin") or "")
                instrument = from_hyperliquid_coin(coin, is_spot=False)
                position = translate_position(leg, instrument=instrument, timestamp=timestamp)
            except Exception:
                logger.exception("Skipping malformed position leg: %s", leg)
                continue
            if position.quantity != 0:
                result.append(position)
        return result

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch per-currency balances from ``spotClearinghouseState``.

        Zero-total dust slots (the venue returns every token slot for the
        address) are excluded — only coins with a non-zero total seed the
        DB mirror on ``Engine.connect`` and reconcile. ``free`` derives as
        ``total - hold`` (see ``translate_balance``).
        """
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.spot_user_state, self._config.wallet_address)
        timestamp = _utcnow()
        result: dict[str, Balance] = {}
        rows = state.get("balances") if isinstance(state, dict) else None
        for row in rows or []:
            try:
                currency = str(row.get("coin") or "")
                if not currency:
                    continue
                balance = translate_balance(row, currency=currency, timestamp=timestamp)
                if balance.total == 0:
                    continue
                result[currency] = balance
            except Exception:
                logger.exception("Skipping malformed balance row: %s", row)
                continue
        return result

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch every open order, keyed by client order id.

        Derived TP/SL child legs are attachments of their parent in the
        unified model, not orders of their own, so they are excluded — the
        parent is reported, keyed by client id.  Venue-created legs we never
        minted a cloid for key by platform oid; entries with neither id are
        skipped, never collapsed onto an empty key.
        """
        result: dict[str, OrderRecord] = {}
        cloid_to_client: dict[str, str] = {}
        child_cloids: set[str] = set()
        for client_order_id in list(self._client_coins):
            cloid_to_client[client_order_id_to_cloid(client_order_id)] = client_order_id
            child_cloids.update(
                client_order_id_to_cloid(f"{client_order_id}:{suffix}")
                for suffix in (TP_CLOID_SUFFIX, SL_CLOID_SUFFIX)
            )
        child_cloids.update(self._child_parents)
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("cloid") or "") in child_cloids:
                continue
            try:
                coin = str(entry.get("coin") or "")
                instrument = from_hyperliquid_coin(
                    coin,
                    is_spot=self._is_spot_coin(coin),
                    spot_pair_coins=self._reverse_pair_coins(),
                )
                order = translate_order_entry(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed open order entry: %s", entry)
                continue
            resolved = cloid_to_client.get(order.client_order_id)
            key: str | None = resolved or order.client_order_id
            if not key:
                key = order.platform_order_id
            if not key:
                logger.error("Open order entry has no order id: %s", entry)
                continue
            if resolved is not None and resolved != order.client_order_id:
                # The venue echoes our hashed cloid, not the original id —
                # restore it so the record agrees with its key (consumers
                # read values, not keys; a hex cloid there breaks orphan
                # matching against caller-held client ids).
                order = dataclasses.replace(order, client_order_id=resolved)
            result[key] = order
        return result

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills, grouped by client order id.

        Without ``since`` reads the recent window (``userFills``, ≤2000);
        with ``since`` reads ``userFillsByTime`` from that bound (server
        filters; a client-side guard holds the boundary).  Entries dedupe
        on ``(hash, tid)``; oid-attributed fills key by client id (TP/SL
        children by parent with their reason), unknown oids key by raw oid.
        Costs one open-orders scan (both shapes) plus one ``userState`` read
        for oid attribution, plus the fills call itself.
        """
        await self._refresh_oid_index()
        exchange = self._require_exchange()
        if since is not None:
            if since.tzinfo is None:
                raise ValueError("since must be timezone-aware (UTC)")
            entries = await self._run_exchange(
                exchange.info.user_fills_by_time,
                self._config.wallet_address,
                int(since.timestamp() * 1000),
            )
        else:
            entries = await self._run_exchange(
                exchange.info.user_fills, self._config.wallet_address
            )
        if not isinstance(entries, list):
            raise PlatformError(f"Unexpected fills shape {entries!r}")
        result: dict[str, list[FillRecord]] = {}
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            raw_hash, raw_tid = entry.get("hash"), entry.get("tid")
            if raw_hash in (None, "") or raw_tid in (None, ""):
                continue
            key = (str(raw_hash), str(raw_tid))
            if key in seen:
                continue
            seen.add(key)
            oid = str(entry.get("oid") or "")
            attributed = self._oid_clients.get(oid)
            if attributed is not None:
                client_order_id, reason, entry_side = attributed
            else:
                client_order_id, reason, entry_side = (oid or key[0]), None, None
            try:
                coin = str(entry.get("coin") or "")
                instrument = from_hyperliquid_coin(
                    coin,
                    is_spot=self._is_spot_coin(coin),
                    spot_pair_coins=self._reverse_pair_coins(),
                )
                fill = translate_fill(
                    entry,
                    instrument=instrument,
                    client_order_id=client_order_id,
                    reason=reason,
                    fill_entry=entry_side,
                )
            except Exception:
                logger.exception("Skipping malformed fill entry: %s", entry)
                continue
            if since is not None and fill.fill_timestamp < since:
                continue
            result.setdefault(client_order_id, []).append(fill)
        return result

    # ---- Leverage + margin intent ----

    async def _require_store(self) -> StateStore:
        if self._state_store is None:
            raise PlatformError(
                "HyperliquidAdapter was constructed without a state_store — "
                "leverage/margin intent persistence is unavailable"
            )
        return self._state_store

    async def _venue_leverage_map(self) -> dict[str, tuple[int, bool]]:
        """Read live ``{coin: (leverage, is_cross)}`` for every open leg in one fetch."""
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        result: dict[str, tuple[int, bool]] = {}
        for leg in legs or []:
            position = leg.get("position") if isinstance(leg, dict) else None
            if not isinstance(position, dict):
                continue
            coin = position.get("coin")
            if not coin:
                continue
            leverage = position.get("leverage") or {}
            try:
                result[str(coin)] = int(leverage.get("value", 1)), leverage.get("type") == "cross"
            except (TypeError, ValueError):
                continue
        return result

    async def _venue_leverage(self, coin: str) -> tuple[int, bool] | None:
        """Read live ``(leverage, is_cross)`` for a coin; None with no open leg."""
        return (await self._venue_leverage_map()).get(coin)

    async def _tier_max_leverage(self, coin: str) -> int:
        """Effective max leverage: universe max capped by the zero-position tier."""
        exchange = self._require_exchange()
        meta = await self._run_exchange(exchange.info.meta)
        universe = meta.get("universe") if isinstance(meta, dict) else None
        entry = next(
            (e for e in universe or [] if isinstance(e, dict) and e.get("name") == coin),
            None,
        )
        if entry is None:
            raise InvalidSymbolError(f"Unknown coin {coin!r} on Hyperliquid")
        try:
            universe_max = int(entry.get("maxLeverage") or 1)
        except (TypeError, ValueError):
            universe_max = 1
        tables = meta.get("marginTables") if isinstance(meta, dict) else None
        if isinstance(tables, list):
            for table in tables:
                if (
                    not isinstance(table, list)
                    or len(table) != 2
                    or table[0] != entry.get("marginTableId")
                ):
                    continue
                tiers = table[1].get("marginTiers") if isinstance(table[1], dict) else None
                for tier in tiers or []:
                    if not isinstance(tier, dict):
                        continue
                    try:
                        if Decimal(str(tier.get("lowerBound", "0"))) != 0:
                            continue
                        return min(universe_max, int(tier.get("maxLeverage") or universe_max))
                    except (TypeError, ValueError):
                        continue
        return universe_max

    async def _stored_leverage(self, coin: str) -> int | None:
        if self._state_store is None:
            return None
        raw = await self._state_store.get_adapter_config(f"leverage.value:{coin}")
        try:
            return int(raw) if raw is not None else None
        except ValueError:
            return None

    async def _stored_margin_mode(self, coin: str) -> MarginMode | None:
        if self._state_store is None:
            return None
        raw = await self._state_store.get_adapter_config(f"margin.mode:{coin}")
        try:
            return MarginMode(raw) if raw is not None else None
        except ValueError:
            return None

    async def _resolved_is_cross(
        self, coin: str, *, legs: dict[str, tuple[int, bool]] | None = None
    ) -> bool:
        """Mode for an ``updateLeverage`` call: stored intent, else venue, else default.

        ``legs`` is a pre-fetched :meth:`_venue_leverage_map` snapshot, sparing
        one ``userState`` call per coin on multi-coin passes.
        """
        stored = await self._stored_margin_mode(coin)
        if stored is not None:
            return stored is MarginMode.CROSS
        venue = legs.get(coin) if legs is not None else await self._venue_leverage(coin)
        if venue is not None:
            return venue[1]
        return self._config.default_margin_mode is MarginMode.CROSS

    async def _resolved_leverage(
        self, coin: str, *, legs: dict[str, tuple[int, bool]] | None = None
    ) -> int:
        """Leverage for an ``updateLeverage`` call: stored intent, else venue, else default.

        ``legs`` is a pre-fetched :meth:`_venue_leverage_map` snapshot, sparing
        one ``userState`` call per coin on multi-coin passes.
        """
        stored = await self._stored_leverage(coin)
        if stored is not None:
            return stored
        venue = legs.get(coin) if legs is not None else await self._venue_leverage(coin)
        if venue is not None:
            return venue[0]
        return self._config.default_leverage

    async def _submit_leverage(self, coin: str, leverage: int, is_cross: bool) -> None:
        exchange = self._require_exchange()
        response = await self._run_exchange(exchange.update_leverage, leverage, coin, is_cross)
        self._check_action_ok(response, context=f"setting leverage for {coin}")

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        strict_check: bool = True,
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent.

        Owns the leverage number only — the mode resolves stored intent,
        else venue, else default.  Above the tier cap raises
        ``LeverageExceedsMaxError`` (never clamped); spot raises
        ``InvalidSymbolError``.  The store is required up front so the venue
        is never mutated when intent cannot be persisted.  With the default
        ``block_on_open_position`` an open leg refuses the change (the venue
        would recut live margin); ``strict_check`` arms pre-order
        verification for the coin.  Both knobs persist per coin.
        """
        raw_leverage: Any = leverage
        if isinstance(raw_leverage, bool) or not isinstance(raw_leverage, int) or raw_leverage < 1:
            raise InvalidOrderError(f"leverage must be an integer >= 1, got {leverage}")
        if on_drift not in ("reapply", "notify", "halt"):
            raise ValueError(f"on_drift must be reapply/notify/halt, got {on_drift}")
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no leverage")
        coin = to_hyperliquid_coin(instrument)
        store = await self._require_store()
        if block_on_open_position:
            await self._block_on_open_position(
                instrument, action="change leverage", kind="leverage"
            )
        cap = await self._tier_max_leverage(coin)
        if leverage > cap:
            raise LeverageExceedsMaxError(f"Leverage {leverage} exceeds max {cap} for {coin}")
        await self._submit_leverage(coin, leverage, await self._resolved_is_cross(coin))
        await store.set_adapter_config(f"leverage.value:{coin}", str(leverage))
        await store.set_adapter_config(f"leverage.on_drift:{coin}", on_drift)
        await store.set_adapter_config(
            f"leverage.strict_check:{coin}", "1" if strict_check else "0"
        )
        await store.set_adapter_config(
            f"leverage.block_on_open:{coin}", "1" if block_on_open_position else "0"
        )
        await store.set_adapter_config(
            f"leverage.auto_apply:{coin}", "1" if auto_apply_on_connect else "0"
        )

    async def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)``; None with no open leg."""
        if instrument.asset_class == AssetClass.SPOT:
            return None
        return await self._venue_leverage(to_hyperliquid_coin(instrument))

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent and its knobs (venue untouched)."""
        store = await self._require_store()
        coin = to_hyperliquid_coin(instrument)
        await store.delete_adapter_config(f"leverage.value:{coin}")
        await store.delete_adapter_config(f"leverage.on_drift:{coin}")
        await store.delete_adapter_config(f"leverage.strict_check:{coin}")
        await store.delete_adapter_config(f"leverage.block_on_open:{coin}")
        await store.delete_adapter_config(f"leverage.auto_apply:{coin}")

    async def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Add (positive) or remove (negative) isolated margin by delta.

        Goes through ``update_isolated_margin`` — the SDK wraps no
        leverage-targeted top-up, so targeting a leverage number is out of
        scope; strict-isolated removal refusals surface as mapped venue
        errors.  ``isBuy`` is sent ``true``: a documented venue no-op.
        """
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no isolated margin")
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        response = await self._run_exchange(
            exchange.update_isolated_margin, float(amount_usdc), coin
        )
        self._check_action_ok(response, context=f"topping up isolated margin for {coin}")

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent.

        Owns the mode only — leverage is preserved (stored, else venue,
        else default, which the venue then enforces).  ``mode`` is the enum
        or ``"cross"``/``"isolated"``.  The store is required up front so the
        venue is never mutated when intent cannot be persisted.  With the
        default ``block_on_open_position`` an open leg refuses the change.  A
        ``MarginModeChangedEvent`` publishes when the mode actually changes.
        """
        try:
            resolved = MarginMode(mode)
        except ValueError:
            raise ValueError(
                f"mode must be one of {[m.value for m in MarginMode]}, got {mode!r}"
            ) from None
        if on_drift not in ("reapply", "notify", "halt"):
            raise ValueError(f"on_drift must be reapply/notify/halt, got {on_drift}")
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no margin mode")
        coin = to_hyperliquid_coin(instrument)
        store = await self._require_store()
        if block_on_open_position:
            await self._block_on_open_position(
                instrument, action="change margin mode", kind="margin.mode"
            )
        legs = await self._venue_leverage_map()
        stored_mode = await self._stored_margin_mode(coin)
        venue = legs.get(coin)
        previous = (
            stored_mode
            if stored_mode is not None
            else (
                None if venue is None else (MarginMode.CROSS if venue[1] else MarginMode.ISOLATED)
            )
        )
        await self._submit_leverage(
            coin, await self._resolved_leverage(coin, legs=legs), resolved is MarginMode.CROSS
        )
        await store.set_adapter_config(f"margin.mode:{coin}", resolved.value)
        await store.set_adapter_config(f"margin.mode.on_drift:{coin}", on_drift)
        await store.set_adapter_config(
            f"margin.mode.block_on_open:{coin}", "1" if block_on_open_position else "0"
        )
        await store.set_adapter_config(
            f"margin.mode.auto_apply:{coin}", "1" if auto_apply_on_connect else "0"
        )
        if previous is not resolved:
            self._publish(
                MarginModeChangedEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    instrument=instrument,
                    previous=previous,
                    current=resolved,
                )
            )

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode; None with no open leg (or spot)."""
        if instrument.asset_class == AssetClass.SPOT:
            return None
        venue = await self._venue_leverage(to_hyperliquid_coin(instrument))
        if venue is None:
            return None
        return MarginMode.CROSS if venue[1] else MarginMode.ISOLATED

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent and its knobs (venue untouched)."""
        store = await self._require_store()
        coin = to_hyperliquid_coin(instrument)
        await store.delete_adapter_config(f"margin.mode:{coin}")
        await store.delete_adapter_config(f"margin.mode.on_drift:{coin}")
        await store.delete_adapter_config(f"margin.mode.block_on_open:{coin}")
        await store.delete_adapter_config(f"margin.mode.auto_apply:{coin}")

    async def _policy_knob(self, kind: str, knob: str, coin: str) -> str | None:
        """Read one persisted behavior knob (None if unset or storeless)."""
        if self._state_store is None:
            return None
        return await self._state_store.get_adapter_config(f"{kind}.{knob}:{coin}")

    async def _has_open_position(self, coin: str) -> bool:
        """True when the coin carries a nonzero-size leg."""
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        for leg in legs or []:
            position = leg.get("position") if isinstance(leg, dict) else None
            if not isinstance(position, dict) or position.get("coin") != coin:
                continue
            try:
                return Decimal(str(position.get("szi") or "0")) != 0
            except InvalidOperation:
                continue
        return False

    async def _block_on_open_position(
        self, instrument: Instrument, *, action: str, kind: Literal["leverage", "margin.mode"]
    ) -> None:
        """Raise if the instrument has an open leg and the guard is enabled.

        The guard reads the family's persisted ``block_on_open`` knob
        (``leverage.block_on_open:{coin}`` / ``margin.mode.block_on_open:{coin}``);
        unconfigured coins default to blocked.  An open leg recalculates
        margin immediately on ``updateLeverage``, so the default refuses.
        Spot has no legs and never blocks.
        """
        if instrument.asset_class == AssetClass.SPOT:
            return
        coin = to_hyperliquid_coin(instrument)
        raw = await self._policy_knob(kind, _POLICY_KNOB_BLOCK_ON_OPEN, coin)
        enabled = DEFAULT_BLOCK_ON_OPEN_POSITION if raw is None else raw == "1"
        if not enabled:
            return
        if await self._has_open_position(coin):
            raise PlatformError(f"Cannot {action} with open position for {coin}")

    def _coin_instrument(self, coin: str) -> Instrument | None:
        """Resolve a stored coin to its instrument; None (logged) when unresolvable."""
        try:
            is_spot = self._is_spot_coin(coin)
            return from_hyperliquid_coin(
                coin,
                is_spot=is_spot,
                spot_pair_coins=self._reverse_pair_coins() if is_spot else None,
            )
        except Exception:
            logger.warning("Skipping stored intent for unresolvable coin %r", coin)
            return None

    def _decode_lev_intent_key(self, key: str) -> str | None:
        """Extract the coin from a ``leverage.value:{coin}`` row (None otherwise).

        The ``leverage.`` listing also returns policy rows; the exact prefix
        excludes them, and the coin guard rejects malformed coins.
        """
        if not key.startswith("leverage.value:"):
            return None
        coin = key.removeprefix("leverage.value:")
        if not coin or "." in coin or ":" in coin:
            return None
        return coin

    def _decode_mode_intent_key(self, key: str) -> str | None:
        """Extract the coin from a ``margin.mode:{coin}`` row (None otherwise)."""
        if not key.startswith("margin.mode:"):
            return None
        coin = key.removeprefix("margin.mode:")
        if not coin or "." in coin or ":" in coin:
            return None
        return coin

    async def _halt_for_drift(self, coin: str, *, reason: str, detail: str) -> None:
        """Enter an instrument halt for drift, degrading to a log without setup."""
        if self._halt_machine is None:
            logger.warning("Cannot enter %s halt for %s — no halt machine attached", reason, coin)
            return
        try:
            instrument = from_hyperliquid_coin(
                coin,
                is_spot=self._is_spot_coin(coin),
                spot_pair_coins=self._reverse_pair_coins(),
            )
        except Exception:
            logger.exception("Cannot resolve instrument for %s halt on %s", reason, coin)
            return
        self._halt_machine.enter_halt(
            scope="instrument", instrument=instrument, reason=reason, detail=detail
        )

    async def _reconcile_leverage_row(
        self, coin: str, stored: int, *, legs: dict[str, tuple[int, bool]]
    ) -> Literal["reapplied", "notified", "halted", "failed"] | None:
        """Reconcile one coin's leverage intent, publishing drift/failure events.

        Returns the action taken, ``"failed"`` when a reapply submit raised,
        or None when venue already matches intent.
        """
        venue = legs.get(coin)
        if venue is None:
            return None
        if venue[0] == stored:
            return None
        instrument = self._coin_instrument(coin)
        if instrument is None:
            return None
        policy = None
        if self._state_store is not None:
            policy = await self._state_store.get_adapter_config(f"leverage.on_drift:{coin}")
        on_drift = policy or "reapply"
        detail = f"stored={stored} venue={venue[0]}"
        action: Literal["reapplied", "notified", "halted"]
        if on_drift == "reapply":
            try:
                await self._submit_leverage(
                    coin, stored, await self._resolved_is_cross(coin, legs=legs)
                )
            except Exception as exc:
                logger.exception("Leverage reapply failed for %s", coin)
                self._publish(
                    LeverageApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        leverage=stored,
                        reason=str(exc),
                    )
                )
                return "failed"
            action = "reapplied"
        elif on_drift == "notify":
            logger.warning("Leverage drift on %s: %s", coin, detail)
            action = "notified"
        else:
            action = "halted"
        self._publish(
            LeverageDriftEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                instrument=instrument,
                stored=stored,
                platform=venue[0],
                action_taken=action,
            )
        )
        if on_drift == "halt":
            await self._halt_for_drift(coin, reason="leverage_drift", detail=detail)
        return action

    async def _reconcile_margin_row(
        self, coin: str, stored: MarginMode, *, legs: dict[str, tuple[int, bool]]
    ) -> Literal["reapplied", "notified", "halted", "failed"] | None:
        """Reconcile one coin's margin-mode intent, publishing drift/failure events.

        Returns the action taken, ``"failed"`` when a reapply submit raised,
        or None when venue already matches intent.
        """
        venue = legs.get(coin)
        if venue is None:
            return None
        platform = MarginMode.CROSS if venue[1] else MarginMode.ISOLATED
        if platform is stored:
            return None
        instrument = self._coin_instrument(coin)
        if instrument is None:
            return None
        policy = None
        if self._state_store is not None:
            policy = await self._state_store.get_adapter_config(f"margin.mode.on_drift:{coin}")
        on_drift = policy or "reapply"
        detail = f"stored={stored.value} venue={platform.value}"
        action: Literal["reapplied", "notified", "halted"]
        if on_drift == "reapply":
            try:
                await self._submit_leverage(
                    coin, await self._resolved_leverage(coin, legs=legs), stored is MarginMode.CROSS
                )
            except Exception as exc:
                logger.exception("Margin mode reapply failed for %s", coin)
                self._publish(
                    MarginModeApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        mode=stored,
                        reason=str(exc),
                    )
                )
                return "failed"
            action = "reapplied"
        elif on_drift == "notify":
            logger.warning("Margin mode drift on %s: %s", coin, detail)
            action = "notified"
        else:
            action = "halted"
        self._publish(
            MarginModeDriftEvent(
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
        if on_drift == "halt":
            await self._halt_for_drift(coin, reason="margin_mode_drift", detail=detail)
        return action

    async def _strict_check_leverage(self, instrument: Instrument) -> None:
        """Pre-order leverage verification.

        The coin's persisted ``strict_check`` knob decides whether the check
        runs at all (default on; ``"0"`` disables).  Intent is stored
        leverage, else the configured default — a flat venue (no leg) always
        passes, since nothing contradicts intent and the submit ack is the
        verification.  On drift the coin's ``on_drift`` policy executes
        exactly as reconcile does; anything but a successful reapply —
        notify, halt, or a reapply that itself failed, leaving leverage
        still drifted — rejects the order with ``LeverageDriftError``.
        """
        coin = to_hyperliquid_coin(instrument)
        raw = await self._policy_knob("leverage", _POLICY_KNOB_STRICT_CHECK, coin)
        if raw == "0":
            return
        if raw is None and not DEFAULT_STRICT_CHECK:
            return
        stored = await self._stored_leverage(coin)
        intent = stored if stored is not None else self._config.default_leverage
        venue = await self._venue_leverage(coin)
        legs = {coin: venue} if venue is not None else {}
        action = await self._reconcile_leverage_row(coin, intent, legs=legs)
        if action is not None and action != "reapplied":
            raise LeverageDriftError(f"Platform leverage differs from intent {intent} for {coin}")

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue.

        Each drifted coin executes its own stored policy; failures are
        logged per coin without aborting the pass.  No position-mode
        reconciliation — one-way is asserted, not managed.
        """
        if self._state_store is None:
            return
        # One userState fetch per non-empty pass — a row never changes
        # another coin's venue state, so a pass snapshot reads exactly what
        # per-row fetches did.  Empty passes (first run, intent removed)
        # cost no venue call at all.
        lev_rows = await self._state_store.list_adapter_config("leverage.")
        if lev_rows:
            legs = await self._venue_leverage_map()
            for key, value in lev_rows.items():
                coin = self._decode_lev_intent_key(key)
                if coin is None:
                    continue
                try:
                    stored = int(value)
                except ValueError:
                    continue
                try:
                    await self._reconcile_leverage_row(coin, stored, legs=legs)
                except Exception:
                    logger.exception("Leverage reconcile failed for %s", coin)
        mode_rows = await self._state_store.list_adapter_config("margin.")
        if mode_rows:
            legs = await self._venue_leverage_map()
            for key, value in mode_rows.items():
                coin = self._decode_mode_intent_key(key)
                if coin is None:
                    continue
                try:
                    stored_mode = MarginMode(value)
                except ValueError:
                    continue
                try:
                    await self._reconcile_margin_row(coin, stored_mode, legs=legs)
                except Exception:
                    logger.exception("Margin mode reconcile failed for %s", coin)

    async def _reapply_stored_intent(self) -> None:
        """Impose auto-apply stored intent after connect; failures never break connect."""
        if self._state_store is None:
            return
        lev_rows = await self._state_store.list_adapter_config("leverage.")
        if lev_rows:
            await self._reapply_leverage_rows(lev_rows, await self._venue_leverage_map())
        mode_rows = await self._state_store.list_adapter_config("margin.")
        if mode_rows:
            await self._reapply_margin_rows(mode_rows, await self._venue_leverage_map())

    async def _reapply_leverage_rows(
        self, rows: dict[str, str], legs: dict[str, tuple[int, bool]]
    ) -> None:
        """Reapply one pass of stored leverage intent; failures publish and continue."""
        assert self._state_store is not None
        for key, value in rows.items():
            coin = self._decode_lev_intent_key(key)
            if coin is None:
                continue
            try:
                leverage = int(value)
            except ValueError:
                logger.warning("Ignoring malformed leverage intent %r for %s", value, coin)
                continue
            instrument: Instrument | None = None
            try:
                if await self._state_store.get_adapter_config(f"leverage.auto_apply:{coin}") == "0":
                    continue
                instrument = self._coin_instrument(coin)
                if instrument is None:
                    continue
                await self._submit_leverage(
                    coin, leverage, await self._resolved_is_cross(coin, legs=legs)
                )
            except Exception as exc:
                logger.exception("Leverage reapply failed for %s on connect", coin)
                if instrument is None:
                    continue
                self._publish(
                    LeverageApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        leverage=leverage,
                        reason=str(exc),
                    )
                )
                continue
            self._publish(
                LeverageAppliedEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    instrument=instrument,
                    leverage=leverage,
                )
            )

    async def _reapply_margin_rows(
        self, rows: dict[str, str], legs: dict[str, tuple[int, bool]]
    ) -> None:
        """Reapply one pass of stored margin-mode intent; failures publish and continue."""
        assert self._state_store is not None
        for key, value in rows.items():
            coin = self._decode_mode_intent_key(key)
            if coin is None:
                continue
            try:
                stored_mode = MarginMode(value)
            except ValueError:
                logger.warning("Ignoring malformed margin-mode intent %r for %s", value, coin)
                continue
            mode_instrument: Instrument | None = None
            try:
                if (
                    await self._state_store.get_adapter_config(f"margin.mode.auto_apply:{coin}")
                    == "0"
                ):
                    continue
                mode_instrument = self._coin_instrument(coin)
                if mode_instrument is None:
                    continue
                venue = legs.get(coin)
                previous = (
                    None
                    if venue is None
                    else (MarginMode.CROSS if venue[1] else MarginMode.ISOLATED)
                )
                await self._submit_leverage(
                    coin,
                    await self._resolved_leverage(coin, legs=legs),
                    stored_mode is MarginMode.CROSS,
                )
            except Exception as exc:
                logger.exception("Margin mode reapply failed for %s on connect", coin)
                if mode_instrument is None:
                    continue
                self._publish(
                    MarginModeApplyFailedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=mode_instrument,
                        mode=stored_mode,
                        reason=str(exc),
                    )
                )
                continue
            if previous is not stored_mode:
                assert mode_instrument is not None
                self._publish(
                    MarginModeChangedEvent(
                        event_id=_new_id(),
                        timestamp=_utcnow(),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=mode_instrument,
                        previous=previous,
                        current=stored_mode,
                    )
                )

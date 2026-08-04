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
from collections import OrderedDict
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pybit.exceptions import FailedRequestError, InvalidRequestError
from pybit.unified_trading import HTTP
from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import map_bybit_error
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
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
)
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


class BybitAdapter(Adapter):
    """Concrete Adapter ABC implementation for Bybit.

    Construction follows the Adapter ABC convention (Section 17.10):
    configuration is supplied as a ``BybitConfig`` dataclass, not loose
    strings; the EventBus reference is required so the adapter can publish
    translated events from its internal WebSocket handlers.
    """

    def __init__(self, config: BybitConfig, *, event_bus: EventBus) -> None:
        self._config = config
        self._event_bus = event_bus
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
        self._event_bus.publish(
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
        loop.call_soon_threadsafe(self._event_bus.publish, event)

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

        Cached per Section 17.3: each entry re-fetches after an expiry set by
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

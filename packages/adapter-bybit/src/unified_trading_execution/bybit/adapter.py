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
from collections.abc import Callable
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
from unified_trading_execution.bybit.symbols import to_bybit_symbol
from unified_trading_execution.bybit.websocket import BybitWebSocket
from unified_trading_execution.errors import InvalidSymbolError, OrderNotFoundError
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.types.enums import AssetClass, OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    UnifiedOrder,
)

_DEFAULT_REQUESTS_PER_INTERVAL = 120
_DEFAULT_INTERVAL_SECONDS = 60
_CONNECTION_MONITOR_INTERVAL_SECONDS = 5.0
_ORDER_CATEGORIES: tuple[str, ...] = ("spot", "linear", "inverse")


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


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
        ws = BybitWebSocket(self._config)
        await asyncio.to_thread(ws.connect)
        self._ws = ws
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
        """Detect platform-initiated drops/reconnects and publish state changes."""
        while True:
            await asyncio.sleep(_CONNECTION_MONITOR_INTERVAL_SECONDS)
            connected = self._ws is not None and self._ws.is_connected()
            if connected != self._connected:
                self._connected = connected
                self._publish_connection_state(connected)

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
        await self._run_request(self._session.place_order, **payload)
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
        await self._run_request(self._session.amend_order, **payload)
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
        data, headers, _ = result
        self._update_rate_limits(headers or {})
        return data, headers

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

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        bybit_symbol = to_bybit_symbol(instrument)
        category = self._instrument_to_category(instrument)

        try:
            result = await asyncio.to_thread(
                self._session.get_instruments_info,
                category=category,
                symbol=bybit_symbol,
            )
            data, _, _ = result
        except FailedRequestError as exc:
            raise map_bybit_error(
                http_status=exc.status_code,
                ret_msg=exc.message,
            ) from exc
        except InvalidRequestError as exc:
            raise map_bybit_error(
                ret_code=exc.status_code,
                ret_msg=exc.message,
            ) from exc

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

        return InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=Decimal(str(lot_filter.get("minOrderQty", "0"))),
            max_qty=Decimal(str(lot_filter.get("maxOrderQty", "0"))),
            min_notional=Decimal(str(lot_filter.get("minNotionalValue", "0"))),
            price_precision=-int(tick_size.as_tuple().exponent),
            qty_precision=-int(lot_size.as_tuple().exponent),
        )

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

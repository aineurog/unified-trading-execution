"""Binance adapter implementation.

The adapter will translate the core's product-neutral contract into Binance
Spot, USDⓈ-M Futures, and COIN-M Futures APIs. Symbol and instrument metadata
mapping, REST client lifecycle, order operations, account streams, and
reconciliation snapshots are implemented. Market data is handled separately.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING

import binance  # type: ignore[import-untyped]
from aiohttp import ClientError
from binance import AsyncClient
from binance.exceptions import (  # type: ignore[import-untyped]
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
)
from uuid_extensions import uuid7

from unified_trading_execution.adapter import (
    Adapter,
    MarketDataCapability,
    PositionProtectionCapability,
    RateLimits,
    TrailingCapability,
)
from unified_trading_execution.binance.config import BinanceConfig
from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.errors import map_binance_error
from unified_trading_execution.binance.orders import (
    OrderReference,
    _MissingAverageFillPriceError,
    build_modify_order_payload,
    build_place_order_payload,
    parse_order_result,
    to_binance_client_order_id,
)
from unified_trading_execution.binance.rate_limits import BinanceRateLimitTracker
from unified_trading_execution.binance.streams import (
    translate_fill_entry,
    translate_futures_balance,
    translate_order_entry,
    translate_position,
    translate_spot_balance,
)
from unified_trading_execution.binance.symbols import (
    from_binance_symbol,
    parse_instrument_spec,
    product_for_instrument,
    resolve_binance_symbol,
)
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    NativeTrailingRequest,
    NativeTrailingResult,
    TrailingBasis,
)
from unified_trading_execution.types.enums import LIVE_ORDER_STATUSES, OrderStatus, OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Candle, Ticker
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

if TYPE_CHECKING:
    from binance import BinanceSocketManager

    from unified_trading_execution.state.halt import HaltStateMachine
    from unified_trading_execution.state.store import StateStore


logger = logging.getLogger(__name__)


def _order_response(response: object) -> dict[str, object]:
    if not isinstance(response, dict):
        raise PlatformError("Binance order endpoint returned a non-object response")
    return response


_EXPECTED_REST_PING_URLS: dict[str, tuple[str, str, str]] = {
    "production": (
        "https://api.binance.com/api/v3/ping",
        "https://fapi.binance.com/fapi/v1/ping",
        "https://dapi.binance.com/dapi/v1/ping",
    ),
    "testnet": (
        "https://testnet.binance.vision/api/v3/ping",
        "https://testnet.binancefuture.com/fapi/v1/ping",
        "https://testnet.binancefuture.com/dapi/v1/ping",
    ),
    "demo": (
        "https://demo-api.binance.com/api/v3/ping",
        "https://demo-fapi.binance.com/fapi/v1/ping",
        "https://demo-dapi.binance.com/dapi/v1/ping",
    ),
}
_REST_PRODUCTS = ("Spot", "USDⓈ-M Futures", "COIN-M Futures")
_PRODUCT_ORDER: tuple[BinanceProduct, ...] = (
    BinanceProduct.SPOT,
    BinanceProduct.USD_M,
    BinanceProduct.COIN_M,
)
_ORDER_HISTORY_LIMIT = 100_000
_MAX_TRADE_PAGES = 100
_MAX_FINAL_ORDER_IDS = 10_000
_STREAM_RETRY_MAX_SECONDS = 30.0
_STREAM_START_TIMEOUT_SECONDS = 30.0


class BinanceAdapter(Adapter):
    """Binance execution adapter for Spot and both Futures products.

    One ``AsyncClient`` instance is intended to provide asynchronous REST
    access for the selected Binance products. Product selection must remain
    explicit when resolving symbols and endpoint families: Spot, USDⓈ-M, and
    COIN-M are not interchangeable markets. ``BinanceConfig.testnet`` and
    ``BinanceConfig.demo`` select distinct sandbox environments; both false
    selects production. The REST client is created by :meth:`connect`;
    private account streams start as part of :meth:`connect`.

    Args:
        config: Immutable API credentials and environment selection.
        event_bus: Optional core event bus. When omitted, a local bus is
            created for direct adapter construction. Engine construction
            replaces it with the core-owned bus via ``attach_event_bus``.

    Attributes:
        _client: The async REST client, or ``None`` while disconnected.
        _socket_manager: Binance private-stream manager for Spot and USDⓈ-M.
        _stream_tasks: Background account-stream tasks keyed by product.
        _order_refs: Product/symbol routing references for known orders.
        _instrument_spec_cache: Successful instrument specs keyed by canonical
            instrument identity.
        _exchange_info_cache: Product-wide exchange metadata, fetched once per
            adapter instance to avoid repeating the full catalog request.
        _native_trailing_ids: Native protection references keyed by core
            trailing identity once trailing support is implemented.
    """

    def __init__(
        self,
        config: BinanceConfig,
        *,
        event_bus: EventBus | None = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus if event_bus is not None else EventBus()
        self._connected = False
        self._lifecycle_lock = asyncio.Lock()

        self._client: AsyncClient | None = None
        self._socket_manager: BinanceSocketManager | None = None
        self._stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._stream_states: dict[BinanceProduct, bool] = {}
        self._stream_ready: dict[BinanceProduct, asyncio.Event] = {}

        self._order_refs: dict[str, OrderReference] = {}
        self._symbol_instruments: dict[tuple[BinanceProduct, str], Instrument] = {}
        self._known_instruments: set[Instrument] = set()
        self._platform_order_keys: dict[tuple[BinanceProduct, str, str], str] = {}
        self._open_order_ids: set[str] = set()
        self._final_order_ids: OrderedDict[str, None] = OrderedDict()
        self._seen_fill_ids: OrderedDict[str, None] = OrderedDict()
        self._balance_refresh_tasks: dict[BinanceProduct, asyncio.Task[None]] = {}
        self._instrument_spec_cache: dict[
            tuple[Instrument, str | None], tuple[InstrumentSpec, datetime]
        ] = {}
        self._exchange_info_cache: dict[BinanceProduct, list[object]] = {}
        self._rate_limit_tracker = BinanceRateLimitTracker()
        self._native_trailing_ids: dict[str, str] = {}

        self._halt_machine: HaltStateMachine | None = None
        self._state_store: StateStore | None = None

    # ------------------------------------------------------------------
    # Identity and environment
    # ------------------------------------------------------------------

    @property
    def platform_name(self) -> str:
        """Return the canonical platform name used by core persistence."""
        return "binance"

    @property
    def account_id(self) -> str:
        """Return configured identity or a stable, non-secret per-key label.

        A key-derived value avoids collapsing separate API keys onto one local
        state-store path before authenticated exchange identity resolution is
        implemented. The API key itself is never returned or logged here.
        """
        if self._config.account_id is not None:
            return self._config.account_id
        key_digest = hashlib.sha256(self._config.api_key.encode("utf-8")).hexdigest()[:16]
        return f"binance-{key_digest}"

    @property
    def is_connected(self) -> bool:
        """Whether the adapter has completed its connection lifecycle."""
        return self._connected and all(self._stream_states.values())

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the async REST client using the configured Binance environment.

        The pinned SDK factory validates connectivity with Spot ping/server
        time requests. All three product URL builders are then checked before
        the client is retained, preventing an incorrect or live fallback for a
        sandbox configuration. Private streams are started before the
        connection is considered ready.
        """
        async with self._lifecycle_lock:
            if self._connected:
                return
            if self._client is not None:
                raise PlatformConnectionError(
                    "Binance adapter has an unclosed client; disconnect before reconnecting"
                )

            creation_task = asyncio.create_task(
                AsyncClient.create(
                    api_key=self._config.api_key,
                    api_secret=self._config.api_secret,
                    testnet=self._config.testnet,
                    demo=self._config.demo,
                )
            )
            try:
                client = await asyncio.shield(creation_task)
            except asyncio.CancelledError:
                try:
                    client = await creation_task
                except Exception:
                    logger.exception(
                        "Binance client creation failed while connection was cancelled"
                    )
                else:
                    try:
                        await client.close_connection()
                    except Exception:
                        logger.exception(
                            "Failed to close Binance client after connection was cancelled"
                        )
                raise
            except (
                BinanceAPIException,
                BinanceOrderException,
                BinanceRequestException,
                ClientError,
                OSError,
                TimeoutError,
            ) as exc:
                raise map_binance_error(exc) from exc

            try:
                self._validate_client_urls(client)
            except BaseException:
                try:
                    await client.close_connection()
                except Exception:
                    logger.exception("Failed to close Binance client after connection setup failed")
                raise

            self._client = client
            self._connected = True
            self._stream_states = dict.fromkeys(_PRODUCT_ORDER, False)
            self._stream_ready = {
                product: asyncio.Event() for product in _PRODUCT_ORDER
            }
            try:
                await asyncio.gather(
                    *(self._load_exchange_info(product) for product in _PRODUCT_ORDER)
                )
                await self._restore_order_references()
                await self._start_private_streams()
            except BaseException:
                was_connected = self.is_connected
                await self._stop_private_streams()
                self._connected = False
                self._socket_manager = None
                self._stream_states.clear()
                self._stream_ready.clear()
                if was_connected:
                    self._publish_connection_state(False)
                try:
                    await client.close_connection()
                except Exception:
                    self._client = client
                    logger.exception(
                        "Failed to close Binance client after account-stream startup failed"
                    )
                else:
                    self._client = None
                    self._clear_session_order_state()
                raise

    async def disconnect(self) -> None:
        """Stop private streams, close the REST client, and publish disconnection."""
        async with self._lifecycle_lock:
            client = self._client
            if client is None:
                if self._connected:
                    raise PlatformConnectionError(
                        "Binance adapter is marked connected without an active REST client"
                    )
                self._clear_session_order_state()
                return

            was_connected = self.is_connected
            await self._stop_private_streams()
            self._stream_states = dict.fromkeys(_PRODUCT_ORDER, False)
            if was_connected:
                self._publish_connection_state(False)
            self._connected = False
            try:
                await client.close_connection()
            except (
                BinanceAPIException,
                BinanceOrderException,
                BinanceRequestException,
                ClientError,
                OSError,
                TimeoutError,
            ) as exc:
                raise map_binance_error(exc) from exc

            self._client = None
            self._socket_manager = None
            self._clear_session_order_state()

    def _clear_session_order_state(self) -> None:
        self._order_refs.clear()
        self._platform_order_keys.clear()
        self._open_order_ids.clear()
        self._final_order_ids.clear()
        self._seen_fill_ids.clear()
        self._known_instruments.clear()

    def _validate_client_urls(self, client: AsyncClient) -> None:
        """Fail closed unless SDK builders resolve to the configured environment."""
        environment = (
            "testnet" if self._config.testnet else "demo" if self._config.demo else "production"
        )
        if (
            getattr(client, "testnet", None) != self._config.testnet
            or getattr(client, "demo", None) != self._config.demo
        ):
            raise PlatformConnectionError(
                f"Binance SDK environment flags do not match configured {environment}"
            )

        spot_url_builder = getattr(client, "_create_api_uri", None)
        usdm_url_builder = getattr(client, "_create_futures_api_uri", None)
        coinm_url_builder = getattr(client, "_create_futures_coin_api_url", None)
        if (
            not callable(spot_url_builder)
            or not callable(usdm_url_builder)
            or not callable(coinm_url_builder)
        ):
            raise PlatformConnectionError(
                "Binance SDK does not expose the expected Spot and Futures REST URL builders"
            )

        actual_urls = (
            spot_url_builder("ping", signed=False),
            usdm_url_builder("ping"),
            coinm_url_builder("ping"),
        )
        expected_urls = _EXPECTED_REST_PING_URLS[environment]
        for product, actual_url, expected_url in zip(
            _REST_PRODUCTS, actual_urls, expected_urls, strict=True
        ):
            if actual_url != expected_url:
                raise PlatformConnectionError(
                    f"Binance {product} REST endpoint does not match configured {environment}; "
                    "refusing to connect"
                )

    def _publish_connection_state(self, connected: bool) -> None:
        self._event_bus.publish(
            ConnectionStateEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                connected=connected,
            )
        )

    async def _load_exchange_info(self, product: BinanceProduct) -> list[object]:
        cached_entries = self._exchange_info_cache.get(product)
        if cached_entries is not None:
            return cached_entries
        client = self._client
        if client is None:
            raise PlatformConnectionError(
                "Binance adapter must be connected before fetching instrument metadata"
            )
        method_names = {
            BinanceProduct.SPOT: "get_exchange_info",
            BinanceProduct.USD_M: "futures_exchange_info",
            BinanceProduct.COIN_M: "futures_coin_exchange_info",
        }
        method = getattr(client, method_names[product])
        try:
            response = await method()
        except (
            BinanceAPIException,
            BinanceOrderException,
            BinanceRequestException,
            ClientError,
            OSError,
            TimeoutError,
        ) as exc:
            error_response = getattr(exc, "response", None)
            if error_response is None and isinstance(exc, BinanceRequestException):
                error_response = getattr(client, "response", None)
            self._record_rate_limit_response(
                product,
                error_response,
                status_code=getattr(exc, "status_code", None),
            )
            raise map_binance_error(exc, product=product) from exc
        self._record_rate_limit_response(product, getattr(client, "response", None), response)
        response_symbols = response.get("symbols") if isinstance(response, dict) else None
        if not isinstance(response_symbols, list):
            raise PlatformError(
                f"Binance {product.value} exchange-info response has no symbols list",
                platform_error=response,
            )
        entries: list[object] = response_symbols
        self._exchange_info_cache[product] = entries
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("symbol"), str):
                logger.warning("Ignoring malformed Binance %s exchange-info entry", product.value)
                continue
            try:
                instrument = from_binance_symbol(entry, product)
            except InvalidSymbolError:
                continue
            self._symbol_instruments[(product, entry["symbol"])] = instrument
        return entries

    async def _restore_order_references(self) -> None:
        if self._state_store is None:
            return
        orders = await self._state_store.query_orders(limit=_ORDER_HISTORY_LIMIT)
        open_orders = await self._state_store.query_open_orders(limit=_ORDER_HISTORY_LIMIT)
        positions = await self._state_store.query_positions(limit=_ORDER_HISTORY_LIMIT)
        for record in orders:
            try:
                product_for_instrument(record.instrument)
            except InvalidSymbolError:
                continue
            self._known_instruments.add(record.instrument)

        records_by_instrument: dict[Instrument, list[OrderRecord]] = {}
        for record in open_orders:
            try:
                product_for_instrument(record.instrument)
            except InvalidSymbolError:
                continue
            if record.status in LIVE_ORDER_STATUSES:
                records_by_instrument.setdefault(record.instrument, []).append(record)

        for instrument, instrument_orders in records_by_instrument.items():
            product = product_for_instrument(instrument)
            try:
                entries = await self._load_exchange_info(product)
                symbol, _ = resolve_binance_symbol(instrument, product, entries)
            except InvalidSymbolError:
                continue
            for record in instrument_orders:
                self._cache_order_reference(
                    record,
                    product=product,
                    symbol=symbol,
                )

        known_positions = {position.instrument for position in positions}
        for instrument in known_positions:
            try:
                product = product_for_instrument(instrument)
                entries = await self._load_exchange_info(product)
                resolve_binance_symbol(instrument, product, entries)
            except InvalidSymbolError:
                continue
            self._known_instruments.add(instrument)

    @staticmethod
    def _order_reference_from_record(
        record: OrderRecord,
        *,
        product: BinanceProduct,
        symbol: str,
    ) -> OrderReference:
        conditional = (
            product == BinanceProduct.USD_M
            and record.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)
        )
        return OrderReference(
            product=product,
            symbol=symbol,
            conditional=conditional,
            order_type=record.order_type,
            platform_order_id=record.platform_order_id,
        )

    def _cache_order_reference(
        self,
        record: OrderRecord,
        *,
        product: BinanceProduct,
        symbol: str,
    ) -> OrderReference:
        reference = self._order_reference_from_record(
            record,
            product=product,
            symbol=symbol,
        )
        self._known_instruments.add(record.instrument)
        if record.status in LIVE_ORDER_STATUSES:
            self._order_refs[record.client_order_id] = reference
            if record.platform_order_id is not None:
                self._platform_order_keys[(product, symbol, record.platform_order_id)] = (
                    record.client_order_id
                )
                self._open_order_ids.add(
                    self._order_identity(product, symbol, record.platform_order_id)
                )
        return reference

    async def _restore_order_reference(
        self,
        client_order_id: str,
    ) -> OrderReference | None:
        reference = self._order_refs.get(client_order_id)
        if reference is not None:
            return reference
        if self._state_store is None:
            return None

        record = await self._state_store.get_order(client_order_id)
        if record is None:
            return None
        if record.client_order_id != client_order_id:
            raise PlatformError(
                f"StateStore returned order {record.client_order_id!r} for "
                f"requested client order id {client_order_id!r}"
            )
        product = product_for_instrument(record.instrument)
        entries = await self._load_exchange_info(product)
        try:
            symbol, _ = resolve_binance_symbol(record.instrument, product, entries)
        except InvalidSymbolError:
            if record.instrument.platform_symbol is None:
                raise
            symbol = record.instrument.platform_symbol
        return self._cache_order_reference(
            record,
            product=product,
            symbol=symbol,
        )

    async def _start_private_streams(self) -> None:
        self._stream_tasks = {
            product.value: asyncio.create_task(
                self._run_private_stream(product, self._stream_ready[product]),
                name=f"binance-{product.value}-user-stream",
            )
            for product in _PRODUCT_ORDER
        }
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *(
                        self._wait_for_stream_ready(product, ready)
                        for product, ready in self._stream_ready.items()
                    )
                ),
                timeout=_STREAM_START_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise PlatformConnectionError(
                "Timed out waiting for all Binance private account streams to connect"
            ) from exc

    async def _wait_for_stream_ready(
        self,
        product: BinanceProduct,
        ready: asyncio.Event,
    ) -> None:
        worker = self._stream_tasks[product.value]
        ready_waiter = asyncio.create_task(ready.wait())
        try:
            done, _ = await asyncio.wait(
                (ready_waiter, worker),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if worker in done:
                exception = worker.exception()
                if exception is not None:
                    raise exception
                raise PlatformConnectionError(
                    f"Binance {product.value} user stream stopped during startup"
                )
        finally:
            if not ready_waiter.done():
                ready_waiter.cancel()
            await asyncio.gather(ready_waiter, return_exceptions=True)

    async def _stop_private_streams(self) -> None:
        tasks = list(self._stream_tasks.values())
        self._stream_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                logger.error("Binance private stream stopped with an error: %s", result)
        for task in self._balance_refresh_tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(
            *self._balance_refresh_tasks.values(),
            return_exceptions=True,
        )
        self._balance_refresh_tasks.clear()

    async def _run_private_stream(
        self,
        product: BinanceProduct,
        ready: asyncio.Event,
    ) -> None:
        retry_delay = 1.0
        while self._connected:
            try:
                client = self._require_client()
                manager = binance.BinanceSocketManager(client)
                self._socket_manager = manager
                socket = {
                    BinanceProduct.SPOT: manager.user_socket,
                    BinanceProduct.USD_M: manager.futures_user_socket,
                    BinanceProduct.COIN_M: manager.coin_futures_socket,
                }[product]()
                async with socket:
                    self._set_stream_available(product, ready, True)
                    retry_delay = 1.0
                    while self._connected:
                        payload = await socket.recv()
                        if payload is None:
                            continue
                        if not isinstance(payload, dict):
                            raise PlatformError(
                                f"Binance {product.value} user stream payload is not an object"
                            )
                        if payload.get("e") == "listenKeyExpired":
                            break
                        await self._handle_user_stream_message(product, payload)

                if self._connected:
                    self._set_stream_available(product, ready, False)
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, _STREAM_RETRY_MAX_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                if not ready.is_set():
                    raise
                logger.exception("Binance %s user stream failed; reconnecting", product.value)
                self._set_stream_available(product, ready, False)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, _STREAM_RETRY_MAX_SECONDS)

    def _set_stream_available(
        self,
        product: BinanceProduct,
        ready: asyncio.Event,
        available: bool,
    ) -> None:
        was_connected = self._connected and all(self._stream_states.values())
        self._stream_states[product] = available
        if available:
            ready.set()
        is_now_connected = self._connected and all(self._stream_states.values())
        if was_connected != is_now_connected:
            self._publish_connection_state(is_now_connected)

    async def _resolve_stream_instrument(
        self,
        product: BinanceProduct,
        symbol: object,
    ) -> Instrument | None:
        if not isinstance(symbol, str) or not symbol:
            logger.warning("Ignoring Binance %s stream entry without a symbol", product.value)
            return None
        instrument = self._symbol_instruments.get((product, symbol))
        if instrument is None:
            entries = await self._load_exchange_info(product)
            entry = next(
                (
                    candidate
                    for candidate in entries
                    if isinstance(candidate, dict) and candidate.get("symbol") == symbol
                ),
                None,
            )
            if entry is None:
                logger.warning(
                    "Ignoring Binance %s stream entry for unknown symbol %s",
                    product.value,
                    symbol,
                )
                return None
            instrument = from_binance_symbol(entry, product)
            self._symbol_instruments[(product, symbol)] = instrument
        self._known_instruments.add(instrument)
        return instrument

    async def _handle_user_stream_message(
        self,
        product: BinanceProduct,
        message: dict[str, object],
    ) -> None:
        payload = message.get("event")
        if isinstance(payload, dict):
            message = payload
        event_type = message.get("e")
        timestamp = self._stream_timestamp(message.get("E"))

        try:
            if product == BinanceProduct.SPOT:
                if event_type == "executionReport":
                    await self._handle_order_update(product, message, timestamp)
                elif event_type == "outboundAccountPosition":
                    balances = message.get("B")
                    if isinstance(balances, list):
                        for item in balances:
                            if isinstance(item, dict):
                                self._publish_balance(
                                    translate_spot_balance(item, timestamp=timestamp)
                                )
                            else:
                                logger.warning("Ignoring malformed Binance Spot balance entry")
                elif event_type == "balanceUpdate":
                    self._schedule_balance_refresh(product)
                elif event_type != "listenKeyExpired":
                    logger.debug("Ignoring Binance Spot stream event %r", event_type)
                return

            if event_type == "ACCOUNT_UPDATE":
                account = message.get("a")
                if not isinstance(account, dict):
                    raise PlatformError("Binance Futures ACCOUNT_UPDATE has no account object")
                self._schedule_balance_refresh(product)
                positions = account.get("P")
                if isinstance(positions, list):
                    for item in positions:
                        if not isinstance(item, dict):
                            logger.warning(
                                "Ignoring malformed Binance %s position update",
                                product.value,
                            )
                            continue
                        instrument = await self._resolve_stream_instrument(
                            product, item.get("s", item.get("symbol"))
                        )
                        if instrument is None:
                            continue
                        self._publish_position(
                            translate_position(
                                item,
                                product=product,
                                instrument=instrument,
                                timestamp=timestamp,
                            )
                        )
                return

            if event_type == "ORDER_TRADE_UPDATE":
                order = message.get("o")
                if not isinstance(order, dict):
                    raise PlatformError("Binance Futures ORDER_TRADE_UPDATE has no order object")
                await self._handle_order_update(product, order, timestamp)
                return

            if product == BinanceProduct.USD_M and event_type == "ALGO_UPDATE":
                order = message.get("o")
                if not isinstance(order, dict):
                    raise PlatformError("Binance USDⓈ-M ALGO_UPDATE has no order object")
                await self._handle_order_update(
                    product,
                    order,
                    timestamp,
                    conditional=True,
                )
                return

            if event_type != "listenKeyExpired":
                logger.debug("Ignoring Binance %s stream event %r", product.value, event_type)
        except PlatformError:
            logger.exception(
                "Could not translate Binance %s account-stream event %r",
                product.value,
                event_type,
            )

    @staticmethod
    def _stream_timestamp(value: object) -> datetime:
        if type(value) is int or (isinstance(value, str) and value.isdigit()):
            try:
                return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
            except (OverflowError, OSError, ValueError) as exc:
                raise PlatformError(
                    "Binance account-stream event has an invalid event timestamp"
                ) from exc
        raise PlatformError("Binance account-stream event is missing a valid event timestamp")

    def _publish_balance(self, balance: Balance) -> None:
        self._event_bus.publish(
            BalanceUpdateEvent(
                event_id=str(uuid7()),
                timestamp=balance.updated_at,
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                balance=balance,
            )
        )

    def _publish_position(self, position: Position) -> None:
        self._event_bus.publish(
            PositionUpdateEvent(
                event_id=str(uuid7()),
                timestamp=position.updated_at,
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                position=position,
            )
        )

    async def _handle_order_update(
        self,
        product: BinanceProduct,
        entry: dict[str, object],
        timestamp: datetime,
        *,
        conditional: bool = False,
    ) -> None:
        symbol = entry.get("s", entry.get("symbol"))
        instrument = await self._resolve_stream_instrument(product, symbol)
        if instrument is None:
            return
        exchange_order_id = entry.get(
            "i",
            entry.get("orderId", entry.get("algoId", entry.get("aid"))),
        )
        if exchange_order_id in (None, ""):
            raise PlatformError("Binance order update is missing exchange order id")
        order_identity = self._order_identity(product, str(symbol), str(exchange_order_id))
        raw_client_id = (
            entry.get("c")
            or entry.get("clientOrderId")
            or entry.get("clientAlgoId")
            or entry.get("caid")
        )
        if raw_client_id in (None, ""):
            raw_client_id = self._platform_order_keys.get(
                (product, str(symbol), str(exchange_order_id))
            )
        if raw_client_id in (None, ""):
            raw_client_id = f"binance-{product.value}:{exchange_order_id}"
        client_order_id = str(raw_client_id)
        if order_identity in self._final_order_ids:
            return

        reference = self._order_refs.get(client_order_id)
        if reference is None:
            reference = await self._restore_order_reference(client_order_id)
        if reference is not None and (
            reference.product != product or reference.symbol != str(symbol)
        ):
            raise PlatformError(
                f"Binance stream order {client_order_id} does not match its stored "
                f"{reference.product.value} {reference.symbol} routing reference"
            )
        if reference is None:
            reference = OrderReference(
                product=product,
                symbol=str(symbol),
                conditional=conditional,
                order_type=OrderType.MARKET,
                platform_order_id=str(exchange_order_id),
            )
        self._order_refs[client_order_id] = reference

        if conditional and str(entry.get("algoStatus", entry.get("X", ""))).upper() == "FINISHED":
            actual = await self.get_order_by_client_id(client_order_id)
            if actual is not None:
                entry = {
                    **entry,
                    "status": actual.status.value.upper(),
                    "executedQty": str(actual.filled_quantity),
                    "avgPrice": (
                        str(actual.average_fill_price)
                        if actual.average_fill_price is not None
                        else "0"
                    ),
                    "algoStatus": actual.status.value.upper(),
                }

        order = translate_order_entry(
            entry,
            product=product,
            instrument=instrument,
            conditional=conditional,
            client_order_id=client_order_id,
        )
        self._platform_order_keys[(product, str(symbol), str(exchange_order_id))] = (
            order.client_order_id
        )
        actual_order_id = entry.get("ai")
        if conditional and actual_order_id not in (None, ""):
            self._platform_order_keys[(product, str(symbol), str(actual_order_id))] = (
                order.client_order_id
            )
        self._order_refs[order.client_order_id] = replace(
            reference,
            conditional=conditional,
            order_type=order.order_type,
            platform_order_id=str(exchange_order_id),
        )

        already_open = order_identity in self._open_order_ids
        if not already_open:
            self._event_bus.publish(
                OrderPlacedEvent(
                    event_id=str(uuid7()),
                    timestamp=timestamp,
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=order.correlation_id,
                    order=order,
                )
            )
            self._open_order_ids.add(order_identity)

        if entry.get("x", entry.get("executionType")) == "TRADE":
            fill = translate_fill_entry(
                entry,
                product=product,
                instrument=instrument,
                client_order_id=order.client_order_id,
            )
            if fill is not None and fill.platform_fill_id not in self._seen_fill_ids:
                self._seen_fill_ids[fill.platform_fill_id] = None
                if len(self._seen_fill_ids) > _MAX_FINAL_ORDER_IDS * 2:
                    self._seen_fill_ids.popitem(last=False)
                self._event_bus.publish(
                    FillEvent(
                        event_id=str(uuid7()),
                        timestamp=fill.fill_timestamp,
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=fill.correlation_id,
                        fill=fill,
                    )
                )

        if order.status in (
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ):
            self._event_bus.publish(
                OrderCancelledEvent(
                    event_id=str(uuid7()),
                    timestamp=timestamp,
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=order.correlation_id,
                    client_order_id=order.client_order_id,
                    instrument=order.instrument,
                )
            )
            self._open_order_ids.discard(order_identity)
            self._remember_final_order_id(order_identity)
            self._forget_order_reference(order.client_order_id)
        elif order.status == OrderStatus.FILLED:
            if already_open:
                self._publish_order_status(order, timestamp)
            self._open_order_ids.discard(order_identity)
            self._remember_final_order_id(order_identity)
            self._forget_order_reference(order.client_order_id)
        elif already_open:
            self._publish_order_status(order, timestamp)

    def _publish_order_status(self, order: OrderRecord, timestamp: datetime) -> None:
        self._event_bus.publish(
            OrderStatusEvent(
                event_id=str(uuid7()),
                timestamp=timestamp,
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=order.correlation_id,
                order=order,
            )
        )

    @staticmethod
    def _order_identity(
        product: BinanceProduct,
        symbol: str,
        platform_order_id: str,
    ) -> str:
        return f"{product.value}:{symbol}:{platform_order_id}"

    def _remember_final_order_id(self, order_identity: str) -> None:
        self._final_order_ids[order_identity] = None
        self._final_order_ids.move_to_end(order_identity)
        while len(self._final_order_ids) > _MAX_FINAL_ORDER_IDS:
            self._final_order_ids.popitem(last=False)

    def _forget_order_reference(self, client_order_id: str) -> None:
        self._order_refs.pop(client_order_id, None)
        stale_platform_keys = [
            key
            for key, mapped_client_order_id in self._platform_order_keys.items()
            if mapped_client_order_id == client_order_id
        ]
        for key in stale_platform_keys:
            del self._platform_order_keys[key]

    def _schedule_balance_refresh(self, product: BinanceProduct) -> None:
        task = self._balance_refresh_tasks.get(product)
        if task is None or task.done():
            self._balance_refresh_tasks[product] = asyncio.create_task(
                self._refresh_product_balances(product),
                name=f"binance-{product.value}-balance-refresh",
            )

    async def _refresh_product_balances(self, product: BinanceProduct) -> None:
        try:
            for balance in (await self._fetch_product_balances(product)).values():
                self._publish_balance(balance)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Could not refresh Binance %s balances", product.value)
        finally:
            self._balance_refresh_tasks.pop(product, None)

    # ------------------------------------------------------------------
    # Order lifecycle
    # ------------------------------------------------------------------

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Submit a supported order and retain lookup data before network I/O."""
        client = self._require_client()
        product = product_for_instrument(order.instrument)
        symbol = await self._resolve_order_symbol(order.instrument, product)
        core_client_order_id = order.client_order_id
        if core_client_order_id is None:
            raise PlatformError("Core must assign client_order_id before Binance order placement")
        binance_client_order_id = to_binance_client_order_id(core_client_order_id)
        payload, conditional = build_place_order_payload(
            order,
            product=product,
            symbol=symbol,
            client_order_id=binance_client_order_id,
        )
        reference = OrderReference(
            product=product,
            symbol=symbol,
            conditional=conditional,
            order_type=order.order_type,
        )
        self._order_refs[core_client_order_id] = reference

        method_names = {
            BinanceProduct.SPOT: "create_order",
            BinanceProduct.USD_M: "futures_create_order",
            BinanceProduct.COIN_M: "futures_coin_create_order",
        }
        response = await self._run_order_request(
            product,
            lambda: getattr(client, method_names[product])(**payload),
        )
        response_entry = _order_response(response)
        try:
            result = parse_order_result(
                response_entry,
                client_order_id=core_client_order_id,
                conditional=conditional,
            )
        except _MissingAverageFillPriceError:
            platform_order_id = response_entry.get("orderId")
            if platform_order_id is None and conditional:
                platform_order_id = response_entry.get("algoId")
            if platform_order_id is None:
                raise
            self._order_refs[core_client_order_id] = OrderReference(
                product=product,
                symbol=symbol,
                conditional=conditional,
                order_type=order.order_type,
                platform_order_id=str(platform_order_id),
            )
            try:
                queried = await self.get_order_by_client_id(core_client_order_id)
            except _MissingAverageFillPriceError as exc:
                raise PlatformError(
                    f"Binance filled order {core_client_order_id} has no average fill price "
                    "in its placement or order-query response"
                ) from exc
            if queried is None:
                raise PlatformError(
                    f"Binance filled order {core_client_order_id} could not be queried "
                    "to obtain its average fill price"
                ) from None
            result = queried
        self._order_refs[core_client_order_id] = OrderReference(
            product=product,
            symbol=symbol,
            conditional=conditional,
            order_type=order.order_type,
            platform_order_id=result.platform_order_id,
        )
        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify price/quantity for a working Futures LIMIT order only."""
        client = self._require_client()
        reference = self._order_refs.get(modification.client_order_id)
        if reference is None:
            raise OrderNotFoundError(
                f"Order {modification.client_order_id} is not known to this Binance adapter"
            )
        if reference.product == BinanceProduct.SPOT:
            raise UnsupportedOrderTypeError(
                "Spot order modification is not supported; cancel and submit a new order"
            )
        if reference.conditional or reference.order_type != OrderType.LIMIT:
            raise UnsupportedOrderTypeError(
                "Binance Futures modification is supported only for ordinary LIMIT orders"
            )
        if (
            modification.stop_price is not None
            or modification.take_profit is not None
            or modification.stop_loss is not None
        ):
            raise UnsupportedOrderTypeError(
                "Binance order modification supports price and quantity only"
            )

        payload = build_modify_order_payload(
            symbol=reference.symbol,
            client_order_id=modification.client_order_id,
            price=modification.price,
            quantity=modification.quantity,
        )
        request_method = (
            client.futures_modify_order
            if reference.product == BinanceProduct.USD_M
            else client.futures_coin_v1_put_order
        )

        async def request() -> object:
            return await request_method(**payload)

        response = await self._run_order_request(reference.product, request)
        try:
            return parse_order_result(
                _order_response(response),
                client_order_id=modification.client_order_id,
            )
        except _MissingAverageFillPriceError:
            queried = await self.get_order_by_client_id(modification.client_order_id)
            if queried is None:
                raise PlatformError(
                    f"Binance modified order {modification.client_order_id} could not be "
                    "queried to obtain its average fill price"
                ) from None
            return queried

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel by the deterministic venue client ID and return the final state."""
        client = self._require_client()
        reference = await self._restore_order_reference(client_order_id)
        if reference is None:
            raise OrderNotFoundError(f"Order {client_order_id} is not known to this adapter")
        method_names = {
            BinanceProduct.SPOT: "cancel_order",
            BinanceProduct.USD_M: "futures_cancel_order",
            BinanceProduct.COIN_M: "futures_coin_cancel_order",
        }
        params = {"symbol": reference.symbol}
        if reference.conditional:
            if reference.platform_order_id is None:
                params["clientAlgoId"] = client_order_id
            else:
                params["algoId"] = reference.platform_order_id
        else:
            if reference.platform_order_id is None:
                params["origClientOrderId"] = client_order_id
            else:
                params["orderId"] = reference.platform_order_id
        if reference.conditional:
            try:
                await self._run_order_request(
                    reference.product,
                    lambda: getattr(client, method_names[reference.product])(**params),
                )
            except OrderNotFoundError:
                current = await self._wait_for_conditional_terminal_order(client_order_id)
                if current is None:
                    raise
                await self._publish_cancel_result(client_order_id, reference, current)
                return current

            current = await self._wait_for_conditional_terminal_order(client_order_id)
            if current is None:
                raise PlatformError(
                    f"Binance accepted cancellation for conditional order {client_order_id}, "
                    "but its terminal state could not be verified"
                )
            await self._publish_cancel_result(client_order_id, reference, current)
            return current

        response = await self._run_order_request(
            reference.product,
            lambda: getattr(client, method_names[reference.product])(**params),
        )
        result = parse_order_result(
            _order_response(response),
            client_order_id=client_order_id,
            conditional=reference.conditional,
        )
        await self._publish_cancel_result(client_order_id, reference, result)
        return result

    async def _publish_cancel_result(
        self,
        client_order_id: str,
        reference: OrderReference,
        result: OrderResult,
    ) -> None:
        if result.status not in (
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ):
            return

        if result.platform_order_id is None:
            order_identity = (
                f"{reference.product.value}:{reference.symbol}:{client_order_id}"
            )
        else:
            order_identity = self._order_identity(
                reference.product,
                reference.symbol,
                result.platform_order_id,
            )
        if order_identity in self._final_order_ids:
            return

        instrument = await self._resolve_stream_instrument(
            reference.product,
            reference.symbol,
        )
        if instrument is None:
            raise PlatformError(
                f"Cannot publish cancellation for Binance order {client_order_id}: "
                f"unknown {reference.product.value} symbol {reference.symbol}"
            )
        if order_identity in self._final_order_ids:
            return

        self._open_order_ids.discard(order_identity)
        self._remember_final_order_id(order_identity)
        if self._state_store is None:
            self._event_bus.publish(
                OrderCancelledEvent(
                    event_id=str(uuid7()),
                    timestamp=result.updated_at,
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    client_order_id=client_order_id,
                    instrument=instrument,
                )
            )
        self._forget_order_reference(client_order_id)

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query a known order; return ``None`` only for confirmed absence."""
        client = self._require_client()
        reference = await self._restore_order_reference(client_order_id)
        if reference is None:
            raise PlatformError(
                f"Cannot resolve Binance product and symbol for order {client_order_id}; "
                "no in-memory routing reference or persisted OrderRecord exists"
            )
        method_names = {
            BinanceProduct.SPOT: "get_order",
            BinanceProduct.USD_M: "futures_get_order",
            BinanceProduct.COIN_M: "futures_coin_get_order",
        }
        params = {"symbol": reference.symbol}
        if reference.conditional:
            if reference.platform_order_id is None:
                params["clientAlgoId"] = client_order_id
            else:
                params["algoId"] = reference.platform_order_id
        else:
            if reference.platform_order_id is None:
                params["origClientOrderId"] = client_order_id
            else:
                params["orderId"] = reference.platform_order_id
        try:
            response = await self._run_order_request(
                reference.product,
                lambda: getattr(client, method_names[reference.product])(**params),
            )
        except OrderNotFoundError:
            history_methods = {
                (BinanceProduct.SPOT, False): "get_all_orders",
                (BinanceProduct.USD_M, False): "futures_get_all_orders",
                (BinanceProduct.USD_M, True): "futures_get_all_algo_orders",
                (BinanceProduct.COIN_M, False): "futures_coin_get_all_orders",
            }
            history_method = history_methods.get((reference.product, reference.conditional))
            if history_method is None:
                return None
            history = await self._run_order_request(
                reference.product,
                lambda: getattr(client, history_method)(symbol=reference.symbol),
            )
            if not isinstance(history, list):
                raise PlatformError(
                    f"Binance {reference.product.value} order history response is not a list"
                ) from None
            for entry in history:
                if not isinstance(entry, dict):
                    raise PlatformError(
                        f"Binance {reference.product.value} order history "
                        "contains a non-object entry"
                    ) from None
            id_field = "algoId" if reference.conditional else "orderId"
            client_id_field = "clientAlgoId" if reference.conditional else "clientOrderId"
            response = next(
                (
                    entry
                    for entry in history
                    if isinstance(entry, dict)
                    and (
                        (
                            reference.platform_order_id is not None
                            and str(entry.get(id_field)) == reference.platform_order_id
                        )
                        or (
                            reference.platform_order_id is None
                            and entry.get(client_id_field) == client_order_id
                        )
                    )
                ),
                None,
            )
            if response is None:
                return None
        response_entry = _order_response(response)
        if reference.conditional:
            resolved = await self._resolve_conditional_actual_order(
                client,
                reference=reference,
                entry=response_entry,
                client_order_id=client_order_id,
            )
            if resolved is not None:
                if (
                    resolved.status not in LIVE_ORDER_STATUSES
                    and self._state_store is not None
                ):
                    self._forget_order_reference(client_order_id)
                return resolved
        result = parse_order_result(
            response_entry,
            client_order_id=client_order_id,
            conditional=reference.conditional,
        )
        if result.status not in LIVE_ORDER_STATUSES and self._state_store is not None:
            self._forget_order_reference(client_order_id)
        return result

    async def _resolve_conditional_actual_order(
        self,
        client: AsyncClient,
        *,
        reference: OrderReference,
        entry: dict[str, object],
        client_order_id: str,
    ) -> OrderResult | None:
        """Resolve a finished algo order through its triggered exchange order.

        Algo status FINISHED does not say whether the triggered order filled or
        was canceled, so the actual order is the authoritative state.
        """
        algo_status = entry.get("algoStatus")
        actual_order_id = entry.get("actualOrderId")
        if (
            not isinstance(algo_status, str)
            or algo_status.upper() != "FINISHED"
            or actual_order_id in (None, "")
        ):
            return None
        try:
            response = await self._run_order_request(
                reference.product,
                lambda: client.futures_get_order(
                    symbol=reference.symbol,
                    orderId=str(actual_order_id),
                ),
            )
        except OrderNotFoundError:
            return None
        result = parse_order_result(
            _order_response(response),
            client_order_id=client_order_id,
        )
        algo_id = entry.get("algoId") or reference.platform_order_id
        if algo_id is not None:
            result = replace(result, platform_order_id=str(algo_id))
        return result

    async def _wait_for_conditional_terminal_order(
        self,
        client_order_id: str,
    ) -> OrderResult | None:
        for attempt in range(8):
            current = await self.get_order_by_client_id(client_order_id)
            if current is not None and current.status in (
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            ):
                return current
            if attempt < 7:
                await asyncio.sleep(0.25)
        return None

    def _require_client(self) -> AsyncClient:
        if not self._connected or self._client is None:
            raise PlatformConnectionError("Binance adapter is not connected")
        return self._client

    async def _resolve_order_symbol(
        self,
        instrument: Instrument,
        product: BinanceProduct,
    ) -> str:
        await self.fetch_instrument_spec(instrument)
        entries = self._exchange_info_cache.get(product)
        if entries is None:
            raise PlatformError(f"Binance {product.value} exchange-info cache was not populated")
        symbol, _ = resolve_binance_symbol(instrument, product, entries)
        return symbol

    async def _run_order_request(
        self,
        product: BinanceProduct,
        request: Callable[[], Awaitable[object]],
    ) -> object:
        client = self._require_client()
        try:
            response = await request()
        except TimeoutError:
            raise
        except (
            BinanceAPIException,
            BinanceOrderException,
            BinanceRequestException,
            ClientError,
            OSError,
        ) as exc:
            error_response = getattr(exc, "response", None)
            self._record_rate_limit_response(
                product,
                error_response,
                status_code=getattr(exc, "status_code", None),
            )
            raise map_binance_error(exc, product=product) from exc
        self._record_rate_limit_response(product, getattr(client, "response", None), response)
        return response

    # ------------------------------------------------------------------
    # Instrument metadata and capabilities
    # ------------------------------------------------------------------

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch product-specific exchange metadata and map its trading rules.

        Binance publishes separate Spot, USDⓈ-M, and COIN-M exchange-info
        catalogs. Resolution requires exactly one row matching the canonical
        identity; ambiguous listings require ``platform_symbol``. Trading
        constraints are parsed from the row's filters using exact Decimals.
        """
        product = product_for_instrument(instrument)
        cache_key = (instrument, instrument.platform_symbol)
        cached = self._instrument_spec_cache.get(cache_key)
        if cached is not None:
            spec, fetched_at = cached
            ttl = self._config.instrument_spec_cache_ttl
            if ttl is None or (datetime.now(UTC) - fetched_at).total_seconds() < ttl:
                return spec
            self._instrument_spec_cache.pop(cache_key, None)

        entries = await self._load_exchange_info(product)

        symbol, entry = resolve_binance_symbol(instrument, product, entries)
        status = entry.get("status")
        if status is None:
            status = entry.get("contractStatus")
        if status != "TRADING":
            raise InvalidSymbolError(f"Binance symbol {symbol} is not trading (status: {status!r})")

        spec = parse_instrument_spec(entry)
        self._instrument_spec_cache[cache_key] = (spec, datetime.now(UTC))
        return spec

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return portable types available on at least one supported product.

        Product-specific constraints are enforced in the payload builder; in
        particular, COIN-M conditional orders are not exposed by the pinned SDK.
        """
        return frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )

    def position_protection_capability(
        self, instrument: Instrument
    ) -> PositionProtectionCapability:
        """Report position TP/SL operations actually supported for a product.

        The scaffold inherits the core's fully unsupported default. Override
        only after per-leg identity, preservation of unmentioned sides, and
        read-back behavior are implemented and verified.
        """
        return super().position_protection_capability(instrument)

    def trailing_capability(self, instrument: Instrument) -> TrailingCapability:
        """Report native trailing features verified for this instrument.

        The scaffold inherits the core's fully unsupported default. Do not
        advertise a capability until its Binance parameter mapping and
        position-leg lifecycle are implemented.
        """
        return super().trailing_capability(instrument)

    def market_data_capability(self, instrument: Instrument) -> MarketDataCapability:
        """Report implemented ticker and candle features for this instrument.

        No market-data capabilities are advertised by the scaffold.
        """
        return super().market_data_capability(instrument)

    async def get_rate_limits(self) -> RateLimits:
        """Return the tightest observed Binance REST rate-limit budget."""
        return self._rate_limit_tracker.snapshot()

    def _record_rate_limit_response(
        self,
        product: BinanceProduct,
        response: object,
        payload: object | None = None,
        *,
        status_code: int | None = None,
    ) -> None:
        headers = getattr(response, "headers", None)
        if not isinstance(headers, Mapping):
            headers = None
        rate_limits = payload.get("rateLimits") if isinstance(payload, dict) else None
        self._rate_limit_tracker.update(
            product,
            headers=headers,
            rate_limits=rate_limits,
            status_code=status_code,
        )

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    async def fetch_ticker(self, instrument: Instrument) -> Ticker | None:
        """Fetch a fresh current-price snapshot for a supported symbol."""
        raise NotImplementedError("Binance adapter scaffold: fetch_ticker is not implemented")

    async def fetch_candles(
        self,
        instrument: Instrument,
        timeframe: CandleTimeframe,
        *,
        limit: int,
    ) -> list[Candle]:
        """Fetch closed OHLC candles, oldest first, excluding the forming bar."""
        raise NotImplementedError("Binance adapter scaffold: fetch_candles is not implemented")

    # ------------------------------------------------------------------
    # Position protection and native trailing
    # ------------------------------------------------------------------

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
        trigger_basis: TrailingBasis | None = None,
    ) -> None:
        """Modify TP/SL for one exact Futures position leg."""
        raise NotImplementedError(
            "Binance adapter scaffold: modify_position_tpsl is not implemented"
        )

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read TP/SL for a Futures leg, or return ``None`` if it is absent."""
        raise NotImplementedError("Binance adapter scaffold: get_position_tpsl is not implemented")

    async def clear_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: bool = False,
        stop_loss: bool = False,
    ) -> None:
        """Clear only named, adapter-owned protection sides on one leg."""
        if not take_profit and not stop_loss:
            raise ValueError("at least one of take_profit or stop_loss must be true")
        raise NotImplementedError(
            "Binance adapter scaffold: clear_position_tpsl is not implemented"
        )

    async def place_trailing_stop(self, request: NativeTrailingRequest) -> NativeTrailingResult:
        """Place native trailing protection for exactly one Futures position leg."""
        raise NotImplementedError(
            "Binance adapter scaffold: place_trailing_stop is not implemented"
        )

    async def cancel_trailing_stop(self, native_id: str, instrument: Instrument) -> None:
        """Cancel the tracked native trailing order by its Binance identifier."""
        raise NotImplementedError(
            "Binance adapter scaffold: cancel_trailing_stop is not implemented"
        )

    async def get_trailing_stop(
        self, native_id: str, instrument: Instrument
    ) -> NativeTrailingResult | None:
        """Read a native trailing order; return ``None`` only when confirmed absent."""
        raise NotImplementedError("Binance adapter scaffold: get_trailing_stop is not implemented")

    async def get_position_trailing_stop(
        self, instrument: Instrument, position_id: str
    ) -> NativeTrailingResult | None:
        """Read native trailing protection attached to a specific position leg."""
        raise NotImplementedError(
            "Binance adapter scaffold: get_position_trailing_stop is not implemented"
        )

    # ------------------------------------------------------------------
    # Reconciliation snapshots
    # ------------------------------------------------------------------

    async def fetch_positions(self) -> list[Position]:
        """Fetch derivative position legs; Spot holdings remain balances."""
        client = self._require_client()
        positions: list[Position] = []
        methods = {
            BinanceProduct.USD_M: "futures_position_information",
            BinanceProduct.COIN_M: "futures_coin_position_information",
        }
        for product, method_name in methods.items():
            response = await self._run_order_request(
                product,
                getattr(client, method_name),
            )
            if not isinstance(response, list):
                raise PlatformError(
                    f"Binance {product.value} positions response is not a list",
                    platform_error=response,
                )
            for entry in response:
                if not isinstance(entry, dict):
                    raise PlatformError(
                        f"Binance {product.value} positions response contains a non-object entry"
                    )
                instrument = await self._resolve_stream_instrument(product, entry.get("symbol"))
                if instrument is None:
                    continue
                position = translate_position(
                    entry,
                    product=product,
                    instrument=instrument,
                )
                if position.quantity != 0:
                    positions.append(position)
        return positions

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances keyed by asset/currency."""
        responses = await asyncio.gather(
            *(self._fetch_product_balances(product) for product in _PRODUCT_ORDER)
        )
        return {
            key: value
            for product_balances in responses
            for key, value in product_balances.items()
        }

    async def _fetch_product_balances(
        self,
        product: BinanceProduct,
    ) -> dict[str, Balance]:
        client = self._require_client()
        updated_at = datetime.now(UTC)
        balances: dict[str, Balance] = {}
        if product == BinanceProduct.SPOT:
            response = await self._run_order_request(product, client.get_account)
            if not isinstance(response, dict) or not isinstance(response.get("balances"), list):
                raise PlatformError(
                    "Binance Spot account response has no balances list",
                    platform_error=response,
                )
            entries = response["balances"]
            for entry in entries:
                if not isinstance(entry, dict):
                    raise PlatformError(
                        "Binance Spot balances response contains a non-object entry"
                    )
                balance = translate_spot_balance(entry, timestamp=updated_at)
                balances[balance.currency] = balance
            return balances

        method_name = (
            "futures_account_balance"
            if product == BinanceProduct.USD_M
            else "futures_coin_account_balance"
        )
        response = await self._run_order_request(product, getattr(client, method_name))
        if not isinstance(response, list):
            raise PlatformError(
                f"Binance {product.value} balances response is not a list",
                platform_error=response,
            )
        for entry in response:
            if not isinstance(entry, dict):
                raise PlatformError(
                    f"Binance {product.value} balances response contains a non-object entry"
                )
            balance = translate_futures_balance(entry, product=product, timestamp=updated_at)
            balances[balance.currency] = balance
        return balances

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch user-owned open orders keyed by core client order ID."""
        client = self._require_client()
        previous_order_references = set(self._order_refs)
        methods = {
            BinanceProduct.SPOT: "get_open_orders",
            BinanceProduct.USD_M: "futures_get_open_orders",
            BinanceProduct.COIN_M: "futures_coin_get_open_orders",
        }
        orders: dict[str, OrderRecord] = {}
        open_ids: set[str] = set()
        for product, method_name in methods.items():
            response = await self._run_order_request(product, getattr(client, method_name))
            for entry in self._response_list(response, product, "open orders"):
                order = await self._translate_snapshot_order(product, entry)
                if order is not None:
                    orders[order.client_order_id] = order
                    reference = self._order_refs[order.client_order_id]
                    if reference.platform_order_id is not None:
                        open_ids.add(
                            self._order_identity(
                                product,
                                reference.symbol,
                                reference.platform_order_id,
                            )
                        )

        algo_response = await self._run_order_request(
            BinanceProduct.USD_M,
            client.futures_get_open_algo_orders,
        )
        for entry in self._response_list(algo_response, BinanceProduct.USD_M, "open algo orders"):
            order = await self._translate_snapshot_order(
                BinanceProduct.USD_M,
                entry,
                conditional=True,
            )
            if order is not None:
                orders[order.client_order_id] = order
                reference = self._order_refs[order.client_order_id]
                if reference.platform_order_id is not None:
                    open_ids.add(
                        self._order_identity(
                            BinanceProduct.USD_M,
                            reference.symbol,
                            reference.platform_order_id,
                        )
                    )
        self._open_order_ids = open_ids
        for client_order_id in previous_order_references.difference(orders):
            self._forget_order_reference(client_order_id)
        return orders

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent executions keyed by core client order ID."""
        client = self._require_client()
        known_instruments = set(self._known_instruments)

        start_time: int | None = None
        if since is not None:
            if since.tzinfo is None or since.utcoffset() is None:
                raise ValueError("since must be timezone-aware")
            start_time = int(since.timestamp() * 1000)

        methods = {
            BinanceProduct.SPOT: "get_my_trades",
            BinanceProduct.USD_M: "futures_account_trades",
            BinanceProduct.COIN_M: "futures_coin_account_trades",
        }
        fills_by_order: dict[str, list[FillRecord]] = {}
        for instrument in known_instruments:
            product = product_for_instrument(instrument)
            try:
                if instrument.platform_symbol is not None:
                    symbol = instrument.platform_symbol
                else:
                    exchange_entries = await self._load_exchange_info(product)
                    symbol, _ = resolve_binance_symbol(
                        instrument,
                        product,
                        exchange_entries,
                    )
            except InvalidSymbolError:
                logger.warning(
                    "Skipping stored instrument unavailable on Binance: %s",
                    instrument,
                )
                continue
            platform_order_keys = await self._platform_order_keys_for_instrument(
                instrument,
                product=product,
                symbol=symbol,
            )
            cursor: int | None = None
            for _ in range(_MAX_TRADE_PAGES):
                params: dict[str, object] = {"symbol": symbol, "limit": 1000}
                if cursor is not None:
                    params["fromId"] = cursor
                elif start_time is not None:
                    params["startTime"] = start_time
                request = partial(getattr(client, methods[product]), **params)
                response = await self._run_order_request(product, request)
                trade_entries = self._response_list(response, product, "account trades")
                for entry in trade_entries:
                    order_id = entry.get("orderId")
                    if order_id in (None, ""):
                        raise PlatformError(
                            f"Binance {product.value} trade entry is missing orderId"
                        )
                    mapped_id = platform_order_keys.get(str(order_id))
                    client_order_id = mapped_id or f"binance-{product.value}:{order_id}"
                    fill = translate_fill_entry(
                        entry,
                        product=product,
                        instrument=instrument,
                        client_order_id=client_order_id,
                    )
                    if fill is not None:
                        fills_by_order.setdefault(client_order_id, []).append(fill)
                if len(trade_entries) < 1000:
                    break
                last_id = trade_entries[-1].get("id")
                if not isinstance(last_id, (int, str)) or not str(last_id).isdigit():
                    raise PlatformError(
                        f"Binance {product.value} trade page has no valid last trade id"
                    )
                cursor = int(last_id) + 1
            else:
                raise PlatformError(
                    f"Binance {product.value} trade history exceeded "
                    f"{_MAX_TRADE_PAGES} pages for {symbol}"
                )
        return fills_by_order

    async def _platform_order_keys_for_instrument(
        self,
        instrument: Instrument,
        *,
        product: BinanceProduct,
        symbol: str,
    ) -> dict[str, str]:
        keys = {
            platform_order_id: client_order_id
            for (key_product, key_symbol, platform_order_id), client_order_id in (
                self._platform_order_keys.items()
            )
            if key_product == product and key_symbol == symbol
        }
        if self._state_store is None:
            return keys

        records = await self._state_store.query_orders(
            instrument=instrument,
            limit=_ORDER_HISTORY_LIMIT,
        )
        for record in records:
            if record.platform_order_id is None:
                continue
            try:
                record_product = product_for_instrument(record.instrument)
                if record_product != product:
                    continue
                if record.instrument.platform_symbol is not None:
                    record_symbol = record.instrument.platform_symbol
                else:
                    exchange_entries = await self._load_exchange_info(product)
                    record_symbol, _ = resolve_binance_symbol(
                        record.instrument,
                        product,
                        exchange_entries,
                    )
            except InvalidSymbolError:
                continue
            if record_symbol == symbol:
                keys[record.platform_order_id] = record.client_order_id
        return keys

    @staticmethod
    def _response_list(
        response: object,
        product: BinanceProduct,
        name: str,
    ) -> list[dict[str, object]]:
        if isinstance(response, dict) and isinstance(response.get("orders"), list):
            response = response["orders"]
        if not isinstance(response, list):
            raise PlatformError(
                f"Binance {product.value} {name} response is not a list",
                platform_error=response,
            )
        entries: list[dict[str, object]] = []
        for entry in response:
            if not isinstance(entry, dict):
                raise PlatformError(
                    f"Binance {product.value} {name} response contains a non-object entry"
                )
            entries.append(entry)
        return entries

    async def _translate_snapshot_order(
        self,
        product: BinanceProduct,
        entry: dict[str, object],
        *,
        conditional: bool = False,
    ) -> OrderRecord | None:
        instrument = await self._resolve_stream_instrument(
            product,
            entry.get("symbol"),
        )
        if instrument is None:
            return None
        order = translate_order_entry(
            entry,
            product=product,
            instrument=instrument,
            conditional=conditional,
        )
        reference = OrderReference(
            product=product,
            symbol=str(entry.get("symbol")),
            conditional=conditional,
            order_type=order.order_type,
            platform_order_id=order.platform_order_id,
        )
        self._order_refs[order.client_order_id] = reference
        if order.platform_order_id is not None:
            self._platform_order_keys[
                (product, reference.symbol, order.platform_order_id)
            ] = order.client_order_id
        return order

    # ------------------------------------------------------------------
    # Core-managed resources
    # ------------------------------------------------------------------

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Retain the core halt machine for future adapter-owned intent work."""
        self._halt_machine = halt_machine

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Use the single event bus owned by the core engine."""
        self._event_bus = event_bus

    def attach_state_store(self, state_store: StateStore) -> None:
        """Retain the core state store for adapter-owned durable metadata only."""
        self._state_store = state_store

    async def reconcile_user_intent(self) -> None:
        """Reconcile adapter-owned user intent when Binance-specific intent exists."""
        raise NotImplementedError(
            "Binance adapter scaffold: reconcile_user_intent is not implemented"
        )

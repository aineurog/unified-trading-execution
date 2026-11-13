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
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import TYPE_CHECKING, Literal

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
from unified_trading_execution.binance.enums import (
    BinanceProduct,
    MarginMode,
    PositionMode,
)
from unified_trading_execution.binance.errors import map_binance_error
from unified_trading_execution.binance.events import (
    FuturesMarginModeChangedEvent,
    FuturesPositionModeChangedEvent,
    FuturesSettingAppliedEvent,
    FuturesSettingApplyFailedEvent,
    FuturesSettingDriftEvent,
)
from unified_trading_execution.binance.market_data import (
    MAX_KLINE_LIMIT,
    book_ticker_method,
    coin_m_contract_type,
    kline_interval,
    klines_method,
    last_price_field,
    last_price_method,
    mark_price_method,
    optional_price,
    parse_kline_row,
    supported_timeframes,
    symbol_entry,
)
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
    HaltClearedEvent,
    HaltEnteredEvent,
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
_FUTURES_PRODUCTS = (BinanceProduct.USD_M, BinanceProduct.COIN_M)
_CONFIG_DRIFT_REASON = "binance_futures_config_drift"
_ON_DRIFT_VALUES = frozenset({"reapply", "notify", "halt"})
_LEVERAGE_KIND_PREFIX = "leverage."
_MARGIN_MODE_KIND_PREFIX = "margin_mode."
_POSITION_MODE_KIND_PREFIX = "position_mode"
_CONFIG_KIND_PREFIXES = (
    _LEVERAGE_KIND_PREFIX,
    _MARGIN_MODE_KIND_PREFIX,
    _POSITION_MODE_KIND_PREFIX,
)


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
        self._futures_config_lock = asyncio.Lock()

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
            self._stream_ready = {product: asyncio.Event() for product in _PRODUCT_ORDER}
            try:
                await asyncio.gather(
                    *(self._load_exchange_info(product) for product in _PRODUCT_ORDER)
                )
                await self._restore_order_references()
                await self._start_private_streams()
                await self._reapply_stored_intent()
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
        conditional = product == BinanceProduct.USD_M and record.order_type in (
            OrderType.STOP,
            OrderType.STOP_LIMIT,
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
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
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
        """Remove all routing metadata for a terminal or cancelled order.

        Intentionally does *not* touch ``_open_order_ids``; callers are
        responsible for discarding the order identity from that set before
        calling this method.  Mixing the two responsibilities here would
        create silent double-discard races in the stream handler path.
        """
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
        async with self._futures_config_lock:
            return await self._place_order_locked(order)

    async def _place_order_locked(self, order: UnifiedOrder) -> OrderResult:
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
        async with self._futures_config_lock:
            return await self._modify_order_locked(modification)

    async def _modify_order_locked(self, modification: OrderModification) -> OrderResult:
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
            order_identity = f"{reference.product.value}:{reference.symbol}:{client_order_id}"
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
            # Direct adapter users have no engine dispatch to publish this event;
            # engine-managed cancellation publishes it after persisting the result.
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
                if resolved.status not in LIVE_ORDER_STATUSES and self._state_store is not None:
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
        if reference.product != BinanceProduct.USD_M:
            raise PlatformError(
                "Binance conditional actual-order lookup is only supported for USDⓈ-M Futures"
            )
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

    async def _resolve_market_symbol(
        self,
        instrument: Instrument,
    ) -> tuple[BinanceProduct, str, dict[str, object]]:
        """Resolve a market-data target without order-only checks.

        Unlike ``_resolve_order_symbol`` this skips ``fetch_instrument_spec``:
        public reads must not require TRADING status or the signed
        leverage-bracket call, and historical candles remain meaningful for
        symbols that are not currently trading. The exchange-info row is
        returned so COIN-M callers can derive ``contractType``.
        """
        product = product_for_instrument(instrument)
        entries = await self._load_exchange_info(product)
        symbol, entry = resolve_binance_symbol(instrument, product, entries)
        return product, symbol, entry

    async def _run_order_request(
        self,
        product: BinanceProduct,
        request: Callable[[], Awaitable[object]],
    ) -> object:
        return await self._dispatch_request(product, self._require_client(), request)

    async def _run_market_data_request(
        self,
        product: BinanceProduct,
        request: Callable[[], Awaitable[object]],
    ) -> object:
        """Dispatch a public market-data call without requiring ``connect``.

        Market-data endpoints are unsigned, so an adapter holding a client —
        including one injected for read-only metadata use — can serve ticker
        and candle reads without private streams or credentials.
        """
        client = self._client
        if client is None:
            raise PlatformConnectionError("Binance adapter is not connected")
        return await self._dispatch_request(product, client, request)

    async def _dispatch_request(
        self,
        product: BinanceProduct,
        client: AsyncClient,
        request: Callable[[], Awaitable[object]],
    ) -> object:
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

        # Fetch max leverage for Futures products (requires authentication)
        if product in _FUTURES_PRODUCTS:
            max_leverage = await self._fetch_max_leverage(product, symbol)
            if max_leverage is not None:
                spec = replace(spec, max_leverage=max_leverage)

        self._instrument_spec_cache[cache_key] = (spec, datetime.now(UTC))
        return spec

    async def _fetch_max_leverage(
        self,
        product: BinanceProduct,
        symbol: str,
    ) -> Decimal | None:
        """Fetch user-specific max leverage from Binance leverage brackets.

        Returns the initialLeverage from bracket 1 (notionalFloor=0), which
        is typically the highest leverage tier for the symbol. Returns None
        if the adapter is not connected or the endpoint is unavailable.
        """
        method_name = (
            "futures_leverage_bracket"
            if product == BinanceProduct.USD_M
            else "futures_coin_leverage_bracket"
        )

        try:
            client = self._require_client()
            response = await self._run_order_request(
                product,
                partial(getattr(client, method_name), symbol=symbol),
            )
        except Exception as exc:
            logger.warning(
                "Could not fetch Binance %s leverage bracket for %s: %s",
                product.value,
                symbol,
                exc,
            )
            return None

        # Response can be a list of symbol entries or a single object
        if isinstance(response, dict):
            response = [response]
        if not isinstance(response, list):
            logger.warning(
                "Binance %s leverage bracket response is not a list for %s",
                product.value,
                symbol,
            )
            return None

        # Find the entry for our symbol
        for entry in response:
            if not isinstance(entry, dict):
                continue
            if entry.get("symbol") != symbol:
                continue

            brackets = entry.get("brackets")
            if not isinstance(brackets, list) or not brackets:
                continue

            # Find bracket with notionalFloor=0 (first tier, highest leverage)
            for bracket in brackets:
                if not isinstance(bracket, dict):
                    continue
                if bracket.get("notionalFloor") in (0, "0", 0.0, "0.0"):
                    leverage = bracket.get("initialLeverage")
                    if leverage is not None:
                        try:
                            return Decimal(str(leverage))
                        except (InvalidOperation, ValueError):
                            logger.warning(
                                "Malformed leverage value %r for %s",
                                leverage,
                                symbol,
                            )
                            return None

            # Fallback: use first bracket if no notionalFloor=0 found
            first_bracket = brackets[0]
            if isinstance(first_bracket, dict):
                leverage = first_bracket.get("initialLeverage")
                if leverage is not None:
                    try:
                        return Decimal(str(leverage))
                    except (InvalidOperation, ValueError):
                        pass

        return None

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
        """Declare ticker and candle reads verified for this instrument.

        ``fetch_ticker`` populates last, bid, and ask for every product and
        mark for Futures (premium index); Spot has no mark. All nine core
        candle timeframes map to Binance kline intervals on every product.
        """
        try:
            product = product_for_instrument(instrument)
        except InvalidSymbolError:
            return super().market_data_capability(instrument)

        bases = {TrailingBasis.LAST, TrailingBasis.BID, TrailingBasis.ASK}
        if product is not BinanceProduct.SPOT:
            bases.add(TrailingBasis.MARK)
        return MarketDataCapability(
            ticker_bases=frozenset(bases),
            min_ticker_interval_seconds=2.0,
            candle_timeframes=supported_timeframes(),
            streaming_ticker=False,
        )

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
        """Fetch a fresh price snapshot for *instrument*.

        Spot reads the 24-hour ticker, which carries ``lastPrice``,
        ``bidPrice``, and ``askPrice`` in one call. Futures read the
        dedicated latest-price ticker for ``last`` (``/fapi/v2`` and
        ``/dapi/v1`` ``ticker/price``), the book ticker for the book side —
        price tickers carry no book fields — and the premium index for
        ``markPrice``; the three public calls run concurrently. ``None`` is
        returned only when the venue has no quote row at all for the
        resolved symbol.
        """
        product, symbol, _entry = await self._resolve_market_symbol(instrument)
        client = self._client
        if client is None:
            raise PlatformConnectionError("Binance adapter is not connected")

        if product == BinanceProduct.SPOT:
            response = await self._run_market_data_request(
                product,
                partial(getattr(client, last_price_method(product)), symbol=symbol),
            )
            entry = symbol_entry(response, symbol=symbol, product=product)
            if entry is None:
                return None
            ticker = Ticker(
                bid=optional_price(entry, "bidPrice", symbol=symbol, product=product),
                ask=optional_price(entry, "askPrice", symbol=symbol, product=product),
                last=optional_price(
                    entry, last_price_field(product), symbol=symbol, product=product
                ),
            )
        else:
            mark_method = mark_price_method(product)
            if mark_method is None:
                raise PlatformError(f"Binance {product.value} has no mark-price endpoint")
            responses = await asyncio.gather(
                self._run_market_data_request(
                    product,
                    partial(getattr(client, last_price_method(product)), symbol=symbol),
                ),
                self._run_market_data_request(
                    product,
                    partial(getattr(client, book_ticker_method(product)), symbol=symbol),
                ),
                self._run_market_data_request(
                    product,
                    partial(getattr(client, mark_method), symbol=symbol),
                ),
            )
            ticker_entry = symbol_entry(responses[0], symbol=symbol, product=product)
            book_entry = symbol_entry(responses[1], symbol=symbol, product=product)
            mark_entry = symbol_entry(responses[2], symbol=symbol, product=product)
            if ticker_entry is None and book_entry is None and mark_entry is None:
                return None
            ticker = Ticker(
                bid=(
                    optional_price(book_entry, "bidPrice", symbol=symbol, product=product)
                    if book_entry is not None
                    else None
                ),
                ask=(
                    optional_price(book_entry, "askPrice", symbol=symbol, product=product)
                    if book_entry is not None
                    else None
                ),
                last=(
                    optional_price(
                        ticker_entry,
                        last_price_field(product),
                        symbol=symbol,
                        product=product,
                    )
                    if ticker_entry is not None
                    else None
                ),
                mark=(
                    optional_price(mark_entry, "markPrice", symbol=symbol, product=product)
                    if mark_entry is not None
                    else None
                ),
            )
        if (
            ticker.bid is None
            and ticker.ask is None
            and ticker.last is None
            and ticker.mark is None
        ):
            return None
        return ticker

    async def fetch_candles(
        self,
        instrument: Instrument,
        timeframe: CandleTimeframe,
        *,
        limit: int,
    ) -> list[Candle]:
        """Fetch closed OHLC candles for ATR, oldest first.

        Binance klines already arrive oldest-first with the still-forming bar
        last, so rows are filtered by close time rather than reversed. One
        extra bar is requested so dropping the forming candle still yields
        ``limit`` closed candles when available.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        interval = kline_interval(timeframe)
        if interval is None:
            raise UnsupportedOrderTypeError(
                f"Binance does not support the {timeframe.value} candle timeframe"
            )
        product, symbol, entry = await self._resolve_market_symbol(instrument)
        client = self._client
        if client is None:
            raise PlatformConnectionError("Binance adapter is not connected")

        params: dict[str, object] = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(limit + 1, MAX_KLINE_LIMIT),
        }
        if product == BinanceProduct.COIN_M:
            params["contractType"] = coin_m_contract_type(entry, symbol=symbol)
        response = await self._run_market_data_request(
            product,
            partial(getattr(client, klines_method(product)), **params),
        )
        if not isinstance(response, list):
            raise PlatformError(
                f"Binance {product.value} klines response is not a list",
                platform_error=response,
            )

        now = datetime.now(UTC)
        candles: list[Candle] = []
        for row in response:
            candle = parse_kline_row(row, timeframe=timeframe, now=now)
            if candle is not None:
                candles.append(candle)
        return candles[-limit:]

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
            key: value for product_balances in responses for key, value in product_balances.items()
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

    # ------------------------------------------------------------------
    # Futures configuration
    # ------------------------------------------------------------------

    def _require_state_store(self) -> StateStore:
        if self._state_store is None:
            raise PlatformError(
                "BinanceAdapter requires the shared StateStore to manage Futures configuration"
            )
        return self._state_store

    async def _resolve_futures_config_target(
        self,
        instrument: Instrument,
    ) -> tuple[BinanceProduct, str]:
        product = product_for_instrument(instrument)
        if product == BinanceProduct.SPOT:
            raise InvalidSymbolError("Binance Futures configuration does not support Spot")
        symbol = await self._resolve_order_symbol(instrument, product)
        return product, symbol

    async def _futures_position_risk(
        self,
        product: BinanceProduct,
        *,
        symbol: str | None = None,
    ) -> list[dict[str, object]]:
        client = self._require_client()
        if product == BinanceProduct.USD_M:
            # USD-M positionRisk v3 omits leverage; v2 includes it.
            params: dict[str, object] = {}
            if symbol is not None:
                params["symbol"] = symbol
            response = await self._run_order_request(
                product,
                partial(
                    client._request_futures_api, "get", "positionRisk", True, version=2, data=params
                ),
            )
        else:
            # COIN-M positionRisk uses 'pair' parameter, not 'symbol'.
            # Convert symbol like 'BTCUSD_PERP' or 'BTCUSD_260626' to pair 'BTCUSD'.
            method = client.futures_coin_position_information
            if symbol is not None:
                # Extract pair by removing contract suffix (_PERP, _YYMMDD, etc.)
                pair = symbol.split("_")[0] if "_" in symbol else symbol
                request = partial(method, pair=pair)
            else:
                request = method
            response = await self._run_order_request(product, request)
        entries = self._response_list(response, product, "position risk")
        if symbol is None:
            return entries
        if any(not entry.get("symbol") for entry in entries):
            raise PlatformError(
                f"Binance {product.value} position risk response has no symbol",
                platform_error=entries,
            )
        return [entry for entry in entries if entry.get("symbol") == symbol]

    @staticmethod
    def _parse_leverage(entries: list[dict[str, object]], product: BinanceProduct) -> int | None:
        values: set[int] = set()
        for entry in entries:
            raw = entry.get("leverage")
            if raw in (None, ""):
                continue
            try:
                value = int(str(raw))
            except ValueError as exc:
                raise PlatformError(
                    f"Malformed Binance {product.value} leverage response",
                    platform_error=entry,
                ) from exc
            if value <= 0:
                raise PlatformError(
                    f"Invalid Binance {product.value} leverage response",
                    platform_error=entry,
                )
            values.add(value)
        if len(values) > 1:
            raise PlatformError(
                f"Binance {product.value} returned conflicting leverage values",
                platform_error=entries,
            )
        return next(iter(values), None)

    @staticmethod
    def _parse_margin_mode(
        entries: list[dict[str, object]],
        product: BinanceProduct,
    ) -> MarginMode | None:
        values: set[MarginMode] = set()
        for entry in entries:
            raw = entry.get("marginType")
            if raw in (None, ""):
                raw = entry.get("isolated")
                if isinstance(raw, bool):
                    raw = "isolated" if raw else "cross"
                elif isinstance(raw, str) and raw.lower() in {"true", "false"}:
                    raw = "isolated" if raw.lower() == "true" else "cross"
            if raw in (None, ""):
                continue
            normalized = str(raw).lower()
            if normalized in {"cross", "crossed"}:
                values.add(MarginMode.CROSS)
            elif normalized == "isolated":
                values.add(MarginMode.ISOLATED)
            else:
                raise PlatformError(
                    f"Unknown Binance {product.value} margin mode {raw!r}",
                    platform_error=entry,
                )
        if len(values) > 1:
            raise PlatformError(
                f"Binance {product.value} returned conflicting margin modes",
                platform_error=entries,
            )
        return next(iter(values), None)

    # ---- Futures account configuration (leverage / margin / position mode) ----

    @staticmethod
    def _leverage_key(product: BinanceProduct, symbol: str) -> str:
        """adapter_config key for stored symbol leverage intent."""
        return f"leverage.{product.value}.{symbol}"

    @staticmethod
    def _margin_mode_key(product: BinanceProduct, symbol: str) -> str:
        """adapter_config key for stored symbol margin-mode intent."""
        return f"margin_mode.{product.value}.{symbol}"

    @staticmethod
    def _leverage_policy_key(
        setting: str,
        product: BinanceProduct,
        symbol: str,
    ) -> str:
        """adapter_config key for one symbol leverage behavior knob."""
        return f"leverage.policy.{setting}.{product.value}.{symbol}"

    @staticmethod
    def _margin_mode_policy_key(
        setting: str,
        product: BinanceProduct,
        symbol: str,
    ) -> str:
        """adapter_config key for one symbol margin-mode behavior knob."""
        return f"margin_mode.policy.{setting}.{product.value}.{symbol}"

    @staticmethod
    def _position_mode_key() -> str:
        """adapter_config key for stored account position-mode intent."""
        return "position_mode"

    @staticmethod
    def _position_mode_policy_key(setting: str) -> str:
        """adapter_config key for one account position-mode behavior knob."""
        return f"position_mode.policy.{setting}"

    async def get_leverage(self, instrument: Instrument) -> int | None:
        """Read the symbol's current leverage for USDⓈ-M or COIN-M Futures."""
        product, symbol = await self._resolve_futures_config_target(instrument)
        return self._parse_leverage(
            await self._futures_position_risk(product, symbol=symbol),
            product,
        )

    async def set_leverage(
        self,
        instrument: Instrument,
        leverage: int,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        async with self._futures_config_lock:
            await self._set_leverage(
                instrument,
                leverage,
                on_drift=on_drift,
                auto_apply_on_connect=auto_apply_on_connect,
            )

    async def _set_leverage(
        self,
        instrument: Instrument,
        leverage: int,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set and persist symbol-scoped Futures leverage.

        A live change is refused while the symbol has open orders or a
        non-flat position. The desired value is persisted only after Binance
        accepts the change.
        """
        if type(leverage) is not int or leverage <= 0:
            raise ValueError("leverage must be a positive integer")
        self._validate_config_policy(on_drift, auto_apply_on_connect)
        self._require_state_store()
        product, symbol = await self._resolve_futures_config_target(instrument)
        current = await self.get_leverage(instrument)
        if current != leverage:
            await self._assert_futures_config_change_safe(
                product=product,
                symbol=symbol,
                setting="leverage",
            )
            client = self._require_client()
            method_name = (
                "futures_change_leverage"
                if product == BinanceProduct.USD_M
                else "futures_coin_change_leverage"
            )
            try:
                response = await self._run_order_request(
                    product,
                    partial(getattr(client, method_name), symbol=symbol, leverage=leverage),
                )
            except PlatformError as exc:
                if self._is_ambiguous_config_error(exc):
                    self._report_config_failure(
                        "leverage",
                        instrument,
                        str(leverage),
                        exc,
                    )
                raise
            except TimeoutError as exc:
                self._report_config_failure("leverage", instrument, str(leverage), exc)
                raise
            if not isinstance(response, dict):
                error = PlatformError(
                    f"Binance {product.value} leverage response is not an object",
                    platform_error=response,
                )
                self._report_config_failure("leverage", instrument, str(leverage), error)
                raise error
            try:
                returned = self._parse_leverage([response], product)
            except PlatformError as exc:
                self._report_config_failure("leverage", instrument, str(leverage), exc)
                raise
            if returned is not None and returned != leverage:
                error = PlatformError(
                    f"Binance applied leverage {returned}, expected {leverage}",
                    platform_error=response,
                )
                self._report_config_failure("leverage", instrument, str(leverage), error)
                raise error
        await self._persist_futures_setting(
            "leverage",
            instrument,
            str(leverage),
            [
                (self._leverage_key(product, symbol), str(leverage)),
                (self._leverage_policy_key("on_drift", product, symbol), on_drift),
                (
                    self._leverage_policy_key("auto_apply_on_connect", product, symbol),
                    "1" if auto_apply_on_connect else "0",
                ),
            ],
        )
        self._publish_setting_applied("leverage", instrument, str(leverage))

    async def remove_leverage(self, instrument: Instrument) -> None:
        async with self._futures_config_lock:
            await self._remove_leverage(instrument)

    async def _remove_leverage(self, instrument: Instrument) -> None:
        """Stop managing leverage without changing the Binance-side value."""
        product, symbol = await self._resolve_futures_config_target(instrument)
        store = self._require_state_store()
        await store.delete_adapter_config(self._leverage_key(product, symbol))
        await store.delete_adapter_config(self._leverage_policy_key("on_drift", product, symbol))
        await store.delete_adapter_config(
            self._leverage_policy_key("auto_apply_on_connect", product, symbol)
        )

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Read symbol-scoped Futures margin mode when Binance returns it."""
        product, symbol = await self._resolve_futures_config_target(instrument)
        return self._parse_margin_mode(
            await self._futures_position_risk(product, symbol=symbol),
            product,
        )

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        async with self._futures_config_lock:
            await self._set_margin_mode(
                instrument,
                mode,
                on_drift=on_drift,
                auto_apply_on_connect=auto_apply_on_connect,
            )

    async def _set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set and persist symbol-scoped Cross or Isolated margin mode.

        Binance's REST parameter values are ``CROSSED`` and ``ISOLATED``.
        Changes are refused while the symbol has open orders or a non-flat
        position.
        """
        try:
            mode = MarginMode(mode)
        except ValueError as exc:
            raise ValueError(f"unsupported Binance margin mode {mode!r}") from exc
        self._validate_config_policy(on_drift, auto_apply_on_connect)
        self._require_state_store()
        product, symbol = await self._resolve_futures_config_target(instrument)
        current = await self.get_margin_mode(instrument)
        if current is not mode:
            await self._assert_futures_config_change_safe(
                product=product,
                symbol=symbol,
                setting="margin mode",
            )
            client = self._require_client()
            method_name = (
                "futures_change_margin_type"
                if product == BinanceProduct.USD_M
                else "futures_coin_change_margin_type"
            )
            unchanged = False
            try:
                response = await self._run_order_request(
                    product,
                    partial(
                        getattr(client, method_name),
                        symbol=symbol,
                        marginType="CROSSED" if mode is MarginMode.CROSS else "ISOLATED",
                    ),
                )
            except PlatformError as exc:
                if not self._is_binance_error_code(exc, -4046):
                    if self._is_ambiguous_config_error(exc):
                        self._report_config_failure("margin_mode", instrument, mode.value, exc)
                    raise
                response = {"code": -4046}
                unchanged = True
            except TimeoutError as exc:
                self._report_config_failure("margin_mode", instrument, mode.value, exc)
                raise
            if not isinstance(response, dict):
                error = PlatformError(
                    f"Binance {product.value} margin-mode response is not an object",
                    platform_error=response,
                )
                self._report_config_failure("margin_mode", instrument, mode.value, error)
                raise error
            try:
                applied_mode = await self.get_margin_mode(instrument)
            except Exception as exc:
                self._report_config_failure("margin_mode", instrument, mode.value, exc)
                raise
            if applied_mode is not mode:
                error = PlatformError(
                    f"Binance {product.value} margin-mode read-back did not confirm {mode.value}",
                    platform_error=response,
                )
                self._report_config_failure("margin_mode", instrument, mode.value, error)
                raise error
            if not unchanged:
                self._event_bus.publish(
                    FuturesMarginModeChangedEvent(
                        event_id=str(uuid7()),
                        timestamp=datetime.now(UTC),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        instrument=instrument,
                        previous=current,
                        current=mode,
                    )
                )
        await self._persist_futures_setting(
            "margin_mode",
            instrument,
            mode.value,
            [
                (self._margin_mode_key(product, symbol), mode.value),
                (self._margin_mode_policy_key("on_drift", product, symbol), on_drift),
                (
                    self._margin_mode_policy_key("auto_apply_on_connect", product, symbol),
                    "1" if auto_apply_on_connect else "0",
                ),
            ],
        )
        self._publish_setting_applied("margin_mode", instrument, mode.value)

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        async with self._futures_config_lock:
            await self._remove_margin_mode(instrument)

    async def _remove_margin_mode(self, instrument: Instrument) -> None:
        """Stop managing margin mode without changing the Binance-side value."""
        product, symbol = await self._resolve_futures_config_target(instrument)
        store = self._require_state_store()
        await store.delete_adapter_config(self._margin_mode_key(product, symbol))
        await store.delete_adapter_config(self._margin_mode_policy_key("on_drift", product, symbol))
        await store.delete_adapter_config(
            self._margin_mode_policy_key("auto_apply_on_connect", product, symbol)
        )

    @staticmethod
    def _parse_position_mode(response: object) -> PositionMode:
        if not isinstance(response, dict):
            raise PlatformError(
                "Binance position-mode response is not an object",
                platform_error=response,
            )
        raw = response.get("dualSidePosition")
        if isinstance(raw, bool):
            hedge = raw
        elif isinstance(raw, str) and raw.lower() in {"true", "false"}:
            hedge = raw.lower() == "true"
        else:
            raise PlatformError(
                "Binance position-mode response has no valid dualSidePosition value",
                platform_error=response,
            )
        return PositionMode.HEDGE if hedge else PositionMode.ONE_WAY

    async def get_position_mode(self) -> PositionMode:
        """Read the account-wide Binance Futures One-way or Hedge mode.

        USD-M and COIN-M expose separate ``positionSide/dual`` endpoints, but
        the platform treats position mode as a single account setting. Both
        endpoints are read and must agree; divergence is surfaced as an error
        rather than silently picking one product's value.
        """
        client = self._require_client()
        usd_m_response, coin_m_response = await asyncio.gather(
            self._run_order_request(
                BinanceProduct.USD_M,
                client.futures_get_position_mode,
            ),
            self._run_order_request(
                BinanceProduct.COIN_M,
                client.futures_coin_get_position_mode,
            ),
        )
        usd_m_mode = self._parse_position_mode(usd_m_response)
        coin_m_mode = self._parse_position_mode(coin_m_response)
        if usd_m_mode is not coin_m_mode:
            raise PlatformError(
                "Binance USD-M and COIN-M position modes disagree",
                platform_error={
                    "usd_m": usd_m_response,
                    "coin_m": coin_m_response,
                },
            )
        return usd_m_mode

    @staticmethod
    def _is_binance_error_code(error: PlatformError, code: int) -> bool:
        platform_error = error.platform_error
        return isinstance(platform_error, dict) and platform_error.get("code") == code

    @staticmethod
    def _is_ambiguous_config_error(error: PlatformError) -> bool:
        if isinstance(error, PlatformConnectionError):
            return True
        platform_error = error.platform_error
        return (
            isinstance(platform_error, dict)
            and platform_error.get("exception") == "BinanceRequestException"
        )

    async def set_position_mode(
        self,
        mode: PositionMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        async with self._futures_config_lock:
            await self._set_position_mode(
                mode,
                on_drift=on_drift,
                auto_apply_on_connect=auto_apply_on_connect,
            )

    async def _set_position_mode(
        self,
        mode: PositionMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "halt",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set account-wide Futures position mode and persist its intent.

        Position mode is not stored per symbol. Binance changes this setting
        for every symbol; therefore this adapter also refuses a change while
        either Futures product has open orders or a non-flat position.
        """
        try:
            mode = PositionMode(mode)
        except ValueError as exc:
            raise ValueError(f"unsupported Binance position mode {mode!r}") from exc
        self._validate_config_policy(on_drift, auto_apply_on_connect)
        self._require_state_store()
        current = await self.get_position_mode()
        if current is not mode:
            await self._assert_futures_config_change_safe(
                setting="position mode",
            )
            client = self._require_client()
            unchanged = False
            try:
                response = await self._run_order_request(
                    BinanceProduct.USD_M,
                    partial(
                        client.futures_change_position_mode,
                        dualSidePosition=mode is PositionMode.HEDGE,
                    ),
                )
            except PlatformError as exc:
                if not self._is_binance_error_code(exc, -4059):
                    if self._is_ambiguous_config_error(exc):
                        self._report_config_failure("position_mode", None, mode.value, exc)
                    raise
                response = {"code": -4059}
                unchanged = True
            except TimeoutError as exc:
                self._report_config_failure("position_mode", None, mode.value, exc)
                raise
            if not isinstance(response, dict):
                error = PlatformError(
                    "Binance position-mode change response is not an object",
                    platform_error=response,
                )
                self._report_config_failure("position_mode", None, mode.value, error)
                raise error
            try:
                applied_mode = await self.get_position_mode()
            except Exception as exc:
                self._report_config_failure("position_mode", None, mode.value, exc)
                raise
            if applied_mode is not mode:
                error = PlatformError(
                    f"Binance position-mode read-back did not confirm {mode.value}",
                    platform_error=response,
                )
                self._report_config_failure("position_mode", None, mode.value, error)
                raise error
            if not unchanged:
                self._event_bus.publish(
                    FuturesPositionModeChangedEvent(
                        event_id=str(uuid7()),
                        timestamp=datetime.now(UTC),
                        adapter_name=self.platform_name,
                        account_id=self.account_id,
                        correlation_id=None,
                        previous=current,
                        current=mode,
                    )
                )
        await self._persist_futures_setting(
            "position_mode",
            None,
            mode.value,
            [
                (self._position_mode_key(), mode.value),
                (self._position_mode_policy_key("on_drift"), on_drift),
                (
                    self._position_mode_policy_key("auto_apply_on_connect"),
                    "1" if auto_apply_on_connect else "0",
                ),
            ],
        )
        self._publish_setting_applied("position_mode", None, mode.value)

    async def remove_position_mode(self) -> None:
        async with self._futures_config_lock:
            await self._remove_position_mode()

    async def _remove_position_mode(self) -> None:
        """Stop managing position mode without changing Binance's live value."""
        store = self._require_state_store()
        await store.delete_adapter_config(self._position_mode_key())
        await store.delete_adapter_config(self._position_mode_policy_key("on_drift"))
        await store.delete_adapter_config(self._position_mode_policy_key("auto_apply_on_connect"))

    @staticmethod
    def _validate_config_policy(
        on_drift: str,
        auto_apply_on_connect: bool,
    ) -> None:
        if on_drift not in _ON_DRIFT_VALUES:
            raise ValueError(f"on_drift must be one of {sorted(_ON_DRIFT_VALUES)}")
        if type(auto_apply_on_connect) is not bool:
            raise TypeError("auto_apply_on_connect must be a bool")

    async def _persist_futures_setting(
        self,
        setting: Literal["leverage", "margin_mode", "position_mode"],
        instrument: Instrument | None,
        value: str,
        rows: list[tuple[str, str]],
    ) -> None:
        store = self._require_state_store()
        try:
            for key, row_value in rows:
                await store.set_adapter_config(key, row_value)
        except Exception as exc:
            self._publish_setting_failed(setting, instrument, value, exc)
            scope: Literal["instrument", "account"] = (
                "account" if instrument is None else "instrument"
            )
            self._enter_config_halt(
                scope,
                instrument,
                f"Binance {setting} changed but its intent could not be persisted: {exc}",
            )
            raise PlatformError(
                f"Binance {setting} changed but its intent could not be fully persisted"
            ) from exc

    async def _assert_futures_config_change_safe(
        self,
        *,
        setting: str,
        product: BinanceProduct | None = None,
        symbol: str | None = None,
    ) -> None:
        client = self._require_client()
        products = (product,) if product is not None else _FUTURES_PRODUCTS
        for current_product in products:
            order_method = (
                "futures_get_open_orders"
                if current_product == BinanceProduct.USD_M
                else "futures_coin_get_open_orders"
            )
            request = getattr(client, order_method)
            if symbol is not None:
                response = await self._run_order_request(
                    current_product,
                    partial(request, symbol=symbol),
                )
            else:
                response = await self._run_order_request(current_product, request)
            open_orders = self._response_list(response, current_product, "open orders")
            if open_orders:
                product_label = "USDⓈ-M" if current_product == BinanceProduct.USD_M else "COIN-M"
                raise PlatformError(
                    f"Cannot change Binance {setting} while {product_label} open orders exist"
                )

            if current_product == BinanceProduct.USD_M:
                algo_request = client.futures_get_open_algo_orders
                if symbol is not None:
                    algo_response = await self._run_order_request(
                        current_product,
                        partial(algo_request, symbol=symbol),
                    )
                else:
                    algo_response = await self._run_order_request(
                        current_product,
                        algo_request,
                    )
                algo_orders = self._response_list(
                    algo_response,
                    current_product,
                    "open algo orders",
                )
                if algo_orders:
                    raise PlatformError(
                        f"Cannot change Binance {setting} while USDⓈ-M open algo orders exist"
                    )

            positions = await self._futures_position_risk(
                current_product,
                symbol=symbol if current_product == product else None,
            )
            for entry in positions:
                raw_quantity = entry.get("positionAmt")
                if raw_quantity in (None, ""):
                    raise PlatformError(
                        f"Binance {current_product.value} position risk entry has no positionAmt",
                        platform_error=entry,
                    )
                try:
                    quantity = Decimal(str(raw_quantity))
                except InvalidOperation as exc:
                    raise PlatformError(
                        f"Malformed Binance {current_product.value} positionAmt",
                        platform_error=entry,
                    ) from exc
                if not quantity.is_finite():
                    raise PlatformError(
                        f"Malformed Binance {current_product.value} positionAmt",
                        platform_error=entry,
                    )
                if quantity != 0:
                    product_label = (
                        "USDⓈ-M" if current_product == BinanceProduct.USD_M else "COIN-M"
                    )
                    raise PlatformError(
                        f"Cannot change Binance {setting} while a {product_label} position is open"
                    )

    async def _list_futures_config_rows(self) -> dict[str, str]:
        """Return all persisted Futures config rows across the kind prefixes."""
        store = self._require_state_store()
        rows: dict[str, str] = {}
        for prefix in _CONFIG_KIND_PREFIXES:
            rows.update(await store.list_adapter_config(prefix))
        return rows

    async def _reapply_stored_intent(self) -> None:
        """Safely apply eligible persisted Futures configuration after connect."""
        store = self._state_store
        if store is None:
            return
        rows = await self._list_futures_config_rows()

        for key, value in rows.items():
            target = self._stored_symbol_target(key, "leverage.")
            if target is not None:
                product, symbol, instrument = target
                auto_apply = await self._stored_auto_apply(
                    self._leverage_policy_key("auto_apply_on_connect", product, symbol)
                )
                if not auto_apply:
                    continue
                try:
                    leverage = int(value)
                    on_drift = await self._stored_on_drift(
                        self._leverage_policy_key("on_drift", product, symbol)
                    )
                    await self.set_leverage(
                        instrument,
                        leverage,
                        on_drift=on_drift,
                        auto_apply_on_connect=True,
                    )
                except Exception as exc:
                    logger.exception("Could not reapply stored Binance leverage for %s", symbol)
                    self._publish_setting_failed("leverage", instrument, value, exc)
                    self._enter_config_halt(
                        "instrument",
                        instrument,
                        f"Stored leverage could not be applied for {symbol}: {exc}",
                    )
                continue

            target = self._stored_symbol_target(key, "margin_mode.")
            if target is not None:
                product, symbol, instrument = target
                auto_apply = await self._stored_auto_apply(
                    self._margin_mode_policy_key("auto_apply_on_connect", product, symbol)
                )
                if not auto_apply:
                    continue
                try:
                    margin_mode = MarginMode(value)
                    on_drift = await self._stored_on_drift(
                        self._margin_mode_policy_key("on_drift", product, symbol)
                    )
                    await self.set_margin_mode(
                        instrument,
                        margin_mode,
                        on_drift=on_drift,
                        auto_apply_on_connect=True,
                    )
                except Exception as exc:
                    logger.exception(
                        "Could not reapply stored Binance margin mode for %s",
                        symbol,
                    )
                    self._publish_setting_failed("margin_mode", instrument, value, exc)
                    self._enter_config_halt(
                        "instrument",
                        instrument,
                        f"Stored margin mode could not be applied for {symbol}: {exc}",
                    )

        stored_mode = rows.get(self._position_mode_key())
        if stored_mode is not None and await self._stored_auto_apply(
            self._position_mode_policy_key("auto_apply_on_connect")
        ):
            try:
                position_mode = PositionMode(stored_mode)
                on_drift = await self._stored_on_drift(self._position_mode_policy_key("on_drift"))
                await self.set_position_mode(
                    position_mode,
                    on_drift=on_drift,
                    auto_apply_on_connect=True,
                )
            except Exception as exc:
                logger.exception("Could not reapply stored Binance position mode")
                self._publish_setting_failed("position_mode", None, stored_mode, exc)
                self._enter_config_halt(
                    "account",
                    None,
                    f"Stored position mode could not be applied: {exc}",
                )

    def _stored_symbol_target(
        self,
        key: str,
        setting_prefix: str,
    ) -> tuple[BinanceProduct, str, Instrument] | None:
        prefix = setting_prefix
        if not key.startswith(prefix) or ".policy." in key:
            return None
        remainder = key.removeprefix(prefix)
        product_value, separator, symbol = remainder.partition(".")
        if not separator or not symbol:
            logger.error("Ignoring malformed Binance Futures config key %r", key)
            return None
        try:
            product = BinanceProduct(product_value)
        except ValueError:
            logger.error("Ignoring Binance Futures config key with unknown product %r", key)
            return None
        if product not in _FUTURES_PRODUCTS:
            logger.error("Ignoring non-Futures Binance config key %r", key)
            return None
        instrument = self._symbol_instruments.get((product, symbol))
        if instrument is None:
            logger.warning(
                "Keeping stored Binance Futures intent for unknown or delisted %s symbol %s",
                product.value,
                symbol,
            )
            return None
        return product, symbol, instrument

    async def _stored_auto_apply(self, key: str) -> bool:
        store = self._require_state_store()
        raw = await store.get_adapter_config(key)
        if raw is None:
            return True
        if raw not in {"0", "1"}:
            raise PlatformError(f"Invalid stored Binance auto-apply policy {key!r}={raw!r}")
        return raw == "1"

    async def _stored_on_drift(
        self,
        key: str,
    ) -> Literal["reapply", "notify", "halt"]:
        store = self._require_state_store()
        raw = await store.get_adapter_config(key)
        if raw is None:
            return "halt"
        if raw not in _ON_DRIFT_VALUES:
            raise PlatformError(f"Invalid stored Binance drift policy {key!r}={raw!r}")
        return raw  # type: ignore[return-value]

    async def reconcile_user_intent(self) -> None:
        """Compare persisted Futures configuration with Binance and resolve drift."""
        store = self._state_store
        if store is None:
            return
        rows = await self._list_futures_config_rows()
        unresolved_instruments: set[Instrument] = set()
        configured_instruments: set[Instrument] = set()
        account_configured = self._position_mode_key() in rows
        account_unresolved = account_configured

        for key, value in rows.items():
            target = self._stored_symbol_target(key, "leverage.")
            if target is not None:
                product, symbol, instrument = target
                configured_instruments.add(instrument)
                try:
                    stored = int(value)
                    platform = await self.get_leverage(instrument)
                    if platform is None:
                        unresolved_instruments.add(instrument)
                        continue
                    if platform == stored:
                        continue
                    policy = await self._stored_on_drift(
                        self._leverage_policy_key("on_drift", product, symbol)
                    )
                    resolved = await self._reconcile_setting_drift(
                        "leverage",
                        instrument,
                        str(stored),
                        str(platform),
                        policy,
                    )
                    if not resolved:
                        unresolved_instruments.add(instrument)
                except Exception as exc:
                    logger.exception("Binance leverage reconciliation failed for %s", symbol)
                    unresolved_instruments.add(instrument)
                    self._publish_setting_failed("leverage", instrument, value, exc)
                    self._enter_config_halt(
                        "instrument",
                        instrument,
                        f"Could not reconcile stored leverage for {symbol}: {exc}",
                    )
                continue

            target = self._stored_symbol_target(key, "margin_mode.")
            if target is not None:
                product, symbol, instrument = target
                configured_instruments.add(instrument)
                try:
                    stored_margin_mode = MarginMode(value)
                    platform_margin_mode = await self.get_margin_mode(instrument)
                    if platform_margin_mode is None:
                        unresolved_instruments.add(instrument)
                        continue
                    if platform_margin_mode is stored_margin_mode:
                        continue
                    policy = await self._stored_on_drift(
                        self._margin_mode_policy_key("on_drift", product, symbol)
                    )
                    resolved = await self._reconcile_setting_drift(
                        "margin_mode",
                        instrument,
                        stored_margin_mode.value,
                        platform_margin_mode.value,
                        policy,
                    )
                    if not resolved:
                        unresolved_instruments.add(instrument)
                except Exception as exc:
                    logger.exception("Binance margin-mode reconciliation failed for %s", symbol)
                    unresolved_instruments.add(instrument)
                    self._publish_setting_failed("margin_mode", instrument, value, exc)
                    self._enter_config_halt(
                        "instrument",
                        instrument,
                        f"Could not reconcile stored margin mode for {symbol}: {exc}",
                    )

        stored_position_mode = rows.get(self._position_mode_key())
        if stored_position_mode is not None:
            try:
                expected_mode = PositionMode(stored_position_mode)
                platform_position_mode = await self.get_position_mode()
                if platform_position_mode is not expected_mode:
                    policy = await self._stored_on_drift(self._position_mode_policy_key("on_drift"))
                    account_unresolved = not await self._reconcile_setting_drift(
                        "position_mode",
                        None,
                        expected_mode.value,
                        platform_position_mode.value,
                        policy,
                    )
                else:
                    account_unresolved = False
            except Exception as exc:
                logger.exception("Binance position-mode reconciliation failed")
                account_unresolved = True
                self._publish_setting_failed(
                    "position_mode",
                    None,
                    stored_position_mode,
                    exc,
                )
                self._enter_config_halt(
                    "account",
                    None,
                    f"Could not reconcile stored position mode: {exc}",
                )

        if account_configured and not account_unresolved:
            self._try_clear_config_halt("account", None)
        for instrument in configured_instruments.intersection(
            self._instruments_with_config_halts()
        ):
            if instrument not in unresolved_instruments:
                self._try_clear_config_halt("instrument", instrument)

    async def _reconcile_setting_drift(
        self,
        setting: Literal["leverage", "margin_mode", "position_mode"],
        instrument: Instrument | None,
        stored: str,
        platform: str,
        policy: Literal["reapply", "notify", "halt"],
    ) -> bool:
        action: Literal["reapplied", "notified", "halted"]
        if policy == "notify":
            action = "notified"
        elif policy == "halt":
            action = "halted"
        else:
            action = "reapplied"

        self._event_bus.publish(
            FuturesSettingDriftEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                setting=setting,
                instrument=instrument,
                stored_value=stored,
                platform_value=platform,
                action_taken=action,
            )
        )
        if policy == "halt":
            scope: Literal["instrument", "account"] = (
                "account" if instrument is None else "instrument"
            )
            self._enter_config_halt(
                scope,
                instrument,
                f"Binance {setting} drift: stored={stored}, platform={platform}",
            )
            return False
        if policy == "notify":
            return False

        try:
            if setting == "position_mode":
                auto_apply = await self._stored_auto_apply(
                    self._position_mode_policy_key("auto_apply_on_connect")
                )
            else:
                if instrument is None:
                    raise PlatformError(f"{setting} drift has no instrument")
                product, symbol = await self._resolve_futures_config_target(instrument)
                policy_key = (
                    self._leverage_policy_key("auto_apply_on_connect", product, symbol)
                    if setting == "leverage"
                    else self._margin_mode_policy_key("auto_apply_on_connect", product, symbol)
                )
                auto_apply = await self._stored_auto_apply(policy_key)
            if setting == "leverage":
                if instrument is None:
                    raise PlatformError("Leverage drift has no instrument")
                await self.set_leverage(
                    instrument,
                    int(stored),
                    on_drift=policy,
                    auto_apply_on_connect=auto_apply,
                )
                actual = await self.get_leverage(instrument)
                resolved = actual is not None and str(actual) == stored
            elif setting == "margin_mode":
                if instrument is None:
                    raise PlatformError("Margin-mode drift has no instrument")
                await self.set_margin_mode(
                    instrument,
                    MarginMode(stored),
                    on_drift=policy,
                    auto_apply_on_connect=auto_apply,
                )
                actual_mode = await self.get_margin_mode(instrument)
                resolved = actual_mode is not None and actual_mode.value == stored
            else:
                await self.set_position_mode(
                    PositionMode(stored),
                    on_drift=policy,
                    auto_apply_on_connect=auto_apply,
                )
                resolved = (await self.get_position_mode()).value == stored
        except Exception as exc:
            self._publish_setting_failed(setting, instrument, stored, exc)
            scope = "account" if instrument is None else "instrument"
            self._enter_config_halt(
                scope,
                instrument,
                f"Could not reapply Binance {setting} intent: {exc}",
            )
            return False

        if not resolved:
            scope = "account" if instrument is None else "instrument"
            self._enter_config_halt(
                scope,
                instrument,
                f"Binance {setting} read-back does not match stored intent {stored}",
            )
        return resolved

    def _publish_setting_applied(
        self,
        setting: Literal["leverage", "margin_mode", "position_mode"],
        instrument: Instrument | None,
        value: str,
    ) -> None:
        self._event_bus.publish(
            FuturesSettingAppliedEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                setting=setting,
                instrument=instrument,
                value=value,
            )
        )

    def _publish_setting_failed(
        self,
        setting: Literal["leverage", "margin_mode", "position_mode"],
        instrument: Instrument | None,
        value: str,
        error: Exception,
    ) -> None:
        self._event_bus.publish(
            FuturesSettingApplyFailedEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                setting=setting,
                instrument=instrument,
                value=value,
                reason=str(error),
            )
        )

    def _report_config_failure(
        self,
        setting: Literal["leverage", "margin_mode", "position_mode"],
        instrument: Instrument | None,
        value: str,
        error: Exception,
    ) -> None:
        self._publish_setting_failed(setting, instrument, value, error)
        scope: Literal["instrument", "account"] = "account" if instrument is None else "instrument"
        self._enter_config_halt(
            scope,
            instrument,
            f"Binance {setting} could not be confirmed or persisted: {error}",
        )

    def _enter_config_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        detail: str,
    ) -> None:
        halt_machine = self._halt_machine
        if halt_machine is None:
            logger.error("Cannot halt Binance %s configuration drift: %s", scope, detail)
            return
        if not halt_machine.enter_halt(
            scope,
            instrument,
            _CONFIG_DRIFT_REASON,
            detail,
        ):
            return
        self._event_bus.publish(
            HaltEnteredEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                scope=scope,
                instrument=instrument,
                reason=_CONFIG_DRIFT_REASON,
                detail=detail,
            )
        )

    def _instruments_with_config_halts(self) -> set[Instrument]:
        halt_machine = self._halt_machine
        if halt_machine is None:
            return set()
        return {
            entry.instrument
            for entry in halt_machine.active_halts()
            if entry.scope == "instrument"
            and entry.instrument is not None
            and entry.reason == _CONFIG_DRIFT_REASON
        }

    def _try_clear_config_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
    ) -> None:
        halt_machine = self._halt_machine
        if halt_machine is None:
            return
        entry = next(
            (
                active
                for active in halt_machine.active_halts()
                if active.scope == scope
                and active.instrument == instrument
                and active.reason == _CONFIG_DRIFT_REASON
            ),
            None,
        )
        if entry is None or not halt_machine.try_clear_halt(
            scope,
            instrument=instrument,
            reconciliation_is_clean=True,
        ):
            return
        self._event_bus.publish(
            HaltClearedEvent(
                event_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                scope=scope,
                instrument=instrument,
                cleared_by="automatic",
            )
        )

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
            self._platform_order_keys[(product, reference.symbol, order.platform_order_id)] = (
                order.client_order_id
            )
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
        """Retain the core state store for durable Binance Futures intent."""
        self._state_store = state_store

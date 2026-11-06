"""Binance adapter implementation.

The adapter will translate the core's product-neutral contract into Binance
Spot, USDⓈ-M Futures, and COIN-M Futures APIs. Symbol and instrument metadata
mapping, REST client lifecycle, and initial order operations are implemented.
Account streams, reconciliation, and market data are handled separately.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from aiohttp import ClientError
from binance import AsyncClient  # type: ignore[import-untyped]
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
from unified_trading_execution.binance.symbols import (
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
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    NativeTrailingRequest,
    NativeTrailingResult,
    TrailingBasis,
)
from unified_trading_execution.types.enums import OrderStatus, OrderType
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


class BinanceAdapter(Adapter):
    """Partially implemented adapter surface for Binance.

    One ``AsyncClient`` instance is intended to provide asynchronous REST
    access for the selected Binance products. Product selection must remain
    explicit when resolving symbols and endpoint families: Spot, USDⓈ-M, and
    COIN-M are not interchangeable markets. ``BinanceConfig.testnet`` and
    ``BinanceConfig.demo`` select distinct sandbox environments; both false
    selects production. The REST client is created by :meth:`connect`;
    private account streams are added separately.

    Args:
        config: Immutable API credentials and environment selection.
        event_bus: Optional core event bus. When omitted, a local bus is
            created for direct adapter construction. Engine construction
            replaces it with the core-owned bus via ``attach_event_bus``.

    Attributes:
        _client: The async REST client, or ``None`` while disconnected.
        _socket_manager: Reserved for public/private stream management.
        _stream_tasks: Background stream tasks keyed by stream purpose.
        _order_refs: Product/symbol routing references for orders placed during
            this adapter session.
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

        self._order_refs: dict[str, OrderReference] = {}
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
        return self._connected

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the async REST client using the configured Binance environment.

        The pinned SDK factory validates connectivity with Spot ping/server
        time requests. All three product URL builders are then checked before
        the client is retained, preventing an incorrect or live fallback for a
        sandbox configuration. Private streams are initialized separately.
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
            self._publish_connection_state(True)

    async def disconnect(self) -> None:
        """Close the REST client and publish a disconnected state.

        This method does not cancel or mutate exchange-side orders or
        protection. Stream workers are owned by the separate account-stream
        lifecycle.
        """
        async with self._lifecycle_lock:
            client = self._client
            if client is None:
                if self._connected:
                    raise PlatformConnectionError(
                        "Binance adapter is marked connected without an active REST client"
                    )
                return

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

            was_connected = self._connected
            self._client = None
            self._connected = False
            if was_connected:
                self._publish_connection_state(False)

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
        reference = self._order_refs.get(client_order_id)
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
                return current

            current = await self._wait_for_conditional_terminal_order(client_order_id)
            if current is None:
                raise PlatformError(
                    f"Binance accepted cancellation for conditional order {client_order_id}, "
                    "but its terminal state could not be verified"
                )
            return current

        response = await self._run_order_request(
            reference.product,
            lambda: getattr(client, method_names[reference.product])(**params),
        )
        return parse_order_result(
            _order_response(response),
            client_order_id=client_order_id,
            conditional=reference.conditional,
        )

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query a known order; return ``None`` only for confirmed absence."""
        client = self._require_client()
        reference = self._order_refs.get(client_order_id)
        if reference is None:
            raise PlatformError(
                f"Cannot resolve Binance product and symbol for order {client_order_id}; "
                "routing references are available only for orders placed in this adapter session"
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
                return resolved
        return parse_order_result(
            response_entry,
            client_order_id=client_order_id,
            conditional=reference.conditional,
        )

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

        client = self._client
        if client is None:
            raise PlatformConnectionError(
                "Binance adapter must be connected before fetching instrument metadata"
            )

        entries = self._exchange_info_cache.get(product)
        if entries is None:
            exchange_info_method_names = {
                BinanceProduct.SPOT: "get_exchange_info",
                BinanceProduct.USD_M: "futures_exchange_info",
                BinanceProduct.COIN_M: "futures_coin_exchange_info",
            }
            exchange_info = getattr(client, exchange_info_method_names[product])
            try:
                response = await exchange_info()
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
            if not isinstance(response, dict):
                raise PlatformError(
                    f"Binance {product.value} exchange-info response is not an object",
                    platform_error=response,
                )
            response_symbols = response.get("symbols")
            if not isinstance(response_symbols, list):
                raise PlatformError(
                    f"Binance {product.value} exchange-info response has no symbols list",
                    platform_error=response,
                )
            entries = response_symbols
            self._exchange_info_cache[product] = entries

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
        raise NotImplementedError("Binance adapter scaffold: fetch_positions is not implemented")

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances keyed by asset/currency."""
        raise NotImplementedError("Binance adapter scaffold: fetch_balances is not implemented")

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch user-owned open orders keyed by core client order ID."""
        raise NotImplementedError("Binance adapter scaffold: fetch_open_orders is not implemented")

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent executions keyed by core client order ID."""
        raise NotImplementedError("Binance adapter scaffold: fetch_fills is not implemented")

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

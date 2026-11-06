"""Binance adapter implementation.

The adapter will translate the core's product-neutral contract into Binance
Spot, USDⓈ-M Futures, and COIN-M Futures APIs. Symbol and instrument metadata
mapping and REST client lifecycle are implemented; trading operations remain
deferred to their respective feature branches.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Mapping
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
from unified_trading_execution.binance.rate_limits import BinanceRateLimitTracker
from unified_trading_execution.binance.symbols import (
    parse_instrument_spec,
    product_for_instrument,
    resolve_binance_symbol,
)
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    PlatformError,
)
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    NativeTrailingRequest,
    NativeTrailingResult,
    TrailingBasis,
)
from unified_trading_execution.types.enums import OrderType
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
        _client_order_ids: Local mapping between core client IDs and native
            order IDs. It is not authoritative across process restarts.
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

        self._client_order_ids: dict[str, str] = {}
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

            try:
                client = await AsyncClient.create(
                    api_key=self._config.api_key,
                    api_secret=self._config.api_secret,
                    testnet=self._config.testnet,
                    demo=self._config.demo,
                )
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
        """Translate and submit a core-validated Spot or Futures order.

        The implementation must select the correct product API, preserve
        Decimal precision and client order identity, translate venue errors,
        and resolve ambiguous submission outcomes before any retry. Unsupported
        product/order combinations must raise the core's explicit error rather
        than being approximated.
        """
        raise NotImplementedError("Binance adapter scaffold: place_order is not implemented")

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify a native order when the selected Binance product permits it.

        Unsupported modification fields or product combinations must fail
        explicitly. Never emulate an unsupported modify as cancel-and-replace
        without a separately verified safe contract.
        """
        raise NotImplementedError("Binance adapter scaffold: modify_order is not implemented")

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an order by its core client ID.

        The implementation must resolve the ID in the correct product scope
        and raise the unified not-found error when Binance confirms the order
        is absent.
        """
        raise NotImplementedError("Binance adapter scaffold: cancel_order is not implemented")

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query a native order by core client ID.

        Return ``None`` only when the venue confirms no matching order exists;
        transport and authentication failures must propagate as translated
        exceptions.
        """
        raise NotImplementedError(
            "Binance adapter scaffold: get_order_by_client_id is not implemented"
        )

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
        """Return only the order types implemented consistently by product.

        Capability claims are intentionally withheld by this scaffold. The
        completed implementation must not infer that a portable order type is
        supported merely because a similarly named Binance order exists.
        """
        raise NotImplementedError(
            "Binance adapter scaffold: supported_order_types is not implemented"
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

"""Binance adapter contract scaffold.

The adapter will translate the core's product-neutral contract into Binance
Spot, USDⓈ-M Futures, and COIN-M Futures APIs. This scaffold deliberately does
not construct an SDK client, connect to an exchange, or claim support for any
exchange operation. Each operation raises ``NotImplementedError`` until its
implementation branch is completed.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime
from typing import TYPE_CHECKING

from unified_trading_execution.adapter import (
    Adapter,
    MarketDataCapability,
    PositionProtectionCapability,
    RateLimits,
    TrailingCapability,
)
from unified_trading_execution.binance.config import BinanceConfig
from unified_trading_execution.events import EventBus
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
    from binance import AsyncClient, BinanceSocketManager  # type: ignore[import-untyped]

    from unified_trading_execution.state.halt import HaltStateMachine
    from unified_trading_execution.state.store import StateStore


class BinanceAdapter(Adapter):
    """Concrete but unimplemented adapter surface for Binance.

    One ``AsyncClient`` instance is intended to provide asynchronous REST
    access for the selected Binance products. Product selection must remain
    explicit when resolving symbols and endpoint families: Spot, USDⓈ-M, and
    COIN-M are not interchangeable markets. ``BinanceConfig.testnet`` and
    ``BinanceConfig.demo`` select distinct sandbox environments; both false
    selects production. Client creation and endpoint routing are deferred to
    the client branch.

    Args:
        config: Immutable API credentials and environment selection.
        event_bus: Optional core event bus. When omitted, a local bus is
            created for direct adapter construction. Engine construction
            replaces it with the core-owned bus via ``attach_event_bus``.

    Attributes:
        _client: Reserved for the async REST client; remains ``None`` until
            client lifecycle work is implemented.
        _socket_manager: Reserved for public/private stream management.
        _stream_tasks: Background stream tasks keyed by stream purpose.
        _client_order_ids: Local mapping between core client IDs and native
            order IDs. It is not authoritative across process restarts.
        _instrument_spec_cache: Metadata cache populated by the symbols branch.
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

        self._client: AsyncClient | None = None
        self._socket_manager: BinanceSocketManager | None = None
        self._stream_tasks: dict[str, asyncio.Task[None]] = {}

        self._client_order_ids: dict[str, str] = {}
        self._instrument_spec_cache: dict[Instrument, tuple[InstrumentSpec, datetime]] = {}
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
        """Create the async REST client and start required account streams.

        The completed implementation must honor the mutually exclusive
        Testnet/Demo configuration, fail closed if the selected endpoint
        resolves to the wrong environment, clean up partial startup, and
        publish a connected event only after the required resources are ready.
        Repeated calls and concurrent callers must not create duplicate
        clients or streams.
        """
        raise NotImplementedError("Binance adapter scaffold: connect is not implemented")

    async def disconnect(self) -> None:
        """Stop stream workers and close resources without mutating protection.

        The implementation must be safe after partial startup and repeated
        shutdown, cancel and await owned background tasks, close the async
        client, and publish the disconnected state. Exchange-side orders and
        position protection must remain untouched.
        """
        raise NotImplementedError("Binance adapter scaffold: disconnect is not implemented")

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
        """Fetch and map exchange metadata for a product-specific symbol.

        Symbol identity must come from the corresponding Spot, USDⓈ-M, or
        COIN-M exchange metadata. The finished implementation must reject
        ambiguous symbols and preserve all price/quantity increments using
        Decimal values.
        """
        raise NotImplementedError(
            "Binance adapter scaffold: fetch_instrument_spec is not implemented"
        )

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
        """Return a conservative, observed Binance request-budget snapshot.

        The completed implementation must not invent remaining capacity when
        response headers or endpoint-specific limits are unknown.
        """
        raise NotImplementedError("Binance adapter scaffold: get_rate_limits is not implemented")

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

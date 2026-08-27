"""Interactive Brokers adapter implementation.

Connection handler: local TCP socket to IB Gateway or TWS via the
``ib_async`` package.

Unlike MT5, this adapter is fully asynchronous and event-driven.
State updates arrive via push callbacks (``execDetailsEvent``,
``orderStatusEvent``, ``positionEvent``, ``accountValueEvent``)
rather than polling loops.

This module contains no business logic, no retry policy, no risk decisions.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.events import (
    Event,
    EventBus,
)
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
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
    from ib_async import IB, Trade

    from unified_trading_execution.ibkr.config import IBKRConfig
    from unified_trading_execution.state import StateStore


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class IBKRAdapter(Adapter):
    """Adapter for Interactive Brokers — equities, options, futures, forex, CFDs.

    Parameters:
        config: ``IBKRConfig`` with host, port, client_id, and account settings.
        event_bus: Where translated events are published.
    """

    def __init__(self, config: IBKRConfig, *, event_bus: EventBus | None = None) -> None:
        self._config = config
        self._event_bus = event_bus

        # Will be initialized in connect() to keep __init__ I/O free
        self._ib: IB | None = None

        # The specific account this adapter manages
        self._managed_account: str | None = None

        # Instrument spec cache: Instrument → (InstrumentSpec, fetched_at)
        self._spec_cache: dict[Instrument, tuple[InstrumentSpec, datetime]] = {}

        # Wired by the engine via attach_* hooks (see Engine.__init__).
        self._state_store: StateStore | None = None

    # ------------------------------------------------------------------
    # Engine wiring (attach_* hooks — override the ABC no-op defaults)
    # ------------------------------------------------------------------

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so push callbacks can publish.

        The engine owns the single ``EventBus`` and hands it to the adapter
        via this hook (see ``Engine.__init__``).  Overriding the ABC default
        so ``IBKREngine``-constructed adapters publish correctly even when no
        bus was passed to ``IBKRAdapter.__init__``.
        """
        self._event_bus = event_bus

    def attach_state_store(self, state_store: StateStore) -> None:
        """Store the engine-managed ``StateStore`` for any future mapping recovery.

        Core attaches its ``SQLiteStateStore`` before ``connect()``.  IBKR is
        push-driven and does not currently persist adapter-owned intent, but the
        store is retained for symmetry with the other adapters (e.g. future
        ``client_order_id ↔ orderRef`` recovery after a restart).
        """
        self._state_store = state_store

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via IBKREngine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

    # ------------------------------------------------------------------
    # Identification
    # ------------------------------------------------------------------

    @property
    def platform_name(self) -> str:
        return "ibkr"

    @property
    def account_id(self) -> str:
        """Return the resolved account ID (e.g., 'DU123456').

        After ``connect()`` succeeds, this is the actual managed account.
        Before connect, falls back to the config account (if any).  When
        neither is set, a stable ``"ibkr-account"`` placeholder keeps the
        default state-store path (``./unified_trading_execution_data/ibkr_ibkr-account.db``)
        deterministic across restarts — set ``IBKRConfig.account`` to get a
        per-account store file.
        """
        if self._managed_account is not None:
            return self._managed_account
        return self._config.account or "ibkr-account"

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Initialize the ib_async.IB instance and connect to TWS/Gateway.

        - Instantiates ``IB()``.
        - Hooks up all push event callbacks (``positionEvent``, etc.).
        - Calls ``connectAsync(host, port, clientId)``.
        - Resolves the managed account ID.
        - Requests initial market data / account updates if needed.

        Raises ``PlatformConnectionError`` if connection fails or times out.
        """
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Disconnect from TWS/Gateway and cleanup callbacks.

        Publishes ``ConnectionStateEvent(connected=False)``.
        """
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to IBKR.

        - Converts ``Instrument`` to ``Contract``.
        - Converts ``UnifiedOrder`` to one or more ``Order`` objects
          (handling bracket orders for TP/SL).
        - Assigns the UUID7 ``client_order_id`` to ``orderRef``.
        - Calls ``self._ib.placeOrder()``.
        - Returns immediately with a PENDING/SUBMITTED result.

        Note: Actual fills arrive asynchronously via ``_on_exec_details``.
        """
        raise NotImplementedError

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing open order.

        Looks up the open trade via ``client_order_id`` (matching ``orderRef``).
        Mutates the quantity, price, or stop price.
        Calls ``self._ib.placeOrder()`` with the modified order.
        """
        raise NotImplementedError

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by ``client_order_id``.

        Finds the active order by ``orderRef`` and calls ``self._ib.cancelOrder()``.
        Raises ``OrderNotFoundError`` if the order is not active or unknown.
        """
        raise NotImplementedError

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by ``client_order_id``.

        Scans ``self._ib.openTrades()`` for a matching ``orderRef``.
        Returns ``None`` if not found.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Instrument metadata
    # ------------------------------------------------------------------

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules from IBKR via ``reqContractDetailsAsync()``.

        Extracts min tick size, price magnifier, and valid order sizes.
        Cached with TTL per ``IBKRConfig.instrument_spec_cache_ttl``.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Capability reporting
    # ------------------------------------------------------------------

    def supported_order_types(self) -> frozenset[OrderType]:
        """IBKR supports the guaranteed four order types."""
        return frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )

    # ------------------------------------------------------------------
    # Rate limits
    # ------------------------------------------------------------------

    async def get_rate_limits(self) -> RateLimits:
        """Return IBKR rate-limit state.

        IBKR generally allows up to 50 requests per second. ``ib_async``
        handles pacing internally. We return a conservative static estimate.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Position TP/SL modification (optional ABC method)
    # ------------------------------------------------------------------

    async def modify_position_tpsl(
        self,
        position_id: str,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an existing open position.

        IBKR models TP/SL as bracket child orders linked to a parent order via
        ``parentId``.  ``position_id`` is the platform position reference
        (the contract ``conId``), obtained from ``PositionUpdateEvent`` or the
        state store — not ``UnifiedOrder.position_id``.

        TODO(ibkr): implement via ``ib.placeOrder`` on the bracket children.
        At least one of *take_profit* / *stop_loss* must be provided.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Reconciliation data (optional ABC methods)
    # ------------------------------------------------------------------

    async def fetch_positions(self) -> list[Position]:
        """Fetch all open positions from ``self._ib.positions()`` as legs.

        One ``Position`` per terminal position leg, with ``position_id`` set
        to the contract's ``conId`` (``str(contract.conId)``).  Resolves each
        ``ib_async`` contract back to a canonical ``Instrument``.
        """
        raise NotImplementedError

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances from ``self._ib.accountValues()``.

        Extracts TotalCashValue and NetLiquidation by currency.
        """
        raise NotImplementedError

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch all open orders from ``self._ib.openTrades()``."""
        raise NotImplementedError

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills from ``self._ib.fills()`` or ``reqExecutionsAsync()``.

        *since* is an optional UTC lower bound for the fill window (used by
        reconciliation's watermark-bounded query).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Event Callbacks (adapter-internal push handlers)
    # ------------------------------------------------------------------

    def _on_connected(self) -> None:
        """Callback fired by ib_async when connection is established."""
        raise NotImplementedError

    def _on_disconnected(self) -> None:
        """Callback fired by ib_async when connection is lost."""
        raise NotImplementedError

    def _on_position_update(self, position: Any) -> None:
        """Callback fired by ib_async on position changes.

        Translates to ``PositionUpdateEvent`` and publishes to EventBus.
        """
        raise NotImplementedError

    def _on_account_value(self, value: Any) -> None:
        """Callback fired by ib_async on account value changes.

        Translates to ``BalanceUpdateEvent`` and publishes to EventBus.
        """
        raise NotImplementedError

    def _on_exec_details(self, trade: Trade, fill: Any, execution: Any) -> None:
        """Callback fired by ib_async when an order is filled.

        Translates to ``FillEvent`` and publishes to EventBus.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _invalidate_spec_cache(self, instrument: Instrument) -> None:
        """Remove a cached ``InstrumentSpec``, forcing a re-fetch on next access."""
        self._spec_cache.pop(instrument, None)

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

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ib_async import IB, Trade
from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
)
from unified_trading_execution.events import (
    ConnectionStateEvent,
    Event,
    EventBus,
)
from unified_trading_execution.ibkr.orders import (
    apply_ibkr_modification,
    build_ibkr_orders,
    is_final_order_status,
    map_ibkr_status,
    parse_ibkr_trade,
)
from unified_trading_execution.ibkr.symbols import to_ibkr_contract
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
    from unified_trading_execution.ibkr.config import IBKRConfig
    from unified_trading_execution.state import StateStore


logger = logging.getLogger(__name__)


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


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

        # Serialize overlapping connect() calls — prevents duplicate IB instances.
        self._connect_lock = asyncio.Lock()

        # Whether the adapter considers itself connected (mirrors IB.isConnected).
        self._connected = False

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
        - Publishes ``ConnectionStateEvent(connected=True)``.

        Raises ``PlatformConnectionError`` if connection fails or times out.
        Idempotent — a second call while already connected is a no-op.
        Overlapping concurrent calls are serialized so only one IB instance
        is ever created (mirrors MT5's process-global guard spirit).
        """
        # Fast path: already connected — idempotent no-op.
        if self._connected and self._ib is not None and self._ib.isConnected():
            return

        async with self._connect_lock:
            # Re-check inside the lock — another waiter may have connected while
            # we were queued.
            if self._connected and self._ib is not None and self._ib.isConnected():
                return

            # If a half-torn-down IB still lingers, clean it before re-creating.
            if self._ib is not None:
                with contextlib.suppress(Exception):
                    self._unwire_events(self._ib)
                with contextlib.suppress(Exception):
                    self._ib.disconnect()
                self._ib = None
                self._managed_account = None
                self._connected = False

            ib = IB()
            self._wire_events(ib)
            self._ib = ib

            try:
                await ib.connectAsync(
                    host=self._config.host,
                    port=self._config.port,
                    clientId=self._config.client_id,
                    timeout=self._config.timeout_seconds,
                    readonly=self._config.readonly,
                    account=self._config.account or "",
                )
            except TimeoutError as exc:
                await self._cleanup_after_failed_connect(ib)
                raise PlatformConnectionError(
                    f"IBKR connection to {self._config.host}:{self._config.port} timed out"
                ) from exc
            except Exception as exc:
                # ib_async already called disconnect() internally on failure
                # (connectAsync's except BaseException: self.disconnect()), but
                # we still need to unwire and clear our own state.
                await self._cleanup_after_failed_connect(ib)
                if isinstance(exc, PlatformConnectionError):
                    raise
                raise PlatformConnectionError(
                    f"failed to connect to IBKR at {self._config.host}:{self._config.port}: {exc}"
                ) from exc

            # Resolve managed account — config wins, else sole account, else first.
            try:
                accounts: list[str] = ib.managedAccounts()
            except Exception:
                accounts = []
            if self._config.account:
                self._managed_account = self._config.account
            elif accounts:
                self._managed_account = accounts[0]
            else:
                # Leave None — account_id falls back to config.account, then to
                # the deterministic "ibkr-account" placeholder (see account_id).
                self._managed_account = None

            self._connected = True
            self._publish_connection_state(True)

    async def _cleanup_after_failed_connect(self, ib: IB) -> None:
        """Unwire events and clear state after a failed connectAsync."""
        with contextlib.suppress(Exception):
            self._unwire_events(ib)
        with contextlib.suppress(Exception):
            ib.disconnect()
        if self._ib is ib:
            self._ib = None
        self._managed_account = None
        self._connected = False

    def _wire_events(self, ib: IB) -> None:
        """Subscribe adapter callbacks to ib_async push events (eventkit +=)."""
        ib.connectedEvent += self._on_connected
        ib.disconnectedEvent += self._on_disconnected
        # Push streams — keep wired even though handlers are still stubs;
        # they become live without a reconnect when implemented.
        ib.positionEvent += self._on_position_update
        ib.accountValueEvent += self._on_account_value
        ib.execDetailsEvent += self._on_exec_details

    def _unwire_events(self, ib: IB) -> None:
        """Unsubscribe adapter callbacks (eventkit -=) — best-effort, never raises."""
        for event_name, handler in (
            ("connectedEvent", self._on_connected),
            ("disconnectedEvent", self._on_disconnected),
            ("positionEvent", self._on_position_update),
            ("accountValueEvent", self._on_account_value),
            ("execDetailsEvent", self._on_exec_details),
        ):
            try:
                event = getattr(ib, event_name, None)
                if event is not None:
                    event -= handler
            except Exception:
                pass

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

    async def disconnect(self) -> None:
        """Disconnect from TWS/Gateway and cleanup callbacks.

        Publishes ``ConnectionStateEvent(connected=False)`` once.
        Idempotent — safe to call when already disconnected; a second call
        is a no-op and does not publish a duplicate event.
        """
        # Idempotent fast path — no IB or already torn down.
        if self._ib is None and not self._connected:
            return
        # If we have an IB but it already reports disconnected and we have
        # already flipped _connected, treat as already disconnected.
        if self._ib is not None and not self._connected:
            # Still need to ensure IB instance is cleared.
            with contextlib.suppress(Exception):
                self._unwire_events(self._ib)
            with contextlib.suppress(Exception):
                self._ib.disconnect()
            self._ib = None
            return

        ib = self._ib
        # Flip flag first so concurrent is_connected checks see disconnecting.
        self._connected = False

        if ib is not None:
            with contextlib.suppress(Exception):
                self._unwire_events(ib)
            try:
                ib.disconnect()
            except Exception:
                logger.exception("IB.disconnect() raised during adapter disconnect")
            finally:
                if self._ib is ib:
                    self._ib = None

        self._managed_account = None
        # Only publish if we actually transitioned from connected — caller
        # already returned above for the double-disconnect case.
        try:
            self._publish_connection_state(False)
        except RuntimeError:
            # No event_bus wired — still consider disconnect successful;
            # Engine-constructed adapters always have a bus wired.
            logger.warning("disconnect: event_bus not wired, skipping ConnectionStateEvent")

    @property
    def is_connected(self) -> bool:
        if self._connected and self._ib is not None:
            try:
                return bool(self._ib.isConnected())
            except Exception:
                return False
        return False

    # ------------------------------------------------------------------
    # Order operations — pure translation via ibkr.orders + ibkr.symbols,
    # thin I/O via ib_async.IB. No business logic here.
    # ------------------------------------------------------------------

    def _require_ib(self) -> IB:
        """Return the live ``IB`` or raise if not connected."""
        if self._ib is None or not self.is_connected:
            raise PlatformConnectionError("IBKR adapter is not connected — call connect() first")
        return self._ib

    def _find_trade(self, client_order_id: str) -> Trade | None:
        """Find any Trade (open or done) by ``orderRef``."""
        ib = self._ib
        if ib is None:
            return None
        for trade in ib.trades():
            if trade.order.orderRef == client_order_id:
                return trade
        return None

    def _find_open_trade(self, client_order_id: str) -> Trade | None:
        """Find an open Trade by ``orderRef`` (only live orders)."""
        ib = self._ib
        if ib is None:
            return None
        for trade in ib.openTrades():
            if trade.order.orderRef == client_order_id:
                return trade
        # Fallback: trades() may contain a PendingSubmit not yet in openTrades(),
        # but must still be live — a terminal order (filled/cancelled/rejected/
        # expired) must never be returned for modification or cancellation.
        fallback = self._find_trade(client_order_id)
        if fallback is None:
            return None
        if is_final_order_status(map_ibkr_status(fallback.orderStatus.status)):
            return None
        return fallback

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to IBKR.

        - Converts ``Instrument`` to ``Contract`` via ``to_ibkr_contract``.
        - Converts ``UnifiedOrder`` to one or more ``Order`` objects
          (handling bracket orders for TP/SL) via ``build_ibkr_orders``.
        - Calls ``IB.placeOrder(contract, order)`` for each leg; bracket
          children are linked via ``parentId`` after the parent's ``orderId``
          is assigned.
        - Returns the parent ``OrderResult`` immediately (PENDING/OPEN);
          fills arrive via ``_on_exec_details``.
        """
        ib = self._require_ib()
        if self._config.readonly:
            raise PlatformError("adapter is in readonly mode — order placement blocked")

        contract = to_ibkr_contract(order.instrument, self._config)
        orders = build_ibkr_orders(order)

        # Single order — fast path
        if len(orders) == 1:
            trade = ib.placeOrder(contract, orders[0])
            return parse_ibkr_trade(trade)

        # Bracket: parent first, then children linked to parent's orderId
        parent_trade = ib.placeOrder(contract, orders[0])
        parent_id = parent_trade.order.orderId
        for child in orders[1:]:
            child.parentId = parent_id
            ib.placeOrder(contract, child)
        return parse_ibkr_trade(parent_trade)

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing open order.

        Looks up the open trade via ``client_order_id`` (matching ``orderRef``),
        mutates price/stop/quantity in place, and re-submits via ``placeOrder``.
        """
        ib = self._require_ib()
        trade = self._find_open_trade(modification.client_order_id)
        if trade is None:
            raise OrderNotFoundError(
                f"no open order for client_order_id {modification.client_order_id!r}"
            )
        apply_ibkr_modification(modification, trade.order)
        updated = ib.placeOrder(trade.contract, trade.order)
        return parse_ibkr_trade(updated)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by ``client_order_id``.

        Finds the active order by ``orderRef`` and calls ``IB.cancelOrder``.
        Raises ``OrderNotFoundError`` if the order is not active or unknown.
        """
        ib = self._require_ib()
        trade = self._find_open_trade(client_order_id)
        if trade is None:
            raise OrderNotFoundError(f"no open order for client_order_id {client_order_id!r}")
        result = ib.cancelOrder(trade.order)
        # cancelOrder mutates the trade in place; fall back to original trade
        target = result if result is not None else trade
        return parse_ibkr_trade(target)

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by ``client_order_id``.

        Scans all trades for a matching ``orderRef``. Returns ``None`` if not
        found. Raises ``PlatformConnectionError`` if not connected (consistent
        with the other order operations).
        """
        self._require_ib()
        trade = self._find_trade(client_order_id)
        if trade is None:
            return None
        return parse_ibkr_trade(trade)

    # ------------------------------------------------------------------
    # Instrument metadata
    # ------------------------------------------------------------------

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules from IBKR via ``reqContractDetailsAsync()``.

        Extracts ``minTick`` → ``tick_size``, ``sizeIncrement`` → ``lot_size``,
        ``minSize`` → ``min_qty``. Cached with TTL per
        ``IBKRConfig.instrument_spec_cache_ttl`` (``None`` = infinite).

        Raises ``InvalidSymbolError`` if the contract is unknown on this
        gateway, ``PlatformConnectionError`` if the gateway is unreachable.
        """
        # Fast path: cached and still fresh
        cached = self._spec_cache.get(instrument)
        if cached is not None:
            spec, fetched_at = cached
            ttl = self._config.instrument_spec_cache_ttl
            if ttl is None or (_utcnow() - fetched_at).total_seconds() < ttl:
                return spec
            self._spec_cache.pop(instrument, None)

        ib = self._require_ib()
        contract = to_ibkr_contract(instrument, self._config)

        try:
            details_list = await ib.reqContractDetailsAsync(contract)
        except Exception as exc:
            # Connection-level failures (socket closed, timeout) bubble as
            # PlatformConnectionError so Engine can retry; contract-level
            # failures are InvalidSymbol — empty list handled below.
            raise PlatformConnectionError(
                f"failed to fetch contract details for {instrument.symbol!r}: {exc}"
            ) from exc

        if not details_list:
            raise InvalidSymbolError(
                f"IBKR symbol {instrument.symbol!r} is not available: no contract details"
            )

        details = details_list[0]

        # IBKR guarantees minTick for tradable contracts; fall back to 0.01
        # only for malformed test fixtures (never on a live gateway).
        raw_tick = getattr(details, "minTick", 0) or 0
        tick_size = Decimal(str(raw_tick)) if raw_tick else Decimal("0.01")
        if tick_size <= 0:
            tick_size = Decimal("0.01")

        raw_increment = getattr(details, "sizeIncrement", 0) or 0
        lot_size = Decimal(str(raw_increment)) if raw_increment else Decimal("1")
        if lot_size <= 0:
            lot_size = Decimal("1")

        raw_min = getattr(details, "minSize", 0) or 0
        min_qty = Decimal(str(raw_min)) if raw_min else lot_size
        if min_qty <= 0:
            min_qty = lot_size

        # IBKR has no explicit max size in ContractDetails — use a safe large cap
        max_qty = Decimal("1000000000")
        min_notional = Decimal("0")

        # Precision derived from the Decimal exponent, not float formatting
        def _places(value: Decimal) -> int:
            normalized = value.normalize()
            exp = normalized.as_tuple().exponent
            return max(0, -int(exp)) if isinstance(exp, int) else 0

        spec = InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=min_qty,
            max_qty=max_qty,
            min_notional=min_notional,
            price_precision=_places(tick_size),
            qty_precision=_places(lot_size),
        )
        self._spec_cache[instrument] = (spec, _utcnow())
        return spec

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

    def _on_connected(self, *args: Any) -> None:
        """Callback fired by ib_async when connection is established.

        ib_async emits connectedEvent from inside connectAsync while
        adapter.connect() holds _connect_lock. That emission would race
        connect()'s own publish and create a duplicate True. Suppress
        while the lock is held — connect() publishes exactly once itself.
        Unsolicited reconnects (Gateway bounce) happen outside the lock
        and are published here.
        """
        if self._connected:
            return
        if self._connect_lock.locked():
            return
        self._connected = True
        with contextlib.suppress(RuntimeError):
            self._publish_connection_state(True)

    def _on_disconnected(self, *args: Any) -> None:
        """Callback fired by ib_async when connection is lost.

        Handles unsolicited drops (Gateway/TWS closed). Explicit
        disconnect() already unwired this handler, so this only fires for
        external drops — publish once and clear state.
        """
        if not self._connected and self._ib is None:
            return
        was_connected = self._connected
        self._connected = False
        self._managed_account = None
        if was_connected:
            with contextlib.suppress(RuntimeError):
                self._publish_connection_state(False)

    def _on_position_update(self, position: Any) -> None:
        """Callback fired by ib_async on position changes.

        Translates to ``PositionUpdateEvent`` and publishes to EventBus.
        Not yet implemented — stubbed to keep event wiring live without
        a reconnect.
        """
        # TODO: translate to PositionUpdateEvent (needs from_ibkr_contract)
        return

    def _on_account_value(self, value: Any) -> None:
        """Callback fired by ib_async on account value changes.

        Translates to ``BalanceUpdateEvent`` and publishes to EventBus.
        Not yet implemented — stubbed to keep event wiring live.
        """
        return

    def _on_exec_details(self, trade: Trade, fill: Any, execution: Any) -> None:
        """Callback fired by ib_async when an order is filled.

        Translates to ``FillEvent`` and publishes to EventBus.
        Not yet implemented — stubbed to keep event wiring live.
        """
        return

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _invalidate_spec_cache(self, instrument: Instrument) -> None:
        """Remove a cached ``InstrumentSpec``, forcing a re-fetch on next access."""
        self._spec_cache.pop(instrument, None)

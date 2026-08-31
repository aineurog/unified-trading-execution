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
from unified_trading_execution.ibkr.symbols import from_ibkr_contract, to_ibkr_contract
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
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

            # Enforce TWS/Gateway Time Zone = UTC for Engine UTC purity.
            # TWS sends Execution.time naive in its display zone; Engine watermark
            # `since` is UTC. If TWS is not UTC, `since` filtering is hours off
            # and `reconcile` misses fills. Block early with a clear message.
            try:
                self._assert_tws_utc(ib)
            except PlatformConnectionError:
                await self._cleanup_after_failed_connect(ib)
                raise

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

    def _assert_tws_utc(self, ib: IB) -> None:
        """Reject a known non-UTC TWS/Gateway timezone.

        Engine is UTC-only (watermark `since` is UTC). TWS sends
        Execution.time naive in its display zone. ``ib_async`` does not expose
        the Gateway GUI timezone reliably: an empty ``TimezoneTWS`` means
        unknown, not necessarily non-UTC.
        """
        tws_tz = str(getattr(ib, "TimezoneTWS", "") or "").strip()
        if not tws_tz:
            logger.warning(
                "IBKR TWS/Gateway timezone is unknown (ib_async TimezoneTWS is empty); "
                "verify the Gateway/TWS GUI timezone is UTC before using fill watermarks"
            )
            return
        if tws_tz.upper() not in ("UTC", "ETC/UTC", "GMT") and "UTC" not in tws_tz.upper():
            raise PlatformConnectionError(
                f"TWS/Gateway Time Zone is {tws_tz!r} — "
                "set TWS/Gateway → Configure → Settings → General → Time Zone = UTC "
                "and reconnect. Engine is UTC-only; non-UTC TWS makes fetch_fills(since) "
                "filter fills incorrectly by hours."
            )

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
        now = _utcnow()
        return RateLimits(
            requests_per_interval=50,
            interval_seconds=1.0,
            remaining=50,
            reset_at=now,
        )

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
    # Reconciliation data — proper ib_async usage, covering all cases
    # ------------------------------------------------------------------

    async def fetch_positions(self) -> list[Position]:
        """Fetch all open positions as legs (one Position per IBKR Position).

        Uses ``IB.positions()`` (all accounts, or ``self._managed_account``
        when set). Skips zero-quantity and unmappable contracts with a
        warning, never aborting the whole snapshot. ``position_id`` is
        ``str(contract.conId)`` when available.
        """
        ib = self._require_ib()
        now = _utcnow()
        account = self._managed_account if self._managed_account not in (None, "UNKNOWN") else ""
        try:
            raw_positions = ib.positions(account=account) if account else ib.positions()
        except Exception as exc:
            raise PlatformConnectionError(f"failed to fetch IBKR positions: {exc}") from exc

        result: list[Position] = []
        for pos in raw_positions:
            qty = Decimal(str(pos.position))
            if qty == 0:
                continue
            try:
                instrument = from_ibkr_contract(pos.contract)
            except Exception as exc:
                logger.warning(
                    "Skipping IBKR position with unmappable contract %r: %s", pos.contract, exc
                )
                continue
            avg_price = Decimal(str(pos.avgCost)) if pos.avgCost else Decimal("0")
            # IBKR avgCost is per-share cost; for FX it's the price. Use as entry price.
            position_id = str(pos.contract.conId) if getattr(pos.contract, "conId", 0) else None
            try:
                result.append(
                    Position(
                        instrument=instrument,
                        quantity=qty,
                        average_entry_price=avg_price,
                        updated_at=now,
                        position_id=position_id,
                    )
                )
            except Exception as exc:
                logger.warning("Skipping invalid Position for %s: %s", instrument, exc)
        return result

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances from ``IB.accountValues()``.

        Groups ``AccountValue`` by currency and uses ``TotalCashValue`` as
        ``free`` and ``NetLiquidation`` as ``total`` (``used = total - free``).
        Falls back to ``CashBalance``/``AvailableFunds`` when the primary tags
        are absent. Skips currencies with no usable value.
        """
        ib = self._require_ib()
        now = _utcnow()
        account = self._managed_account if self._managed_account not in (None, "UNKNOWN") else ""
        try:
            values = ib.accountValues(account=account) if account else ib.accountValues()
        except Exception as exc:
            raise PlatformConnectionError(f"failed to fetch IBKR account values: {exc}") from exc

        per_ccy: dict[str, dict[str, Decimal]] = {}
        for av in values:
            ccy = (getattr(av, "currency", "") or "").strip() or "USD"
            tag = getattr(av, "tag", "")
            raw = getattr(av, "value", "")
            if not tag or raw in (None, ""):
                continue
            try:
                dec = Decimal(str(raw))
            except Exception:
                continue
            bucket = per_ccy.setdefault(ccy, {})
            # Keep last value for tag (IBKR sends one per account/currency/tag)
            bucket[tag] = dec

        result: dict[str, Balance] = {}
        for ccy, tags in per_ccy.items():
            # Prefer NetLiquidation as total, else TotalCashValue, else CashBalance
            total = tags.get("NetLiquidation")
            if total is None:
                total = tags.get("TotalCashValue")
            if total is None:
                total = tags.get("CashBalance")
            if total is None:
                continue
            free = tags.get("AvailableFunds")
            if free is None:
                free = tags.get("TotalCashValue")
            if free is None:
                free = tags.get("CashBalance", total)
            # Clamp free to total
            if free > total:
                free = total
            used = total - free
            try:
                result[ccy] = Balance(
                    currency=ccy, free=free, used=used, total=total, updated_at=now
                )
            except Exception as exc:
                logger.warning("Skipping balance for %s: %s", ccy, exc)
        return result

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch all open orders from ``IB.openTrades()``.

        Translates each ``Trade`` to an ``OrderRecord`` via ``from_ibkr_contract``
        + order-field mapping. Skips unmappable contracts with a warning.
        Keyed by ``client_order_id`` (``orderRef``) falling back to
        ``platform_order_id`` so orphans remain reconcilable.
        """
        ib = self._require_ib()
        now = _utcnow()
        try:
            trades = ib.openTrades()
        except Exception as exc:
            raise PlatformConnectionError(f"failed to fetch IBKR open orders: {exc}") from exc

        result: dict[str, OrderRecord] = {}
        for trade in trades:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            if order is None or status is None or contract is None:
                continue
            try:
                instrument = from_ibkr_contract(contract)
            except Exception as exc:
                logger.warning("Skipping open order with unmappable contract %r: %s", contract, exc)
                continue

            # Map IBKR wire fields to unified enums. IBKR has four order actions
            # (BUY / SELL / SLONG / SSHORT); the unified side collapses the
            # borrow direction — SLONG (buy-to-cover) is BUY, SSHORT is SELL.
            action = str(getattr(order, "action", "")).upper()
            if action in ("BUY", "SLONG"):
                side = OrderSide.BUY
            elif action in ("SELL", "SSHORT"):
                side = OrderSide.SELL
            else:
                # Unknown action — skip rather than guess a side.
                logger.warning("Skipping open order with unknown action %r", action)
                continue

            otype = str(getattr(order, "orderType", "")).upper()
            if otype == "MKT":
                order_type = OrderType.MARKET
            elif otype == "LMT":
                order_type = OrderType.LIMIT
            elif otype == "STP":
                order_type = OrderType.STOP
            elif otype in ("STP LMT", "STP_LMT"):
                order_type = OrderType.STOP_LIMIT
            else:
                # Unknown order type — skip rather than guess
                logger.warning("Skipping order with unknown orderType %r", otype)
                continue

            tif_raw = str(getattr(order, "tif", "")).upper()
            tif_map = {
                "GTC": TimeInForce.GTC,
                "DAY": TimeInForce.DAY,
                "IOC": TimeInForce.IOC,
                "FOK": TimeInForce.FOK,
                "GTD": TimeInForce.GTD,
            }
            time_in_force = tif_map.get(tif_raw, TimeInForce.GTC)

            # Prices: UNSET_DOUBLE sentinel (DBL_MAX ~1.797e308) or float('inf')
            # means "not set" — treat anything absurdly large as unset.
            def _as_decimal(val: object) -> Decimal | None:
                try:
                    if val is None:
                        return None
                    f = float(str(val))
                    if f in (float("inf"), float("-inf")) or abs(f) > 1e12:
                        return None
                    return Decimal(str(val))
                except Exception:
                    return None

            price = _as_decimal(getattr(order, "lmtPrice", None))
            # lmtPrice 0 or UNSET means no limit
            if price is not None and price == 0:
                price = None
            stop_price = _as_decimal(getattr(order, "auxPrice", None))
            if stop_price is not None and stop_price == 0:
                stop_price = None

            # Quantity
            try:
                qty = Decimal(str(getattr(order, "totalQuantity", 0) or 0))
            except Exception:
                qty = Decimal("0")
            if qty == 0:
                continue

            client_order_id = str(getattr(order, "orderRef", "") or "")
            perm_id = getattr(order, "permId", 0) or 0
            order_id = getattr(order, "orderId", 0) or 0
            platform_order_id = str(perm_id or order_id) if (perm_id or order_id) else None

            # Status — an unknown IBKR status is skipped, never silently mapped
            # to OPEN. ``map_ibkr_status`` raises PlatformError for a status it
            # does not recognise (a new IBKR status must not be misrepresented).
            ib_status = str(getattr(status, "status", "") or "")
            try:
                unified_status = map_ibkr_status(ib_status)
            except PlatformError as exc:
                logger.warning("Skipping open order with unknown status %r: %s", ib_status, exc)
                continue

            filled_qty = Decimal(str(getattr(status, "filled", 0) or 0))
            avg_price_raw = getattr(status, "avgFillPrice", 0) or 0
            try:
                avg_fill = Decimal(str(avg_price_raw)) if avg_price_raw else None
            except Exception:
                avg_fill = None

            # Timestamps from Trade log — ib_async already UTC
            created_at = now
            updated_at = now
            log = getattr(trade, "log", None)
            if log:
                try:
                    first = log[0].time
                    last = log[-1].time
                    if first is not None:
                        created_at = first if first.tzinfo else first.replace(tzinfo=UTC)
                    if last is not None:
                        updated_at = last if last.tzinfo else last.replace(tzinfo=UTC)
                except Exception:
                    pass

            # GTD expiry not available on wire — keep None
            try:
                record = OrderRecord(
                    instrument=instrument,
                    order_type=order_type,
                    side=side,
                    quantity=qty,
                    time_in_force=time_in_force,
                    client_order_id=client_order_id or platform_order_id or str(order_id),
                    price=price,
                    stop_price=stop_price,
                    reduce_only=False,
                    client_tag=None,
                    take_profit=None,
                    stop_loss=None,
                    platform_order_id=platform_order_id,
                    status=unified_status,
                    filled_quantity=filled_qty,
                    average_fill_price=avg_fill,
                    correlation_id=client_order_id,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            except Exception as exc:
                logger.warning("Skipping invalid OrderRecord for %r: %s", client_order_id, exc)
                continue

            key = record.client_order_id or record.platform_order_id or str(order_id)
            result[key] = record
        return result

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch fills from ``IB.fills()`` grouped by ``client_order_id``.

        Uses the session's ``Fill`` cache (``Execution`` + ``CommissionReport``).
        Filters with ``since`` on ``Execution.time`` when provided. Skips
        fills with unmappable contracts or zero quantity/price.
        """
        ib = self._require_ib()
        try:
            fills = ib.fills()
        except Exception as exc:
            raise PlatformConnectionError(f"failed to fetch IBKR fills: {exc}") from exc

        grouped: dict[str, list[FillRecord]] = {}
        for fill in fills:
            execution = getattr(fill, "execution", None)
            contract = getattr(fill, "contract", None)
            commission = getattr(fill, "commissionReport", None)
            if execution is None or contract is None:
                continue
            exec_time = getattr(execution, "time", None)
            if exec_time is None:
                continue
            if exec_time.tzinfo is None:
                exec_time = exec_time.replace(tzinfo=UTC)
            if since is not None and exec_time < since:
                continue
            try:
                instrument = from_ibkr_contract(contract)
            except Exception as exc:
                logger.warning("Skipping fill with unmappable contract %r: %s", contract, exc)
                continue
            shares = getattr(execution, "shares", 0) or 0
            price = getattr(execution, "price", 0) or 0
            try:
                qty = Decimal(str(shares))
                fill_price = Decimal(str(price))
            except Exception:
                continue
            if qty <= 0 or fill_price <= 0:
                continue
            client_order_id = str(getattr(execution, "orderRef", "") or "")
            # Fallback to permId if orderRef empty (manual TWS orders)
            if not client_order_id:
                perm = getattr(execution, "permId", 0) or 0
                client_order_id = str(perm) if perm else ""
            if not client_order_id:
                # No attribution — skip (Engine needs client_order_id to reconcile)
                logger.warning(
                    "Skipping fill without orderRef/execId %r", getattr(execution, "execId", "")
                )
                continue
            exec_id = str(getattr(execution, "execId", "") or "")
            if not exec_id:
                exec_id = f"{client_order_id}-{exec_time.isoformat()}"
            fee_amount: Decimal | None = None
            fee_currency: str | None = None
            if commission is not None:
                try:
                    fee_amount = Decimal(str(getattr(commission, "commission", 0) or 0))
                    fee_currency = str(getattr(commission, "currency", "") or "") or None
                    if fee_amount == 0:
                        fee_amount = None
                        fee_currency = None
                except Exception:
                    fee_amount = None
                    fee_currency = None
            # position_id not applicable to IBKR fills — keep None
            try:
                record = FillRecord(
                    client_order_id=client_order_id,
                    platform_fill_id=exec_id,
                    instrument=instrument,
                    fill_quantity=qty,
                    fill_price=fill_price,
                    fill_timestamp=exec_time,
                    fee_currency=fee_currency,
                    fee_amount=fee_amount,
                    correlation_id=client_order_id,
                    position_id=None,
                )
            except Exception as exc:
                logger.warning("Skipping invalid FillRecord for %r: %s", client_order_id, exc)
                continue
            grouped.setdefault(client_order_id, []).append(record)

        # Sort each client's fills by timestamp for deterministic reconciliation
        for lst in grouped.values():
            lst.sort(key=lambda r: r.fill_timestamp)
        return grouped

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

"""MetaTrader 5 adapter implementation.

Connection handler: local terminal process via IPC (the ``MetaTrader5``
package).  No REST session, no WebSocket.  Every call is a blocking
round-trip into a running terminal on the same machine, wrapped in
``asyncio.to_thread()``.  State updates arrive by polling
(``orders_get``, ``positions_get``, ``history_deals_get``,
``account_info``), not push events.

Two hard platform realities:

- **Process-global connection.**  ``mt5.initialize()`` / ``mt5.shutdown()``
  are per-process singletons.  Only one ``MT5Adapter`` can be connected
  per Python process.  Multiple accounts require multiple terminal
  installations, each addressed by ``MT5Config.path``.
- **Windows-only module.**  ``MetaTrader5`` ships win32/win64 wheels only.
  It must be imported lazily inside methods (never at module top) so this
  package stays importable — and lintable — on non-Windows CI.

This module contains no business logic, no retry policy, no risk decisions.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    UteError,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.mt5.errors import map_mt5_error
from unified_trading_execution.mt5.orders import build_order_record
from unified_trading_execution.mt5.symbols import from_mt5_symbol, to_mt5_symbol
from unified_trading_execution.types.enums import AssetClass, OrderType
from unified_trading_execution.types.instrument import (
    Instrument,
    InstrumentSpec,
    _with_broker_override,
)
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from unified_trading_execution.mt5.config import MT5Config

# ---------------------------------------------------------------------------
# Process-global connection guard
# ---------------------------------------------------------------------------

_connected_lock = threading.Lock()
_connected = False


def _get_mt5() -> Any:
    """Lazy-import ``MetaTrader5``.  Raises ``ImportError`` with a clear
    message on non-Windows platforms."""
    try:
        import MetaTrader5 as mt5  # type: ignore[import-not-found]
    except ImportError:
        raise ImportError(
            "MetaTrader5 package is required for MT5 adapter. "
            "It is only available on Windows. "
            "Install with: pip install unified-trading-execution-metatrader[mt5]"
        ) from None
    return mt5


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _decimal_places(value: Decimal) -> int:
    """Number of fractional digits in a ``Decimal``'s stored representation.

    ``Decimal("0.01")`` → 2, ``Decimal("0.1")`` → 1, ``Decimal("500")`` → 0.
    Normalizes first so whole-number steps like ``Decimal("1.0")`` (from
    ``Decimal(str(1.0))``) report 0, not 1.  Used to derive
    ``InstrumentSpec.qty_precision`` from MT5's volume step.
    """
    normalized = value.normalize()
    exponent = normalized.as_tuple().exponent
    if not isinstance(exponent, int):
        return 0
    return max(0, -exponent)


# MT5 symbol market-path segment → canonical AssetClass.  The mapping is
# broker-dependent (the segment names come from the broker's own market tree);
# an unrecognized path is an error, never a silent default.
_PATH_ASSET_CLASS: dict[str, AssetClass] = {
    "FOREX": AssetClass.MARGIN_FX,
    "METALS": AssetClass.MARGIN_FX,  # spot metals (XAUUSD) trade as quoted pairs on margin
    "INDICES": AssetClass.CFD,  # index CFDs
    "STOCKS": AssetClass.STOCK,
    "CRYPTOCURRENCIES": AssetClass.SPOT,
    "CRYPTO": AssetClass.SPOT,
    "FUTURES": AssetClass.FUTURES,
    "BONDS": AssetClass.BOND,
    "FUNDS": AssetClass.FUND,
}


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class MT5Adapter(Adapter):
    """Adapter for the MetaTrader 5 terminal — forex, CFDs, stocks, futures.

    Parameters:
        config: ``MT5Config`` with login, password, server, path,
            symbol alias table, and poll interval settings.
        event_bus: Where translated events are published.
    """

    def __init__(self, config: MT5Config, *, event_bus: EventBus | None = None) -> None:
        self._config = config
        self._event_bus = event_bus
        self._connected = False

        # Actual account login, resolved from terminal after connect().
        self._account_login: int | None = None

        # Background polling task — started in connect(), cancelled in disconnect().
        self._poll_task: asyncio.Task[None] | None = None

        # -- Internal state tracking for diff-based polling --
        # client_order_id → ticket (int) mapping for active orders
        self._order_id_to_ticket: dict[str, int] = {}
        self._ticket_to_order_id: dict[int, str] = {}

        # Last known state snapshots.  Orders are keyed by MT5 ticket (raw
        # tuples), positions are NETTED per instrument (one Position per
        # instrument regardless of netting/hedging mode), and balance is the
        # single-currency account balance.
        self._last_orders: dict[int, object] = {}
        self._last_positions: dict[Instrument, Position] = {}
        self._last_balance: Balance | None = None
        self._last_deal_time: datetime = datetime.now(tz=UTC)
        self._last_deal_ticket: int = 0

        # Broker symbol → canonical Instrument cache for inbound reconstruction.
        # Populated lazily on first sighting via symbol_info().path.
        self._symbol_to_instrument: dict[str, Instrument] = {}
        self._failed_symbols: set[str] = set()

        # Instrument spec cache: Instrument → (InstrumentSpec, fetched_at)
        self._spec_cache: dict[Instrument, tuple[InstrumentSpec, datetime]] = {}

        # Reverse alias table built from config's forward table.
        self._reverse_alias: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Identification
    # ------------------------------------------------------------------

    @property
    def platform_name(self) -> str:
        return "metatrader"

    @property
    def account_id(self) -> str:
        """Return the resolved account login from the terminal.

        After ``connect()`` succeeds this is the actual ``account_info().login``.
        Before connect, falls back to the config login.
        """
        if self._account_login is not None:
            return str(self._account_login)
        return str(self._config.login)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so the adapter can publish events.

        The engine owns the single ``EventBus`` and hands it to the adapter
        via this hook (see ``Engine.__init__``).  Overriding the ABC default
        so ``MT5Engine``-constructed adapters publish correctly even when no
        bus was passed to ``MT5Adapter.__init__``.
        """
        self._event_bus = event_bus

    async def connect(self) -> None:
        """Initialize the MT5 terminal connection and start the polling loop.

        Connection lifecycle:

        1. Acquire the process-global guard — ``mt5.initialize()`` /
           ``mt5.shutdown()`` are per-process singletons.
        2. ``mt5.initialize()`` via ``asyncio.to_thread()`` — ``path`` is the
           only positional parameter; ``login``/``password``/``server`` are
           passed as keyword arguments (omitting ``path`` auto-detects the
           terminal).
        3. ``mt5.account_info()`` — resolve the actual account login.
        4. Build the reverse alias table.
        5. Publish ``ConnectionStateEvent(connected=True)``.
        6. Start ``_poll_task = asyncio.create_task(self._poll_loop())``.

        The guard is held for the lifetime of the connection and released by
        ``disconnect()``.  On any failure the guard is released and a
        ``PlatformConnectionError`` is raised.

        Raises ``PlatformConnectionError`` if:
        - Another adapter is already connected in this process
        - ``mt5.initialize()`` fails
        - ``account_info()`` returns ``None`` after successful initialize
        """
        if self._connected:
            return

        # Acquire the process-global connection guard.  Only one adapter may
        # hold it — MetaTrader5's initialize()/shutdown() are process-wide.
        if not _connected_lock.acquire(blocking=False):
            raise PlatformConnectionError(
                "another MT5 adapter is already connected in this process — "
                "MetaTrader5.initialize()/shutdown() is a process-wide singleton"
            )

        try:
            mt5 = _get_mt5()

            initialize_kwargs: dict[str, Any] = {
                "login": self._config.login,
                "password": self._config.password,
                "server": self._config.server,
            }
            if self._config.path is not None:
                initialize_kwargs["path"] = self._config.path

            initialized = await asyncio.to_thread(mt5.initialize, **initialize_kwargs)
            if not initialized:
                code, desc = mt5.last_error()
                raise map_mt5_error(code, desc or "mt5.initialize() failed")

            account_info = await asyncio.to_thread(mt5.account_info)
            if account_info is None:
                code, desc = mt5.last_error()
                raise map_mt5_error(
                    code,
                    desc or "mt5.account_info() returned None after initialize",
                )

            self._account_login = int(account_info.login)
            self._build_reverse_alias()
            self._connected = True
            self._publish_connection_state(True)
            self._poll_task = asyncio.create_task(self._poll_loop())
        except Exception as exc:
            self._connected = False
            self._account_login = None
            self._poll_task = None
            _connected_lock.release()
            raise PlatformConnectionError(f"failed to connect to MT5 terminal: {exc}") from exc

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via MT5Engine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

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
        """Cancel the polling loop and shut down the MT5 terminal connection.

        Connection teardown (see mt5.md Step 10):

        1. Cancel ``_poll_task`` and await its cancellation.
        2. ``mt5.shutdown()`` via ``asyncio.to_thread()``.
        3. Release the process-global guard.
        4. Publish ``ConnectionStateEvent(connected=False)``.

        Idempotent — safe to call when already disconnected; a second call
        is a no-op.  Teardown of the local state (flags, guard, event) always
        runs even if ``mt5.shutdown()`` itself fails, so a failed shutdown
        never leaves the process-global connection permanently reserved.
        """
        if not self._connected:
            return

        # 1. Cancel the background polling loop and wait for it to stop.
        if self._poll_task is not None:
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)
            self._poll_task = None

        try:
            # 2. Shut down the process-global terminal connection.
            mt5 = _get_mt5()
            await asyncio.to_thread(mt5.shutdown)
        finally:
            # 3-4. Release the guard and report the state change regardless
            # of whether shutdown() succeeded.
            self._connected = False
            self._account_login = None
            _connected_lock.release()
            self._publish_connection_state(False)

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to MT5.

        - Resolves the MT5 symbol via the alias table.
        - For MARKET orders: fetches current bid/ask from ``symbol_info_tick()``.
        - Selects the filling mode per symbol.
        - Calls ``mt5.order_send()`` via ``asyncio.to_thread()``.
        - Maps errors via ``map_mt5_error()``.
        - Records the ``client_order_id → ticket`` mapping.

        Raises:
            InvalidSymbolError: symbol unknown to this broker, or market closed.
            UnsupportedOrderTypeError: TP/SL with limit_price set (not natively supported).
        """
        raise NotImplementedError

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing pending order via ``TRADE_ACTION_MODIFY``.

        Can change: price, stop_price, take_profit, stop_loss.
        Cannot change: quantity — raises ``UnsupportedOrderTypeError``
        (MT5 limitation — cancel and re-place is required).

        Looks up the MT5 ticket from ``client_order_id``.
        """
        raise NotImplementedError

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by ``client_order_id``.

        Looks up the MT5 ticket, then calls ``TRADE_ACTION_REMOVE``.
        Raises ``OrderNotFoundError`` if unknown.
        """
        raise NotImplementedError

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by ``client_order_id``.

        Looks up the MT5 ticket, then calls ``mt5.order_get(ticket=...)``.
        Returns ``None`` if not found in the mapping or on the platform.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Position TP/SL modification
    # ------------------------------------------------------------------

    async def modify_position_tpsl(
        self,
        position_id: str,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an existing position via ``TRADE_ACTION_SLTP``.

        *position_id* is the MT5 position ticket (as a string).  At least
        one of *take_profit* or *stop_loss* must be provided.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Instrument metadata
    # ------------------------------------------------------------------

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules from MT5 via ``symbol_info()``.

        If ``symbol_info()`` returns ``None`` the symbol does not exist for
        this broker — raises ``InvalidSymbolError`` with a message that
        distinguishes "doesn't exist for this broker" from "not tradable."

        Cached with TTL per ``MT5Config.instrument_spec_cache_ttl``.
        Cache is invalidated on:
        - Order rejection due to invalid symbol / market closed
        - ``symbol_info()`` returning ``None`` on re-fetch (symbol delisted)
        """
        cached = self._spec_cache.get(instrument)
        if cached is not None:
            spec, fetched_at = cached
            ttl = self._config.instrument_spec_cache_ttl
            if ttl is None or (_utcnow() - fetched_at).total_seconds() < ttl:
                return spec
            self._spec_cache.pop(instrument, None)

        mt5_symbol = self._resolve_mt5_symbol(instrument)
        mt5 = _get_mt5()
        info = await asyncio.to_thread(mt5.symbol_info, mt5_symbol)
        if info is None:
            _, desc = mt5.last_error()
            detail = desc or "symbol not found on this broker"
            raise InvalidSymbolError(f"MT5 symbol {mt5_symbol!r} is not available: {detail}")
        if info.trade_mode == 0:  # SYMBOL_TRADE_MODE_DISABLED
            raise InvalidSymbolError(
                f"MT5 symbol {mt5_symbol!r} is not tradable (trade mode disabled)"
            )

        volume_step = Decimal(str(info.volume_step))
        spec = InstrumentSpec(
            tick_size=Decimal(str(info.trade_tick_size)),
            lot_size=volume_step,
            min_qty=Decimal(str(info.volume_min)),
            max_qty=Decimal(str(info.volume_max)),
            min_notional=Decimal("0"),
            price_precision=int(info.digits),
            qty_precision=_decimal_places(volume_step),
        )
        self._spec_cache[instrument] = (spec, _utcnow())
        return spec

    # ------------------------------------------------------------------
    # Capability reporting
    # ------------------------------------------------------------------

    def supported_order_types(self) -> frozenset[OrderType]:
        """MT5 supports the guaranteed four order types."""
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
        """Return MT5 rate-limit state.

        MT5 has no explicit rate-limit endpoint.  The adapter reports a
        conservative estimate based on known MT5 broker limits
        (typically ~1 request per second per symbol for order operations).
        Configurable in a future revision.
        """
        return RateLimits(
            requests_per_interval=1,
            interval_seconds=1.0,
            remaining=1,
            reset_at=_utcnow(),
        )

    # ------------------------------------------------------------------
    # Reconciliation data (optional ABC methods)
    # ------------------------------------------------------------------

    async def fetch_positions(self) -> dict[Instrument, Position]:
        """Fetch all open positions via ``mt5.positions_get()``.

        In hedging mode, individual legs are netted per instrument before
        returning — core sees one ``Position`` per instrument regardless
        of the account's netting/hedging mode.
        """
        mt5 = _get_mt5()

        def _fetch() -> dict[Instrument, Position]:
            positions = mt5.positions_get()
            self._check_call_result("positions_get", positions)
            instruments = self._resolve_poll_instruments(mt5, positions, ())
            return self._net_positions(positions, instruments, _utcnow())

        return await asyncio.to_thread(_fetch)

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances via ``mt5.account_info()``."""
        mt5 = _get_mt5()

        def _fetch() -> dict[str, Balance]:
            account = mt5.account_info()
            self._check_call_result("account_info", account)
            balance = self._build_balance(account, _utcnow())
            return {balance.currency: balance}

        return await asyncio.to_thread(_fetch)

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch all open orders via ``mt5.orders_get()``.

        Keyed by ``client_order_id``; an order placed outside the engine
        (unknown ticket) falls back to the platform order id as a stable
        non-colliding key so it can still be reconciled.  Orders whose
        symbol cannot be resolved are skipped with a warning.
        """
        mt5 = _get_mt5()

        def _fetch() -> dict[str, OrderRecord]:
            orders = mt5.orders_get()
            self._check_call_result("orders_get", orders)
            instruments = self._resolve_poll_instruments(mt5, orders, ())
            result: dict[str, OrderRecord] = {}
            for order in orders or ():
                instrument = instruments.get(order.symbol)
                if instrument is None:
                    continue
                client_order_id = self._ticket_to_order_id.get(order.ticket, "")
                record = build_order_record(order, client_order_id, instrument)
                key = record.client_order_id or record.platform_order_id or ""
                result[key] = record
            return result

        return await asyncio.to_thread(_fetch)

    async def fetch_fills(self) -> dict[str, list[FillRecord]]:
        """Fetch recent fills via ``mt5.history_deals_get()``.

        Only trading deals (DEAL_TYPE_BUY/SELL) are fills; balance
        operations (DEAL_TYPE_IN/OUT) are excluded.  Results are grouped by
        ``client_order_id``.  Unlike the polling loop, this read does not
        advance the fill baseline — reconciliation must not disturb the
        poll loop's dedup state.
        """
        mt5 = _get_mt5()

        def _fetch() -> dict[str, list[FillRecord]]:
            account = mt5.account_info()
            self._check_call_result("account_info", account)
            deals = mt5.history_deals_get(self._last_deal_time, _utcnow())
            self._check_call_result("history_deals_get", deals)
            instruments = self._resolve_poll_instruments(mt5, (), deals)
            result: dict[str, list[FillRecord]] = {}
            for deal in deals or ():
                if deal.type not in (0, 1):
                    continue
                if deal.symbol not in instruments:
                    continue
                fill = self._build_fill(deal, instruments, account)
                result.setdefault(fill.client_order_id, []).append(fill)
            return result

        return await asyncio.to_thread(_fetch)

    # ------------------------------------------------------------------
    # Polling loop (adapter-internal)
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Background coroutine — runs until ``disconnect()`` cancels it.

        Each cycle fetches orders, positions, balances, and new deals in
        a single ``to_thread()`` block, then diffs against last-known state
        and publishes events for any changes.
        """
        while self._connected:
            try:
                await self._poll_once()
            except Exception:
                logger.warning(
                    "Poll cycle failed; last-known state preserved. Continuing.",
                    exc_info=True,
                )
            await asyncio.sleep(self._config.poll_interval_seconds)

    async def _poll_once(self) -> None:
        """One complete poll cycle.

        Fetches orders, positions, balance, and new deals inside a single
        ``asyncio.to_thread()`` block (one GIL handoff per cycle), diffs
        each dataset against the last-known state, and publishes
        ``FillEvent`` / ``PositionUpdateEvent`` / ``BalanceUpdateEvent``
        for changes.  Order diffs only update internal state — order
        lifecycle events are the engine's responsibility (Section 6.1),
        never the polling loop's.

        A single-cycle failure raises so ``_poll_loop`` can log and
        continue.  Baselines are committed per-dataset as each processor
        runs (baseline first, then publish), so a failure mid-cycle never
        causes the *next* cycle to re-report unchanged state — earlier
        datasets may already be committed when a later one fails.
        """
        mt5 = _get_mt5()

        def _fetch_snapshot() -> tuple[Any, ...]:
            now = _utcnow()
            orders = mt5.orders_get()
            self._check_call_result("orders_get", orders)
            positions = mt5.positions_get()
            self._check_call_result("positions_get", positions)
            account = mt5.account_info()
            self._check_call_result("account_info", account)
            deals = mt5.history_deals_get(self._last_deal_time, now)
            self._check_call_result("history_deals_get", deals)
            instruments = self._resolve_poll_instruments(mt5, positions, deals)
            return orders, positions, account, deals, instruments, now

        orders, positions, account, deals, instruments, now = await asyncio.to_thread(
            _fetch_snapshot
        )

        self._process_orders(orders)
        self._process_positions(positions, instruments, now)
        self._process_balance(account, now)
        self._process_fills(deals, instruments, account, now)

    def _process_orders(self, orders: Any) -> None:
        """Update the last-known open-order snapshot; publish nothing.

        ``orders_get()`` returns only live (non-final) orders, so simply
        replacing ``_last_orders`` both detects new/updated orders and
        drops filled/cancelled ones.
        """
        new_state: dict[int, object] = {}
        for order in orders or ():
            new_state[order.ticket] = order
        self._last_orders = new_state

    def _process_positions(
        self,
        positions: Any,
        instruments: dict[str, Instrument],
        now: datetime,
    ) -> None:
        """Net position legs per instrument and publish changes.

        Hedging-mode accounts return one position tuple per leg; they are
        netted into a single ``Position`` per instrument before the diff.
        A position that disappears from ``positions_get()`` is published as
        a flat (quantity 0) position so the mirror learns of the close.
        """
        netted = self._net_positions(positions, instruments, now)
        previous_positions = self._last_positions
        # Commit the baseline from the fetched snapshot *before* publishing —
        # a mid-cycle publish failure must not leave a stale baseline that
        # makes the next cycle re-report the same state.
        self._last_positions = netted
        for instrument, position in netted.items():
            previous = previous_positions.get(instrument)
            # Diff on quantity/price only — updated_at is snapshot metadata
            # and must not make an unchanged position look "changed".
            if (
                previous is None
                or previous.quantity != position.quantity
                or previous.average_entry_price != position.average_entry_price
            ):
                self._publish_position(position)
        for instrument in previous_positions:
            if instrument not in netted:
                previous = previous_positions[instrument]
                closed = Position(
                    instrument=instrument,
                    quantity=Decimal("0"),
                    average_entry_price=previous.average_entry_price,
                    updated_at=now,
                )
                self._publish_position(closed)

    def _net_positions(
        self,
        positions: Any,
        instruments: dict[str, Instrument],
        now: datetime,
    ) -> dict[Instrument, Position]:
        """Net raw ``positions_get()`` legs into one ``Position`` per instrument.

        Signed quantity follows the core convention: BUY leg = +volume,
        SELL leg = -volume.  The average entry price is the volume-weighted
        average across the legs.
        """
        legs_by_symbol: dict[str, list[Any]] = {}
        for position in positions or ():
            legs_by_symbol.setdefault(position.symbol, []).append(position)

        result: dict[Instrument, Position] = {}
        for symbol, legs in legs_by_symbol.items():
            instrument = instruments.get(symbol)
            if instrument is None:
                continue
            net_quantity = Decimal("0")
            weighted_price = Decimal("0")
            total_volume = Decimal("0")
            for leg in legs:
                volume = Decimal(str(leg.volume))
                # POSITION_TYPE_BUY = 0, POSITION_TYPE_SELL = 1.
                quantity = volume if leg.type == 0 else -volume
                net_quantity += quantity
                total_volume += volume
                weighted_price += volume * Decimal(str(leg.price_open))
            average = weighted_price / total_volume if total_volume else Decimal("0")
            result[instrument] = Position(
                instrument=instrument,
                quantity=net_quantity,
                average_entry_price=average,
                updated_at=now,
            )
        return result

    def _build_balance(self, account: Any, now: datetime) -> Balance:
        """Reconstruct a ``Balance`` from an ``account_info()`` snapshot.

        MT5 reports ``equity``, ``margin``, and ``margin_free`` as three
        independent floats.  Reconstructing all three via
        ``Decimal(str(float))`` can break the core invariant
        ``free + used == total`` on tiny float-rounding errors
        (``Balance.__post_init__`` enforces exact Decimal equality), so
        derive ``free`` from the two primary reported values instead of
        using the raw ``margin_free``.
        """
        used = Decimal(str(account.margin))
        total = Decimal(str(account.equity))
        return Balance(
            currency=str(account.currency),
            free=total - used,
            used=used,
            total=total,
            updated_at=now,
        )

    def _process_balance(self, account: Any, now: datetime) -> None:
        """Translate ``account_info()`` into a ``Balance`` and publish changes."""
        balance = self._build_balance(account, now)
        # Diff on monetary fields only — updated_at is snapshot metadata and
        # must not make an unchanged balance look "changed".
        last = self._last_balance
        self._last_balance = balance
        if (
            last is None
            or last.currency != balance.currency
            or last.free != balance.free
            or last.used != balance.used
            or last.total != balance.total
        ):
            self._publish(
                BalanceUpdateEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=None,
                    balance=balance,
                )
            )

    def _process_fills(
        self,
        deals: Any,
        instruments: dict[str, Instrument],
        account: Any,
        now: datetime,
    ) -> None:
        """Publish a ``FillEvent`` for each new fill since ``_last_deal_time``.

        Only DEAL_TYPE_BUY (0) / DEAL_TYPE_SELL (1) deals are fills —
        DEAL_TYPE_IN/OUT are non-trading balance operations.  Deal
        timestamps are second-granular, so exact dedup uses the monotonic
        deal ticket (``_last_deal_ticket``): a deal is new iff its time is
        not older than the window *and* its ticket exceeds the last seen.
        This both catches same-second deals that time alone would miss and
        prevents re-reporting already-published fills.

        The ticket/time baseline is committed immediately after each
        successful publish, so an exception mid-loop re-tries only the
        unpublished deals — no duplicates for the ones already reported.
        """
        for deal in deals or ():
            deal_time = datetime.fromtimestamp(int(deal.time), tz=UTC)
            if deal_time < self._last_deal_time:
                continue
            if deal.type not in (0, 1):
                continue
            if deal.ticket <= self._last_deal_ticket:
                continue
            if deal.symbol not in instruments:
                # Unresolvable symbol — already warned at resolve time; skip
                # its fills so one bad symbol can't fail the whole cycle.
                continue
            volume = Decimal(str(deal.volume))
            price = Decimal(str(deal.price))
            if volume <= 0 or price <= 0:
                continue
            fill = self._build_fill(deal, instruments, account)
            self._publish(
                FillEvent(
                    event_id=_new_id(),
                    timestamp=_utcnow(),
                    adapter_name=self.platform_name,
                    account_id=self.account_id,
                    correlation_id=fill.correlation_id,
                    fill=fill,
                )
            )
            if deal.ticket > self._last_deal_ticket:
                self._last_deal_ticket = deal.ticket
            if deal_time > self._last_deal_time:
                self._last_deal_time = deal_time

    def _build_fill(
        self,
        deal: Any,
        instruments: dict[str, Instrument],
        account: Any,
    ) -> FillRecord:
        """Build a ``FillRecord`` from one MT5 deal tuple.

        MT5 deals carry no client order id — the ``order`` ticket is resolved
        through the ``client_order_id → ticket`` mapping recorded by
        ``place_order``; an unknown ticket (order placed from the terminal)
        yields an empty ``client_order_id``.
        """
        client_order_id = self._ticket_to_order_id.get(deal.order, "")
        fee = Decimal(str(deal.commission)) + Decimal(str(deal.fee))
        return FillRecord(
            client_order_id=client_order_id,
            platform_fill_id=str(deal.ticket),
            instrument=instruments[deal.symbol],
            fill_quantity=Decimal(str(deal.volume)),
            fill_price=Decimal(str(deal.price)),
            fill_timestamp=datetime.fromtimestamp(int(deal.time), tz=UTC),
            fee_currency=str(account.currency) if fee else None,
            fee_amount=fee if fee else None,
            correlation_id=client_order_id,
            position_id=str(deal.position_id) if deal.position_id else None,
        )

    def _resolve_poll_instruments(
        self,
        mt5: Any,
        positions: Any,
        deals: Any,
    ) -> dict[str, Instrument]:
        """Resolve canonical ``Instrument`` for every symbol reported by
        positions and deals.  Cached — a symbol is resolved only on its
        first sighting.

        Unresolvable symbols (unknown broker path, ``symbol_info()``
        failure) are skipped with a one-time warning per symbol rather than
        failing the whole cycle — one bad symbol must not silence events for
        every other instrument.
        """
        symbols: set[str] = set()
        for position in positions or ():
            symbols.add(position.symbol)
        for deal in deals or ():
            symbols.add(deal.symbol)
        resolved: dict[str, Instrument] = {}
        for symbol in symbols:
            try:
                resolved[symbol] = self._resolve_instrument(symbol, mt5)
            except (ValueError, UteError) as exc:
                if symbol not in self._failed_symbols:
                    self._failed_symbols.add(symbol)
                    logger.warning(
                        "Skipping symbol %s — cannot build Instrument: %s",
                        symbol,
                        exc,
                    )
        return resolved

    def _resolve_instrument(self, mt5_symbol: str, mt5: Any) -> Instrument:
        """Build the canonical ``Instrument`` for an MT5 symbol.

        Combines ``from_mt5_symbol()`` (symbol/quote from the reverse alias
        table) with ``symbol_info().path`` (asset class).  Must only be
        called from within a ``to_thread()`` block — ``symbol_info()`` is an
        MT5 IPC call.
        """
        cached = self._symbol_to_instrument.get(mt5_symbol)
        if cached is not None:
            return cached
        symbol, quote = from_mt5_symbol(mt5_symbol, self._reverse_alias)
        info = mt5.symbol_info(mt5_symbol)
        if info is None:
            code, desc = mt5.last_error()
            raise map_mt5_error(
                code,
                desc or f"mt5.symbol_info() returned None for {mt5_symbol}",
            )
        asset_class = self._asset_class_from_path(info.path)
        instrument = _with_broker_override(
            Instrument(symbol=symbol, quote_currency=quote, asset_class=asset_class),
            mt5_symbol,
        )
        self._symbol_to_instrument[mt5_symbol] = instrument
        return instrument

    # ------------------------------------------------------------------
    # Internal helpers (implement these)
    # ------------------------------------------------------------------

    def _resolve_mt5_symbol(self, instrument: Instrument) -> str:
        """Apply the alias table and return the MT5 broker symbol string.

        The alias table is authoritative per D-8: an entry for the
        instrument's shorthand wins over any pre-set
        ``broker_symbol_override``.  ``str()`` only produces a "BASE/QUOTE"
        shorthand for pairs (forex/crypto/perp) — it raises ``ValueError``
        for stocks, CFDs, bonds, funds, and dated futures, so the alias
        lookup is guarded and those instruments resolve via
        ``to_mt5_symbol`` (which honours ``broker_symbol_override`` and
        finally falls back to ``symbol + quote_currency``).
        """
        try:
            alias_key = str(instrument)
        except ValueError:
            alias_key = None
        override = self._config.symbol_alias_table.get(alias_key) if alias_key is not None else None
        if override is not None:
            instrument = _with_broker_override(instrument, override)
        return to_mt5_symbol(instrument)

    def _invalidate_spec_cache(self, instrument: Instrument) -> None:
        """Remove a cached ``InstrumentSpec``, forcing a re-fetch on next access."""
        self._spec_cache.pop(instrument, None)

    def _build_reverse_alias(self) -> None:
        """Build the reverse alias table from ``MT5Config.symbol_alias_table``."""
        self._reverse_alias = {v: k for k, v in self._config.symbol_alias_table.items()}

    def _publish_position(self, position: Position) -> None:
        self._publish(
            PositionUpdateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                position=position,
            )
        )

    def _asset_class_from_path(self, path: str) -> AssetClass:
        """Derive the canonical ``AssetClass`` from an MT5 symbol's market path.

        ``symbol_info().path`` is the broker's market tree (e.g. ``"Forex\\EURUSD"``,
        ``"Metals\\XAUUSD"``, ``"Indices\\US500"``, ``"Stocks\\AAPL"``).  This is
        the authoritative source for asset class — never guessed from the symbol
        string.  Used by the inbound reconstruction path: ``symbol_info()`` gives
        the path, ``from_mt5_symbol()`` gives the ``(symbol, quote)`` pair, and this
        function completes the ``Instrument``.

        The exact mapping is broker-dependent; raise ``ValueError`` for an
        unrecognized path rather than defaulting to a wrong asset class.
        """
        # Match on the first path component only — substring containment
        # could misclassify e.g. "Stocks\\CryptoMining" as SPOT.
        segment = path.upper().split("\\")[0].split("/")[0]
        asset_class = _PATH_ASSET_CLASS.get(segment)
        if asset_class is None:
            raise ValueError(
                f"Unrecognized MT5 symbol path {path!r} — cannot derive asset class. "
                "Add a mapping to _PATH_ASSET_CLASS."
            )
        return asset_class

    def _check_call_result(self, call_name: str, result: Any) -> None:
        """Raise the mapped MT5 error if *result* is ``None``.

        Must be called immediately after the corresponding MT5 call — a
        later successful call would reset ``last_error()`` and hide the
        failure.  Empty tuples are valid "no data" for orders/positions/
        deals, so only ``None`` is treated as failure.
        """
        if result is None:
            mt5 = _get_mt5()
            code, desc = mt5.last_error()
            raise map_mt5_error(code, desc or f"{call_name}() returned None")

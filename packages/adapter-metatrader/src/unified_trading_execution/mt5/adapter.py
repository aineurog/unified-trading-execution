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
import time
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
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
from unified_trading_execution.mt5.comments import decode_comment, encode_client_order_id
from unified_trading_execution.mt5.errors import map_mt5_error
from unified_trading_execution.mt5.orders import (
    _MT5_ORDER_TYPE_TO_UNIFIED,
    _price_stop_price,
    _select_filling,
    build_mt5_cancel_request,
    build_mt5_modify_request,
    build_mt5_request,
    build_mt5_sltp_request,
    build_order_record,
    from_mt5_epoch,
    parse_mt5_result,
    parse_order_record,
)
from unified_trading_execution.mt5.symbols import to_mt5_symbol
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import (
    Instrument,
    InstrumentSpec,
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

# Deal stamps are second-granular: rewind `from` and pad `to` so edge fills
# are never clipped; the ticket/time dedup in _process_fills absorbs extras.
_DEAL_QUERY_BACKLOG_SECONDS = 5
_DEAL_QUERY_FORWARD_SECONDS = 10

# Order-mapping recovery scans MT5 history back this far for ``U:`` comments.
_RECOVERY_DEAL_LOOKBACK_SECONDS = 24 * 60 * 60

# Offset probe: bounded symbol scan; reject impossible offsets (>±24h).
_MAX_OFFSET_PROBE_SYMBOLS = 20
_MAX_SERVER_TIME_OFFSET_SECONDS = 24 * 60 * 60

# After symbol_select() the terminal subscribes to quotes asynchronously —
# symbol_info_tick() can briefly return None.  MARKET orders retry the fetch.
_MARKET_TICK_RETRIES = 3
_MARKET_TICK_RETRY_DELAY_SECONDS = 0.1

if TYPE_CHECKING:
    from unified_trading_execution.mt5.config import MT5Config
    from unified_trading_execution.state import StateStore

# ---------------------------------------------------------------------------
# Process-global connection guard
# ---------------------------------------------------------------------------

_connected_lock = threading.Lock()


def _get_mt5() -> Any:
    """Lazy-import ``MetaTrader5``.  Raises ``ImportError`` with a clear
    message on non-Windows platforms."""
    try:
        import MetaTrader5 as mt5
    except ImportError:
        raise ImportError(
            "MetaTrader5 package is required for MT5 adapter. "
            "It is only available on Windows. "
            "Install with: pip install unified-trading-execution-metatrader[mt5]"
        ) from None
    return mt5


def _new_id() -> str:
    return str(uuid7())


def _wait_for_market_tick(
    mt5: Any,
    mt5_symbol: str,
    *,
    retries: int = _MARKET_TICK_RETRIES,
    delay: float = _MARKET_TICK_RETRY_DELAY_SECONDS,
) -> Any:
    """Fetch a live tick, retrying briefly after Market Watch selection.

    ``symbol_select()`` subscribes asynchronously — the terminal may not
    have the first quote the instant after selection.  Returns the first
    non-``None`` tick, or ``None`` if every retry came back empty (market
    closed / no data).
    """
    for _ in range(retries):
        tick = mt5.symbol_info_tick(mt5_symbol)
        if tick is not None:
            return tick
        time.sleep(delay)
    return None


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


# MT5 symbol market-path segment → canonical AssetClass.  This is a *thesaurus*
# of the standard market-tree folder names, matched case-insensitively against
# ANY segment of ``symbol_info().path`` (not just the first), so a broker's
# account-group root (e.g. Oanda's ``"PRO\\Noble\\GOLD.pro"``) cannot hide the
# meaningful segment.  It is the primary signal; ``trade_calc_mode`` is only a
# fallback because the same asset can report different calc modes per broker
# (a metal is 0 on one broker and 4 on another).  Extendable per-broker via
# ``MT5Config.asset_class_path_map``.
#
# Soft commodities map to CFD: precious metals are caught earlier by the
# broker-independent ``_METAL_BASE_CURRENCIES`` check, so a "Commodities"
# folder that holds silver (XAG) resolves as metal before reaching this table.
_PATH_ASSET_CLASS: dict[str, AssetClass] = {
    "FOREX": AssetClass.MARGIN_FX,
    "FX": AssetClass.MARGIN_FX,
    "CURRENCIES": AssetClass.MARGIN_FX,
    "METALS": AssetClass.MARGIN_FX,
    "NOBLE": AssetClass.MARGIN_FX,       # precious metals (Oanda's market folder)
    "PRECIOUS": AssetClass.MARGIN_FX,
    "COMMODITIES": AssetClass.CFD,       # soft commodities (sugar, oil, coffee)
    "ENERGY": AssetClass.CFD,
    "INDICES": AssetClass.CFD,
    "INDEX": AssetClass.CFD,
    "STOCKS": AssetClass.STOCK,
    "STOCK": AssetClass.STOCK,
    "EQUITIES": AssetClass.STOCK,
    "EQUITIES_CFD": AssetClass.STOCK,
    "SHARES": AssetClass.STOCK,
    "CRYPTOCURRENCIES": AssetClass.SPOT,
    "CRYPTO": AssetClass.SPOT,
    "FUTURES": AssetClass.FUTURES,
    "BONDS": AssetClass.BOND,
    "FUNDS": AssetClass.FUND,
    "ETF": AssetClass.FUND,
}

# MT5 ``ENUM_SYMBOL_CALC_MODE`` → canonical AssetClass.  Broker-independent in
# *type* but not in *value* for a given asset (metals and indices report
# different modes across brokers), so it is only a last-resort fallback when
# neither the metal-base check nor the path thesaurus resolves.
_CALC_MODE_ASSET_CLASS: dict[int, AssetClass] = {
    0: AssetClass.MARGIN_FX,    # SYMBOL_CALC_MODE_FOREX
    1: AssetClass.FUTURES,      # SYMBOL_CALC_MODE_FUTURES
    2: AssetClass.CFD,          # SYMBOL_CALC_MODE_CFD
    3: AssetClass.CFD,          # SYMBOL_CALC_MODE_CFDINDEX
    4: AssetClass.CFD,          # SYMBOL_CALC_MODE_CFDLEVERAGE
    32: AssetClass.STOCK,       # SYMBOL_CALC_MODE_EXCH_STOCKS
    33: AssetClass.FUTURES,     # SYMBOL_CALC_MODE_EXCH_FUTURES
    64: AssetClass.MARGIN_FX,   # SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE
    66: AssetClass.BOND,        # SYMBOL_CALC_MODE_EXCH_BONDS
    67: AssetClass.STOCK,       # SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX
    68: AssetClass.BOND,        # SYMBOL_CALC_MODE_EXCH_BONDS_MOEX
}

# Base currencies that are precious metals.  Broker-independent — ``currency_base``
# is MT5's own field.  Disambiguates a metal (XAUUSD/XAGUSD) that a broker groups
# under a "Commodities" folder from a soft commodity (SUGAR).
_METAL_BASE_CURRENCIES: frozenset[str] = frozenset({"XAU", "XAG", "XPT", "XPD"})

# Broker symbol suffixes to strip before splitting a name into base/quote
# (matched case-insensitively, longest-most-specific, applied repeatedly so
# ``AAPL_CFD.US`` → ``AAPL``).  Pure name cleanup — never an asset-class guess.
_BROKER_SYMBOL_SUFFIXES: tuple[str, ...] = (
    ".PRO",
    ".US",
    ".UK",
    ".DE",
    "_CFD",
    "-CASH",
    "+",
)

# Quote-currency suffixes to split a crypto/forex name (``SOLUSD`` → ``SOL``/``USD``).
# Ordered longest-first so ``USDT`` is matched before ``USD``.
_QUOTE_CURRENCY_SUFFIXES: tuple[str, ...] = (
    "USDT",
    "USDC",
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "AUD",
    "NZD",
    "CAD",
    "TRY",
    "ZAR",
)


def _path_segments(path: str) -> list[str]:
    """Split a broker market path into uppercased segments, empty dropped.

    Handles both ``\\`` and ``/`` separators so the classifier is agnostic to
    how the broker writes its market tree.
    """
    return [seg.upper() for seg in path.replace("/", "\\").split("\\") if seg]


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

        # Merge the broker-specific path thesaurus from config over the built-in
        # defaults, keyed case-insensitively (segments are uppercased on lookup).
        self._path_asset_class = dict(_PATH_ASSET_CLASS)
        if config.asset_class_path_map:
            self._path_asset_class.update(
                {
                    str(segment).upper(): asset_class
                    for segment, asset_class in config.asset_class_path_map.items()
                }
            )

        # Actual account login, resolved from terminal after connect().
        self._account_login: int | None = None

        # Background polling task — started in connect(), cancelled in disconnect().
        self._poll_task: asyncio.Task[None] | None = None

        # -- Internal state tracking for diff-based polling --
        # client_order_id → ticket (int) mapping for active orders
        self._order_id_to_ticket: dict[str, int] = {}
        self._ticket_to_order_id: dict[int, str] = {}

        # Engine-managed store (attached by core via attach_state_store) —
        # the authoritative client_order_id → ticket record.  The adapter
        # only reads it at connect() to seed the maps; it never writes it.
        self._state_store: StateStore | None = None

        # Last known state snapshots.  Orders are keyed by MT5 ticket (raw
        # tuples), positions are NETTED per instrument (one Position per
        # instrument regardless of netting/hedging mode), and balance is the
        # single-currency account balance.
        self._last_orders: dict[int, object] = {}
        self._last_positions: dict[Instrument, Position] = {}
        self._last_balance: Balance | None = None
        # Deal dedup baseline in the server-as-epoch basis (raw deal.time).
        # Anchored lazily on the first window build after the server offset is
        # measured, then advanced to the newest deal seen.  Keeping it in the
        # raw basis makes dedup immune to offset-measurement jitter.
        self._last_deal_time: int | None = None
        self._last_deal_ticket: int = 0

        # Server-as-epoch minus real-UTC epoch (seconds); measured from a live
        # tick and refreshed each poll cycle — see _server_time_offset_seconds.
        self._server_time_offset: int = 0

        # Broker symbol → canonical Instrument cache for inbound reconstruction.
        # Seeded at connect from the state store, extended on every outbound
        # order, and completed lazily via symbol_info() metadata for symbols
        # the engine has never traded (e.g. manual terminal positions).
        self._symbol_to_instrument: dict[str, Instrument] = {}
        self._failed_symbols: set[str] = set()

        # Symbols selected in Market Watch this session.  MT5 streams real-time
        # quotes only for selected symbols — see _ensure_symbol_selected().
        self._selected_symbols: set[str] = set()

        # Instrument spec cache: Instrument → (InstrumentSpec, fetched_at)
        self._spec_cache: dict[Instrument, tuple[InstrumentSpec, datetime]] = {}

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

    def attach_state_store(self, state_store: StateStore) -> None:
        """Store the engine-managed ``StateStore`` for mapping recovery.

        Core attaches its ``SQLiteStateStore`` before ``connect()``.  At
        connect the adapter seeds its ``client_order_id ↔ ticket`` maps from
        the store (authoritative, survives broker comment rewriting) and only
        then scans MT5 comments to fill any gaps.
        """
        self._state_store = state_store

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
        4. Seed ``platform_symbol → Instrument`` and ``client_order_id → ticket``
           maps from the state store (authoritative), then cross-check
           ``client_order_id ↔ ticket`` against ``U:`` order comments, so
           pre-restart orders can still be managed.
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
            await self._seed_symbol_mappings_from_state_store()
            await self._seed_mappings_from_state_store()
            await asyncio.to_thread(self._recover_order_mappings, mt5)
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

    def _ticket_for_order(self, client_order_id: str) -> int:
        """Return the MT5 ticket recorded for *client_order_id*.

        Raises ``OrderNotFoundError`` when the id was never placed by this
        engine (no ticket mapping exists).
        """
        ticket = self._order_id_to_ticket.get(client_order_id)
        if ticket is None:
            raise OrderNotFoundError(
                f"no known MT5 ticket for client_order_id {client_order_id!r} — "
                "the order was not placed by this engine"
            )
        return ticket

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to MT5.

        - Resolves the MT5 symbol via the alias table.
        - Ensures the symbol is selected in Market Watch so real-time quotes
          flow (``symbol_select``); a broker-missing symbol raises early.
        - For MARKET orders: fetches current bid/ask from ``symbol_info_tick()``
          so the deal goes in at the live quote.
        - Selects the filling mode per symbol.
        - Calls ``mt5.order_send()`` via ``asyncio.to_thread()``.
        - Maps errors via ``map_mt5_error()``.
        - Records the ``client_order_id → ticket`` mapping on success.

        A missing ``client_order_id`` is generated (UUID7) so the ticket
        mapping always has a stable key.  A rejection invalidates the cached
        instrument spec — stale rules are a common cause (Section 17.3).

        Raises:
            InvalidSymbolError: symbol unknown to this broker, or no filling
                mode compatible with the requested time_in_force.
            UnsupportedOrderTypeError: TP/SL with limit_price set (not
                natively supported).
            UteError subclasses: mapped ``order_send`` retcode failures.
        """
        mt5_symbol = self._resolve_mt5_symbol(order.instrument)
        client_order_id = order.client_order_id or _new_id()
        mt5 = _get_mt5()

        def _submit() -> OrderResult:
            self._ensure_symbol_selected(mt5_symbol, mt5)
            info = mt5.symbol_info(mt5_symbol)
            if info is None:
                code, desc = mt5.last_error()
                if not self._symbol_exists(mt5_symbol, mt5):
                    self._failed_symbols.add(mt5_symbol)
                    raise InvalidSymbolError(
                        f"symbol {mt5_symbol!r} is not available on this broker"
                    )
                raise map_mt5_error(code, desc or f"symbol_info() failed for {mt5_symbol}")

            # Correct a caller-supplied Instrument against the broker's own
            # symbol_info() so the DB stores the true identity.  platform_symbol
            # is mandatory and was used to resolve mt5_symbol; symbol /
            # quote_currency / asset_class are optional and are derived here,
            # then tested against the user's values — wrong ones are corrected
            # with a logged warning (never silently) and cached so the inbound
            # polling path reconstructs the same corrected instrument.
            try:
                canonical = self._build_instrument_from_symbol_info(mt5_symbol, info)
                if not self._identity_matches(order.instrument, canonical):
                    logger.warning(
                        "Correcting Instrument %r for platform_symbol %r: "
                        "broker reports symbol=%r quote_currency=%r asset_class=%r",
                        order.instrument,
                        mt5_symbol,
                        canonical.symbol,
                        canonical.quote_currency,
                        canonical.asset_class,
                    )
                    order.instrument = replace(
                        order.instrument,
                        symbol=canonical.symbol,
                        quote_currency=canonical.quote_currency,
                        asset_class=canonical.asset_class,
                    )
                    self._symbol_to_instrument[mt5_symbol] = order.instrument
            except ValueError as exc:
                logger.warning(
                    "Skipping instrument correction for %r — %s",
                    mt5_symbol,
                    exc,
                )

            request = build_mt5_request(order, mt5_module=mt5)
            request["symbol"] = mt5_symbol
            comment = encode_client_order_id(client_order_id)
            if comment is not None:
                request["comment"] = comment
            else:
                logger.warning(
                    "client_order_id %r is not encodable in an MT5 comment — "
                    "the id won't be recoverable after a restart",
                    client_order_id,
                )
            if order.order_type == OrderType.MARKET:
                tick = _wait_for_market_tick(mt5, mt5_symbol)
                if tick is None:
                    code, desc = mt5.last_error()
                    raise map_mt5_error(code, desc or f"no market quote for {mt5_symbol}")
                request["price"] = float(tick.ask if order.side == OrderSide.BUY else tick.bid)
            request["type_filling"] = _select_filling(info, order.time_in_force, mt5_module=mt5)
            result = mt5.order_send(request)
            return parse_mt5_result(result, client_order_id, mt5_module=mt5)

        try:
            result = await asyncio.to_thread(_submit)
        except UteError:
            self._invalidate_spec_cache(order.instrument)
            raise

        if result.platform_order_id is not None:
            ticket = int(result.platform_order_id)
            self._order_id_to_ticket[client_order_id] = ticket
            self._ticket_to_order_id[ticket] = client_order_id
        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing pending order via ``TRADE_ACTION_MODIFY``.

        Can change: price, stop_price, take_profit, stop_loss.
        Cannot change: quantity — raises ``UnsupportedOrderTypeError``
        (MT5 limitation — cancel and re-place is required).

        The current order type is queried live via ``orders_get()`` because
        MT5 stores the limit price in ``price`` and a stop-limit's limit
        price in ``stoplimit`` — the request must know which field to use.
        """
        ticket = self._ticket_for_order(modification.client_order_id)
        mt5 = _get_mt5()

        def _modify() -> OrderResult:
            existing_orders = mt5.orders_get(ticket=ticket)
            if existing_orders is None or len(existing_orders) == 0:
                code, desc = mt5.last_error()
                raise map_mt5_error(code, desc or f"order {ticket} not found for modification")
            existing = existing_orders[0]
            unified = _MT5_ORDER_TYPE_TO_UNIFIED.get(existing.type)
            if unified is None:
                raise PlatformError(f"Unknown MT5 order type {existing.type}")
            order_type, _ = unified
            current_price, current_stop_price = _price_stop_price(order_type, existing)
            request = build_mt5_modify_request(
                modification,
                ticket,
                order_type,
                mt5_module=mt5,
                current_price=current_price,
                current_stop_price=current_stop_price,
            )
            result = mt5.order_send(request)
            return parse_mt5_result(result, modification.client_order_id, mt5_module=mt5)

        return await asyncio.to_thread(_modify)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by ``client_order_id``.

        Looks up the MT5 ticket, then calls ``TRADE_ACTION_REMOVE``.
        Raises ``OrderNotFoundError`` if unknown.
        """
        ticket = self._ticket_for_order(client_order_id)
        mt5 = _get_mt5()

        def _cancel() -> OrderResult:
            request = build_mt5_cancel_request(ticket, mt5_module=mt5)
            result = mt5.order_send(request)
            parsed = parse_mt5_result(result, client_order_id, mt5_module=mt5)
            # A successful REMOVE produces no deal, so parse_mt5_result maps it
            # to the generic "pending order" status (OPEN) — but the order is
            # gone.  Override to CANCELLED so the engine persists the correct
            # lifecycle status.
            return replace(parsed, status=OrderStatus.CANCELLED)

        return await asyncio.to_thread(_cancel)

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by ``client_order_id``.

        Looks up the MT5 ticket, then calls ``mt5.orders_get(ticket=...)``
        (the Python wrapper has no singular ``order_get``).  Returns
        ``None`` if the id is unknown to the engine or the order is no
        longer active (filled/cancelled/expired).
        """
        ticket = self._order_id_to_ticket.get(client_order_id)
        if ticket is None:
            return None
        mt5 = _get_mt5()

        def _query() -> OrderResult | None:
            existing_orders = mt5.orders_get(ticket=ticket)
            if existing_orders is None or len(existing_orders) == 0:
                code, desc = mt5.last_error()
                # No error (0 / RES_S_OK) or "order not found" (10035) means the
                # order is simply no longer active — that is None, not a failure.
                if code == 0 or code == mt5.RES_S_OK or code == 10035:
                    return None
                raise map_mt5_error(code, desc or "orders_get() failed")
            self._server_time_offset_seconds(mt5)
            return parse_order_record(
                existing_orders[0],
                client_order_id,
                mt5_module=mt5,
                server_time_offset=self._server_time_offset,
            )

        return await asyncio.to_thread(_query)

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

        Raises ``UnsupportedOrderTypeError`` if an attachment carries a
        ``limit_price`` — MT5 TP/SL are price levels, not orders.
        """
        if take_profit is not None and take_profit.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "take_profit.limit_price is not supported by MT5 — take profit is a price level"
            )
        if stop_loss is not None and stop_loss.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "stop_loss.limit_price is not supported by MT5 — stop loss is a price level"
            )
        mt5 = _get_mt5()

        def _modify() -> None:
            request = build_mt5_sltp_request(
                position_id,
                take_profit=float(take_profit.trigger_price) if take_profit is not None else None,
                stop_loss=float(stop_loss.trigger_price) if stop_loss is not None else None,
                mt5_module=mt5,
            )
            result = mt5.order_send(request)
            # parse_mt5_result raises the mapped error on a failed retcode; the
            # returned OrderResult is discarded — the method is a convenience.
            parse_mt5_result(result, position_id, mt5_module=mt5)

        await asyncio.to_thread(_modify)

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
        await asyncio.to_thread(self._ensure_symbol_selected, mt5_symbol, mt5)
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
            reset_at=_utcnow() + timedelta(seconds=1.0),
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

    async def fetch_account_leverage(self) -> int:
        """Return the account-level leverage from ``account_info()`` (read-only).

        MT5 leverage is account-level and configured by the broker back-office;
        there is no setter (no MQL5 or Python API).  This accessor surfaces the
        current value as a read-only integer ratio (e.g. ``500`` for 1:500).

        Deliberately not on the ``Adapter`` ABC: most venues expose leverage
        per-instrument (Bybit's ``InstrumentSpec.max_leverage``), and MT5 has no
        per-symbol leverage at all — so this is an MT5-specific convenience, not
        a cross-platform contract.
        """
        mt5 = _get_mt5()

        def _fetch() -> int:
            account = mt5.account_info()
            self._check_call_result("account_info", account)
            return int(account.leverage)

        return await asyncio.to_thread(_fetch)

    async def resolve_instrument(self, platform_symbol: str) -> Instrument:
        """Build the canonical ``Instrument`` for an MT5 ``platform_symbol``.

        Fetches ``symbol_info()`` from the broker and reconstructs the
        canonical identity — a convenience for callers that want to discover
        (or verify) an instrument's ``symbol``/``quote_currency``/
        ``asset_class`` without placing an order.  Results are cached in the
        same ``_symbol_to_instrument`` map used by the polling path.
        """
        mt5 = _get_mt5()

        def _resolve() -> Instrument:
            return self._resolve_instrument(platform_symbol, mt5)

        return await asyncio.to_thread(_resolve)

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
            self._server_time_offset_seconds(mt5)
            instruments = self._resolve_poll_instruments(mt5, orders, ())
            result: dict[str, OrderRecord] = {}
            for order in orders or ():
                instrument = instruments.get(order.symbol)
                if instrument is None:
                    continue
                client_order_id = decode_comment(order.comment) or self._ticket_to_order_id.get(
                    order.ticket, ""
                )
                record = build_order_record(
                    order,
                    client_order_id,
                    instrument,
                    server_time_offset=self._server_time_offset,
                )
                key = record.client_order_id or record.platform_order_id or ""
                result[key] = record
            return result

        return await asyncio.to_thread(_fetch)

    async def fetch_fills(
        self, *, since: datetime | None = None
    ) -> dict[str, list[FillRecord]]:
        """Fetch recent fills via ``mt5.history_deals_get()``.

        Only trading deals (DEAL_TYPE_BUY/SELL) are fills; balance
        operations (DEAL_TYPE_IN/OUT) are excluded.  Results are grouped by
        ``client_order_id``.  Unlike the polling loop, this read does not
        advance the fill baseline — reconciliation must not disturb the
        poll loop's dedup state.

        *since* is an optional lower bound (UTC) for the window.  When omitted
        the poll baseline (``_last_deal_time``) is used, preserving the recent
        window for direct callers.  When provided (reconciliation), the backlog
        rewind is disabled so the returned window is symmetric with the
        engine's own watermark-bounded local fill query.
        """
        mt5 = _get_mt5()

        def _fetch() -> dict[str, list[FillRecord]]:
            account = mt5.account_info()
            self._check_call_result("account_info", account)
            backlog = 0 if since is not None else _DEAL_QUERY_BACKLOG_SECONDS
            deals = mt5.history_deals_get(
                *self._server_deal_window(
                    mt5, _utcnow(), from_time=since, backlog_seconds=backlog
                )
            )
            self._check_call_result("history_deals_get", deals)
            instruments = self._resolve_poll_instruments(mt5, (), deals)
            result: dict[str, list[FillRecord]] = {}
            for deal in deals or ():
                if deal.type not in (0, 1):
                    continue
                if deal.symbol not in instruments:
                    continue
                volume = Decimal(str(deal.volume))
                price = Decimal(str(deal.price))
                if volume <= 0 or price <= 0:
                    # Same guard as the poll loop: a non-positive deal would
                    # violate FillRecord's fill_quantity/fill_price > 0.
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
            # Probe the server-time offset from symbols that are actively
            # trading this cycle (positions/orders).
            probes = tuple({o.symbol for o in orders or ()} | {p.symbol for p in positions or ()})
            deals = mt5.history_deals_get(*self._server_deal_window(mt5, now, candidates=probes))
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

        The time baseline (``_last_deal_time``) is kept in the raw
        server-as-epoch basis — never the offset-converted value — so a
        jittering offset measurement between cycles cannot reorder deals
        relative to the baseline.

        The ticket/time baseline is committed immediately after each
        successful publish, so an exception mid-loop re-tries only the
        unpublished deals — no duplicates for the ones already reported.
        """
        for deal in deals or ():
            # deal.time is server-as-epoch; the dedup baseline is kept in the
            # same raw basis, so offset-measurement jitter between cycles can
            # never make an in-order deal look older than the baseline (which
            # previously lost fills whose conversion straddled an offset change).
            deal_time = int(deal.time)
            baseline = self._last_deal_time
            if baseline is not None and deal_time < baseline:
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
            if baseline is None or deal_time > baseline:
                self._last_deal_time = deal_time

    def _build_fill(
        self,
        deal: Any,
        instruments: dict[str, Instrument],
        account: Any,
    ) -> FillRecord:
        """Build a ``FillRecord`` from one MT5 deal tuple.

        The ``client_order_id`` is recovered from the deal's comment when it
        carries our ``U:`` tag (the engine's own orders, recoverable even
        after a restart).  MT5 deals otherwise carry no client order id, so
        it falls back to the ``ticket → client_order_id`` mapping recorded
        by ``place_order``: a pending order's deal references its order
        ticket (``deal.order``); a market order's deal has ``deal.order == 0``
        and is instead keyed by the deal ticket itself (``deal.ticket``).
        An unknown ticket (order placed from the terminal) yields an empty
        ``client_order_id``.
        """
        client_order_id = (
            decode_comment(deal.comment)
            or self._ticket_to_order_id.get(deal.order)
            or self._ticket_to_order_id.get(deal.ticket, "")
        )
        fee = Decimal(str(deal.commission)) + Decimal(str(deal.fee))
        return FillRecord(
            client_order_id=client_order_id,
            platform_fill_id=str(deal.ticket),
            instrument=instruments[deal.symbol],
            fill_quantity=Decimal(str(deal.volume)),
            fill_price=Decimal(str(deal.price)),
            fill_timestamp=from_mt5_epoch(deal.time, self._server_time_offset),
            fee_currency=str(account.currency) if fee else None,
            fee_amount=fee if fee else None,
            correlation_id=client_order_id,
            position_id=str(deal.position_id) if deal.position_id else None,
        )

    def _server_time_offset_seconds(
        self,
        mt5: Any,
        *,
        candidates: tuple[str, ...] = (),
    ) -> int:
        """Server-as-epoch minus real-UTC epoch (seconds).

        MT5 stamps ``deal.time`` / ``tick.time_msc`` in the server's timezone
        as if they were Unix epochs (a 06:58 UTC deal on a UTC+3 broker reads
        09:58), so ``history_deals_get`` windows and deal times must be shifted
        by this offset.

        Measured live from any tick (``time_msc/1000 - time.time()``), hence
        broker-agnostic; re-measured each poll cycle (DST-safe), falling back
        to the last-known value (0) when no tick exists — safe, since no new
        deals occur while the market is closed.  Values beyond ±24h are
        rejected as corrupt.
        """
        probes = list(candidates)
        if not probes:
            probes = [
                symbol.name for symbol in (mt5.symbols_get() or ())[:_MAX_OFFSET_PROBE_SYMBOLS]
            ]
        best: tuple[float, int] | None = None
        for symbol in probes:
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                continue
            time_msc = tick.time_msc
            if not time_msc:
                continue
            offset = int(time_msc / 1000) - int(time.time())
            if abs(offset) > _MAX_SERVER_TIME_OFFSET_SECONDS:
                continue
            # Prefer the freshest tick across the probes — a stale tick (a
            # symbol that has not traded for a while) would skew the offset
            # and shift deal timestamps away from their real execution time.
            if best is None or time_msc > best[0]:
                best = (time_msc, offset)
        if best is not None:
            self._server_time_offset = best[1]
        return self._server_time_offset

    def _server_deal_window(
        self,
        mt5: Any,
        to_time: datetime,
        *,
        from_time: datetime | None = None,
        candidates: tuple[str, ...] = (),
        backlog_seconds: int = _DEAL_QUERY_BACKLOG_SECONDS,
    ) -> tuple[int, int]:
        """Build a ``history_deals_get`` window in the server-as-epoch basis.

        Deal timestamps are stored shifted by the server offset, so the query
        window must be shifted by the same amount (plus small margins to absorb
        second-granularity rounding).  The lower edge is anchored to the deal
        dedup baseline (``_last_deal_time``), kept in the same raw
        server-as-epoch basis, so the window stays correct even when the
        measured offset jitters between cycles.  Pass ``from_time`` to
        override the lower edge (the connect-time recovery scan, or
        reconciliation's explicit ``since``).

        *backlog_seconds* rewinds the lower bound.  Reconciliation passes an
        explicit ``since`` and disables the backlog (0) so its window is exactly
        symmetric with the engine's local fill query; in that case a sub-second
        ``from_time`` is rounded *up* to a whole second (deal timestamps are
        second-granular) so MT5 does not return the boundary-second fill that
        the engine's ``fill_timestamp >= watermark`` query excludes.  The
        polling path keeps the floor because its backlog rewind already absorbs
        the difference.
        """
        offset = self._server_time_offset_seconds(mt5, candidates=candidates)
        if from_time is not None:
            start = int(from_time.timestamp()) + offset
            if from_time.microsecond and backlog_seconds == 0:
                start += 1
        else:
            if self._last_deal_time is None:
                # Anchor the baseline to server-now on first use so the first
                # window covers only recent deals, not the account's history.
                self._last_deal_time = int(to_time.timestamp()) + offset
            start = self._last_deal_time
        return (
            start - backlog_seconds,
            int(to_time.timestamp()) + offset + _DEAL_QUERY_FORWARD_SECONDS,
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

        Unresolvable symbols are skipped rather than failing the whole cycle
        — one bad symbol must not silence events for every other instrument.
        Permanent failures (unknown broker symbol, unrecognized asset-class
        path, trade disabled) are remembered in ``_failed_symbols`` and
        reported once; transient failures (e.g. a flaky ``symbol_select``
        IPC call) are logged and retried on the next cycle.
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
            except (ValueError, InvalidSymbolError) as exc:
                if symbol not in self._failed_symbols:
                    self._failed_symbols.add(symbol)
                    logger.warning(
                        "Skipping symbol %s — cannot build Instrument: %s",
                        symbol,
                        exc,
                    )
            except UteError as exc:
                logger.warning(
                    "Temporarily skipping symbol %s — will retry next cycle: %s",
                    symbol,
                    exc,
                )
        return resolved

    def _resolve_instrument(self, mt5_symbol: str, mt5: Any) -> Instrument:
        """Build the canonical ``Instrument`` for an MT5 symbol.

        Resolution order:

        1. ``_symbol_to_instrument`` — seeded from the state store at connect
           and extended on every outbound order, so anything the engine has
           traded resolves exactly (full field round-trip).
        2. ``symbol_info()`` broker metadata — ``currency_base`` /
           ``currency_profit`` give the base/quote, ``path`` and
           ``trade_calc_mode`` give the asset class, and the name gives the
           symbol for non-decomposable instruments.  This covers symbols the
           engine has never traded (e.g. manual terminal positions).

        Must only be called from within a ``to_thread()`` block —
        ``symbol_info()`` is an MT5 IPC call.
        """
        cached = self._symbol_to_instrument.get(mt5_symbol)
        if cached is not None:
            return cached
        self._ensure_symbol_selected(mt5_symbol, mt5)
        info = mt5.symbol_info(mt5_symbol)
        if info is None:
            code, desc = mt5.last_error()
            raise map_mt5_error(
                code,
                desc or f"mt5.symbol_info() returned None for {mt5_symbol}",
            )
        instrument = self._build_instrument_from_symbol_info(mt5_symbol, info)
        self._symbol_to_instrument[mt5_symbol] = instrument
        return instrument

    # ------------------------------------------------------------------
    # Internal helpers (implement these)
    # ------------------------------------------------------------------

    def _ensure_symbol_selected(self, mt5_symbol: str, mt5: Any) -> None:
        """Make sure *mt5_symbol* is selected in Market Watch so quotes flow.

        MT5 streams real-time ticks only for symbols present in Market Watch:
        ``symbol_info_tick()`` returns ``None`` and ``order_send()`` can reject
        requests for symbols that are not selected.  ``symbol_select()`` is
        idempotent, so already-selected symbols are skipped via
        ``_selected_symbols``, and symbols the broker does not provide are
        remembered in ``_failed_symbols`` to avoid re-issuing the IPC call
        every poll cycle.

        Must be called from within a ``to_thread()`` block (or wrapped) —
        ``symbol_select()`` is an MT5 IPC call.

        Raises:
            InvalidSymbolError: the symbol does not exist for this broker.
            PlatformError: ``symbol_select()`` failed for another reason
                (transient) — not cached, so a later call retries.
        """
        if mt5_symbol in self._selected_symbols or mt5_symbol in self._failed_symbols:
            return
        if not mt5.symbol_select(mt5_symbol, True):
            # last_error() codes for unknown symbols are unreliable across
            # terminal builds, so classify via the symbol catalogue directly.
            code, desc = mt5.last_error()
            if not self._symbol_exists(mt5_symbol, mt5):
                self._failed_symbols.add(mt5_symbol)
                raise InvalidSymbolError(
                    f"symbol {mt5_symbol!r} is not available on this broker"
                )
            raise map_mt5_error(code, desc or f"symbol_select() failed for {mt5_symbol}")
        self._selected_symbols.add(mt5_symbol)

    def _symbol_exists(self, mt5_symbol: str, mt5: Any) -> bool:
        """Authoritative existence test via ``symbols_get(name)``.

        ``mt5.last_error()`` codes for unknown symbols are unreliable across
        terminal builds (``ERR_UNKNOWN_SYMBOL`` vs a generic IPC "terminal
        call failed"), so existence is checked against the symbol catalogue
        directly rather than trusting the error code from a failed
        ``symbol_select()`` / ``symbol_info()`` call.

        Must be called from within a ``to_thread()`` block — ``symbols_get()``
        is an MT5 IPC call.
        """
        try:
            return bool(mt5.symbols_get(mt5_symbol))
        except Exception:
            # symbols_get() itself failed (IPC/connection) — don't classify as
            # "not on broker"; let the caller's mapped error propagate instead.
            return True

    async def _seed_symbol_mappings_from_state_store(self) -> None:
        """Seed ``platform_symbol → Instrument`` from the state store at connect.

        The engine persists an ``OrderRecord``/``Position`` (each carrying the
        instrument's ``platform_symbol``) for everything it trades, so the
        store is the authoritative inbound-resolution map: it gives an exact
        full-field round-trip for symbols the engine has previously traded,
        independent of broker metadata semantics or comment rewriting.

        Best-effort by design: without an attached store, or if a query fails,
        the map simply stays empty and ``_resolve_instrument`` falls back to
        ``symbol_info()`` metadata.  Never raises into ``connect()``.
        """
        if self._state_store is None:
            return
        seeded = 0
        orders: Sequence[OrderRecord]
        try:
            orders = await self._state_store.query_orders(limit=100_000)
        except Exception as exc:
            logger.warning("Symbol-mapping recovery: orders query failed: %s", exc)
            orders = ()
        for record in orders:
            instrument = getattr(record, "instrument", None)
            if instrument is not None:
                seeded += self._record_symbol_mapping(instrument)

        positions: Sequence[Position]
        try:
            positions = await self._state_store.query_positions(limit=100_000)
        except Exception as exc:
            logger.warning("Symbol-mapping recovery: positions query failed: %s", exc)
            positions = ()
        for position in positions:
            instrument = getattr(position, "instrument", None)
            if instrument is not None:
                seeded += self._record_symbol_mapping(instrument)

        if seeded:
            logger.info(
                "Seeded %d platform_symbol → Instrument mapping(s) from the state store", seeded
            )

    def _record_symbol_mapping(self, instrument: Instrument) -> int:
        """Record a ``platform_symbol → Instrument`` mapping if unknown.

        Returns 1 when a new mapping was added, otherwise 0.  Instruments
        without a ``platform_symbol`` are skipped — there is nothing to key on.
        """
        broker_symbol = instrument.platform_symbol
        if broker_symbol is None or broker_symbol in self._symbol_to_instrument:
            return 0
        self._symbol_to_instrument[broker_symbol] = instrument
        return 1

    async def _seed_mappings_from_state_store(self) -> None:
        """Seed ``client_order_id ↔ ticket`` maps from the engine's state store.

        The engine persists an ``OrderRecord`` for every placed order at
        ``place_order`` time (``dispatch_place_order`` → ``upsert_order``),
        so the store is the authoritative mapping — it survives broker
        comment rewriting and does not depend on the ``U:`` comment round
        trip.  This runs before ``_recover_order_mappings`` so comment scans
        never overwrite a store entry.

        Best-effort by design: without an attached store, or if the query
        fails, the maps stay empty and comment recovery is the only source.
        Never raises into ``connect()``.
        """
        if self._state_store is None:
            return
        seeded = 0
        try:
            records = await self._state_store.query_orders(limit=100_000)
        except Exception as exc:
            logger.warning("Order-mapping recovery: state-store query failed: %s", exc)
            return
        for record in records:
            cid = record.client_order_id
            if not cid:
                continue
            platform_id = record.platform_order_id
            if platform_id is None:
                logger.warning("Order-mapping recovery: skipping order %r with no platform id", cid)
                continue
            try:
                ticket = int(platform_id)
            except ValueError:
                logger.warning(
                    "Order-mapping recovery: skipping order %r with non-numeric platform id %r",
                    cid,
                    platform_id,
                )
                continue
            seeded += self._record_mapping(cid, ticket)
        if seeded:
            logger.info(
                "Seeded %d client_order_id ↔ ticket mapping(s) from the state store", seeded
            )

    def _recover_order_mappings(self, mt5: Any) -> None:
        """Rebuild ``client_order_id ↔ ticket`` maps from MT5 comments.

        Runs once at ``connect()`` so orders placed before a restart can
        still be modified/cancelled and their fills attributed.  Every open
        order and every deal in the recent history window is scanned for our
        ``U:`` comment tag (see ``comments.py``).  Idempotent, and never
        fails the connection — a scan error is logged and skipped, leaving
        the in-memory maps as they are (best-effort by design).  Entries
        already recorded by ``_seed_mappings_from_state_store`` are never
        overwritten, so a rewritten comment cannot clobber the authoritative
        store mapping.
        """
        recovered = 0
        try:
            orders = mt5.orders_get()
        except Exception as exc:
            logger.warning("Order-mapping recovery: orders_get() failed: %s", exc)
            orders = None
        if not isinstance(orders, (tuple, list)):
            orders = ()
        for order in orders:
            recovered += self._record_mapping(decode_comment(order.comment), order.ticket)

        try:
            from_epoch, to_epoch = self._server_deal_window(
                mt5,
                _utcnow(),
                from_time=_utcnow() - timedelta(seconds=_RECOVERY_DEAL_LOOKBACK_SECONDS),
            )
            deals = mt5.history_deals_get(from_epoch, to_epoch)
        except Exception as exc:
            logger.warning("Order-mapping recovery: history scan failed: %s", exc)
            deals = None
        if not isinstance(deals, (tuple, list)):
            deals = ()
        for deal in deals:
            recovered += self._record_mapping(
                decode_comment(deal.comment), deal.order or deal.ticket
            )

        if recovered:
            logger.info(
                "Recovered %d client_order_id ↔ ticket mapping(s) from MT5 comments", recovered
            )

    def _record_mapping(self, client_order_id: str | None, ticket: int) -> int:
        """Record a recovered ``client_order_id → ticket`` pair if unknown.

        Returns 1 when a new mapping was added, otherwise 0.
        """
        if client_order_id is None or client_order_id in self._order_id_to_ticket:
            return 0
        self._order_id_to_ticket[client_order_id] = ticket
        self._ticket_to_order_id[ticket] = client_order_id
        return 1

    def _resolve_mt5_symbol(self, instrument: Instrument) -> str:
        """Return the MT5 broker symbol for *instrument*, registering it.

        ``platform_symbol`` is mandatory for MT5 — there is no symbol
        derivation from ``symbol``/``quote_currency`` (broker suffixes are
        not standardized).  The mapping is recorded so the inbound polling
        path reconstructs the exact same canonical ``Instrument``.
        """
        self._record_symbol_mapping(instrument)
        return to_mt5_symbol(instrument)

    def _invalidate_spec_cache(self, instrument: Instrument) -> None:
        """Remove a cached ``InstrumentSpec``, forcing a re-fetch on next access."""
        self._spec_cache.pop(instrument, None)

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

    def _asset_class_from_path(
        self,
        path: str,
        *,
        currency_base: str | None = None,
        calc_mode: int | None = None,
    ) -> AssetClass:
        """Derive the canonical ``AssetClass`` for an MT5 symbol.

        Layered, broker-agnostic classification — no broker names are
        hardcoded; a new broker's market folder is accommodated either by an
        existing thesaurus entry or, failing that, the config escape hatch
        ``MT5Config.asset_class_path_map``:

        1. Precious-metal base currency (``XAU``/``XAG``/``XPT``/``XPD``) —
           a broker-independent field, so a metal grouped under
           "Commodities" still resolves to MARGIN_FX before any path is
           consulted.
        2. Any ``symbol_info().path`` segment matched against the thesaurus
           (built-in + config overrides).  Scanning ALL segments — not just
           the first — means an account-group root such as Oanda's ``PRO``
           cannot hide the meaningful ``Noble``/``Indices``/``Equities_CFD``
           segment beneath it.
        3. ``trade_calc_mode`` fallback (MT5's ``ENUM_SYMBOL_CALC_MODE``),
           used only when neither of the above resolves.

        Raises ``ValueError`` for a symbol none of the three layers
        recognise — never guesses, so a wrong asset class cannot silently
        corrupt the DB.
        """
        base = (currency_base or "").upper()
        if base in _METAL_BASE_CURRENCIES:
            return AssetClass.MARGIN_FX

        for segment in _path_segments(path):
            asset_class = self._path_asset_class.get(segment)
            if asset_class is not None:
                return asset_class

        if calc_mode is not None:
            asset_class = _CALC_MODE_ASSET_CLASS.get(calc_mode)
            if asset_class is not None:
                return asset_class

        raise ValueError(
            f"Unrecognized MT5 symbol path {path!r} "
            f"(currency_base={base!r}, calc_mode={calc_mode!r}) — cannot derive "
            "asset class. Add a mapping to MT5Config.asset_class_path_map."
        )

    @staticmethod
    def _split_symbol_name(name: str) -> tuple[str, str | None]:
        """Split a broker symbol name into ``(symbol, quote_currency|None)``.

        Strips broker suffixes first (``AAPL_CFD.US`` → ``AAPL``), then, if
        the remaining name ends with a known quote currency
        (``SOLUSD`` → ``SOL``/``USD``), splits it off.  Returns
        ``(name, None)`` when no quote suffix matches.  Pure string logic —
        never an asset-class guess.
        """
        cleaned = name.upper()
        changed = True
        while changed:
            changed = False
            for suffix in _BROKER_SYMBOL_SUFFIXES:
                if cleaned.endswith(suffix) and len(cleaned) > len(suffix):
                    cleaned = cleaned[: -len(suffix)]
                    changed = True
                    break
        for quote in _QUOTE_CURRENCY_SUFFIXES:
            if cleaned.endswith(quote) and len(cleaned) > len(quote):
                return cleaned[: -len(quote)], quote
        return cleaned, None

    def _build_instrument_from_symbol_info(self, mt5_symbol: str, info: Any) -> Instrument:
        """Build the canonical ``Instrument`` from a ``symbol_info()`` row.

        Reconstructs ``symbol``/``quote_currency``/``asset_class`` (and the
        settlement ``currency`` for single-name instruments) from broker
        metadata:

        * ``currency_base``/``currency_profit``: when they differ the pair is
          decomposable and carried verbatim (generic, zero config).
        * ``path``/``trade_calc_mode``: feed ``_asset_class_from_path``.
        * ``name``: for non-decomposable symbols (base == profit, e.g. crypto
          ``SOLUSD``, metal ``GOLD.pro``, stock ``AAPL``) the tradable's
          identity lives only in the name, so it is split into symbol (+quote).

        Must be called from within a ``to_thread()`` block — the caller has
        already fetched ``info`` via ``symbol_info()``.
        """
        base = (info.currency_base or "").upper()
        profit = (info.currency_profit or "").upper()
        asset_class = self._asset_class_from_path(
            info.path,
            currency_base=base or None,
            calc_mode=info.trade_calc_mode,
        )
        name = getattr(info, "name", None) or mt5_symbol
        is_pair = asset_class in (AssetClass.SPOT, AssetClass.MARGIN_FX)

        if base and profit and base != profit:
            # Decomposable — base/quote carried verbatim in the broker fields.
            if is_pair:
                return Instrument(
                    symbol=base,
                    quote_currency=profit,
                    asset_class=asset_class,
                    platform_symbol=mt5_symbol,
                )
            return Instrument(
                symbol=base,
                quote_currency=None,
                currency=profit,
                asset_class=asset_class,
                platform_symbol=mt5_symbol,
            )

        # Non-decomposable (base == profit, or fields missing) — the
        # tradable's identity lives only in the name.
        symbol, quote = self._split_symbol_name(name)
        if is_pair:
            return Instrument(
                symbol=symbol,
                quote_currency=quote or profit or None,
                asset_class=asset_class,
                platform_symbol=mt5_symbol,
            )
        return Instrument(
            symbol=symbol,
            quote_currency=None,
            currency=profit or None,
            asset_class=asset_class,
            platform_symbol=mt5_symbol,
        )

    @staticmethod
    def _identity_matches(current: Instrument, canonical: Instrument) -> bool:
        """Whether *current* already carries the broker-derived identity.

        Only the fields MT5 can derive from ``symbol_info()`` are tested —
        ``symbol``, ``quote_currency``, ``asset_class`` — since those are the
        optional fields the adapter corrects on behalf of the caller.
        """
        return (
            current.symbol == canonical.symbol
            and current.quote_currency == canonical.quote_currency
            and current.asset_class == canonical.asset_class
        )

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

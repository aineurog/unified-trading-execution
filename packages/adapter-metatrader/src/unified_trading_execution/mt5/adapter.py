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
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
)
from unified_trading_execution.events import (
    ConnectionStateEvent,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
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
            "It is only available on Windows. Install with: pip install unified-trading-execution-metatrader[mt5]"
        ) from None
    return mt5


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

        # Last known state snapshots (keyed by ticket for orders, by
        # position ticket for positions, by currency for balances).
        self._last_orders: dict[int, object] = {}
        self._last_positions: dict[int, object] = {}
        self._last_balance: object | None = None
        self._last_deal_time: datetime = datetime.now(tz=UTC)

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

    async def connect(self) -> None:
        """Initialize the MT5 terminal connection and start the polling loop.

        Raises ``PlatformConnectionError`` if:
        - Another adapter is already connected in this process
        - ``mt5.initialize()`` fails
        - ``account_info()`` returns ``None`` after successful initialize
        """
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Cancel the polling loop and shut down the MT5 terminal connection.

        Publishes ``ConnectionStateEvent(connected=False)``.
        Releases the process-global connection guard.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Reconciliation data (optional ABC methods)
    # ------------------------------------------------------------------

    async def fetch_positions(self) -> dict[Instrument, Position]:
        """Fetch all open positions via ``mt5.positions_get()``.

        In hedging mode, individual legs are netted per instrument before
        returning — core sees one ``Position`` per instrument regardless
        of the account's netting/hedging mode.
        """
        raise NotImplementedError

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch account balances via ``mt5.account_info()``."""
        raise NotImplementedError

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch all open orders via ``mt5.orders_get()``."""
        raise NotImplementedError

    async def fetch_fills(self) -> dict[str, list[FillRecord]]:
        """Fetch recent fills via ``mt5.history_deals_get()``."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Polling loop (adapter-internal)
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Background coroutine — runs until ``disconnect()`` cancels it.

        Each cycle fetches orders, positions, balances, and new deals in
        a single ``to_thread()`` block, then diffs against last-known state
        and publishes events for any changes.
        """
        raise NotImplementedError

    async def _poll_once(self) -> None:
        """One complete poll cycle.

        Fetches all state in a single ``asyncio.to_thread()`` call
        for efficiency (single GIL handoff per cycle).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers (implement these)
    # ------------------------------------------------------------------

    def _resolve_mt5_symbol(self, instrument: Instrument) -> str:
        """Apply the alias table and return the MT5 broker symbol string."""
        raise NotImplementedError

    def _invalidate_spec_cache(self, instrument: Instrument) -> None:
        """Remove a cached ``InstrumentSpec``, forcing a re-fetch on next access."""
        self._spec_cache.pop(instrument, None)

    def _build_reverse_alias(self) -> None:
        """Build the reverse alias table from ``MT5Config.symbol_alias_table``."""
        self._reverse_alias = {v: k for k, v in self._config.symbol_alias_table.items()}

    def _check_mt5_error(self) -> None:
        """Check ``mt5.last_error()`` and raise the mapped exception if non-zero."""
        raise NotImplementedError

"""MetaTrader 5 adapter implementation stub.

Connection handler: local terminal process via IPC (the `MetaTrader5` package) —
no REST session, no WebSocket. Every call is a blocking round-trip into a
running terminal on the same machine, so the sync API is wrapped with
`asyncio.to_thread` on the async surface and updates arrive by polling
(`orders_get`, `positions_get`, `history_deals_get`), not push.
Translation layer: engine types ↔ MetaTrader 5 API types.
Error mapping: `mt5.last_error()` → common exception hierarchy.

Two hard platform realities this adapter must respect once implemented:

- **Process-global connection.** The `MetaTrader5` module holds one terminal
  connection for the whole process (`mt5.initialize()` / `mt5.shutdown()`).
  Only a single `MT5Adapter` can be connected per process — concurrent
  adapter instances must serialize on connect/disconnect.
- **Windows-only module.** `MetaTrader5` ships win32/win64 wheels only, so it
  must be imported lazily inside methods (never at module top) to keep this
  package importable — and lintable — on non-Windows CI.

This module contains no business logic, no retry policy, no risk decisions.
"""

from __future__ import annotations

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


class MT5Adapter(Adapter):
    """Adapter for the MetaTrader 5 terminal — forex, CFDs, stocks, futures.

    Construct with:
        path: str | None = None — path to the terminal executable
            (auto-detected when None)
        login: int | None = None — account number (None = use the terminal's
            currently logged-in account)
        password: str | None = None — account password
        server: str | None = None — broker server name
        event_bus: EventBus — where translated events are published
    """

    def __init__(
        self,
        *,
        path: str | None = None,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self._path = path
        self._login = login
        self._password = password
        self._server = server
        self._event_bus = event_bus
        self._connected = False

    @property
    def platform_name(self) -> str:
        return "metatrader"

    @property
    def account_id(self) -> str:
        return str(self._login) if self._login is not None else "mt5-account"

    # ---- Connection lifecycle ----

    async def connect(self) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        raise NotImplementedError

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        raise NotImplementedError

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        raise NotImplementedError

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        raise NotImplementedError

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        raise NotImplementedError

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        return frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        raise NotImplementedError

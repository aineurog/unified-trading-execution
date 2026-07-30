"""BybitAdapter — concrete Adapter ABC implementation for Bybit (Section 17.10).

Usage::

    from unified_trading_execution.bybit import BybitAdapter, BybitConfig
    from unified_trading_execution.events import EventBus

    config = BybitConfig(api_key="...", api_secret="...", testnet=True)
    adapter = BybitAdapter(config, event_bus=EventBus())
    await adapter.connect()
    # ...
    await adapter.disconnect()
"""

from __future__ import annotations

from datetime import datetime, timezone

from pybit.unified_trading import HTTP

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    UnifiedOrder,
)


_DEFAULT_REQUESTS_PER_INTERVAL = 120
_DEFAULT_INTERVAL_SECONDS = 60


def _parse_rate_limits(headers: dict[str, str]) -> RateLimits:
    limit = int(headers.get("X-Bapi-Limit", _DEFAULT_REQUESTS_PER_INTERVAL))
    remaining = int(headers.get("X-Bapi-Remaining", _DEFAULT_REQUESTS_PER_INTERVAL))
    reset_ts = int(headers.get("X-Bapi-Reset-Timestamp", 0))
    reset_at = (
        datetime.fromtimestamp(reset_ts / 1000, tz=timezone.utc)
        if reset_ts
        else datetime.now(timezone.utc)
    )
    return RateLimits(
        requests_per_interval=limit,
        interval_seconds=_DEFAULT_INTERVAL_SECONDS,
        remaining=remaining,
        reset_at=reset_at,
    )


class BybitAdapter(Adapter):
    """Concrete Adapter ABC implementation for Bybit.

    Construction follows the Adapter ABC convention (Section 17.10):
    configuration is supplied as a ``BybitConfig`` dataclass, not loose
    strings; the EventBus reference is required so the adapter can publish
    translated events from its internal WebSocket handlers.
    """

    def __init__(self, config: BybitConfig, *, event_bus: EventBus) -> None:
        self._config = config
        self._event_bus = event_bus
        self._connected = False
        self._last_rate_limits = _parse_rate_limits({})

        self._session = HTTP(
            testnet=config.testnet,
            demo=config.demo,
            api_key=config.api_key,
            api_secret=config.api_secret,
            return_response_headers=True,
        )

    def _update_rate_limits(self, headers: dict[str, str]) -> None:
        self._last_rate_limits = _parse_rate_limits(headers)

    # ---- Identification (Section 17.10) ----

    @property
    def platform_name(self) -> str:
        return self._config.platform_name

    @property
    def account_id(self) -> str:
        return self._config.account_id

    # ---- Connection lifecycle ----

    async def connect(self) -> None:
        """Open persistent connections — REST session + WebSocket streams.

        Must publish ``ConnectionStateEvent(connected=True)`` on success.
        See Section 17.10, "Connection lifecycle."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Connection lifecycle")

    async def disconnect(self) -> None:
        """Close all connections gracefully.

        Must publish ``ConnectionStateEvent(connected=False)`` on disconnect.
        See Section 17.10, "Connection lifecycle."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Connection lifecycle")

    @property
    def is_connected(self) -> bool:
        """Return True if connections are currently established."""
        return self._connected

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to Bybit.

        Receives a ``UnifiedOrder`` that has already passed all risk checks.
        If Bybit supports native TP/SL attachment, use it; otherwise raise
        ``UnsupportedOrderTypeError`` — never approximate.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Order operations")

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification to Bybit.

        Core runs risk checks against the resulting order before calling.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Order operations")

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by client_order_id.

        Raises ``OrderNotFoundError`` if Bybit does not know the order.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Order operations")

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by client_order_id. Returns None if not found.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Order operations")

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules for a single instrument from Bybit.

        Raises ``InvalidSymbolError`` if the instrument is not tradable.
        See Section 17.10, "Instrument metadata."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Instrument metadata")

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the set of order types Bybit supports.

        Must always include at minimum: {MARKET, LIMIT, STOP, STOP_LIMIT}.
        See Section 17.10, "Capability reporting."
        """
        raise NotImplementedError("TODO: implement — see Section 17.10, Capability reporting")

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        return self._last_rate_limits

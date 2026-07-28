"""BybitAdapter — concrete Adapter ABC implementation for Bybit (Section 17.10).

This is a stub.  Every public method raises ``NotImplementedError`` with
a docstring pointing to the relevant specification section so the dev
knows exactly what contract each method must satisfy.

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

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position


class BybitAdapter(Adapter):
    """Concrete Adapter ABC implementation for Bybit.

    All order/connection logic is currently unimplemented — each method
    raises ``NotImplementedError`` with a pointer to the relevant spec
    section.  The dev fills in these bodies one by one.

    Construction follows the Adapter ABC convention (Section 17.10):
    configuration is supplied as a ``BybitConfig`` dataclass, not loose
    strings; the EventBus reference is required so the adapter can publish
    translated events from its internal WebSocket handlers.
    """

    def __init__(self, config: BybitConfig, *, event_bus: EventBus) -> None:
        self._config = config
        self._event_bus = event_bus
        self._connected = False

        # TODO: initialise HTTP session, WebSocket connection handles, etc.

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
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Connection lifecycle"
        )

    async def disconnect(self) -> None:
        """Close all connections gracefully.

        Must publish ``ConnectionStateEvent(connected=False)`` on disconnect.
        See Section 17.10, "Connection lifecycle."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Connection lifecycle"
        )

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
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Order operations"
        )

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification to Bybit.

        Core runs risk checks against the resulting order before calling.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Order operations"
        )

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by client_order_id.

        Raises ``OrderNotFoundError`` if Bybit does not know the order.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Order operations"
        )

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by client_order_id. Returns None if not found.
        See Section 17.10, "Order operations."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Order operations"
        )

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules for a single instrument from Bybit.

        Raises ``InvalidSymbolError`` if the instrument is not tradable.
        See Section 17.10, "Instrument metadata."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Instrument metadata"
        )

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the set of order types Bybit supports.

        Must always include at minimum: {MARKET, LIMIT, STOP, STOP_LIMIT}.
        See Section 17.10, "Capability reporting."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Capability reporting"
        )

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        """Return Bybit's current rate-limit state.

        Queried by the self-throttling validator.  Core may cache this
        briefly (TTL determined by interval_seconds).
        See Section 17.10, "Rate limits."
        """
        raise NotImplementedError(
            "TODO: implement — see Section 17.10, Rate limits"
        )

"""cTrader adapter implementation stub.

Connection handler: REST session + WebSocket streams (public + private).
Translation layer: engine types ↔ cTrader API types.
Error mapping: cTrader error codes → common exception hierarchy.

This module contains no business logic, no retry policy, no risk decisions.
"""

from __future__ import annotations

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


class CTraderAdapter(Adapter):
    """Adapter for the cTrader broker platform — forex, CFDs.

    Construct with:
        client_id: cTrader client ID
        client_secret: cTrader client secret
        access_token: cTrader access token
        testnet: bool = True — use sandbox/demo endpoints
        event_bus: EventBus — where translated events are published
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        access_token: str,
        *,
        testnet: bool = True,
        event_bus: EventBus | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._testnet = testnet
        self._event_bus = event_bus
        self._connected = False

    @property
    def platform_name(self) -> str:
        return "ctrader"

    @property
    def account_id(self) -> str:
        return self._client_id

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

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

import asyncio
from decimal import Decimal

from datetime import UTC, datetime

from pybit.exceptions import FailedRequestError, InvalidRequestError
from pybit.unified_trading import HTTP

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import map_bybit_error
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.bybit.symbols import to_bybit_symbol
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import AssetClass, OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    UnifiedOrder,
)


_DEFAULT_REQUESTS_PER_INTERVAL = 120
_DEFAULT_INTERVAL_SECONDS = 60


def _safe_header_int(headers: dict[str, str], key: str, default: int) -> int:
    raw = headers.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


def _parse_rate_limits(headers: dict[str, str]) -> RateLimits:
    raw_limit = _safe_header_int(headers, "X-Bapi-Limit", _DEFAULT_REQUESTS_PER_INTERVAL)
    limit = max(raw_limit, 1)

    raw_remaining = _safe_header_int(headers, "X-Bapi-Remaining", _DEFAULT_REQUESTS_PER_INTERVAL)
    remaining = max(raw_remaining, 0)

    raw_reset_ts = _safe_header_int(headers, "X-Bapi-Reset-Timestamp", 0)
    reset_at = (
        datetime.fromtimestamp(raw_reset_ts / 1000, tz=UTC)
        if raw_reset_ts > 0
        else datetime.now(UTC)
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

    @staticmethod
    def _instrument_to_category(instrument: Instrument) -> str:
        if instrument.asset_class == AssetClass.SPOT:
            return "spot"
        if instrument.asset_class == AssetClass.FUTURES:
            if instrument.currency == instrument.quote_currency:
                return "linear"
            return "inverse"
        raise InvalidSymbolError(
            f"Asset class {instrument.asset_class} is not supported by Bybit",
        )

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
        bybit_symbol = to_bybit_symbol(instrument)
        category = self._instrument_to_category(instrument)

        try:
            result = await asyncio.to_thread(
                self._session.get_instruments_info,
                category=category,
                symbol=bybit_symbol,
            )
            data, _, _ = result
        except FailedRequestError as exc:
            raise map_bybit_error(
                http_status=exc.status_code,
                ret_msg=exc.message,
            ) from exc
        except InvalidRequestError as exc:
            raise map_bybit_error(
                ret_code=exc.status_code,
                ret_msg=exc.message,
            ) from exc

        listings = (data.get("result", {}) or {}).get("list", [])
        if not listings:
            raise map_bybit_error(
                ret_msg=f"No instrument spec found for {bybit_symbol}",
            )

        entry = listings[0]
        status = entry.get("status", "")
        if status != "Trading":
            raise map_bybit_error(
                ret_msg=f"Instrument {bybit_symbol} is not tradable (status: {status})",
            )

        lot_filter = entry.get("lotSizeFilter", {})
        price_filter = entry.get("priceFilter", {})

        tick_size = Decimal(str(price_filter.get("tickSize", "1")))

        if category == "spot":
            lot_size = Decimal(str(lot_filter.get("basePrecision", "1")))
        else:
            lot_size = Decimal(str(lot_filter.get("qtyStep", "1")))

        return InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=Decimal(str(lot_filter.get("minOrderQty", "0"))),
            max_qty=Decimal(str(lot_filter.get("maxOrderQty", "0"))),
            min_notional=Decimal(str(lot_filter.get("minNotionalValue", "0"))),
            price_precision=-int(tick_size.as_tuple().exponent),
            qty_precision=-int(lot_size.as_tuple().exponent),
        )

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
        return self._last_rate_limits

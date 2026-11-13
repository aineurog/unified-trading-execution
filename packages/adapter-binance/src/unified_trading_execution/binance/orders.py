"""Pure translation between unified orders and Binance REST order payloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.errors import (
    InvalidOrderError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.types.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.order import OrderResult, UnifiedOrder


class _MissingAverageFillPriceError(PlatformError):
    """A filled Binance response lacks its required average execution price."""


_TIME_IN_FORCE: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
}

_ORDER_STATUS: dict[str, OrderStatus] = {
    "PENDING_NEW": OrderStatus.PENDING,
    "NEW": OrderStatus.OPEN,
    "PENDING_CANCEL": OrderStatus.OPEN,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELLED,
    "CANCELLED": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
    "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
}

_ALGO_OPEN_STATUSES = frozenset({"TRIGGERING", "TRIGGERED"})
_CLIENT_ORDER_ID_ALLOWED = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/"
)


@dataclass(frozen=True, slots=True)
class OrderReference:
    """Product and order metadata needed for this process's order lifecycle."""

    product: BinanceProduct
    symbol: str
    conditional: bool
    order_type: OrderType
    platform_order_id: str | None = None


def to_binance_client_order_id(client_order_id: str) -> str:
    """Preserve valid core IDs verbatim; reject IDs Binance will not accept."""
    if (
        not client_order_id
        or len(client_order_id) > 36
        or any(character not in _CLIENT_ORDER_ID_ALLOWED for character in client_order_id)
    ):
        raise InvalidOrderError(
            "Binance client_order_id must be 1-36 characters using only "
            "letters, digits, '-', '_', '.', ':', or '/'"
        )
    return client_order_id


def _decimal_string(value: Decimal | None) -> str:
    """Render a Decimal in plain notation, because Binance rejects exponents."""
    if value is None:
        raise InvalidOrderError("Binance order payload requires a decimal price value")
    return format(value, "f")


def build_place_order_payload(
    order: UnifiedOrder,
    *,
    product: BinanceProduct,
    symbol: str,
    client_order_id: str,
) -> tuple[dict[str, Any], bool]:
    """Translate a validated unified order to product-specific SDK parameters."""
    if order.take_profit is not None or order.stop_loss is not None:
        raise UnsupportedOrderTypeError(
            "Binance order-level take_profit/stop_loss attachments are not supported"
        )
    if order.position_id is not None:
        raise UnsupportedOrderTypeError(
            "Binance order placement cannot target a position_id through the unified order API"
        )
    if product == BinanceProduct.SPOT and order.reduce_only:
        raise UnsupportedOrderTypeError("Binance Spot does not support reduce_only orders")

    is_stop_order = order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)
    if is_stop_order and product == BinanceProduct.COIN_M:
        raise UnsupportedOrderTypeError(
            "COIN-M conditional orders are not supported through python-binance's public "
            "order methods"
        )
    conditional = is_stop_order and product == BinanceProduct.USD_M

    if order.time_in_force not in _TIME_IN_FORCE:
        raise UnsupportedOrderTypeError(
            f"Binance does not support time_in_force={order.time_in_force.value} "
            "through this order interface"
        )
    if order.order_type in (OrderType.MARKET, OrderType.STOP) and (
        order.time_in_force != TimeInForce.GTC
    ):
        raise UnsupportedOrderTypeError(
            f"Binance {order.order_type.value} orders do not accept a time-in-force parameter"
        )

    if order.order_type == OrderType.MARKET:
        binance_type = "MARKET"
    elif order.order_type == OrderType.LIMIT:
        binance_type = "LIMIT"
    elif product == BinanceProduct.SPOT and order.order_type == OrderType.STOP:
        binance_type = "STOP_LOSS"
    elif product == BinanceProduct.SPOT and order.order_type == OrderType.STOP_LIMIT:
        binance_type = "STOP_LOSS_LIMIT"
    elif product == BinanceProduct.USD_M and order.order_type == OrderType.STOP:
        binance_type = "STOP_MARKET"
    elif product == BinanceProduct.USD_M and order.order_type == OrderType.STOP_LIMIT:
        binance_type = "STOP"
    else:
        raise UnsupportedOrderTypeError(
            f"Binance {product.value} does not support {order.order_type.value} orders"
        )

    payload: dict[str, Any] = {
        "symbol": symbol,
        "side": "BUY" if order.side == OrderSide.BUY else "SELL",
        "type": binance_type,
        "quantity": _decimal_string(order.quantity),
    }
    if conditional:
        payload["clientAlgoId"] = client_order_id
    else:
        payload["newClientOrderId"] = client_order_id

    if order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        payload["price"] = _decimal_string(order.price)
        payload["timeInForce"] = _TIME_IN_FORCE[order.time_in_force]
    if order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        payload["stopPrice"] = _decimal_string(order.stop_price)
    if order.reduce_only:
        payload["reduceOnly"] = "true"
    if not conditional:
        payload["newOrderRespType"] = "RESULT"

    return payload, conditional


def build_modify_order_payload(
    *,
    symbol: str,
    client_order_id: str,
    price: Decimal | None,
    quantity: Decimal | None,
) -> dict[str, str]:
    """Build the exact supported Futures limit-order modification fields."""
    payload = {
        "symbol": symbol,
        "origClientOrderId": client_order_id,
    }
    if price is not None:
        payload["price"] = _decimal_string(price)
    if quantity is not None:
        payload["quantity"] = _decimal_string(quantity)
    return payload


def map_order_status(raw_status: object, *, conditional: bool = False) -> OrderStatus:
    """Map a Binance normal/algo status without hiding unknown venue values."""
    if not isinstance(raw_status, str):
        raise PlatformError(f"Binance order response has invalid status {raw_status!r}")
    status_text = raw_status.upper()
    if conditional and status_text in _ALGO_OPEN_STATUSES:
        return OrderStatus.OPEN
    status = _ORDER_STATUS.get(status_text)
    if status is None:
        raise PlatformError(f"Unknown Binance order status {raw_status!r}")
    return status


def parse_order_result(
    entry: dict[str, Any],
    *,
    client_order_id: str,
    conditional: bool = False,
) -> OrderResult:
    """Parse a Spot/Futures order or Futures algo-order response."""
    order_id = entry.get("orderId")
    if order_id is None and conditional:
        order_id = entry.get("algoId")
    if order_id is None:
        raise PlatformError("Binance order response is missing orderId/algoId")

    raw_status = entry.get("algoStatus") if conditional else entry.get("status")
    if raw_status is None:
        raise PlatformError(
            "Binance conditional order response is missing algoStatus"
            if conditional
            else "Binance order response is missing status"
        )

    raw_filled_quantity = entry.get(
        "executedQty",
        entry.get("cumQty", entry.get("actualQty")),
    )
    if raw_filled_quantity is None:
        if conditional:
            raw_filled_quantity = "0"
        else:
            raise PlatformError("Binance order response is missing executedQty")
    filled_quantity = _decimal(raw_filled_quantity, field="executedQty")

    if conditional and isinstance(raw_status, str) and raw_status.upper() == "FINISHED":
        # FINISHED covers both a filled and a canceled triggered order; the
        # executed quantity is the only field that distinguishes them.
        ordered_quantity = _optional_decimal(entry.get("quantity"), field="quantity")
        status = (
            OrderStatus.FILLED
            if ordered_quantity is not None and filled_quantity >= ordered_quantity
            else OrderStatus.CANCELLED
        )
    else:
        status = map_order_status(raw_status, conditional=conditional)
    average_fill_price = _optional_decimal(
        entry.get("avgPrice", entry.get("actualPrice")),
        field="avgPrice",
    )
    if average_fill_price is None and filled_quantity > 0:
        quote_quantity = _optional_decimal(
            entry.get("cummulativeQuoteQty"),
            field="cummulativeQuoteQty",
        )
        if quote_quantity is not None:
            average_fill_price = quote_quantity / filled_quantity
    if filled_quantity > 0 and average_fill_price is None:
        raise _MissingAverageFillPriceError("Binance order response is missing average fill price")

    created_at = _timestamp(
        entry.get("time", entry.get("transactTime", entry.get("createTime"))),
        fallback=entry.get("updateTime"),
        field="time",
    )
    raw_updated_at = entry.get("updateTime", entry.get("transactTime"))
    updated_at = (
        created_at
        if raw_updated_at is None
        else _timestamp(raw_updated_at, fallback=None, field="updateTime")
    )

    return OrderResult(
        client_order_id=client_order_id,
        platform_order_id=str(order_id),
        status=status,
        filled_quantity=filled_quantity,
        average_fill_price=average_fill_price,
        created_at=created_at,
        updated_at=updated_at,
    )


def _decimal(value: object, *, field: str) -> Decimal:
    if value is None:
        raise PlatformError(f"Binance order response is missing {field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PlatformError(f"Binance order response has invalid {field}: {value!r}") from exc
    if not result.is_finite() or result < 0:
        raise PlatformError(f"Binance order response has invalid {field}: {value!r}")
    return result


def _optional_decimal(value: object, *, field: str) -> Decimal | None:
    if value is None or value == "":
        return None
    result = _decimal(value, field=field)
    return result if result > 0 else None


def _timestamp(value: object, *, fallback: object, field: str) -> datetime:
    raw = value if value is not None else fallback
    if raw is None:
        raise PlatformError(f"Binance order response is missing {field}")
    try:
        timestamp_ms = int(str(raw))
        seconds, milliseconds = divmod(timestamp_ms, 1000)
        return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=milliseconds * 1000)
    except (ValueError, OverflowError, OSError) as exc:
        raise PlatformError(f"Binance order response has invalid {field}: {raw!r}") from exc

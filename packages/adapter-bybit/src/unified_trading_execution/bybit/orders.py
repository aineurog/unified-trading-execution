"""UnifiedOrder <-> Bybit order payload translation (Section 17.10, Order operations).

This module is the pure translation layer between the engine's canonical
order model (``UnifiedOrder`` / ``OrderModification``) and Bybit's native
``place_order`` / ``amend_order`` / ``cancel_order`` parameters.  No pybit
imports and no I/O happen here — the adapter passes the already-derived
``category`` and ``symbol`` strings and forwards the returned payload.

Bybit v5 order API facts this module encodes:

- POST /v5/order/create accepts ``orderType`` ``Market`` or ``Limit`` only.
  A conditional (stop) order is expressed by supplying ``triggerPrice``
  together with either order type.
- ``timeInForce`` supports ``GTC``/``IOC``/``FOK`` (plus ``PostOnly``/``RPI``,
  which the engine does not express).  ``DAY`` has no Bybit equivalent and is
  rejected with ``UnsupportedOrderTypeError`` — never approximated.
- Market orders always execute IOC on Bybit, so ``timeInForce`` is omitted for
  market-type orders (``MARKET`` and ``STOP``) — the flag is inexpressible.
- The four guaranteed order types map as follows:

    =============  ============================
    OrderType      Bybit payload
    =============  ============================
    MARKET         orderType=Market
    LIMIT          orderType=Limit, price
    STOP           orderType=Market, triggerPrice
    STOP_LIMIT     orderType=Limit, price, triggerPrice
    =============  ============================

  Conditional orders are only supported on derivatives (``linear`` /
  ``inverse``).  Bybit spot conditional orders (``orderFilter=StopOrder``)
  have different trigger semantics and are NOT approximated here; a
  STOP/STOP_LIMIT order for ``spot`` raises ``UnsupportedOrderTypeError``.
- ``triggerDirection`` is derived from the side: a BUY stop triggers when the
  price rises to ``stop_price`` (1), a SELL stop when it falls (2).
- TP/SL is attached natively.  Spot supports TP/SL on LIMIT orders only;
  derivatives support it on every order type.  ``reduce_only`` cannot be
  combined with TP/SL, and is not a concept for spot — both raise
  ``UnsupportedOrderTypeError``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from unified_trading_execution.errors import PlatformError, UnsupportedOrderTypeError
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)

_BYBIT_ORDER_TYPE: dict[OrderType, str] = {
    OrderType.MARKET: "Market",
    OrderType.LIMIT: "Limit",
    OrderType.STOP: "Market",
    OrderType.STOP_LIMIT: "Limit",
}

_TIME_IN_FORCE: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
}

_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    "New": OrderStatus.OPEN,
    "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
    "Untriggered": OrderStatus.OPEN,
    "Triggered": OrderStatus.OPEN,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELLED,
    "Canceled": OrderStatus.CANCELLED,
    "Deactivated": OrderStatus.CANCELLED,
    "Rejected": OrderStatus.REJECTED,
}


def build_place_order_payload(
    order: UnifiedOrder,
    *,
    category: str,
    symbol: str,
    client_order_id: str,
) -> dict[str, Any]:
    """Translate a validated ``UnifiedOrder`` into Bybit ``place_order`` params.

    Parameters
    ----------
    order :
        The fully-validated order.  Structural invariants (required price /
        stop_price per type, quantity > 0) are enforced by ``UnifiedOrder``
        itself and not re-checked here.
    category :
        Bybit product category (``"spot"``, ``"linear"``, ``"inverse"``).
    symbol :
        Bybit symbol string (e.g. ``"BTCUSDT"``).
    client_order_id :
        The non-empty client order id to send as ``orderLinkId``.

    Raises
    ------
    UnsupportedOrderTypeError
        If the order cannot be expressed natively by Bybit.
    """
    if order.order_type not in _BYBIT_ORDER_TYPE:
        raise UnsupportedOrderTypeError(f"Order type {order.order_type} is not supported by Bybit")
    if order.time_in_force == TimeInForce.DAY:
        raise UnsupportedOrderTypeError(
            "time_in_force=DAY is not supported by Bybit (use GTC, IOC, or FOK)"
        )
    if order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and category == "spot":
        raise UnsupportedOrderTypeError(
            f"{order.order_type.value} is not supported for spot orders on Bybit"
        )
    if category == "spot" and order.reduce_only:
        raise UnsupportedOrderTypeError("reduce_only is not supported for spot orders on Bybit")
    if order.reduce_only and (order.take_profit is not None or order.stop_loss is not None):
        raise UnsupportedOrderTypeError(
            "reduce_only cannot be combined with take_profit/stop_loss on Bybit"
        )
    if (
        category == "spot"
        and order.order_type != OrderType.LIMIT
        and (order.take_profit is not None or order.stop_loss is not None)
    ):
        raise UnsupportedOrderTypeError(
            "take_profit/stop_loss on spot orders is only supported for LIMIT orders on Bybit"
        )

    payload: dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "side": "Buy" if order.side == OrderSide.BUY else "Sell",
        "orderType": _BYBIT_ORDER_TYPE[order.order_type],
        "qty": str(order.quantity),
        "orderLinkId": client_order_id,
    }

    if order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        payload["price"] = str(order.price)
        payload["timeInForce"] = _TIME_IN_FORCE[order.time_in_force]

    if order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        payload["triggerPrice"] = str(order.stop_price)
        payload["triggerDirection"] = 1 if order.side == OrderSide.BUY else 2

    if order.reduce_only and category != "spot":
        payload["reduceOnly"] = "true"

    _attach_tp_sl(order, payload, category=category)

    return payload


def _attach_tp_sl(order: UnifiedOrder, payload: dict[str, Any], *, category: str) -> None:
    """Attach native take-profit / stop-loss fields to a place-order payload."""
    if order.take_profit is None and order.stop_loss is None:
        return

    if category != "spot":
        any_limit = (
            order.take_profit is not None and order.take_profit.limit_price is not None
        ) or (order.stop_loss is not None and order.stop_loss.limit_price is not None)
        payload["tpslMode"] = "Partial" if any_limit else "Full"

    if order.take_profit is not None:
        _attach_tp_sl_place(payload, order.take_profit, kind="takeProfit")
    if order.stop_loss is not None:
        _attach_tp_sl_place(payload, order.stop_loss, kind="stopLoss")


def _attach_tp_sl_place(
    payload: dict[str, Any],
    attachment: TpSlAttachment,
    *,
    kind: str,
) -> None:
    prefix = "tp" if kind == "takeProfit" else "sl"
    payload[kind] = str(attachment.trigger_price)
    if attachment.limit_price is not None:
        payload[f"{prefix}OrderType"] = "Limit"
        payload[f"{prefix}LimitPrice"] = str(attachment.limit_price)
    else:
        payload[f"{prefix}OrderType"] = "Market"


def build_amend_payload(
    modification: OrderModification,
    *,
    category: str,
    symbol: str,
) -> dict[str, Any]:
    """Translate an ``OrderModification`` into Bybit ``amend_order`` params.

    Bybit locates the order by ``orderLinkId`` (the client order id).

    Raises
    ------
    UnsupportedOrderTypeError
        If a modification field has no native Bybit equivalent for this
        category (spot TP/SL modification).
    """
    if category == "spot" and (
        modification.take_profit is not None or modification.stop_loss is not None
    ):
        raise UnsupportedOrderTypeError(
            "take_profit/stop_loss modification is not supported for spot orders on Bybit"
        )

    payload: dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "orderLinkId": modification.client_order_id,
    }

    if modification.quantity is not None:
        payload["qty"] = str(modification.quantity)
    if modification.price is not None:
        payload["price"] = str(modification.price)
    if modification.stop_price is not None:
        payload["triggerPrice"] = str(modification.stop_price)
    if modification.take_profit is not None:
        _attach_tp_sl_amend(payload, modification.take_profit, kind="takeProfit")
    if modification.stop_loss is not None:
        _attach_tp_sl_amend(payload, modification.stop_loss, kind="stopLoss")

    return payload


def _attach_tp_sl_amend(payload: dict[str, Any], attachment: TpSlAttachment, *, kind: str) -> None:
    prefix = "tp" if kind == "takeProfit" else "sl"
    payload[kind] = str(attachment.trigger_price)
    if attachment.limit_price is not None:
        payload[f"{prefix}LimitPrice"] = str(attachment.limit_price)
        payload["tpslMode"] = "Partial"
    else:
        payload.setdefault("tpslMode", "Full")


def build_cancel_payload(
    client_order_id: str,
    *,
    category: str,
    symbol: str,
) -> dict[str, Any]:
    """Translate a client order id into Bybit ``cancel_order`` params."""
    return {
        "category": category,
        "symbol": symbol,
        "orderLinkId": client_order_id,
    }


def map_order_status(bybit_status: str) -> OrderStatus:
    """Map a Bybit ``orderStatus`` string to the unified ``OrderStatus``.

    Raises ``PlatformError`` for an unrecognised status — a new Bybit status
    must never be silently misrepresented.
    """
    status = _ORDER_STATUS_MAP.get(bybit_status)
    if status is None:
        raise PlatformError(f"Unknown Bybit order status {bybit_status!r}")
    return status


def parse_order_result(entry: dict[str, Any], client_order_id: str) -> OrderResult:
    """Build an ``OrderResult`` from a Bybit order object.

    Parameters
    ----------
    entry :
        The ``result`` object of a place/amend/cancel ack or an entry from a
        ``list`` returned by the open-orders / order-history endpoints.
    client_order_id :
        The client order id this result belongs to.
    """
    if entry.get("orderId") is None:
        raise PlatformError("Bybit order response is missing orderId")

    return OrderResult(
        client_order_id=client_order_id,
        platform_order_id=entry["orderId"],
        status=map_order_status(entry.get("orderStatus") or ""),
        filled_quantity=_decimal_or_zero(entry.get("cumExecQty")),
        average_fill_price=_decimal_or_none(entry.get("avgPrice")),
        created_at=_parse_ms_timestamp(entry.get("createdTime")),
        updated_at=_parse_ms_timestamp(entry.get("updatedTime")),
    )


def _decimal_or_zero(raw: object) -> Decimal:
    if raw is None or raw == "":
        return Decimal("0")
    return Decimal(str(raw))


def _decimal_or_none(raw: object) -> Decimal | None:
    if raw is None or raw == "":
        return None
    value = Decimal(str(raw))
    return None if value == 0 else value


def _parse_ms_timestamp(raw: object) -> datetime:
    if raw is None or raw == "":
        raise PlatformError("Bybit order response is missing a required timestamp")
    ms = int(str(raw))
    seconds, millis = divmod(ms, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)

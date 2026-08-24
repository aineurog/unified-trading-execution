"""Bybit private WebSocket message translation into unified core types.

This module is the pure translation layer between Bybit's private WebSocket
streams (``order``, ``execution``, ``position``, ``wallet``) and the core
types carried by unified events (``FillRecord``, ``Position``, ``Balance``,
``OrderRecord``).  No pybit imports and no I/O happen here — the adapter
resolves each symbol to a canonical ``Instrument`` via its registry and hands
it in, then translates the payload and publishes the resulting event.

Source of truth for the wire shapes:
    https://bybit-exchange.github.io/docs/v5/websocket/private/execution
    https://bybit-exchange.github.io/docs/v5/websocket/private/position
    https://bybit-exchange.github.io/docs/v5/websocket/private/wallet
    https://bybit-exchange.github.io/docs/v5/websocket/private/order
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from unified_trading_execution.bybit.orders import map_order_status
from unified_trading_execution.errors import PlatformError
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord, TpSlAttachment
from unified_trading_execution.types.position import Balance, Position

_BYBIT_TO_SIDE: dict[str, OrderSide] = {
    "Buy": OrderSide.BUY,
    "Sell": OrderSide.SELL,
}

_BYBIT_TO_ORDER_TYPE: dict[str, OrderType] = {
    "Market": OrderType.MARKET,
    "Limit": OrderType.LIMIT,
}

_BYBIT_TO_TIME_IN_FORCE: dict[str, TimeInForce] = {
    "GTC": TimeInForce.GTC,
    "IOC": TimeInForce.IOC,
    "FOK": TimeInForce.FOK,
    "DAY": TimeInForce.DAY,
}

# A stop (conditional) order shows its *post-trigger* execution type in
# ``orderType``; a non-empty ``stopOrderType`` marks it as conditional.
_EMPTY: frozenset[Any] = frozenset({None, ""})
_UNKNOWN = "UNKNOWN"

# Terminal states that free the order from the live order set and may be
# echoed by the exchange (Bybit can repeat a ``Filled`` status when a cancel
# races an execution, and may resend terminal states on reconnect).
_TERMINAL_ORDER_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)

# Every status that removes an order from the open-order set, including
# ``FILLED`` (which is final for the live set but not a cancellation).
_FINAL_ORDER_STATUSES: frozenset[OrderStatus] = _TERMINAL_ORDER_STATUSES | frozenset(
    {OrderStatus.FILLED}
)


def _required_string(entry: dict[str, Any], field: str) -> str:
    raw = entry.get(field)
    if raw is None or raw == "":
        raise PlatformError(f"Bybit stream message is missing required field {field!r}")
    return str(raw)


def _optional_string(entry: dict[str, Any], field: str) -> str | None:
    raw = entry.get(field)
    return None if raw is None or raw == "" else str(raw)


def _decimal(raw: object, field: str) -> Decimal:
    if raw is None or raw == "":
        raise PlatformError(f"Bybit stream message is missing required field {field!r}")
    return Decimal(str(raw))


def _optional_decimal(raw: object) -> Decimal | None:
    if raw is None or raw == "" or raw == "0":
        return None
    return Decimal(str(raw))


def _parse_ms(raw: object, field: str) -> datetime:
    if raw is None or raw == "":
        raise PlatformError(f"Bybit stream message is missing timestamp {field!r}")
    ms = int(str(raw))
    seconds, millis = divmod(ms, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)


def _parse_fee(raw: object) -> Decimal | None:
    """Parse the ``execFee`` field — zero fee is distinct from missing data."""
    if raw is None or raw == "":
        return None
    return Decimal(str(raw))


def translate_fill(
    entry: dict[str, Any], *, instrument: Instrument, client_order_id: str
) -> FillRecord:
    """Build a ``FillRecord`` from one ``execution`` topic entry.

    Parameters
    ----------
    entry :
        A single element of the ``execution`` stream ``data`` array.
    instrument :
        The resolved canonical instrument for ``entry["symbol"]``.
    client_order_id :
        The Bybit ``orderLinkId`` or an empty string when the order was placed
        without a client id.  Used as the fill's ``correlation_id`` so a fill
        remains attributable to the request that caused it.
    """
    return FillRecord(
        client_order_id=client_order_id,
        platform_fill_id=_required_string(entry, "execId"),
        instrument=instrument,
        fill_quantity=_decimal(entry.get("execQty"), "execQty"),
        fill_price=_decimal(entry.get("execPrice"), "execPrice"),
        fill_timestamp=_parse_ms(entry.get("execTime"), "execTime"),
        fee_currency=_optional_string(entry, "feeCurrency"),
        fee_amount=_parse_fee(entry.get("execFee")),
        correlation_id=client_order_id,
    )


def translate_position(entry: dict[str, Any], *, instrument: Instrument) -> Position:
    """Build a ``Position`` from one ``position`` stream entry.

    ``quantity`` follows the core convention: positive = long (``Buy``),
    negative = short (``Sell``), zero for a flat position (``side`` is empty).
    ``position_id`` is the Bybit ``positionIdx`` (0 = one-way, 1/2 = hedge
    side), scoped to the instrument.
    """
    side = entry.get("side")
    size = _decimal(entry.get("size"), "size")
    if side == "Buy":
        quantity = size
    elif side == "Sell":
        quantity = -size
    else:
        quantity = Decimal("0")

    return Position(
        instrument=instrument,
        quantity=quantity,
        average_entry_price=_decimal(entry.get("entryPrice") or "0", "entryPrice"),
        updated_at=_parse_ms(entry.get("updatedTime"), "updatedTime"),
        position_id=str(entry.get("positionIdx", 0)),
    )


def translate_wallet_member(member: dict[str, Any], *, timestamp: datetime) -> tuple[Balance, ...]:
    """Build the per-coin ``Balance`` records from one ``wallet`` stream member.

    Bybit does not report coin-level ``free``/``used`` split directly.  ``used``
    is the sum of funds locked by open order margin (``totalOrderIM``),
    position margin (``totalPositionIM``), spot order lock (``locked``) and any
    bonus (``bonus``) - matching Bybit's own available-balance derivation
    ``walletBalance - totalPositionIM - totalOrderIM - locked - bonus``;
    ``free`` is derived as ``total - used`` so the core invariant
    ``free + used == total`` holds exactly.
    """
    result: list[Balance] = []
    for coin in member.get("coin") or []:
        currency = coin.get("coin")
        if not currency:
            continue

        total = _decimal(coin.get("walletBalance"), "walletBalance")
        used = Decimal("0")
        for field in ("totalOrderIM", "totalPositionIM", "locked", "bonus"):
            value = _optional_decimal(coin.get(field))
            if value is not None:
                used += value

        free = total - used
        if free < 0:
            free = Decimal("0")

        result.append(
            Balance(
                currency=str(currency),
                free=free,
                used=used,
                total=free + used,
                updated_at=timestamp,
            )
        )
    return tuple(result)


def translate_order_entry(entry: dict[str, Any], *, instrument: Instrument) -> OrderRecord:
    """Build an ``OrderRecord`` from one ``order`` stream entry."""
    side_raw = entry.get("side")
    side = _BYBIT_TO_SIDE.get(side_raw) if isinstance(side_raw, str) else None
    if side is None:
        raise PlatformError(f"Unknown Bybit order side {side_raw!r}")

    order_type_raw = entry.get("orderType")
    order_type = (
        _BYBIT_TO_ORDER_TYPE.get(order_type_raw) if isinstance(order_type_raw, str) else None
    )
    if order_type is None:
        raise PlatformError(f"Unknown Bybit order type {order_type_raw!r}")
    stop_order_type = entry.get("stopOrderType")
    if stop_order_type not in _EMPTY and stop_order_type != _UNKNOWN:
        order_type = OrderType.STOP_LIMIT if order_type_raw == "Limit" else OrderType.STOP

    # ``timeInForce`` is omitted by Bybit for market/spot orders; record the
    # portable default.  Any explicit unknown value fails loudly.
    tif_raw = entry.get("timeInForce") or "GTC"
    time_in_force = _BYBIT_TO_TIME_IN_FORCE.get(tif_raw)
    if time_in_force is None:
        raise PlatformError(f"Unknown Bybit timeInForce {entry.get('timeInForce')!r}")

    client_order_id = entry.get("orderLinkId") or ""
    platform_order_id = _required_string(entry, "orderId")

    return OrderRecord(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=_decimal(entry.get("qty"), "qty"),
        time_in_force=time_in_force,
        client_order_id=client_order_id,
        price=_optional_decimal(entry.get("price")),
        stop_price=_optional_decimal(entry.get("triggerPrice")),
        reduce_only=bool(entry.get("reduceOnly")),
        client_tag=None,
        take_profit=_translate_tp_sl(entry.get("takeProfit"), entry.get("tpLimitPrice")),
        stop_loss=_translate_tp_sl(entry.get("stopLoss"), entry.get("slLimitPrice")),
        platform_order_id=platform_order_id,
        status=map_order_status(_required_string(entry, "orderStatus")),
        filled_quantity=_decimal(entry.get("cumExecQty") or "0", "cumExecQty"),
        average_fill_price=_optional_decimal(entry.get("avgPrice")),
        correlation_id=client_order_id,
        created_at=_parse_ms(entry.get("createdTime"), "createdTime"),
        updated_at=_parse_ms(entry.get("updatedTime"), "updatedTime"),
    )


def is_terminal_order_status(status: OrderStatus) -> bool:
    """Return True for statuses that remove an order from the open order set."""
    return status in _TERMINAL_ORDER_STATUSES


def is_final_order_status(status: OrderStatus) -> bool:
    """Return True for any status that removes the order from the live set.

    Unlike :func:`is_terminal_order_status`, this includes ``FILLED``: a
    filled order is final on the exchange even though it is not a
    cancellation.  Used to bound the adapter's seen-order bookkeeping.
    """
    return status in _FINAL_ORDER_STATUSES


def _translate_tp_sl(trigger_raw: object, limit_raw: object) -> TpSlAttachment | None:
    if trigger_raw in _EMPTY:
        return None
    trigger = _decimal(trigger_raw, "takeProfit/stopLoss")
    if trigger == 0:
        return None
    limit = _optional_decimal(limit_raw)
    return TpSlAttachment(
        trigger_price=trigger,
        limit_price=limit,
    )

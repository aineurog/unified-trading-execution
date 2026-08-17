"""UnifiedOrder ↔ MetaTrader 5 MqlTradeRequest translation.

MT5's ``order_send()`` takes a ``MqlTradeRequest`` dict-like structure with
fields that differ significantly from the unified types.  This module
translates in both directions:

- ``build_mt5_request(order)`` — ``UnifiedOrder`` → MT5 request dict
- ``parse_mt5_result(result)`` — MT5 ``OrderSendResult`` → ``OrderResult``
- ``parse_order_record(ticket)`` — MT5 order query → ``OrderResult``

Order type mapping (direction-specific — 8 MT5 types for 4 unified x 2 sides):

=========== ====== ============================
Unified     Side   MT5 ``ORDER_TYPE_*``
=========== ====== ============================
MARKET      BUY    ``ORDER_TYPE_BUY``
MARKET      SELL   ``ORDER_TYPE_SELL``
LIMIT       BUY    ``ORDER_TYPE_BUY_LIMIT``
LIMIT       SELL   ``ORDER_TYPE_SELL_LIMIT``
STOP        BUY    ``ORDER_TYPE_BUY_STOP``
STOP        SELL   ``ORDER_TYPE_SELL_STOP``
STOP_LIMIT  BUY    ``ORDER_TYPE_BUY_STOP_LIMIT``
STOP_LIMIT  SELL   ``ORDER_TYPE_SELL_STOP_LIMIT``
=========== ====== ============================

Platform limitations:

- **Quantity modification**: MT5 ``TRADE_ACTION_MODIFY`` cannot change
  quantity.  ``modify_order(quantity=...)`` raises ``UnsupportedOrderTypeError``
  — the caller must cancel and re-place.
- **Stop-limit TP/SL**: MT5 TP/SL are price levels, not orders with a limit
  price.  ``TpSlAttachment.limit_price`` is not supported — setting it raises
  ``UnsupportedOrderTypeError``.
- **Market order pre-flight**: market orders need the current bid/ask from
  ``symbol_info_tick()``.  The adapter fetches this before calling
  ``build_mt5_request`` — it is NOT part of this module.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.mt5.errors import map_mt5_error
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)


def build_mt5_request(order: UnifiedOrder, *, mt5_module: Any) -> dict[str, Any]:
    """Translate a ``UnifiedOrder`` into an MT5 request dict for ``order_send()``.

    The caller is responsible for:
    - Fetching current bid/ask for MARKET orders (set ``request["price"]``)
    - Resolving the filling mode per symbol (set ``request["type_filling"]``)
    - Converting the instrument to the MT5 symbol string

    *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.

    Raises ``UnsupportedOrderTypeError`` if a TP/SL attachment carries a
    ``limit_price`` (MT5 TP/SL are price levels, not orders).
    """
    request: dict[str, Any] = {
        "action": (
            mt5_module.TRADE_ACTION_DEAL
            if order.order_type == OrderType.MARKET
            else mt5_module.TRADE_ACTION_PENDING
        ),
        "type": _ORDER_TYPE_MAP[(order.order_type, order.side)],
        "volume": float(order.quantity),
    }

    if order.order_type == OrderType.LIMIT:
        if order.price is None:
            raise ValueError(f"price is required for {order.order_type}")
        request["price"] = float(order.price)
    elif order.order_type == OrderType.STOP:
        if order.stop_price is None:
            raise ValueError(f"stop_price is required for {order.order_type}")
        request["price"] = float(order.stop_price)
    elif order.order_type == OrderType.STOP_LIMIT:
        if order.price is None or order.stop_price is None:
            raise ValueError(f"price and stop_price are required for {order.order_type}")
        # MT5 stop-limit: `price` = trigger (stop) level, `stoplimit` = limit price.
        # Unified `price` is the limit price and `stop_price` is the trigger.
        request["price"] = float(order.stop_price)
        request["stoplimit"] = float(order.price)

    if order.take_profit is not None:
        if order.take_profit.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "take_profit.limit_price is not supported by MT5 — take profit is a price level"
            )
        request["tp"] = float(order.take_profit.trigger_price)
    if order.stop_loss is not None:
        if order.stop_loss.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "stop_loss.limit_price is not supported by MT5 — stop loss is a price level"
            )
        request["sl"] = float(order.stop_loss.trigger_price)

    if order.time_in_force == TimeInForce.GTD:
        if order.expire_at is None:
            raise ValueError("expire_at is required when time_in_force == GTD")
        request["type_time"] = mt5_module.ORDER_TIME_SPECIFIED
        request["expiration"] = int(order.expire_at.timestamp())

    return request


def build_mt5_modify_request(
    modification: OrderModification,
    ticket: int,
    order_type: OrderType,
    *,
    mt5_module: Any,
) -> dict[str, Any]:
    """Translate an ``OrderModification`` into an MT5 ``TRADE_ACTION_MODIFY``
    request dict.

    *ticket* is the MT5 order ticket obtained from the ``client_order_id → ticket``
    mapping.  *order_type* is the existing order's type (LIMIT, STOP or
    STOP_LIMIT) — it decides which request field a price carries: MT5 puts the
    trigger in ``price`` and the limit price in ``stoplimit`` (the latter only
    for STOP_LIMIT orders).  *mt5_module* is the lazily-imported
    ``MetaTrader5`` module reference.

    Raises ``UnsupportedOrderTypeError`` if *modification* sets ``quantity``
    (MT5 cannot modify quantity — cancel and re-place is required).
    """
    if modification.quantity is not None:
        raise UnsupportedOrderTypeError(
            "quantity modification is not supported by MT5 — cancel and re-place"
        )

    request: dict[str, Any] = {
        "action": mt5_module.TRADE_ACTION_MODIFY,
        "order": ticket,
    }

    if order_type == OrderType.STOP_LIMIT:
        if modification.stop_price is not None:
            request["price"] = float(modification.stop_price)
        if modification.price is not None:
            request["stoplimit"] = float(modification.price)
    else:
        if modification.price is not None:
            request["price"] = float(modification.price)
        if modification.stop_price is not None:
            request["price"] = float(modification.stop_price)
    if modification.take_profit is not None:
        if modification.take_profit.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "take_profit.limit_price is not supported by MT5 — take profit is a price level"
            )
        request["tp"] = float(modification.take_profit.trigger_price)
    if modification.stop_loss is not None:
        if modification.stop_loss.limit_price is not None:
            raise UnsupportedOrderTypeError(
                "stop_loss.limit_price is not supported by MT5 — stop loss is a price level"
            )
        request["sl"] = float(modification.stop_loss.trigger_price)

    return request


def build_mt5_cancel_request(
    ticket: int,
    *,
    mt5_module: Any,
) -> dict[str, Any]:
    """Build an MT5 ``TRADE_ACTION_REMOVE`` request dict for *ticket*.

    The request targets the order via the ``order`` field ("Order ticket. It
    is used for modifying pending orders").
    """
    return {
        "action": mt5_module.TRADE_ACTION_REMOVE,
        "order": ticket,
    }


def build_mt5_sltp_request(
    position_id: str,
    *,
    take_profit: float | None = None,
    stop_loss: float | None = None,
    mt5_module: Any,
) -> dict[str, Any]:
    """Build an MT5 ``TRADE_ACTION_SLTP`` request dict to modify TP/SL on an
    existing position.

    *position_id* is the MT5 position ticket (as a string).
    At least one of *take_profit* or *stop_loss* must be provided (as float).

    *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.
    """
    if take_profit is None and stop_loss is None:
        raise ValueError("at least one of take_profit or stop_loss must be provided")

    request: dict[str, Any] = {
        "action": mt5_module.TRADE_ACTION_SLTP,
        "position": int(position_id),
    }
    if take_profit is not None:
        request["tp"] = take_profit
    if stop_loss is not None:
        request["sl"] = stop_loss
    return request


def parse_mt5_result(
    result: Any,
    client_order_id: str,
    *,
    mt5_module: Any,
) -> OrderResult:
    """Parse an ``OrderSendResult`` named tuple from MT5 into an ``OrderResult``.

    *client_order_id* is the engine's order id this result belongs to — MT5
    never returns it, so the caller supplies it.  *mt5_module* is the
    lazily-imported ``MetaTrader5`` module reference.

    Raises the mapped exception (via ``map_mt5_error``) when the result
    retcode indicates failure, using ``mt5.last_error()`` for the code and
    description.
    """
    if result is None:
        code, desc = mt5_module.last_error()
        raise map_mt5_error(code, desc) from None

    status = _RETCODE_STATUS_MAP.get(result.retcode)
    if status is None:
        code, desc = mt5_module.last_error()
        # A stale success code (0 or RES_S_OK=1) means no error context —
        # the unmapped retcode itself is the failure.
        if code == 0 or code == mt5_module.RES_S_OK:
            code = result.retcode
        raise map_mt5_error(code, desc or result.comment) from None

    now = datetime.now(tz=UTC)
    filled = (
        Decimal(str(result.volume))
        if status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)
        else Decimal("0")
    )
    if result.order is None or result.order == 0:
        platform_order_id = str(result.deal) if result.deal else None
    else:
        platform_order_id = str(result.order)
    return OrderResult(
        client_order_id=client_order_id,
        platform_order_id=platform_order_id,
        status=status,
        filled_quantity=filled,
        average_fill_price=_decimal_or_none(result.price)
        if status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)
        else None,
        created_at=now,
        updated_at=now,
    )


def parse_order_record(
    order_tuple: Any,
    client_order_id: str,
    *,
    mt5_module: Any,
) -> OrderResult | None:
    """Parse an MT5 order tuple (from ``orders_get()`` / ``order_get()``)
    into an ``OrderResult``.

    Returns ``None`` if the tuple is empty or ``None``.  *client_order_id*
    is the engine's order id this record belongs to — MT5 never returns it,
    so the caller supplies it.  *mt5_module* is the lazily-imported
    ``MetaTrader5`` module reference.
    """
    if order_tuple is None or (hasattr(order_tuple, "__len__") and len(order_tuple) == 0):
        return None

    status = _ORDER_STATE_STATUS_MAP.get(order_tuple.state)
    if status is None:
        raise PlatformError(f"Unknown MT5 order state {order_tuple.state}")
    if order_tuple.ticket is None:
        raise PlatformError("MT5 order record is missing ticket")

    volume = Decimal(str(order_tuple.volume))
    volume_current = Decimal(str(order_tuple.volume_current))
    filled = volume - volume_current

    return OrderResult(
        client_order_id=client_order_id,
        platform_order_id=str(order_tuple.ticket),
        status=status,
        filled_quantity=filled,
        average_fill_price=None,
        created_at=datetime.fromtimestamp(order_tuple.time_setup, tz=UTC),
        updated_at=datetime.fromtimestamp(order_tuple.time_done or order_tuple.time_setup, tz=UTC),
    )


def _decimal_or_none(raw: object) -> Decimal | None:
    if raw is None or raw == "":
        return None
    return Decimal(str(raw))


def _price_stop_price(
    order_type: OrderType,
    order_tuple: Any,
) -> tuple[Decimal | None, Decimal | None]:
    """Extract ``(price, stop_price)`` from an MT5 order tuple.

    MT5 stores order prices by type: LIMIT keeps the limit price in
    ``price_open``; STOP keeps its trigger in ``price_open``; STOP_LIMIT
    keeps the trigger in ``price_open`` and the limit price in
    ``price_stoplimit``.  A zero value means "not set" and maps to ``None``.
    """
    if order_type == OrderType.LIMIT:
        return _positive_or_none(order_tuple.price_open), None
    if order_type == OrderType.STOP:
        return None, _positive_or_none(order_tuple.price_open)
    if order_type == OrderType.STOP_LIMIT:
        return (
            _positive_or_none(order_tuple.price_stoplimit),
            _positive_or_none(order_tuple.price_open),
        )
    return None, None


def _positive_or_none(raw: object) -> Decimal | None:
    value = _decimal_or_none(raw)
    if value is None or value <= 0:
        return None
    return value


def _tp_sl_attachment(raw: object) -> TpSlAttachment | None:
    """Build a ``TpSlAttachment`` from an MT5 ``tp``/``sl`` price level.

    MT5 TP/SL are plain price levels (no limit price).  A zero level means
    "not set" and maps to ``None``.
    """
    value = _positive_or_none(raw)
    if value is None:
        return None
    return TpSlAttachment(trigger_price=value)


def build_order_record(
    order_tuple: Any,
    client_order_id: str,
    instrument: Instrument,
) -> OrderRecord:
    """Build a full ``OrderRecord`` from an MT5 ``orders_get()`` tuple.

    Unlike ``parse_order_record`` (which returns the lightweight
    ``OrderResult``), this reconstructs the complete auditable record —
    instrument, type, side, quantity, price levels, TP/SL, status — so
    ``fetch_open_orders()`` can return ``dict[str, OrderRecord]``.

    Raises ``PlatformError`` for an unrecognized order type, state, or
    time-in-force value.
    """
    if order_tuple.ticket is None:
        raise PlatformError("MT5 order record is missing ticket")

    unified = _MT5_ORDER_TYPE_TO_UNIFIED.get(order_tuple.type)
    if unified is None:
        raise PlatformError(f"Unknown MT5 order type {order_tuple.type}")
    order_type, side = unified

    status = _ORDER_STATE_STATUS_MAP.get(order_tuple.state)
    if status is None:
        raise PlatformError(f"Unknown MT5 order state {order_tuple.state}")

    tif = _ORDER_TIME_TIF_MAP.get(order_tuple.type_time)
    if tif is None:
        raise PlatformError(f"Unknown MT5 order time-in-force {order_tuple.type_time}")

    volume = Decimal(str(order_tuple.volume_initial))
    volume_current = Decimal(str(order_tuple.volume_current))
    price, stop_price = _price_stop_price(order_type, order_tuple)

    return OrderRecord(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=volume,
        time_in_force=tif,
        client_order_id=client_order_id,
        price=price,
        stop_price=stop_price,
        reduce_only=False,
        client_tag=None,
        take_profit=_tp_sl_attachment(order_tuple.tp),
        stop_loss=_tp_sl_attachment(order_tuple.sl),
        platform_order_id=str(order_tuple.ticket),
        status=status,
        filled_quantity=volume - volume_current,
        average_fill_price=None,
        correlation_id=client_order_id,
        created_at=datetime.fromtimestamp(order_tuple.time_setup, tz=UTC),
        updated_at=datetime.fromtimestamp(order_tuple.time_done or order_tuple.time_setup, tz=UTC),
    )


# TRADE_RETCODE_* success codes → unified OrderStatus.  Anything not in this
# map is a failure and is routed through map_mt5_error() instead.
_RETCODE_STATUS_MAP: dict[int, OrderStatus] = {
    10008: OrderStatus.OPEN,  # TRADE_RETCODE_PLACED
    10009: OrderStatus.FILLED,  # TRADE_RETCODE_DONE
    10010: OrderStatus.PARTIALLY_FILLED,  # TRADE_RETCODE_DONE_PARTIAL
    10025: OrderStatus.OPEN,  # TRADE_RETCODE_NO_CHANGES
}

# ORDER_STATE_* → unified OrderStatus for order records from orders_get().
# ENUM_ORDER_STATE has 10 members.  The three REQUEST_* and STARTED states
# are transient but can legitimately appear in orders_get() (an order being
# placed/modified/cancelled is still an active order) — treat them as OPEN
# so a transient snapshot never fails the whole reconciliation fetch.
_ORDER_STATE_STATUS_MAP: dict[int, OrderStatus] = {
    0: OrderStatus.OPEN,  # ORDER_STATE_STARTED
    1: OrderStatus.OPEN,  # ORDER_STATE_PLACED
    2: OrderStatus.CANCELLED,  # ORDER_STATE_CANCELED
    3: OrderStatus.PARTIALLY_FILLED,  # ORDER_STATE_PARTIAL
    4: OrderStatus.FILLED,  # ORDER_STATE_FILLED
    5: OrderStatus.REJECTED,  # ORDER_STATE_REJECTED
    6: OrderStatus.EXPIRED,  # ORDER_STATE_EXPIRED
    7: OrderStatus.OPEN,  # ORDER_STATE_REQUEST_ADD
    8: OrderStatus.OPEN,  # ORDER_STATE_REQUEST_MODIFY
    9: OrderStatus.OPEN,  # ORDER_STATE_REQUEST_CANCEL
}

# MT5 ORDER_TIME_* → unified TimeInForce for order records from orders_get().
_ORDER_TIME_TIF_MAP: dict[int, TimeInForce] = {
    0: TimeInForce.GTC,  # ORDER_TIME_GTC
    1: TimeInForce.DAY,  # ORDER_TIME_DAY
    2: TimeInForce.GTD,  # ORDER_TIME_SPECIFIED
    3: TimeInForce.GTD,  # ORDER_TIME_SPECIFIED_DAY
}


# ---- Internal helpers (implement these) ----


_ORDER_TYPE_MAP: dict[tuple[OrderType, OrderSide], int] = {
    (OrderType.MARKET, OrderSide.BUY): 0,  # ORDER_TYPE_BUY
    (OrderType.MARKET, OrderSide.SELL): 1,  # ORDER_TYPE_SELL
    (OrderType.LIMIT, OrderSide.BUY): 2,  # ORDER_TYPE_BUY_LIMIT
    (OrderType.LIMIT, OrderSide.SELL): 3,  # ORDER_TYPE_SELL_LIMIT
    (OrderType.STOP, OrderSide.BUY): 4,  # ORDER_TYPE_BUY_STOP
    (OrderType.STOP, OrderSide.SELL): 5,  # ORDER_TYPE_SELL_STOP
    (OrderType.STOP_LIMIT, OrderSide.BUY): 6,  # ORDER_TYPE_BUY_STOP_LIMIT
    (OrderType.STOP_LIMIT, OrderSide.SELL): 7,  # ORDER_TYPE_SELL_STOP_LIMIT
}
"""The 8 (type, side) → MT5 ``ORDER_TYPE_*`` int mappings."""

# MT5 ORDER_TYPE_* int → (unified OrderType, OrderSide) — the inverse of
# ``_ORDER_TYPE_MAP``, for reconstructing order records from orders_get().
_MT5_ORDER_TYPE_TO_UNIFIED: dict[int, tuple[OrderType, OrderSide]] = {
    mt5_code: (order_type, side) for (order_type, side), mt5_code in _ORDER_TYPE_MAP.items()
}


def _select_filling(symbol_info: Any, tif: TimeInForce, *, mt5_module: Any) -> int:
    """Select the best available filling mode for *tif* given *symbol_info*.

    Falls back through preference order when the ideal mode is unsupported.
    Raises ``InvalidSymbolError`` if no compatible filling mode exists.
    """
    ideal = _IDEAL_FILLING[tif]
    for mode in _FILLING_FALLBACK[ideal]:
        if symbol_info.filling_mode & (1 << mode):
            return mode
    raise InvalidSymbolError(
        f"symbol has no filling mode compatible with time_in_force={tif.value}"
    )


_IDEAL_FILLING: dict[TimeInForce, int] = {
    TimeInForce.FOK: 0,  # ORDER_FILLING_FOK
    TimeInForce.IOC: 1,  # ORDER_FILLING_IOC
    TimeInForce.GTC: 2,  # ORDER_FILLING_RETURN
    TimeInForce.DAY: 2,
    TimeInForce.GTD: 2,
}

_FILLING_FALLBACK: dict[int, tuple[int, ...]] = {
    0: (0, 1, 2),  # FOK → FOK, IOC, RETURN
    1: (1, 0, 2),  # IOC → IOC, FOK, RETURN
    2: (2, 1, 0),  # RETURN → RETURN, IOC, FOK
}

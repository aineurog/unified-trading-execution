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

from typing import Any

from unified_trading_execution.errors import InvalidSymbolError, UnsupportedOrderTypeError
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


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
        request["price"] = float(order.price)
        request["stoplimit"] = float(order.stop_price)

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
    *,
    mt5_module: Any,
) -> dict[str, Any]:
    """Translate an ``OrderModification`` into an MT5 ``TRADE_ACTION_MODIFY``
    request dict.

    *ticket* is the MT5 order ticket obtained from the ``client_order_id → ticket``
    mapping.  *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.

    Raises ``UnsupportedOrderTypeError`` if *modification* sets ``quantity``
    (MT5 cannot modify quantity — cancel and re-place is required).
    """
    if modification.quantity is not None:
        raise UnsupportedOrderTypeError(
            "quantity modification is not supported by MT5 — cancel and re-place"
        )

    request: dict[str, Any] = {
        "action": mt5_module.TRADE_ACTION_MODIFY,
        "ticket": ticket,
    }

    if modification.price is not None:
        request["price"] = float(modification.price)
    if modification.stop_price is not None:
        request["stoplimit"] = float(modification.stop_price)
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
    """Build an MT5 ``TRADE_ACTION_REMOVE`` request dict for *ticket*."""
    return {
        "action": mt5_module.TRADE_ACTION_REMOVE,
        "ticket": ticket,
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


def parse_mt5_result(result: Any, *, mt5_module: Any) -> OrderResult:
    """Parse an ``OrderSendResult`` named tuple from MT5 into an ``OrderResult``.

    *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.
    Calls ``mt5.last_error()`` if the result indicates failure.
    """
    raise NotImplementedError


def parse_order_record(order_tuple: Any, *, mt5_module: Any) -> OrderResult | None:
    """Parse an MT5 order tuple (from ``orders_get()`` / ``order_get()``)
    into an ``OrderResult``.

    Returns ``None`` if the tuple is empty or ``None``.
    *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.
    """
    raise NotImplementedError


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

"""UnifiedOrder ↔ MetaTrader 5 MqlTradeRequest translation.

MT5's ``order_send()`` takes a ``MqlTradeRequest`` dict-like structure with
fields that differ significantly from the unified types.  This module
translates in both directions:

- ``build_mt5_request(order)`` — ``UnifiedOrder`` → MT5 request dict
- ``parse_mt5_result(result)`` — MT5 ``OrderSendResult`` → ``OrderResult``
- ``parse_order_record(ticket)`` — MT5 order query → ``OrderResult``

Order type mapping (direction-specific — 8 MT5 types for 4 unified × 2 sides):

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

from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


def build_mt5_request(order: UnifiedOrder, *, mt5_module: Any) -> dict[str, Any]:
    """Translate a ``UnifiedOrder`` into an MT5 request dict for ``order_send()``.

    The caller is responsible for:
    - Fetching current bid/ask for MARKET orders (set ``request["price"]``)
    - Resolving the filling mode per symbol (set ``request["type_filling"]``)
    - Converting the instrument to the MT5 symbol string

    *mt5_module* is the lazily-imported ``MetaTrader5`` module reference.
    """
    raise NotImplementedError


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
    raise NotImplementedError


def build_mt5_cancel_request(
    ticket: int,
    *,
    mt5_module: Any,
) -> dict[str, Any]:
    """Build an MT5 ``TRADE_ACTION_REMOVE`` request dict for *ticket*."""
    raise NotImplementedError


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
    raise NotImplementedError


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


_ORDER_TYPE_MAP: dict[tuple[OrderType, OrderSide], int] = {}
"""Populate with the 8 (type, side) → MT5 ORDER_TYPE_* int mappings."""


def _select_filling(symbol_info: Any, tif: TimeInForce, *, mt5_module: Any) -> int:
    """Select the best available filling mode for *tif* given *symbol_info*.

    Falls back through preference order when the ideal mode is unsupported.
    Raises ``InvalidSymbolError`` if no compatible filling mode exists.
    """
    raise NotImplementedError

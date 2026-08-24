"""UnifiedOrder ↔ Interactive Brokers Order translation.

IBKR uses explicit ``Order`` objects. This module translates in both directions:

- ``build_ibkr_orders(order)`` — ``UnifiedOrder`` → list of IBKR ``Order`` objects
- ``apply_ibkr_modification(mod, ibkr_order)`` — Updates an existing IBKR ``Order``
- ``parse_ibkr_trade(trade)`` — IBKR ``Trade`` object → ``OrderResult``

Order mapping is simpler than MT5 because IBKR treats Action (BUY/SELL) and
Order Type (MKT/LMT) as separate fields.

Bracket Orders (TP/SL):
IBKR does not have native "attachments" on a single order. TP and SL are created
as separate child orders linked by ``parentId``. If a ``UnifiedOrder`` contains
TP/SL attachments, ``build_ibkr_orders`` will return a list of 2 or 3 orders
(the parent, the profit taker, and the stop loss), with the ``transmit`` flag
handled correctly to submit them as a single block.

Modifications:
Unlike MT5, IBKR allows modifying the quantity of an open order without canceling it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ib_async import Order, Trade

from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


def build_ibkr_orders(order: UnifiedOrder) -> list[Order]:
    """Translate a ``UnifiedOrder`` into a list of IBKR ``Order`` objects.

    Returns a list containing the main order and any associated TP/SL child
    orders (bracket orders).

    The framework's ``client_order_id`` MUST be mapped to the IBKR ``orderRef``
    field for reliable idempotency and polling recovery.
    """
    raise NotImplementedError


def apply_ibkr_modification(modification: OrderModification, ibkr_order: Order) -> Order:
    """Apply an ``OrderModification`` to an existing IBKR ``Order`` object.

    Unlike MT5, IBKR allows quantity modifications. This function mutates and
    returns the provided ``ibkr_order`` with the updated price, auxPrice (stop),
    or totalQuantity.
    """
    raise NotImplementedError


def parse_ibkr_trade(trade: Trade) -> OrderResult:
    """Parse an ``ib_async.Trade`` object into a unified ``OrderResult``.

    The ``Trade`` object contains the ``Order``, the ``OrderStatus``, and the
    execution ``Fill``s.

    Extracts:
    - ``client_order_id`` from ``trade.order.orderRef``
    - ``platform_order_id`` from ``trade.order.permId`` (or ``orderId``)
    - ``filled_quantity`` and ``average_fill_price`` from ``trade.orderStatus``
    """
    raise NotImplementedError


# ---- Internal mapping dictionaries (to be implemented) ----

_ORDER_ACTION_MAP: dict[OrderSide, str] = {
    # Scaffold: BUY -> "BUY", SELL -> "SELL"
}

_ORDER_TYPE_MAP: dict[OrderType, str] = {
    # Scaffold: MARKET -> "MKT", LIMIT -> "LMT", STOP -> "STP", STOP_LIMIT -> "STP LMT"
}

_TIF_MAP: dict[TimeInForce, str] = {
    # Scaffold: GTC -> "GTC", DAY -> "DAY", IOC -> "IOC", FOK -> "FOK"
}

_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    # Scaffold: Map IBKR status strings ("ApiPending", "Submitted", "Filled", "Cancelled")
    # to unified OrderStatus enums.
}

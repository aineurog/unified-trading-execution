"""UnifiedOrder ↔ Interactive Brokers Order translation.

IBKR expresses orders through explicit ``Order`` objects where action (BUY /
SELL) and order type (MKT / LMT / STP / STP LMT) are independent fields —
unlike MT5's direction-specific types.  This module translates in both
directions and performs no I/O:

- ``build_ibkr_orders(order)`` — ``UnifiedOrder`` → list of IBKR ``Order``
  objects (a plain order, or a bracket when TP/SL attachments are present)
- ``apply_ibkr_modification(mod, ibkr_order)`` — mutate an existing IBKR
  ``Order`` in place (price / stop price / quantity)
- ``parse_ibkr_trade(trade)`` — ``ib_async.Trade`` → ``OrderResult``
- ``map_ibkr_status(status)`` — IBKR ``orderStatus`` string → ``OrderStatus``

Wire conventions encoded here (verified against ``ib_async.order.Order``):

- ``lmtPrice`` / ``auxPrice`` accept ``Decimal`` — prices are forwarded
  without float conversion so precision survives the round trip.
- ``totalQuantity`` is a ``float``.
- ``client_order_id`` maps to ``order.orderRef`` — the stable cross-restart
  handle the adapter queries by (``permId`` changes on amend).
- GTD orders set ``tif="GTD"`` plus ``goodTillDate`` formatted
  ``"%Y%m%d %H:%M:%S"`` in UTC.

Bracket orders (TP/SL):

IBKR has no native attachment on a single order.  A ``UnifiedOrder`` with
TP and/or SL becomes a parent plus linked child orders:

- children reverse the parent's action and copy its quantity and TIF;
- the take-profit is always ``LMT`` at the trigger price (an IBKR
  profit-taker has no market form — a ``TpSlAttachment.limit_price`` on the
  TP raises ``UnsupportedOrderTypeError``);
- the stop-loss is ``STP``, or ``STP LMT`` when a limit price is given;
- with both children present they share an OCA group (filling one cancels
  the other);
- ``transmit`` is staged so the whole bracket submits atomically with the
  last child;
- children carry ``parentId=0`` — the adapter assigns real request IDs and
  links the parent immediately before transmission (``IB.placeOrder`` only
  auto-assigns IDs left at 0).

Not expressible on IBKR (both raise ``UnsupportedOrderTypeError``):

- ``UnifiedOrder.reduce_only`` — no native reduce-only flag exists; closing
  exposure must be an explicit opposite-side order.
- ``UnifiedOrder.position_id`` — no per-position leg targeting outside the
  hedge-account close-position flow (not wired in v1).
- ``OrderModification.take_profit`` / ``stop_loss`` — bracket legs are
  separate IBKR orders; amending them requires resolving the child
  ``Trade``, which the adapter owns (v1 rejects at translation time).

Crypto (SPOT) restrictions — IBKR routes CRYPTO contracts through Paxos /
Zero Hash, which accept far less than the generic contract model:

- **Market and Limit orders only** — STOP / STOP_LIMIT raise
  ``UnsupportedOrderTypeError`` (mirrors Bybit's rejection of spot
  conditionals).
- **No bracket support** — TP/SL children are stop/limit-triggered legs,
  so ``take_profit`` / ``stop_loss`` on a SPOT order raise.
- **MARKET is IOC-only** — the TIF is forced to IOC (the only legal value),
  the same spirit as Bybit omitting an inexpressible TIF for markets.
- **MARKET BUY rejected** — IBKR requires a notional ``cashQty`` for crypto
  market buys while ``UnifiedOrder.quantity`` is base-denominated; the
  translation layer cannot convert honestly without a price feed, so it
  raises and suggests a LIMIT order until notional support lands in core.

Capability reporting stays coarse (``supported_order_types()`` still lists
all four types); per-instrument nuance is enforced here at translation time,
matching the Bybit adapter's discipline.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ib_async import Order

from unified_trading_execution.errors import (
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.types.enums import (
    LIVE_ORDER_STATUSES,
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder

if TYPE_CHECKING:
    from ib_async import Trade

__all__ = [
    "apply_ibkr_modification",
    "build_ibkr_orders",
    "is_final_order_status",
    "is_terminal_order_status",
    "map_ibkr_status",
    "parse_ibkr_trade",
]


# ---------------------------------------------------------------------------
# Wire maps
# ---------------------------------------------------------------------------

_ORDER_ACTION_MAP: dict[OrderSide, str] = {
    OrderSide.BUY: "BUY",
    OrderSide.SELL: "SELL",
}

_OPPOSITE_ACTION: dict[str, str] = {"BUY": "SELL", "SELL": "BUY"}

_ORDER_TYPE_MAP: dict[OrderType, str] = {
    OrderType.MARKET: "MKT",
    OrderType.LIMIT: "LMT",
    OrderType.STOP: "STP",
    OrderType.STOP_LIMIT: "STP LMT",
}

_TIF_MAP: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.DAY: "DAY",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
    TimeInForce.GTD: "GTD",
}

# IBKR orderStatus strings -> unified OrderStatus.
#
# - The three "waiting" states (sent, not yet live) map to PENDING.
# - ``Submitted`` / ``ApiUpdate`` / ``ValidationError`` are working states
#   per ``OrderStatus.WorkingStates``; ``PendingCancel`` is still live until
#   the cancellation confirms — all map to OPEN.
# - ``Inactive`` is a DoneState in ib_async (deactivated / destroyed by risk
#   management) and ib_async itself converts it to Cancelled on manual
#   cancel — CANCELLED follows that precedent.
_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    "ApiPending": OrderStatus.PENDING,
    "PendingSubmit": OrderStatus.PENDING,
    "PreSubmitted": OrderStatus.PENDING,
    "Submitted": OrderStatus.OPEN,
    "ApiUpdate": OrderStatus.OPEN,
    "ValidationError": OrderStatus.OPEN,
    "PendingCancel": OrderStatus.OPEN,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELLED,
    "ApiCancelled": OrderStatus.CANCELLED,
    "Inactive": OrderStatus.CANCELLED,
}


def map_ibkr_status(status: str) -> OrderStatus:
    """Map an IBKR ``orderStatus`` string to the unified ``OrderStatus``.

    Raises ``PlatformError`` for an unknown status — a new IBKR status must
    never be silently misrepresented.
    """
    mapped = _ORDER_STATUS_MAP.get(status)
    if mapped is None:
        raise PlatformError(
            f"Unknown IBKR order status {status!r}",
            platform_error={"ibkr_status": status},
        )
    return mapped


def is_terminal_order_status(status: OrderStatus) -> bool:
    """True for statuses that remove an order from the open set (no FILLED)."""
    return status in {OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}


def is_final_order_status(status: OrderStatus) -> bool:
    """True for any status that removes the order from the live set."""
    return is_terminal_order_status(status) or status is OrderStatus.FILLED


# ---------------------------------------------------------------------------
# Outbound: UnifiedOrder -> IBKR Orders
# ---------------------------------------------------------------------------


def build_ibkr_orders(order: UnifiedOrder) -> list[Order]:
    """Translate a ``UnifiedOrder`` into IBKR ``Order`` objects.

    Returns a single-element list for a plain order, or a bracket of two to
    three orders when TP/SL attachments are present (parent first, then
    take-profit, then stop-loss).  See the module docstring for bracket
    conventions (action reversal, OCA grouping, ``transmit`` staging).

    Raises
    ------
    ValueError
        If ``client_order_id`` is missing — every order must carry the
        framework's UUID7 as ``orderRef`` for idempotency and recovery.
    UnsupportedOrderTypeError
        For fields IBKR cannot express natively (see module docstring).
    """
    if not order.client_order_id:
        raise ValueError(
            "UnifiedOrder.client_order_id is required — the engine assigns it before dispatch"
        )
    if order.reduce_only:
        raise UnsupportedOrderTypeError(
            "reduce_only is not supported on IBKR — close exposure with an "
            "explicit opposite-side order"
        )
    if order.position_id is not None:
        raise UnsupportedOrderTypeError("position_id leg targeting is not supported on IBKR in v1")
    if order.take_profit is not None and order.take_profit.limit_price is not None:
        raise UnsupportedOrderTypeError(
            "take_profit.limit_price is not supported on IBKR — the "
            "profit-taker is always a limit order at the trigger price"
        )

    # Crypto contracts carry their own restriction set (see module docstring);
    # it may force the TIF (MARKET → IOC), so it runs before order building.
    tif_override: TimeInForce | None = None
    if order.instrument.asset_class is AssetClass.SPOT:
        tif_override = _apply_crypto_rules(order)

    action = _ORDER_ACTION_MAP[order.side]
    quantity = float(order.quantity)

    parent = _build_single_order(
        order, action=action, quantity=quantity, time_in_force=tif_override
    )
    parent.orderRef = order.client_order_id
    parent.transmit = order.take_profit is None and order.stop_loss is None

    if order.take_profit is None and order.stop_loss is None:
        return [parent]

    child_action = _OPPOSITE_ACTION[action]
    tif_kwargs = _tif_fields(order.time_in_force, order.expire_at)
    children: list[Order] = []
    if order.take_profit is not None:
        tp = Order(
            orderType="LMT",
            action=child_action,
            totalQuantity=quantity,
            lmtPrice=order.take_profit.trigger_price,
            **tif_kwargs,
        )
        children.append(tp)
    if order.stop_loss is not None:
        sl_fields: dict[str, Any] = {
            "orderType": "STP" if order.stop_loss.limit_price is None else "STP LMT",
            "action": child_action,
            "totalQuantity": quantity,
            "auxPrice": order.stop_loss.trigger_price,
        }
        if order.stop_loss.limit_price is not None:
            sl_fields["lmtPrice"] = order.stop_loss.limit_price
        sl_fields.update(tif_kwargs)
        children.append(Order(**sl_fields))

    if len(children) == 2:
        # OCA: filling either protective leg cancels the other.
        oca_group = f"ute-{order.client_order_id}"
        for child in children:
            child.ocaGroup = oca_group

    for child in children[:-1]:
        child.transmit = False
    children[-1].transmit = True

    return [parent, *children]


def _apply_crypto_rules(order: UnifiedOrder) -> TimeInForce | None:
    """Enforce IBKR's CRYPTO contract restrictions (SPOT instruments).

    IBKR routes crypto through Paxos / Zero Hash, which accept Market and
    Limit orders only, with MARKET restricted to an IOC time in force and
    MARKET BUY requiring a notional ``cashQty`` the unified quantity model
    cannot express.  Violations raise ``UnsupportedOrderTypeError`` — never
    approximated into a platform rejection after the fact.

    Returns the TIF override for legal crypto market orders (IOC), or None
    when the order's own TIF stands.
    """
    if order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        raise UnsupportedOrderTypeError(
            f"{order.order_type.value} orders are not supported for crypto on "
            "IBKR — CRYPTO contracts accept Market and Limit orders only"
        )
    if order.take_profit is not None or order.stop_loss is not None:
        raise UnsupportedOrderTypeError(
            "take_profit/stop_loss brackets are not supported for crypto on "
            "IBKR — bracket legs require stop/limit-triggered child orders"
        )
    if order.order_type is not OrderType.MARKET:
        return None
    if order.side is OrderSide.BUY:
        raise UnsupportedOrderTypeError(
            "MARKET BUY is not supported for crypto on IBKR — the platform "
            "requires a notional cashQty instead of base quantity; place a "
            "LIMIT order instead"
        )
    return TimeInForce.IOC


def _build_single_order(
    order: UnifiedOrder,
    *,
    action: str,
    quantity: float,
    time_in_force: TimeInForce | None = None,
) -> Order:
    """Build the parent/plain order body for the unified order type.

    *time_in_force* overrides the order's own TIF — used to force IOC for
    crypto market orders (the only value the platform accepts there).
    """
    kwargs: dict[str, Any] = {"action": action, "totalQuantity": quantity}

    if order.order_type is OrderType.MARKET:
        order_type = "MKT"
    elif order.order_type is OrderType.LIMIT:
        order_type = "LMT"
        kwargs["lmtPrice"] = order.price
    elif order.order_type is OrderType.STOP:
        order_type = "STP"
        kwargs["auxPrice"] = order.stop_price
    else:  # STOP_LIMIT — price/stop presence validated by UnifiedOrder
        order_type = "STP LMT"
        kwargs["lmtPrice"] = order.price
        kwargs["auxPrice"] = order.stop_price

    kwargs["orderType"] = order_type
    kwargs.update(_tif_fields(time_in_force or order.time_in_force, order.expire_at))

    # Route through the base dataclass (specialised helpers annotate float
    # parameters; the fields themselves accept Decimal and preserve precision).
    return Order(**kwargs)


def _tif_fields(tif: TimeInForce, expire_at: datetime | None) -> dict[str, Any]:
    """TIF fields shared by parent and bracket children."""
    fields: dict[str, Any] = {"tif": _TIF_MAP[tif]}
    if tif is TimeInForce.GTD:
        if expire_at is None:  # defensive — UnifiedOrder validates this
            raise ValueError("GTD order is missing expire_at")
        fields["goodTillDate"] = expire_at.astimezone(UTC).strftime("%Y%m%d %H:%M:%S")
    return fields


# ---------------------------------------------------------------------------
# Modification: mutate an existing IBKR Order in place
# ---------------------------------------------------------------------------


def apply_ibkr_modification(modification: OrderModification, ib_order: Order) -> Order:
    """Apply an ``OrderModification`` to an existing IBKR ``Order``.

    Mutates *ib_order* (IBKR amendments re-transmit the mutated order) and
    returns it.  Supported fields: ``price`` (``lmtPrice``), ``stop_price``
    (``auxPrice``), ``quantity`` (``totalQuantity`` — supported natively,
    unlike MT5).

    Raises
    ------
    UnsupportedOrderTypeError
        For TP/SL modifications — bracket legs are separate IBKR orders the
        adapter must resolve individually (see module docstring).
    """
    if modification.take_profit is not None or modification.stop_loss is not None:
        raise UnsupportedOrderTypeError(
            "TP/SL modification is not supported on IBKR in v1 — bracket "
            "legs are separate orders; cancel and re-place instead"
        )
    if modification.price is not None:
        ib_order.lmtPrice = modification.price
    if modification.stop_price is not None:
        ib_order.auxPrice = modification.stop_price
    if modification.quantity is not None:
        ib_order.totalQuantity = float(modification.quantity)
    return ib_order


# ---------------------------------------------------------------------------
# Inbound: Trade -> OrderResult
# ---------------------------------------------------------------------------


def parse_ibkr_trade(trade: Trade) -> OrderResult:
    """Parse an ``ib_async.Trade`` into a unified ``OrderResult``.

    - ``client_order_id`` ← ``trade.order.orderRef``
    - ``platform_order_id`` ← ``permId`` (stable across restarts), falling
      back to the session ``orderId``
    - partial-fill status is *derived*: IBKR keeps the status string at
      ``Submitted`` while ``filled > 0 < remaining``
    - ``average_fill_price`` ← ``orderStatus.avgFillPrice`` (0 until the
      first fill → ``None``)

    Timestamps come from the ``Trade.log`` entries (first = created, last =
    updated); a trade reconstructed from a fresh socket fetch may have an
    empty log, in which case *now* is substituted so the timezone-aware
    invariant holds.
    """
    order = trade.order
    status = trade.orderStatus

    unified_status = map_ibkr_status(status.status)
    filled_quantity = Decimal(str(status.filled))
    total_quantity = Decimal(str(order.totalQuantity))

    if unified_status in LIVE_ORDER_STATUSES and total_quantity > 0:
        if filled_quantity >= total_quantity:
            unified_status = OrderStatus.FILLED
        elif filled_quantity > 0:
            unified_status = OrderStatus.PARTIALLY_FILLED

    average_fill_price = Decimal(str(status.avgFillPrice)) if status.avgFillPrice else None

    created_at, updated_at = _trade_log_times(trade)

    return OrderResult(
        client_order_id=order.orderRef or "",
        platform_order_id=str(order.permId or order.orderId),
        status=unified_status,
        filled_quantity=filled_quantity,
        average_fill_price=average_fill_price,
        created_at=created_at,
        updated_at=updated_at,
    )


def _trade_log_times(trade: Trade) -> tuple[datetime, datetime]:
    """Created/updated timestamps from the trade log, falling back to now."""
    now = datetime.now(tz=UTC)
    if not trade.log:
        return now, now
    created = trade.log[0].time
    updated = trade.log[-1].time
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    return created, updated

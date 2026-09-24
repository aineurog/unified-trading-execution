"""UnifiedOrder ↔ Hyperliquid SDK order-request translation.

Pure translation layer between the engine's canonical order model and the
SDK ``Exchange`` call path (``bulk_orders`` / ``modify_order`` /
``cancel_by_cloid``), plus the numeric order shaping the venue requires
(``szDecimals`` quantization, notional floors, tier caps).  No I/O here.

Call-path facts (SDK ``Exchange`` source): mutating methods take coin
names and resolve ids internally, minting their own millisecond nonces —
so builders take ``coin`` (never ``asset_id``) and return SDK-shaped
requests the adapter passes straight through.  ``float()`` conversion
happens once here at the boundary: validation upstream runs in exact
``Decimal``, and output equality with the SDK renderer was verified live.
``Alo`` is never emitted — core expresses no post-only order.

Venue shapes (tick-and-lot-size reference): prices allow 5 significant
figures and at most ``MAX_DECIMALS - szDecimals`` decimals (6 perps / 8
spot) with integers always legal; sizes carry at most ``szDecimals``
decimals (validated, never reshaped — only adapter-computed prices are
rounded, via ``round_price_to_tick``).
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

from unified_trading_execution.errors import (
    InvalidOrderError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.hyperliquid.errors import (
    is_cancelled_outcome,
    map_hyperliquid_error,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.order import (
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)

# Price decimal caps (``MAX_DECIMALS - szDecimals``).
MAX_DECIMALS_PERPS = 6
MAX_DECIMALS_SPOT = 8
MAX_PRICE_SIGNIFICANT_FIGURES = 5

_HEX_32_RE = re.compile(r"^[0-9a-f]{32}$")

_TIF_WIRE: dict[TimeInForce, str] = {
    TimeInForce.GTC: "Gtc",
    TimeInForce.IOC: "Ioc",
}

_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    "open": OrderStatus.OPEN,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELLED,
    "triggered": OrderStatus.OPEN,
    "rejected": OrderStatus.REJECTED,
    "marginCanceled": OrderStatus.CANCELLED,
    "vaultWithdrawalCanceled": OrderStatus.CANCELLED,
    "openInterestCapCanceled": OrderStatus.CANCELLED,
    "selfTradeCanceled": OrderStatus.CANCELLED,
    "reduceOnlyCanceled": OrderStatus.CANCELLED,
    "siblingFilledCanceled": OrderStatus.CANCELLED,
    "delistedCanceled": OrderStatus.CANCELLED,
    "liquidatedCanceled": OrderStatus.CANCELLED,
    "scheduledCancel": OrderStatus.CANCELLED,
    "tickRejected": OrderStatus.REJECTED,
    "minTradeNtlRejected": OrderStatus.REJECTED,
    "perpMarginRejected": OrderStatus.REJECTED,
    "reduceOnlyRejected": OrderStatus.REJECTED,
    "badAloPxRejected": OrderStatus.REJECTED,
    "iocCancelRejected": OrderStatus.REJECTED,
    "badTriggerPxRejected": OrderStatus.REJECTED,
    "marketOrderNoLiquidityRejected": OrderStatus.REJECTED,
    "positionIncreaseAtOpenInterestCapRejected": OrderStatus.REJECTED,
    "positionFlipAtOpenInterestCapRejected": OrderStatus.REJECTED,
    "tooAggressiveAtOpenInterestCapRejected": OrderStatus.REJECTED,
    "openInterestIncreaseRejected": OrderStatus.REJECTED,
    "insufficientSpotBalanceRejected": OrderStatus.REJECTED,
    "oracleRejected": OrderStatus.REJECTED,
    "perpMaxPositionRejected": OrderStatus.REJECTED,
}


def client_order_id_to_cloid(client_order_id: str) -> str:
    """Derive the venue cloid (``0x`` + 32 lowercase hex) from a client id.

    Hyphenated UUIDs collapse to their hex and bare 32-hex passes through,
    so engine-generated ids map reversibly.  Anything else hashes
    (sha256, first 16 bytes) — deterministic, so the same client id always
    yields the same cloid and venue idempotent dedupe keeps working.  The
    adapter keeps the reverse index for hashed ids; reversible ids need no
    lookup.  Empty ids raise — core always assigns one before dispatch.
    """
    if not client_order_id:
        raise ValueError("client_order_id must be non-empty")
    text = client_order_id.strip().lower().removeprefix("0x").replace("-", "")
    if _HEX_32_RE.match(text):
        return "0x" + text
    return "0x" + hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()[:32]


def validate_size(quantity: Decimal, sz_decimals: int) -> Decimal:
    """Validate a size against the asset's ``szDecimals``, returning it unchanged.

    Strictness over friendliness (Bybit precedent): over-precise sizes
    raise ``InvalidOrderError`` instead of being silently truncated — the
    caller shapes quantities, the adapter never alters requested exposure.
    Harmless trailing zeroes are stripped before counting, per the venue
    signing note.
    """
    if sz_decimals < 0:
        raise ValueError(f"sz_decimals must be >= 0, got {sz_decimals}")
    normalized = quantity.normalize().as_tuple()
    if not isinstance(normalized.exponent, int):
        raise InvalidOrderError(f"Quantity must be finite, got {quantity}")
    if normalized.exponent < 0 and -normalized.exponent > sz_decimals:
        raise InvalidOrderError(f"Quantity {quantity} exceeds szDecimals={sz_decimals}")
    return quantity


def quantize_price(price: Decimal, sz_decimals: int, *, is_spot: bool) -> Decimal:
    """Validate a price against the venue tick rule, returning it unchanged.

    At most 5 significant figures and at most ``MAX_DECIMALS - szDecimals``
    decimals (6 perps / 8 spot); integers are always legal.  Violations
    raise ``InvalidOrderError`` client-side — the price is never silently
    rounded, since rounding changes the execution price.
    """
    if not price.is_finite():
        raise InvalidOrderError(f"Price must be finite, got {price}")
    if price <= 0:
        raise InvalidOrderError(f"Price must be > 0, got {price}")
    if sz_decimals < 0:
        raise ValueError(f"sz_decimals must be >= 0, got {sz_decimals}")
    normalized = price.normalize().as_tuple()
    if not isinstance(normalized.exponent, int):
        raise InvalidOrderError(f"Price must be finite, got {price}")
    exponent = normalized.exponent
    if exponent >= 0:
        return price
    digits = normalized.digits
    if len(digits) > MAX_PRICE_SIGNIFICANT_FIGURES:
        raise InvalidOrderError(f"Price {price} exceeds 5 significant figures")
    cap = MAX_DECIMALS_SPOT if is_spot else MAX_DECIMALS_PERPS
    if -exponent > cap - sz_decimals:
        raise InvalidOrderError(
            f"Price {price} exceeds {cap - sz_decimals} decimals for szDecimals={sz_decimals}"
        )
    return price


def round_price_to_tick(
    price: Decimal,
    sz_decimals: int,
    *,
    is_spot: bool,
    direction: str = "down",
) -> Decimal:
    """Round an adapter-computed price onto the venue tick.

    Unlike ``quantize_price`` (which validates user prices and never alters
    them), this shapes prices the adapter itself derives — band pricing,
    touch offsets — where rounding is correct and rejection would be absurd.
    Coarsens decimals, then significant figures, until the tick rule holds.
    ``direction="up"`` rounds away from zero (keeps buy bands aggressive);
    ``"down"`` rounds toward zero (keeps sell bands aggressive).  Anything
    else raises ``ValueError``.
    """
    if not price.is_finite():
        raise InvalidOrderError(f"Price must be finite, got {price}")
    if price <= 0:
        raise InvalidOrderError(f"Price must be > 0, got {price}")
    if sz_decimals < 0:
        raise ValueError(f"sz_decimals must be >= 0, got {sz_decimals}")
    if direction == "up":
        rounding = ROUND_UP
    elif direction == "down":
        rounding = ROUND_DOWN
    else:
        raise ValueError(f"direction must be 'up' or 'down', got {direction!r}")
    cap = MAX_DECIMALS_SPOT if is_spot else MAX_DECIMALS_PERPS
    places = cap - sz_decimals
    while True:
        candidate = price.quantize(Decimal(1).scaleb(-places), rounding=rounding)
        if candidate <= 0:
            raise InvalidOrderError(f"Price {price} rounds to zero tick")
        digits = candidate.normalize().as_tuple().digits
        if len(digits) <= MAX_PRICE_SIGNIFICANT_FIGURES:
            return candidate
        places -= 1


def max_market_notional(max_leverage: int) -> Decimal:
    """Maximum market-order notional for a max-leverage tier.

    $30M for max leverage >= 25, $5M for [20, 25), $2M for [10, 20),
    otherwise $500K.  Consumed by ``build_place_order_action``: oversize
    market orders have no mapped venue error string, so the tier breach
    must fail client-side instead of surfacing as a generic reject.
    """
    if max_leverage >= 25:
        return Decimal("30000000")
    if max_leverage >= 20:
        return Decimal("5000000")
    if max_leverage >= 10:
        return Decimal("2000000")
    return Decimal("500000")


def max_limit_notional(max_leverage: int) -> Decimal:
    """Maximum limit-order notional: 10x the market tier."""
    return 10 * max_market_notional(max_leverage)


def _is_spot(order: UnifiedOrder) -> bool:
    return order.instrument.asset_class == AssetClass.SPOT


def _check_placeable(order: UnifiedOrder) -> None:
    """Reject orders with no native venue expression — never approximate."""
    if order.time_in_force == TimeInForce.DAY:
        raise UnsupportedOrderTypeError("time_in_force=DAY is not supported (use GTC or IOC)")
    if order.time_in_force == TimeInForce.GTD:
        # NOTE: revisit when GTD functionality lands — the wire has no
        # expiry field, so supporting it needs an expiry mechanism first
        # (client-side hold/cancel or venue support), not silent GTC mapping.
        raise UnsupportedOrderTypeError(
            "time_in_force=GTD is not supported (the wire has no expiry field)"
        )
    if order.time_in_force == TimeInForce.FOK:
        raise UnsupportedOrderTypeError(
            "time_in_force=FOK is not supported (Ioc allows partials; FOK is unexpressible)"
        )
    if _is_spot(order) and order.reduce_only:
        raise UnsupportedOrderTypeError("reduce_only is not supported for spot orders")
    if order.reduce_only and (order.take_profit is not None or order.stop_loss is not None):
        raise UnsupportedOrderTypeError("reduce_only cannot be combined with take_profit/stop_loss")


def _limit_request(
    *,
    coin: str,
    is_buy: bool,
    quantity: Decimal,
    price: Decimal,
    tif: str,
    reduce_only: bool,
    cloid: str,
) -> dict[str, Any]:
    """Build one SDK ``OrderRequest`` with a limit type."""
    return {
        "coin": coin,
        "is_buy": is_buy,
        "sz": float(quantity),
        "limit_px": float(price),
        "order_type": {"limit": {"tif": tif}},
        "reduce_only": reduce_only,
        "cloid": cloid,
    }


def _trigger_request(
    *,
    coin: str,
    is_buy: bool,
    quantity: Decimal,
    price: Decimal,
    trigger_px: Decimal,
    is_market: bool,
    tpsl: str,
    reduce_only: bool,
    cloid: str,
) -> dict[str, Any]:
    """Build one SDK ``OrderRequest`` with a trigger type."""
    return {
        "coin": coin,
        "is_buy": is_buy,
        "sz": float(quantity),
        "limit_px": float(price),
        "order_type": {
            "trigger": {
                "isMarket": is_market,
                "triggerPx": float(trigger_px),
                "tpsl": tpsl,
            }
        },
        "reduce_only": reduce_only,
        "cloid": cloid,
    }


def _tpsl_children(
    order: UnifiedOrder,
    *,
    coin: str,
    parent_cloid: str,
    is_buy: bool,
    quantity: Decimal,
) -> list[dict[str, Any]]:
    """Build reduce-only TP/SL trigger legs batched with the parent.

    Children close the position (opposite side, same size) and carry
    deterministic cloids derived from the parent id, so their fills stay
    attributable without venue-created ids.  Batched under ``normalTpsl``
    (OCO pair) per the venue TP/SL reference flow.
    """
    children: list[dict[str, Any]] = []
    attachments = (
        (order.take_profit, "tp", "take_profit"),
        (order.stop_loss, "sl", "stop_loss"),
    )
    for attachment, tpsl, suffix in attachments:
        if attachment is None:
            continue
        children.append(
            _trigger_request(
                coin=coin,
                is_buy=not is_buy,
                quantity=quantity,
                price=attachment.limit_price
                if attachment.limit_price is not None
                else attachment.trigger_price,
                trigger_px=attachment.trigger_price,
                is_market=attachment.limit_price is None,
                tpsl=tpsl,
                reduce_only=True,
                cloid=client_order_id_to_cloid(f"{parent_cloid}:{suffix}"),
            )
        )
    return children


def build_place_order_action(
    order: UnifiedOrder,
    *,
    coin: str,
    client_order_id: str,
    market_limit_price: Decimal | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Translate a validated ``UnifiedOrder`` into SDK order requests.

    Returns ``(requests, grouping)`` for ``bulk_orders``: the parent
    request plus any TP/SL child legs (``normalTpsl`` OCO when children
    exist, else ``na``).  MARKET has no native type — it is an aggressive
    limit IOC at ``market_limit_price`` (touch ± band), which the adapter
    supplies and is required here; band policy lives adapter-side.  STOP is
    a market-on-trigger leg (``limit_px`` carries the trigger price, which
    the wire requires but execution ignores); STOP_LIMIT is a limit leg at
    ``price`` triggered at ``stop_price``.  Triggers reference mark price.
    Cloid strings travel raw here — the adapter wraps them in ``Cloid`` at
    the SDK boundary, where the shape is validated.
    """
    _check_placeable(order)
    cloid = client_order_id_to_cloid(client_order_id)
    is_buy = order.side == OrderSide.BUY

    if order.order_type == OrderType.MARKET:
        if market_limit_price is None:
            raise ValueError("market_limit_price is required for MARKET orders")
        if market_limit_price <= 0:
            raise InvalidOrderError(f"market_limit_price must be > 0, got {market_limit_price}")
        parent = _limit_request(
            coin=coin,
            is_buy=is_buy,
            quantity=order.quantity,
            price=market_limit_price,
            tif="Ioc",
            reduce_only=order.reduce_only,
            cloid=cloid,
        )
    elif order.order_type == OrderType.LIMIT:
        assert order.price is not None
        parent = _limit_request(
            coin=coin,
            is_buy=is_buy,
            quantity=order.quantity,
            price=order.price,
            tif=_TIF_WIRE[order.time_in_force],
            reduce_only=order.reduce_only,
            cloid=cloid,
        )
    elif order.order_type == OrderType.STOP:
        assert order.stop_price is not None
        parent = _trigger_request(
            coin=coin,
            is_buy=is_buy,
            quantity=order.quantity,
            price=order.stop_price,
            trigger_px=order.stop_price,
            is_market=True,
            tpsl="sl",
            reduce_only=order.reduce_only,
            cloid=cloid,
        )
    else:
        assert order.order_type == OrderType.STOP_LIMIT
        assert order.price is not None
        assert order.stop_price is not None
        parent = _trigger_request(
            coin=coin,
            is_buy=is_buy,
            quantity=order.quantity,
            price=order.price,
            trigger_px=order.stop_price,
            is_market=False,
            tpsl="sl",
            reduce_only=order.reduce_only,
            cloid=cloid,
        )

    children = _tpsl_children(
        order,
        coin=coin,
        parent_cloid=client_order_id,
        is_buy=is_buy,
        quantity=order.quantity,
    )
    return [parent, *children], ("normalTpsl" if children else "na")


def build_modify_action(
    modification: OrderModification,
    *,
    coin: str,
    current: OrderRecord,
) -> dict[str, Any]:
    """Translate an ``OrderModification`` into SDK ``modify_order`` kwargs.

    The modification is merged over the current record (price/quantity;
    TP/SL legs are not amendable in place).  Always addressed by cloid —
    ``oid`` carries the raw cloid string and the adapter wraps both ids in
    ``Cloid`` at the boundary.  The false-``always_place`` semantics are
    enforced client-side: trigger replacements and MARKET/trigger currents
    have no safe amend form and raise instead of silently degrading to
    cancel/replace.
    """
    if modification.take_profit is not None or modification.stop_loss is not None:
        raise UnsupportedOrderTypeError(
            "take_profit/stop_loss changes require cancel/replace (not amendable in place)"
        )
    if current.order_type == OrderType.MARKET:
        raise UnsupportedOrderTypeError("MARKET orders never rest — nothing to amend")
    if current.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        raise UnsupportedOrderTypeError(
            "trigger order replacement requires cancel/replace (not amendable in place)"
        )
    if current.time_in_force not in _TIF_WIRE:
        raise UnsupportedOrderTypeError(
            f"time_in_force={current.time_in_force.value} cannot be amended in place"
        )
    if modification.stop_price is not None:
        raise UnsupportedOrderTypeError(
            "stop_price changes require cancel/replace (adding or moving a trigger "
            "is a type change, not an amend)"
        )
    quantity = modification.quantity if modification.quantity is not None else current.quantity
    price = modification.price if modification.price is not None else current.price
    if price is None:
        raise InvalidOrderError("amended order has no price")
    raw_cloid = client_order_id_to_cloid(modification.client_order_id)
    return {
        "oid": raw_cloid,
        "name": coin,
        "is_buy": current.side == OrderSide.BUY,
        "sz": float(quantity),
        "limit_px": float(price),
        "order_type": {"limit": {"tif": _TIF_WIRE[current.time_in_force]}},
        "reduce_only": current.reduce_only,
        "cloid": raw_cloid,
    }


def build_cancel_action(
    client_order_id: str,
    *,
    coin: str,
) -> tuple[str, str]:
    """Translate a client order id into a ``(coin, cloid)`` cancel pair.

    The cloid path is used end-to-end (stable across restarts); the adapter
    wraps the raw string in ``Cloid`` for ``cancel_by_cloid``.  The ``fast``
    flag is omitted — the SDK omits it too, and hashing ``false`` is
    rejected; it is never true for triggers.
    """
    return coin, client_order_id_to_cloid(client_order_id)


def map_order_status(status: str) -> OrderStatus:
    """Map a native ``orderStatus`` string to the unified ``OrderStatus``.

    Covers the full venue table (open/filled/canceled/triggered/rejected,
    every cancel-family member and every ``*Rejected`` variant).  Raises
    ``PlatformError`` for an unrecognised status — a new status must never
    be silently misrepresented.
    """
    mapped = _ORDER_STATUS_MAP.get(status)
    if mapped is None:
        raise PlatformError(f"Unknown order status {status!r}")
    return mapped


def parse_order_result(
    status_entry: dict[str, Any] | str,
    client_order_id: str,
    *,
    requested_quantity: Decimal,
) -> OrderResult:
    """Build an ``OrderResult`` from one response ``statuses`` entry.

    ``resting`` → OPEN with the venue oid; ``filled`` → FILLED/PARTIAL by
    filled-vs-requested with the average price (zero averages read as
    missing); ``triggered`` → OPEN (working trigger).  Trigger legs may
    also ack as the bare string ``"waitingForTrigger"`` (observed live —
    the SDK passes statuses through opaquely, so this is documented from
    venue behavior): also OPEN, with no oid yet.  An ``error`` entry
    carrying a benign cancel outcome (post-only match, IOC no-match)
    becomes a CANCELLED result; any other error raises its mapped
    exception.  Action responses carry no timestamps, so results are stamped
    at parse time — the adapter re-queries ``orderStatus`` for authority,
    exactly like a post-ack re-query elsewhere.  A batch pre-validation
    failure (single error for the whole batch) raises on the first item —
    never partial-apply.
    """
    now = datetime.now(UTC)
    if isinstance(status_entry, str):
        if status_entry == "waitingForTrigger":
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=None,
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=now,
                updated_at=now,
            )
        raise PlatformError(f"Unrecognised order response entry {status_entry!r}")
    if "resting" in status_entry:
        resting = status_entry["resting"] or {}
        return OrderResult(
            client_order_id=client_order_id,
            platform_order_id=str(resting.get("oid")) if resting.get("oid") is not None else None,
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=now,
            updated_at=now,
        )
    if "filled" in status_entry:
        filled = status_entry["filled"]
        total = Decimal(str(filled.get("totalSz") or "0"))
        average_raw = Decimal(str(filled.get("avgPx") or "0"))
        return OrderResult(
            client_order_id=client_order_id,
            platform_order_id=str(filled.get("oid")),
            status=OrderStatus.FILLED
            if total >= requested_quantity
            else OrderStatus.PARTIALLY_FILLED,
            filled_quantity=total,
            average_fill_price=None if average_raw == 0 else average_raw,
            created_at=now,
            updated_at=now,
        )
    if "triggered" in status_entry:
        detail = status_entry["triggered"] or {}
        return OrderResult(
            client_order_id=client_order_id,
            platform_order_id=str(detail.get("oid")) if detail.get("oid") is not None else None,
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=now,
            updated_at=now,
        )
    if "error" in status_entry:
        message = str(status_entry["error"] or "")
        if is_cancelled_outcome(message=message):
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=None,
                status=OrderStatus.CANCELLED,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=now,
                updated_at=now,
            )
        raise map_hyperliquid_error(message=message)
    raise PlatformError(f"Unrecognised order response entry {status_entry!r}")

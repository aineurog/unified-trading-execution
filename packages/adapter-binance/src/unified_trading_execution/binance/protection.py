"""Binance position TP/SL protection via Futures conditional orders.

USDⓈ-M protection uses conditional algo orders; COIN-M protection uses regular
conditional orders. Adapter-owned protection is identified by a deterministic
client order ID derived from the position leg id, so reads, repricing, and
clearing never touch orders the adapter did not place. Spot has no position legs.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import TypeAlias

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.orders import (
    _decimal_string,
    to_binance_client_order_id,
)
from unified_trading_execution.errors import PlatformError, UnsupportedOrderTypeError
from unified_trading_execution.trailing.types import TrailingBasis
from unified_trading_execution.types.order import TpSlAttachment

_Metadata: TypeAlias = dict[str, object]

# Prefix marking adapter-owned protection orders. Entries carrying it are
# excluded from the ordinary order mirror and identify ownership for reads
# and clears.
PROTECTION_CLIENT_ID_PREFIX = "tpsl-"

_POSITION_SIDES = frozenset({"BOTH", "LONG", "SHORT"})
_OPEN_PROTECTION_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED", "TRIGGERING", "TRIGGERED"})

# Algo order types that close a leg, mapped to the protection side they
# implement. ``*_MARKET`` variants execute at market on trigger;
# ``STOP``/``TAKE_PROFIT`` rest as limit orders at ``price``.
_STOP_LOSS_TYPES = frozenset({"STOP_MARKET", "STOP"})
_TAKE_PROFIT_TYPES = frozenset({"TAKE_PROFIT_MARKET", "TAKE_PROFIT"})
_LIMIT_TYPES = frozenset({"STOP", "TAKE_PROFIT"})

# ``workingType`` values for the trigger feed. CONTRACT_PRICE is the venue
# default (last trade price); MARK_PRICE is the derivatives mark price.
_WORKING_TYPES: dict[TrailingBasis, str] = {
    TrailingBasis.LAST: "CONTRACT_PRICE",
    TrailingBasis.MARK: "MARK_PRICE",
}


class ProtectionSide(StrEnum):
    """The two TP/SL sides position protection can carry."""

    TAKE_PROFIT = "tp"
    STOP_LOSS = "sl"


@dataclass(frozen=True, slots=True)
class PositionLeg:
    """Parsed ``{product}:{symbol}:{positionSide}`` position identifier."""

    product: BinanceProduct
    symbol: str
    position_side: str


@dataclass(frozen=True, slots=True)
class ProtectionOrder:
    """One open adapter-owned conditional order."""

    side: ProtectionSide
    trigger_price: Decimal
    limit_price: Decimal | None
    status: str


def parse_position_id(product: BinanceProduct, position_id: str) -> PositionLeg:
    """Split a ``Position.position_id`` into product, symbol, and side.

    Position ids are built by ``streams.position_identifier`` as
    ``"{product.value}:{symbol}:{positionSide}"``. Symbols never contain
    ``:``, so a three-part split is unambiguous.
    """
    parts = position_id.split(":")
    if len(parts) != 3:
        raise PlatformError(f"Malformed Binance position id {position_id!r}")
    leg_product_raw, symbol, position_side = parts
    try:
        leg_product = BinanceProduct(leg_product_raw)
    except ValueError as exc:
        raise PlatformError(
            f"Binance position id {position_id!r} has unknown product {leg_product_raw!r}"
        ) from exc
    if leg_product is not product:
        raise PlatformError(
            f"Binance position id {position_id!r} belongs to {leg_product.value}, "
            f"not {product.value}"
        )
    if not symbol:
        raise PlatformError(f"Binance position id {position_id!r} has an empty symbol")
    if position_side not in _POSITION_SIDES:
        raise PlatformError(
            f"Binance position id {position_id!r} has unknown position side {position_side!r}"
        )
    return PositionLeg(product=product, symbol=symbol, position_side=position_side)


def protection_client_id(leg: PositionLeg, side: ProtectionSide) -> str:
    """Derive the deterministic client ID for one protection side.

    The id embeds symbol and position side rather than the full position id.
    Product markers keep USDⓈ-M and COIN-M IDs distinct. The generated ID
    uses URL-safe characters for compatibility with Binance Futures requests.
    """
    product_marker = "c" if leg.product is BinanceProduct.COIN_M else "u"
    return to_binance_client_order_id(
        f"{PROTECTION_CLIENT_ID_PREFIX}{product_marker}-{leg.symbol}-"
        f"{leg.position_side}-{side.value}"
    )


def is_protection_client_id(client_order_id: str) -> bool:
    """Return whether an order client id marks adapter-owned protection."""
    return client_order_id.startswith(PROTECTION_CLIENT_ID_PREFIX)


def close_order_side(quantity: Decimal) -> str:
    """Return the order side that reduces a position of the given sign."""
    if quantity > 0:
        return "SELL"
    if quantity < 0:
        return "BUY"
    raise PlatformError("Cannot derive a protection order side for a flat position")


def algo_order_type(side: ProtectionSide, *, market: bool) -> str:
    """Map a protection side and execution style to a Binance conditional type."""
    if side is ProtectionSide.STOP_LOSS:
        return "STOP_MARKET" if market else "STOP"
    return "TAKE_PROFIT_MARKET" if market else "TAKE_PROFIT"


def protection_side_for_type(order_type: str) -> ProtectionSide | None:
    """Map a Binance algo order type back to its protection side."""
    normalized = order_type.upper()
    if normalized in _STOP_LOSS_TYPES:
        return ProtectionSide.STOP_LOSS
    if normalized in _TAKE_PROFIT_TYPES:
        return ProtectionSide.TAKE_PROFIT
    return None


def working_type(trigger_basis: TrailingBasis | None) -> str | None:
    """Map a declared trigger basis to ``workingType``; ``None`` keeps default."""
    if trigger_basis is None:
        return None
    mapped = _WORKING_TYPES.get(trigger_basis)
    if mapped is None:
        raise UnsupportedOrderTypeError(
            f"Binance position protection cannot trigger on {trigger_basis.value} prices"
        )
    return mapped


def build_algo_order_payload(
    *,
    product: BinanceProduct,
    symbol: str,
    position_side: str,
    leg_quantity: Decimal,
    attachment: TpSlAttachment,
    side: ProtectionSide,
    trigger_basis: TrailingBasis | None,
    client_algo_id: str,
) -> dict[str, object]:
    """Build parameters for a USDⓈ-M algo protection order.

    Market-on-trigger attachments use ``closePosition`` so the venue tracks
    the live leg size. Limit attachments snapshot the current leg quantity
    and rest at ``price``; ``reduceOnly`` is sent only in one-way mode where
    the venue accepts it (hedge mode scopes the leg via ``positionSide``).
    """
    market = attachment.limit_price is None
    order_type = algo_order_type(side, market=market)
    payload: dict[str, object] = {
        "symbol": symbol,
        "side": close_order_side(leg_quantity),
        "positionSide": position_side,
        "type": order_type,
        "algoType": "CONDITIONAL",
        "triggerPrice": _decimal_string(attachment.trigger_price),
        "clientAlgoId": client_algo_id,
    }
    if product is BinanceProduct.USD_M:
        payload["newOrderRespType"] = "RESULT"
    if market:
        payload["closePosition"] = "true"
    else:
        payload["price"] = _decimal_string(attachment.limit_price)
        payload["quantity"] = _decimal_string(abs(leg_quantity))
        payload["timeInForce"] = "GTC"
        if position_side == "BOTH":
            payload["reduceOnly"] = "true"
    basis = working_type(trigger_basis)
    if basis is not None:
        payload["workingType"] = basis
    return payload


def build_coin_m_order_payload(
    *,
    symbol: str,
    position_side: str,
    leg_quantity: Decimal,
    attachment: TpSlAttachment,
    side: ProtectionSide,
    trigger_basis: TrailingBasis | None,
    client_order_id: str,
) -> dict[str, object]:
    """Build a regular COIN-M conditional-order payload for position protection.

    Market-on-trigger attachments send ``placeType="position"`` — the same
    field the Binance UI uses for position TP/SL — which links the order to
    the position leg so the venue auto-cancels it when the leg closes and
    surfaces it in the Positions TP/SL panel. Verified against Demo
    ``/dapi/v1/order``. Limit attachments snapshot quantity instead and do
    not carry ``placeType``.
    """
    market = attachment.limit_price is None
    payload: dict[str, object] = {
        "symbol": symbol,
        "side": close_order_side(leg_quantity),
        "positionSide": position_side,
        "type": algo_order_type(side, market=market),
        "stopPrice": _decimal_string(attachment.trigger_price),
        "newClientOrderId": client_order_id,
        "workingType": working_type(trigger_basis) or "CONTRACT_PRICE",
        "priceProtect": "TRUE",
    }
    if market:
        payload["closePosition"] = "true"
        payload["placeType"] = "position"
    else:
        payload["price"] = _decimal_string(attachment.limit_price)
        payload["quantity"] = _decimal_string(abs(leg_quantity))
        payload["timeInForce"] = "GTC"
        if position_side == "BOTH":
            payload["reduceOnly"] = "true"
    return payload


def parse_protection_entry(entry: _Metadata) -> ProtectionOrder | None:
    """Translate one open algo or regular order row into a ``ProtectionOrder``.

    Returns ``None`` for rows that are not adapter-owned protection or are no
    longer open. Raises ``PlatformError`` on malformed protection rows.
    """
    client_order_id = entry.get("clientAlgoId") or entry.get("clientOrderId")
    if not isinstance(client_order_id, str) or not is_protection_client_id(client_order_id):
        return None

    raw_status = entry.get("algoStatus") or entry.get("status")
    status = str(raw_status).upper() if raw_status is not None else ""
    if status not in _OPEN_PROTECTION_STATUSES:
        return None

    raw_type = entry.get("type") or entry.get("orderType")
    if not isinstance(raw_type, str):
        raise PlatformError(
            f"Binance protection order {client_order_id} has no type",
            platform_error=entry,
        )
    side = protection_side_for_type(raw_type)
    if side is None:
        raise PlatformError(
            f"Binance protection order {client_order_id} has unsupported type {raw_type!r}",
            platform_error=entry,
        )

    raw_trigger = entry.get("triggerPrice", entry.get("stopPrice"))
    try:
        trigger_price = Decimal(str(raw_trigger))
    except (InvalidOperation, ValueError) as exc:
        raise PlatformError(
            f"Binance protection order {client_order_id} has invalid triggerPrice {raw_trigger!r}",
            platform_error=entry,
        ) from exc
    if not trigger_price.is_finite() or trigger_price <= 0:
        raise PlatformError(
            f"Binance protection order {client_order_id} has invalid triggerPrice {raw_trigger!r}",
            platform_error=entry,
        )

    limit_price: Decimal | None = None
    if raw_type.upper() in _LIMIT_TYPES:
        raw_price = entry.get("price")
        try:
            parsed_price = Decimal(str(raw_price))
        except (InvalidOperation, ValueError) as exc:
            raise PlatformError(
                f"Binance protection order {client_order_id} has invalid price {raw_price!r}",
                platform_error=entry,
            ) from exc
        if not parsed_price.is_finite() or parsed_price <= 0:
            raise PlatformError(
                f"Binance protection order {client_order_id} has invalid price {raw_price!r}",
                platform_error=entry,
            )
        limit_price = parsed_price

    return ProtectionOrder(
        side=side,
        trigger_price=trigger_price,
        limit_price=limit_price,
        status=status,
    )

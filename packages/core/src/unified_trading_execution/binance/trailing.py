"""Native Futures trailing-stop support via ``TRAILING_STOP_MARKET`` algo orders.

Both margin types expose trailing stops as conditional algo orders:
USDⓈ-M on ``/fapi/v1/algoOrder`` and COIN-M on ``/dapi/v1/algoOrder``
(Binance's unified COIN-M/USDⓈ-M migration, notice C.5). COIN-M has no
pinned-SDK wrapper, so the adapter reaches it through the SDK's own signed
``_request_futures_coin_api`` transport. Spot has no trailing route, so it
falls back to synthetic protection in core.

Trail identity is deterministic per position leg (``trl-{u|c}-{symbol}-
{positionSide}``), mirroring the ``tpsl-`` protection convention: re-attaching
to the same leg cancels and replaces rather than duplicating, and the
``trl-`` prefix keeps trail orders out of the ordinary order mirror.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.orders import to_binance_client_order_id
from unified_trading_execution.binance.protection import PositionLeg, working_type
from unified_trading_execution.errors import PlatformError, UnsupportedOrderTypeError
from unified_trading_execution.trailing.types import NativeTrailingResult, TrailingBasis
from unified_trading_execution.types.enums import OrderSide

TRAILING_CLIENT_ID_PREFIX = "trl-"
TRAILING_ORDER_TYPE = "TRAILING_STOP_MARKET"

# Binance validates callbackRate on a 0.1%-10% band for USDⓈ-M algo orders.
CALLBACK_RATE_MIN = Decimal("0.1")
CALLBACK_RATE_MAX = Decimal("10")

# Algo statuses that mean the trail is still live on the venue.
_OPEN_TRAILING_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED", "TRIGGERING", "TRIGGERED"})


def trailing_client_id(leg: PositionLeg) -> str:
    """Derive the deterministic client algo ID for one leg's trail."""
    product_marker = "c" if leg.product is BinanceProduct.COIN_M else "u"
    return to_binance_client_order_id(
        f"{TRAILING_CLIENT_ID_PREFIX}{product_marker}-{leg.symbol}-{leg.position_side}"
    )


def is_trailing_client_id(client_order_id: str) -> bool:
    """Return whether an order client id marks an adapter-owned trail."""
    return client_order_id.startswith(TRAILING_CLIENT_ID_PREFIX)


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def build_trailing_algo_payload(
    *,
    leg: PositionLeg,
    leg_quantity: Decimal,
    request_side: OrderSide,
    offset: Decimal,
    activation_price: Decimal | None,
    basis: TrailingBasis | None,
) -> dict[str, object]:
    """Build a ``TRAILING_STOP_MARKET`` algo-order payload.

    Binance trails by percentage (``callbackRate``), so only ``PERCENT``
    offsets are expressible natively. The order side is the closing side of
    the leg; ``quantity`` snapshots the current leg size with ``reduceOnly``
    in one-way mode (hedge mode scopes the leg via ``positionSide``).
    ``activation_price`` maps to the venue's ``activatePrice`` field; omit it
    for immediate activation.
    """
    if not (CALLBACK_RATE_MIN <= offset <= CALLBACK_RATE_MAX):
        raise UnsupportedOrderTypeError(
            f"Binance native trailing callbackRate must be within "
            f"{CALLBACK_RATE_MIN}%-{CALLBACK_RATE_MAX}%, got {offset}"
        )
    order_side = OrderSide.SELL if request_side is OrderSide.BUY else OrderSide.BUY
    payload: dict[str, object] = {
        "symbol": leg.symbol,
        "side": order_side.value,
        "positionSide": leg.position_side,
        "type": TRAILING_ORDER_TYPE,
        "algoType": "CONDITIONAL",
        "quantity": _decimal_string(abs(leg_quantity)),
        "callbackRate": _decimal_string(offset),
        "clientAlgoId": trailing_client_id(leg),
        # GTE_GTC ("good till event") makes the venue cancel the trail when
        # the position closes — the same field the Binance UI sends. Plain
        # GTC would leave the trail armed on a flat leg.
        "timeInForce": "GTE_GTC",
    }
    if leg.product is BinanceProduct.USD_M:
        # USDⓈ-M honors RESULT acks; the COIN-M algo route was verified
        # without it, so only send it where it is known to be accepted.
        payload["newOrderRespType"] = "RESULT"
    if leg.position_side == "BOTH":
        payload["reduceOnly"] = "true"
    if activation_price is not None:
        payload["activatePrice"] = _decimal_string(activation_price)
    if basis is not None:
        payload["workingType"] = working_type(basis)
    return payload


def _entry_type(entry: dict[str, Any]) -> str | None:
    raw = entry.get("type") or entry.get("orderType")
    return str(raw).upper() if raw is not None else None


def _entry_status(entry: dict[str, Any]) -> str:
    raw = entry.get("algoStatus") or entry.get("status")
    return str(raw).upper() if raw is not None else ""


def is_trailing_entry(entry: dict[str, Any]) -> bool:
    """Return whether an algo-order row is a trailing-stop order."""
    return _entry_type(entry) == TRAILING_ORDER_TYPE


def parse_trailing_entry(
    entry: dict[str, Any],
    *,
    position_side: str | None = None,
) -> NativeTrailingResult | None:
    """Translate one open Futures algo row into a ``NativeTrailingResult``.

    Returns ``None`` for non-trailing rows, rows that are no longer open, or
    rows on a different position side when ``position_side`` is given. The
    venue does not expose the live computed stop on algo reads, so
    ``current_stop`` stays ``None``.
    """
    if not is_trailing_entry(entry):
        return None
    if _entry_status(entry) not in _OPEN_TRAILING_STATUSES:
        return None
    if position_side is not None and entry.get("positionSide") != position_side:
        return None

    native_id = entry.get("clientAlgoId") or entry.get("algoId")
    if native_id is None:
        raise PlatformError(
            "Binance trailing algo order has no clientAlgoId or algoId",
            platform_error=entry,
        )

    offset: Decimal | None = None
    raw_rate = entry.get("callbackRate")
    if raw_rate is not None:
        try:
            offset = Decimal(str(raw_rate))
        except (InvalidOperation, ValueError) as exc:
            raise PlatformError(
                f"Binance trailing order {native_id} has invalid callbackRate {raw_rate!r}",
                platform_error=entry,
            ) from exc
        if not offset.is_finite() or offset <= 0:
            raise PlatformError(
                f"Binance trailing order {native_id} has invalid callbackRate {raw_rate!r}",
                platform_error=entry,
            )

    current_stop: Decimal | None = None
    raw_stop = entry.get("triggerPrice")
    if raw_stop is not None:
        try:
            stop = Decimal(str(raw_stop))
        except (InvalidOperation, ValueError):
            stop = Decimal("0")
        if stop.is_finite() and stop > 0:
            current_stop = stop

    return NativeTrailingResult(
        native_id=str(native_id),
        confirmed_offset=offset,
        current_stop=current_stop,
        activated=True,
        position_scoped=True,
    )

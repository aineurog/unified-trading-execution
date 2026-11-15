"""Pure translation of Binance account-stream and REST snapshot payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.orders import map_order_status
from unified_trading_execution.errors import PlatformError
from unified_trading_execution.types.enums import (
    FillEntry,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position

_SIDES = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}
_TIME_IN_FORCE = {
    "GTC": TimeInForce.GTC,
    "IOC": TimeInForce.IOC,
    "FOK": TimeInForce.FOK,
    "DAY": TimeInForce.DAY,
    "GTD": TimeInForce.GTD,
}
_SPOT_TYPES = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "LIMIT_MAKER": OrderType.LIMIT,
    "STOP_LOSS": OrderType.STOP,
    "STOP_LOSS_LIMIT": OrderType.STOP_LIMIT,
}
_FUTURES_TYPES = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "STOP_MARKET": OrderType.STOP,
    # TODO: The core models attached TP/SL, but has no distinct order type for standalone
    # take-profit orders. Collapse TAKE_PROFIT_MARKET to STOP for now; add a dedicated
    # OrderType.TAKE_PROFIT and update order/fill translation when that core API is introduced.
    "TAKE_PROFIT_MARKET": OrderType.STOP,
    "STOP": OrderType.STOP_LIMIT,
    "TAKE_PROFIT": OrderType.STOP_LIMIT,
}


def _decimal(value: object, field: str, *, optional: bool = False) -> Decimal | None:
    if value is None or value == "":
        if optional:
            return None
        raise PlatformError(f"Binance stream payload is missing {field}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PlatformError(f"Binance stream payload has invalid {field}: {value!r}") from exc
    if not parsed.is_finite():
        raise PlatformError(f"Binance stream payload has non-finite {field}: {value!r}")
    return parsed


def _timestamp(value: object, field: str, *, fallback: datetime | None = None) -> datetime:
    if value is None or value == "":
        if fallback is not None:
            return fallback
        raise PlatformError(f"Binance stream payload is missing timestamp {field}")
    try:
        return datetime.fromtimestamp(int(str(value)) / 1000, tz=UTC)
    except (OverflowError, OSError, TypeError, ValueError) as exc:
        raise PlatformError(f"Binance stream payload has invalid timestamp {field}") from exc


def balance_currency(product: BinanceProduct, currency: str) -> str:
    """Return a stable product-qualified key for Binance's separate wallets."""
    return f"{product.value.upper()}:{currency}"


def translate_spot_balance(entry: dict[str, Any], *, timestamp: datetime) -> Balance:
    asset = entry.get("a", entry.get("asset"))
    if not isinstance(asset, str) or not asset:
        raise PlatformError("Binance Spot balance update is missing asset")
    free = _decimal(entry.get("f", entry.get("free")), "free balance")
    locked = _decimal(entry.get("l", entry.get("locked")), "locked balance")
    assert free is not None and locked is not None
    return Balance(
        currency=balance_currency(BinanceProduct.SPOT, asset),
        free=free,
        used=locked,
        total=free + locked,
        updated_at=timestamp,
    )


def translate_futures_balance(
    entry: dict[str, Any],
    *,
    product: BinanceProduct,
    timestamp: datetime,
) -> Balance:
    if product == BinanceProduct.SPOT:
        raise PlatformError("Binance Spot balances require the Spot balance format")
    asset = entry.get("asset")
    if not isinstance(asset, str) or not asset:
        raise PlatformError(f"Binance {product.value} balance is missing asset")
    total = _decimal(entry.get("balance", entry.get("walletBalance")), "wallet balance")
    free = _decimal(entry.get("availableBalance"), "available balance")
    assert total is not None and free is not None
    return Balance(
        currency=balance_currency(product, asset),
        free=free,
        used=total - free,
        total=total,
        updated_at=timestamp,
    )


def position_identifier(product: BinanceProduct, symbol: str, position_side: str) -> str:
    return f"{product.value}:{symbol}:{position_side}"


def translate_position(
    entry: dict[str, Any],
    *,
    product: BinanceProduct,
    instrument: Instrument,
    timestamp: datetime | None = None,
) -> Position:
    if product == BinanceProduct.SPOT:
        raise PlatformError("Binance Spot balances are not derivative positions")
    symbol = entry.get("symbol", entry.get("s"))
    side_raw = entry.get("positionSide", entry.get("ps", "BOTH"))
    if not isinstance(symbol, str) or not symbol:
        raise PlatformError(f"Binance {product.value} position is missing symbol")
    if side_raw not in {"BOTH", "LONG", "SHORT"}:
        raise PlatformError(f"Unknown Binance position side {side_raw!r}")

    amount = _decimal(entry.get("positionAmt", entry.get("pa")), "position amount")
    entry_price = _decimal(entry.get("entryPrice", entry.get("ep")), "entry price", optional=True)
    assert amount is not None
    if entry_price is not None and entry_price < 0:
        raise PlatformError(f"Binance {product.value} position has a negative entry price")
    if side_raw == "LONG":
        quantity = abs(amount)
    elif side_raw == "SHORT":
        quantity = -abs(amount)
    else:
        quantity = amount
    updated_at = _timestamp(
        entry.get("updateTime"),
        "updateTime",
        fallback=timestamp or datetime.now(UTC),
    )
    return Position(
        instrument=instrument,
        quantity=quantity,
        average_entry_price=entry_price or Decimal("0"),
        updated_at=updated_at,
        position_id=position_identifier(product, symbol, str(side_raw)),
    )


def _order_type(raw: object, product: BinanceProduct) -> OrderType:
    if not isinstance(raw, str):
        raise PlatformError(f"Binance order has invalid type {raw!r}")
    mapping = _SPOT_TYPES if product == BinanceProduct.SPOT else _FUTURES_TYPES
    order_type = mapping.get(raw.upper())
    if order_type is None:
        raise PlatformError(f"Unsupported Binance {product.value} order type {raw!r}")
    return order_type


def translate_order_entry(
    entry: dict[str, Any],
    *,
    product: BinanceProduct,
    instrument: Instrument,
    conditional: bool = False,
    client_order_id: str | None = None,
) -> OrderRecord:
    """Translate a Spot/Futures order event or REST open-order entry."""
    side_raw = entry.get("S", entry.get("side"))
    side = _SIDES.get(str(side_raw).upper()) if side_raw is not None else None
    if side is None:
        raise PlatformError(f"Unknown Binance order side {side_raw!r}")

    raw_type = entry.get("o", entry.get("type"))
    order_type = _order_type(raw_type, product)
    raw_tif = entry.get("f", entry.get("timeInForce")) or "GTC"
    time_in_force = _TIME_IN_FORCE.get(str(raw_tif).upper())
    if time_in_force is None:
        raise PlatformError(f"Unsupported Binance time in force {raw_tif!r}")

    raw_id = (
        entry.get("c")
        or entry.get("clientOrderId")
        or entry.get("clientAlgoId")
        or entry.get("caid")
        or client_order_id
    )
    order_id = entry.get("i", entry.get("orderId", entry.get("algoId", entry.get("aid"))))
    if raw_id is None or raw_id == "":
        if order_id is None or order_id == "":
            raise PlatformError("Binance order payload has neither client id nor exchange id")
        raw_id = f"binance-{product.value}:{order_id}"
    if order_id is None or order_id == "":
        raise PlatformError("Binance order payload is missing exchange order id")
    cid = str(raw_id)

    quantity = _decimal(entry.get("q", entry.get("origQty", entry.get("quantity"))), "quantity")
    filled = _decimal(
        entry.get(
            "z",
            entry.get(
                "executedQty",
                entry.get("cumQty", entry.get("actualQty", entry.get("aq", "0"))),
            ),
        ),
        "executed quantity",
    )
    assert quantity is not None and filled is not None
    if filled < 0:
        raise PlatformError("Binance order payload has a negative executed quantity")

    status_raw = (
        entry.get("algoStatus", entry.get("X", entry.get("status")))
        if conditional
        else entry.get("X", entry.get("status"))
    )
    if conditional and isinstance(status_raw, str) and status_raw.upper() == "FINISHED":
        status = OrderStatus.FILLED if filled >= quantity else OrderStatus.CANCELLED
    else:
        status = map_order_status(status_raw, conditional=conditional)

    price = _decimal(entry.get("p", entry.get("price")), "price", optional=True)
    if price == 0:
        price = None
    stop_price = _decimal(
        entry.get(
            "P",
            entry.get("stopPrice", entry.get("triggerPrice", entry.get("tp", entry.get("sp")))),
        ),
        "stop price",
        optional=True,
    )
    if stop_price == 0:
        stop_price = None

    average = _decimal(
        entry.get("ap", entry.get("avgPrice", entry.get("actualPrice"))),
        "average fill price",
        optional=True,
    )
    if average in (None, Decimal("0")) and product == BinanceProduct.SPOT and filled > 0:
        quote_quantity = _decimal(
            entry.get("Z", entry.get("cummulativeQuoteQty")),
            "cumulative quote quantity",
            optional=True,
        )
        if quote_quantity is not None and quote_quantity > 0:
            average = quote_quantity / filled

    now = datetime.now(UTC)
    created_at = _timestamp(
        entry.get("O", entry.get("time", entry.get("createTime"))),
        "order creation",
        fallback=now,
    )
    updated_at = _timestamp(
        entry.get("T", entry.get("updateTime", entry.get("E"))),
        "order update",
        fallback=now,
    )

    return OrderRecord(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=quantity,
        time_in_force=time_in_force,
        client_order_id=cid,
        price=price,
        stop_price=stop_price,
        reduce_only=entry.get("R", entry.get("reduceOnly")) in (True, "true", "TRUE"),
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id=str(order_id),
        status=status,
        filled_quantity=filled,
        average_fill_price=average if average is not None and average > 0 else None,
        correlation_id=cid,
        created_at=created_at,
        updated_at=updated_at,
    )


def translate_fill_entry(
    entry: dict[str, Any],
    *,
    product: BinanceProduct,
    instrument: Instrument,
    client_order_id: str,
) -> FillRecord | None:
    """Translate only actual trade executions; non-trade order updates return None."""
    execution_type = entry.get("x", entry.get("executionType"))
    if execution_type is not None and str(execution_type).upper() != "TRADE":
        return None
    quantity = _decimal(
        entry.get("l", entry.get("lastExecutedQty", entry.get("qty"))),
        "last executed quantity",
    )
    if quantity is None or quantity == 0:
        return None
    if quantity < 0:
        raise PlatformError("Binance trade execution has a negative quantity")

    trade_id = entry.get("t", entry.get("tradeId", entry.get("id")))
    if trade_id in (None, "", -1, "-1"):
        raise PlatformError("Binance trade execution is missing trade id")
    symbol = entry.get("s", entry.get("symbol"))
    if not isinstance(symbol, str) or not symbol:
        raise PlatformError("Binance trade execution is missing symbol")

    fill_price = _decimal(entry.get("L", entry.get("price")), "last execution price")
    assert fill_price is not None
    if fill_price <= 0:
        raise PlatformError("Binance trade execution has a non-positive price")
    timestamp = _timestamp(
        entry.get("T", entry.get("time")),
        "trade time",
    )
    commission = _decimal(entry.get("n", entry.get("commission")), "commission", optional=True)
    commission_asset = entry.get("N", entry.get("commissionAsset"))
    position_side = entry.get("ps", entry.get("positionSide"))
    position_id = (
        position_identifier(product, symbol, str(position_side))
        if product != BinanceProduct.SPOT and position_side not in (None, "", "BOTH")
        else None
    )
    reduce_only = any(
        entry.get(key) in (True, "true", "TRUE")
        for key in ("R", "reduceOnly", "cp", "closePosition")
    )

    return FillRecord(
        client_order_id=client_order_id,
        platform_fill_id=f"{product.value}:{symbol}:{trade_id}",
        instrument=instrument,
        fill_quantity=quantity,
        fill_price=fill_price,
        fill_timestamp=timestamp,
        fee_currency=str(commission_asset) if commission_asset not in (None, "") else None,
        fee_amount=commission,
        correlation_id=client_order_id,
        position_id=position_id,
        entry=FillEntry.OUT if reduce_only else None,
    )

"""Unit tests for order payload translation (no network).

Tier bounds are the max market/limit notionals from the venue
contract-specifications reference; wire shapes mirror the exchange
reference plus the SDK ``Exchange``/``Cloid`` source.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.errors import (
    InvalidOrderError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.hyperliquid.orders import (
    build_cancel_action,
    build_modify_action,
    build_place_order_action,
    client_order_id_to_cloid,
    map_order_status,
    max_limit_notional,
    max_market_notional,
    parse_order_result,
    quantize_price,
    round_price_to_tick,
    validate_size,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    OrderModification,
    OrderRecord,
    TpSlAttachment,
    UnifiedOrder,
)


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _spot() -> Instrument:
    return Instrument(symbol="HYPE", quote_currency="USDC", asset_class=AssetClass.SPOT)


def _order(**kwargs: Any) -> UnifiedOrder:
    base: dict[str, Any] = {
        "instrument": _perp(),
        "order_type": OrderType.LIMIT,
        "side": OrderSide.BUY,
        "quantity": Decimal("0.01"),
        "price": Decimal("50000"),
        "time_in_force": TimeInForce.GTC,
    }
    base.update(kwargs)
    return UnifiedOrder(**base)


def test_client_order_id_to_cloid() -> None:
    assert client_order_id_to_cloid("0x" + "ab" * 16) == "0x" + "ab" * 16
    assert client_order_id_to_cloid("AB" * 16) == "0x" + "ab" * 16
    hyphenated = "0193e333-4f1a-7b2c-9d4e-5f6a7b8c9d0e"
    assert client_order_id_to_cloid(hyphenated) == "0x" + hyphenated.replace("-", "")
    hashed = client_order_id_to_cloid("my-strategy-tag-1")
    assert hashed.startswith("0x") and len(hashed) == 34
    assert client_order_id_to_cloid("my-strategy-tag-1") == hashed
    with pytest.raises(ValueError, match="non-empty"):
        client_order_id_to_cloid("")


def test_place_limit() -> None:
    requests, grouping = build_place_order_action(
        _order(), coin="BTC", client_order_id="0x" + "ab" * 16
    )
    assert grouping == "na"
    (request,) = requests
    assert request == {
        "coin": "BTC",
        "is_buy": True,
        "sz": 0.01,
        "limit_px": 50000.0,
        "order_type": {"limit": {"tif": "Gtc"}},
        "reduce_only": False,
        "cloid": "0x" + "ab" * 16,
    }


def test_place_market_requires_price() -> None:
    order = _order(order_type=OrderType.MARKET, price=None)
    with pytest.raises(ValueError, match="market_limit_price"):
        build_place_order_action(order, coin="BTC", client_order_id="x")
    requests, _ = build_place_order_action(
        order, coin="BTC", client_order_id="x", market_limit_price=Decimal("50001")
    )
    (request,) = requests
    assert request["limit_px"] == 50001.0
    assert request["order_type"] == {"limit": {"tif": "Ioc"}}


def test_place_sell_side_and_ioc_and_reduce_only() -> None:
    requests, _ = build_place_order_action(
        _order(side=OrderSide.SELL, time_in_force=TimeInForce.IOC, reduce_only=True),
        coin="BTC",
        client_order_id="x",
    )
    (request,) = requests
    assert request["is_buy"] is False
    assert request["reduce_only"] is True
    assert request["order_type"] == {"limit": {"tif": "Ioc"}}


def test_place_stop_and_stop_limit() -> None:
    stop_requests, _ = build_place_order_action(
        _order(order_type=OrderType.STOP, price=None, stop_price=Decimal("49000")),
        coin="BTC",
        client_order_id="x",
    )
    (wire,) = stop_requests
    assert wire["order_type"] == {"trigger": {"isMarket": True, "triggerPx": 49000.0, "tpsl": "sl"}}
    assert wire["limit_px"] == 49000.0
    stop_limit_requests, _ = build_place_order_action(
        _order(order_type=OrderType.STOP_LIMIT, stop_price=Decimal("49000")),
        coin="BTC",
        client_order_id="x",
    )
    (wire,) = stop_limit_requests
    assert wire["order_type"] == {
        "trigger": {"isMarket": False, "triggerPx": 49000.0, "tpsl": "sl"}
    }
    assert wire["limit_px"] == 50000.0


def test_place_tpsl_children_grouping() -> None:
    requests, grouping = build_place_order_action(
        _order(
            take_profit=TpSlAttachment(trigger_price=Decimal("55000")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("45000"), limit_price=Decimal("44900")),
        ),
        coin="BTC",
        client_order_id="parent-1",
    )
    assert grouping == "normalTpsl"
    parent, tp, sl = requests
    assert tp["is_buy"] is False  # closes the long
    assert tp["reduce_only"] is True
    assert tp["order_type"] == {"trigger": {"isMarket": True, "triggerPx": 55000.0, "tpsl": "tp"}}
    assert sl["order_type"] == {"trigger": {"isMarket": False, "triggerPx": 45000.0, "tpsl": "sl"}}
    assert sl["limit_px"] == 44900.0
    assert tp["cloid"] != sl["cloid"] != parent["cloid"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_in_force": TimeInForce.DAY},
        {
            "time_in_force": TimeInForce.GTD,
            "expire_at": datetime.now(UTC) + timedelta(days=1),
        },
        {"time_in_force": TimeInForce.FOK},
        {"instrument": _spot(), "reduce_only": True},
        {"reduce_only": True, "take_profit": TpSlAttachment(trigger_price=Decimal("1"))},
    ],
)
def test_place_rejections(kwargs: dict[str, Any]) -> None:
    with pytest.raises(UnsupportedOrderTypeError):
        build_place_order_action(_order(**kwargs), coin="BTC", client_order_id="x")


def _record(**kwargs: Any) -> OrderRecord:
    base: dict[str, Any] = {
        "instrument": _perp(),
        "order_type": OrderType.LIMIT,
        "side": OrderSide.BUY,
        "quantity": Decimal("0.01"),
        "time_in_force": TimeInForce.GTC,
        "client_order_id": "0x" + "ab" * 16,
        "price": Decimal("50000"),
        "stop_price": None,
        "reduce_only": False,
        "client_tag": None,
        "take_profit": None,
        "stop_loss": None,
        "platform_order_id": "123",
        "status": OrderStatus.OPEN,
        "filled_quantity": Decimal("0"),
        "average_fill_price": None,
        "correlation_id": "0x" + "ab" * 16,
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }
    base.update(kwargs)
    return OrderRecord(**base)


def test_modify_merges_and_uses_cloid() -> None:
    kwargs = build_modify_action(
        OrderModification(client_order_id="0x" + "ab" * 16, price=Decimal("51000")),
        coin="BTC",
        current=_record(),
    )
    assert kwargs["oid"] == "0x" + "ab" * 16
    assert kwargs["name"] == "BTC"
    assert kwargs["limit_px"] == 51000.0
    assert kwargs["sz"] == 0.01
    assert kwargs["order_type"] == {"limit": {"tif": "Gtc"}}
    assert kwargs["cloid"] == "0x" + "ab" * 16


@pytest.mark.parametrize(
    ("modification", "current_kwargs"),
    [
        (
            OrderModification(
                client_order_id="x", take_profit=TpSlAttachment(trigger_price=Decimal("1"))
            ),
            {},
        ),
        (
            OrderModification(client_order_id="x", price=Decimal("1")),
            {"order_type": OrderType.MARKET},
        ),
        (
            OrderModification(client_order_id="x", price=Decimal("1")),
            {"order_type": OrderType.STOP, "stop_price": Decimal("2")},
        ),
    ],
)
def test_modify_rejections(modification: OrderModification, current_kwargs: dict[str, Any]) -> None:
    with pytest.raises(UnsupportedOrderTypeError):
        build_modify_action(modification, coin="BTC", current=_record(**current_kwargs))


def test_modify_stop_price_rejected() -> None:
    with pytest.raises(UnsupportedOrderTypeError, match="stop_price"):
        build_modify_action(
            OrderModification(client_order_id="x", stop_price=Decimal("1")),
            coin="BTC",
            current=_record(),
        )


def test_cancel_by_cloid() -> None:
    assert build_cancel_action("0x" + "ab" * 16, coin="@107") == ("@107", "0x" + "ab" * 16)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("open", OrderStatus.OPEN),
        ("triggered", OrderStatus.OPEN),
        ("filled", OrderStatus.FILLED),
        ("canceled", OrderStatus.CANCELLED),
        ("marginCanceled", OrderStatus.CANCELLED),
        ("vaultWithdrawalCanceled", OrderStatus.CANCELLED),
        ("openInterestCapCanceled", OrderStatus.CANCELLED),
        ("selfTradeCanceled", OrderStatus.CANCELLED),
        ("reduceOnlyCanceled", OrderStatus.CANCELLED),
        ("siblingFilledCanceled", OrderStatus.CANCELLED),
        ("delistedCanceled", OrderStatus.CANCELLED),
        ("liquidatedCanceled", OrderStatus.CANCELLED),
        ("scheduledCancel", OrderStatus.CANCELLED),
        ("rejected", OrderStatus.REJECTED),
        ("tickRejected", OrderStatus.REJECTED),
        ("minTradeNtlRejected", OrderStatus.REJECTED),
        ("perpMarginRejected", OrderStatus.REJECTED),
        ("reduceOnlyRejected", OrderStatus.REJECTED),
        ("badAloPxRejected", OrderStatus.REJECTED),
        ("iocCancelRejected", OrderStatus.REJECTED),
        ("badTriggerPxRejected", OrderStatus.REJECTED),
        ("marketOrderNoLiquidityRejected", OrderStatus.REJECTED),
        ("positionIncreaseAtOpenInterestCapRejected", OrderStatus.REJECTED),
        ("positionFlipAtOpenInterestCapRejected", OrderStatus.REJECTED),
        ("tooAggressiveAtOpenInterestCapRejected", OrderStatus.REJECTED),
        ("openInterestIncreaseRejected", OrderStatus.REJECTED),
        ("insufficientSpotBalanceRejected", OrderStatus.REJECTED),
        ("oracleRejected", OrderStatus.REJECTED),
        ("perpMaxPositionRejected", OrderStatus.REJECTED),
    ],
)
def test_map_order_status(status: str, expected: OrderStatus) -> None:
    assert map_order_status(status) is expected


def test_map_order_status_unknown() -> None:
    with pytest.raises(PlatformError):
        map_order_status("brandNew")


def test_parse_resting_and_filled() -> None:
    resting = parse_order_result({"resting": {"oid": 777}}, "cid", requested_quantity=Decimal("1"))
    assert (resting.status, resting.platform_order_id) == (OrderStatus.OPEN, "777")
    oid_less = parse_order_result({"resting": {}}, "cid", requested_quantity=Decimal("1"))
    assert (oid_less.status, oid_less.platform_order_id) == (OrderStatus.OPEN, None)
    overfilled = parse_order_result(
        {"filled": {"totalSz": "0.02", "avgPx": "50000", "oid": 778}},
        "cid",
        requested_quantity=Decimal("0.01"),
    )
    assert overfilled.status is OrderStatus.FILLED
    filled = parse_order_result(
        {"filled": {"totalSz": "0.01", "avgPx": "50000", "oid": 778}},
        "cid",
        requested_quantity=Decimal("0.01"),
    )
    assert filled.status is OrderStatus.FILLED
    assert filled.average_fill_price == Decimal("50000")
    partial = parse_order_result(
        {"filled": {"totalSz": "0.005", "avgPx": "0", "oid": 778}},
        "cid",
        requested_quantity=Decimal("0.01"),
    )
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    assert partial.average_fill_price is None


def test_parse_triggered_and_cancelled_outcome() -> None:
    triggered = parse_order_result(
        {"triggered": {"oid": 779}}, "cid", requested_quantity=Decimal("1")
    )
    assert (triggered.status, triggered.platform_order_id) == (OrderStatus.OPEN, "779")
    waiting = parse_order_result("waitingForTrigger", "cid", requested_quantity=Decimal("1"))
    assert (waiting.status, waiting.platform_order_id) == (OrderStatus.OPEN, None)
    with pytest.raises(PlatformError):
        parse_order_result("somethingNew", "cid", requested_quantity=Decimal("1"))
    cancelled = parse_order_result(
        {"error": "Post only order would have immediately matched, bbo was 1."},
        "cid",
        requested_quantity=Decimal("1"),
    )
    assert cancelled.status is OrderStatus.CANCELLED
    assert cancelled.platform_order_id is None


def test_parse_error_raises_mapped() -> None:
    with pytest.raises(InvalidOrderError):
        parse_order_result(
            {"error": "Price must be divisible by tick size."},
            "cid",
            requested_quantity=Decimal("1"),
        )
    with pytest.raises(PlatformError):
        parse_order_result({"bogus": True}, "cid", requested_quantity=Decimal("1"))


@pytest.mark.parametrize(
    ("max_leverage", "expected"),
    [
        (50, "30000000"),
        (25, "30000000"),
        (24, "5000000"),
        (20, "5000000"),
        (19, "2000000"),
        (10, "2000000"),
        (9, "500000"),
        (1, "500000"),
    ],
)
def test_max_market_notional_tiers(max_leverage: int, expected: str) -> None:
    assert max_market_notional(max_leverage) == Decimal(expected)
    assert max_limit_notional(max_leverage) == Decimal(expected) * 10


@pytest.mark.parametrize(
    ("raw", "sz_decimals"),
    [("1.0001", 4), ("1", 0), ("1.001", 3), ("1.0000", 3)],
)
def test_validate_size_accepts(raw: str, sz_decimals: int) -> None:
    assert validate_size(Decimal(raw), sz_decimals) == Decimal(raw)


@pytest.mark.parametrize(
    ("raw", "sz_decimals"),
    [("1.0001", 3), ("1.009", 0), ("0.123456", 5)],
)
def test_validate_size_rejects(raw: str, sz_decimals: int) -> None:
    with pytest.raises(InvalidOrderError):
        validate_size(Decimal(raw), sz_decimals)


@pytest.mark.parametrize(
    ("price", "sz_decimals"),
    [("1234.5", 1), ("0.001234", 0), ("123456", 1), ("1", 1), ("0.01234", 1)],
)
def test_quantize_price_valid_perp(price: str, sz_decimals: int) -> None:
    assert quantize_price(Decimal(price), sz_decimals, is_spot=False) == Decimal(price)


@pytest.mark.parametrize(
    ("price", "sz_decimals", "is_spot"),
    [
        ("1234.56", 1, False),
        ("0.0012345", 5, False),
        ("0.012345", 1, False),
        ("0.0001234", 2, True),
        ("0", 1, False),
        ("-5", 1, False),
    ],
)
def test_quantize_price_rejected(price: str, sz_decimals: int, is_spot: bool) -> None:
    with pytest.raises(InvalidOrderError):
        quantize_price(Decimal(price), sz_decimals, is_spot=is_spot)


def test_quantize_price_spot_boundary() -> None:
    assert quantize_price(Decimal("0.0001234"), 1, is_spot=True) == Decimal("0.0001234")


def test_quantize_rejects_non_finite_and_bad_decimals() -> None:
    with pytest.raises(InvalidOrderError):
        quantize_price(Decimal("NaN"), 1, is_spot=False)
    with pytest.raises(InvalidOrderError):
        quantize_price(Decimal("Infinity"), 1, is_spot=False)
    with pytest.raises(ValueError, match="sz_decimals"):
        quantize_price(Decimal("1"), -1, is_spot=False)
    with pytest.raises(ValueError, match="sz_decimals"):
        validate_size(Decimal("1"), -1)
    with pytest.raises(ValueError, match="sz_decimals"):
        round_price_to_tick(Decimal("1"), -1, is_spot=False)
    with pytest.raises(InvalidOrderError):
        validate_size(Decimal("NaN"), 1)


@pytest.mark.parametrize(
    ("raw", "sz_decimals", "direction", "expected"),
    [
        ("87174.3", 5, "down", "87174"),
        ("87174.3", 5, "up", "87175"),
        ("0.012345", 1, "down", "0.01234"),
        ("1234.5", 1, "down", "1234.5"),
        ("1234.5", 1, "up", "1234.5"),
    ],
)
def test_round_price_to_tick(raw: str, sz_decimals: int, direction: str, expected: str) -> None:
    rounded = round_price_to_tick(Decimal(raw), sz_decimals, is_spot=False, direction=direction)
    assert rounded == Decimal(expected)
    quantize_price(rounded, sz_decimals, is_spot=False)  # roundtrip: always valid


def test_round_price_to_tick_rejects() -> None:
    with pytest.raises(ValueError, match="direction"):
        round_price_to_tick(Decimal("1"), 1, is_spot=False, direction="sideways")
    with pytest.raises(InvalidOrderError):
        round_price_to_tick(Decimal("0"), 1, is_spot=False)

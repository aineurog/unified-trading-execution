"""Unit coverage for Binance order translation and lifecycle operations."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, call

import pytest
from uuid_extensions import uuid7

from unified_trading_execution.binance import BinanceAdapter
from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.orders import (
    OrderReference,
    build_modify_order_payload,
    build_place_order_payload,
    map_order_status,
    parse_order_result,
    to_binance_client_order_id,
)
from unified_trading_execution.errors import (
    InvalidOrderError,
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import Event, OrderCancelledEvent
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import OrderModification, OrderRecord, UnifiedOrder

_TIMESTAMP = 1_684_738_540_559


def _instrument(product: BinanceProduct) -> Instrument:
    if product == BinanceProduct.SPOT:
        return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)
    if product == BinanceProduct.USD_M:
        return Instrument(
            symbol="BTC",
            quote_currency="USDT",
            currency="USDT",
            asset_class=AssetClass.FUTURES,
            multiplier=1,
        )
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        currency="BTC",
        asset_class=AssetClass.FUTURES,
        multiplier=100,
    )


def _metadata(product: BinanceProduct) -> dict[str, object]:
    entry: dict[str, object] = {
        "symbol": "BTCUSDT" if product != BinanceProduct.COIN_M else "BTCUSD_PERP",
        "baseAsset": "BTC",
        "quoteAsset": "USDT" if product != BinanceProduct.COIN_M else "USD",
        "status": "TRADING",
    }
    if product != BinanceProduct.SPOT:
        entry.update(
            {
                "contractType": "PERPETUAL",
                "marginAsset": "USDT" if product == BinanceProduct.USD_M else "BTC",
            }
        )
        if product == BinanceProduct.COIN_M:
            entry["contractSize"] = "100"
    return entry


def _order(
    product: BinanceProduct,
    order_type: OrderType,
    **kwargs: Any,
) -> UnifiedOrder:
    time_in_force = kwargs.pop("time_in_force", TimeInForce.GTC)
    quantity = kwargs.pop("quantity", Decimal("0.001"))
    return UnifiedOrder(
        instrument=_instrument(product),
        order_type=order_type,
        side=OrderSide.BUY,
        quantity=quantity,
        time_in_force=time_in_force,
        client_order_id="test-order-1",
        **kwargs,
    )


def _order_response(
    *,
    order_id: str = "987",
    status: str = "NEW",
    **overrides: object,
) -> dict[str, object]:
    return {
        "orderId": order_id,
        "clientOrderId": to_binance_client_order_id("test-order-1"),
        "status": status,
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "avgPrice": "0",
        "transactTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
        **overrides,
    }


@pytest.mark.parametrize(
    ("product", "order_type", "expected_type", "conditional"),
    [
        (BinanceProduct.SPOT, OrderType.MARKET, "MARKET", False),
        (BinanceProduct.SPOT, OrderType.LIMIT, "LIMIT", False),
        (BinanceProduct.SPOT, OrderType.STOP, "STOP_LOSS", False),
        (BinanceProduct.SPOT, OrderType.STOP_LIMIT, "STOP_LOSS_LIMIT", False),
        (BinanceProduct.USD_M, OrderType.MARKET, "MARKET", False),
        (BinanceProduct.USD_M, OrderType.LIMIT, "LIMIT", False),
        (BinanceProduct.USD_M, OrderType.STOP, "STOP_MARKET", True),
        (BinanceProduct.USD_M, OrderType.STOP_LIMIT, "STOP", True),
        (BinanceProduct.COIN_M, OrderType.MARKET, "MARKET", False),
        (BinanceProduct.COIN_M, OrderType.LIMIT, "LIMIT", False),
    ],
)
def test_builds_product_specific_order_payloads(
    product: BinanceProduct,
    order_type: OrderType,
    expected_type: str,
    conditional: bool,
) -> None:
    kwargs: dict[str, object] = {}
    if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        kwargs["price"] = Decimal("100")
    if order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        kwargs["stop_price"] = Decimal("95")

    payload, is_conditional = build_place_order_payload(
        _order(product, order_type, **kwargs),
        product=product,
        symbol=str(_metadata(product)["symbol"]),
        client_order_id="ute-123",
    )

    assert payload["type"] == expected_type
    assert payload["symbol"] == _metadata(product)["symbol"]
    assert payload["quantity"] == "0.001"
    assert payload["clientAlgoId" if conditional else "newClientOrderId"] == "ute-123"
    assert is_conditional is conditional
    assert payload.get("newOrderRespType") == (None if conditional else "RESULT")


@pytest.mark.parametrize(
    ("order_type", "kwargs"),
    [
        (OrderType.STOP, {"stop_price": Decimal("95")}),
        (
            OrderType.STOP_LIMIT,
            {"price": Decimal("100"), "stop_price": Decimal("95")},
        ),
    ],
)
def test_coin_m_conditional_orders_fail_closed(
    order_type: OrderType,
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(UnsupportedOrderTypeError, match="COIN-M conditional"):
        build_place_order_payload(
            _order(BinanceProduct.COIN_M, order_type, **kwargs),
            product=BinanceProduct.COIN_M,
            symbol="BTCUSD_PERP",
            client_order_id="ute-123",
        )


@pytest.mark.parametrize(
    "order",
    [
        _order(BinanceProduct.SPOT, OrderType.LIMIT, price=Decimal("100"), reduce_only=True),
        _order(
            BinanceProduct.USD_M,
            OrderType.LIMIT,
            price=Decimal("100"),
            position_id="BTCUSDT:LONG",
        ),
        _order(
            BinanceProduct.SPOT,
            OrderType.LIMIT,
            price=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        ),
        _order(
            BinanceProduct.USD_M,
            OrderType.MARKET,
            time_in_force=TimeInForce.FOK,
        ),
    ],
)
def test_rejects_unrepresentable_order_parameters(order: UnifiedOrder) -> None:
    with pytest.raises(UnsupportedOrderTypeError):
        build_place_order_payload(
            order,
            product=BinanceProduct.SPOT
            if order.instrument.asset_class == AssetClass.SPOT
            else BinanceProduct.USD_M,
            symbol="BTCUSDT",
            client_order_id="ute-123",
        )


def test_build_modify_payload_contains_only_supported_fields() -> None:
    assert build_modify_order_payload(
        symbol="BTCUSDT",
        client_order_id="ute-123",
        price=Decimal("101.25"),
        quantity=Decimal("0.002"),
    ) == {
        "symbol": "BTCUSDT",
        "origClientOrderId": "ute-123",
        "price": "101.25",
        "quantity": "0.002",
    }


def test_order_payloads_render_decimals_without_exponent_notation() -> None:
    payload, _ = build_place_order_payload(
        _order(
            BinanceProduct.SPOT,
            OrderType.LIMIT,
            price=Decimal("1E+1"),
            quantity=Decimal("1E-8"),
        ),
        product=BinanceProduct.SPOT,
        symbol="BTCUSDT",
        client_order_id="ute-123",
    )
    assert payload["quantity"] == "0.00000001"
    assert payload["price"] == "10"
    assert build_modify_order_payload(
        symbol="BTCUSDT",
        client_order_id="ute-123",
        price=Decimal("1E+1"),
        quantity=Decimal("1E-8"),
    ) == {
        "symbol": "BTCUSDT",
        "origClientOrderId": "ute-123",
        "price": "10",
        "quantity": "0.00000001",
    }


@pytest.mark.parametrize(
    "client_order_id",
    ["trade1232432453", str(uuid7()), "a_b-c.d:e/f"],
)
def test_valid_client_order_ids_are_passed_through_unchanged(client_order_id: str) -> None:
    assert to_binance_client_order_id(client_order_id) == client_order_id


@pytest.mark.parametrize("client_order_id", ["", "has spaces", "x" * 37, "bad!id"])
def test_invalid_client_order_ids_are_rejected_without_rewriting(
    client_order_id: str,
) -> None:
    with pytest.raises(InvalidOrderError, match="client_order_id"):
        to_binance_client_order_id(client_order_id)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("NEW", OrderStatus.OPEN),
        ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
        ("FILLED", OrderStatus.FILLED),
        ("CANCELED", OrderStatus.CANCELLED),
        ("EXPIRED_IN_MATCH", OrderStatus.EXPIRED),
    ],
)
def test_maps_known_order_statuses(raw: str, expected: OrderStatus) -> None:
    assert map_order_status(raw) is expected


def test_maps_conditional_status_and_rejects_unknown_status() -> None:
    assert map_order_status("TRIGGERING", conditional=True) is OrderStatus.OPEN
    with pytest.raises(PlatformError, match="Unknown Binance order status"):
        map_order_status("NEW_STATUS")


def test_parses_spot_fill_average_from_cumulative_quote_quantity() -> None:
    result = parse_order_result(
        _order_response(
            status="FILLED",
            executedQty="0.5",
            cummulativeQuoteQty="100",
        ),
        client_order_id="test-order-1",
    )
    assert result.status is OrderStatus.FILLED
    assert result.filled_quantity == Decimal("0.5")
    assert result.average_fill_price == Decimal("200")
    assert result.platform_order_id == "987"
    assert result.created_at.tzinfo is not None


def test_filled_order_does_not_use_order_price_as_fill_average() -> None:
    with pytest.raises(PlatformError, match="missing average fill price"):
        parse_order_result(
            _order_response(
                status="FILLED",
                executedQty="0.001",
                avgPrice="0",
                price="100",
            ),
            client_order_id="test-order-1",
        )


def test_parses_futures_algo_order_status_and_finished_outcome() -> None:
    entry = {
        "algoId": 123,
        "algoStatus": "NEW",
        "createTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
    }
    result = parse_order_result(entry, client_order_id="test-order-1", conditional=True)
    assert result.platform_order_id == "123"
    assert result.status is OrderStatus.OPEN
    assert result.filled_quantity == Decimal("0")

    filled = parse_order_result(
        {
            **entry,
            "algoStatus": "FINISHED",
            "quantity": "0.001",
            "actualQty": "0.001",
            "actualPrice": "101.5",
        },
        client_order_id="test-order-1",
        conditional=True,
    )
    assert filled.status is OrderStatus.FILLED
    assert filled.filled_quantity == Decimal("0.001")
    assert filled.average_fill_price == Decimal("101.5")

    canceled = parse_order_result(
        {**entry, "algoStatus": "FINISHED", "quantity": "0.001"},
        client_order_id="test-order-1",
        conditional=True,
    )
    assert canceled.status is OrderStatus.CANCELLED

    partial = parse_order_result(
        {
            **entry,
            "algoStatus": "FINISHED",
            "quantity": "0.002",
            "actualQty": "0.001",
            "actualPrice": "101.5",
        },
        client_order_id="test-order-1",
        conditional=True,
    )
    assert partial.status is OrderStatus.CANCELLED
    assert partial.filled_quantity == Decimal("0.001")


async def test_place_order_routes_spot_and_retains_query_reference(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.SPOT
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("10"),
                min_notional=Decimal("5"),
                price_precision=2,
                qty_precision=3,
            )
        ),
    )
    response = _order_response()
    binance_client_mock.create_order.return_value = response
    order = _order(product, OrderType.LIMIT, price=Decimal("100"))

    result = await adapter.place_order(order)

    venue_id = to_binance_client_order_id("test-order-1")
    binance_client_mock.create_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        side="BUY",
        type="LIMIT",
        quantity="0.001",
        newClientOrderId=venue_id,
        price="100",
        timeInForce="GTC",
        newOrderRespType="RESULT",
    )
    assert result.client_order_id == "test-order-1"
    assert adapter._order_refs["test-order-1"].symbol == "BTCUSDT"

    binance_client_mock.get_order.return_value = response
    queried = await adapter.get_order_by_client_id("test-order-1")
    assert queried == result
    binance_client_mock.get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )

    binance_client_mock.cancel_order.return_value = _order_response(status="CANCELED")
    cancelled_events: list[OrderCancelledEvent] = []
    event_bus.subscribe(OrderCancelledEvent, cancelled_events.append)
    cancelled = await adapter.cancel_order("test-order-1")
    assert cancelled.status is OrderStatus.CANCELLED
    binance_client_mock.cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )
    assert len(cancelled_events) == 1
    assert cancelled_events[0].client_order_id == "test-order-1"
    assert cancelled_events[0].instrument == order.instrument

    await adapter._handle_user_stream_message(
        BinanceProduct.SPOT,
        {
            "e": "executionReport",
            "E": _TIMESTAMP,
            "s": "BTCUSDT",
            "c": "test-order-1",
            "i": 987,
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "z": "0",
            "X": "CANCELED",
            "p": "100",
        },
    )
    assert len(cancelled_events) == 1


@pytest.mark.parametrize(
    ("product", "create_method", "query_method"),
    [
        (BinanceProduct.SPOT, "create_order", "get_order"),
        (BinanceProduct.USD_M, "futures_create_order", "futures_get_order"),
        (BinanceProduct.COIN_M, "futures_coin_create_order", "futures_coin_get_order"),
    ],
    ids=("spot", "usd-m", "coin-m"),
)
@pytest.mark.parametrize(
    "placement_avg_price",
    (None, "0", "100"),
    ids=("missing", "zero", "present"),
)
async def test_filled_market_order_queries_exchange_id_for_average_price(
    product: BinanceProduct,
    create_method: str,
    query_method: str,
    placement_avg_price: str | None,
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    placement_response = _order_response(status="FILLED", executedQty="0.001")
    if placement_avg_price is None:
        placement_response.pop("avgPrice")
    else:
        placement_response["avgPrice"] = placement_avg_price
    getattr(binance_client_mock, create_method).return_value = placement_response

    query_response = _order_response(
        status="FILLED",
        executedQty="0.001",
        avgPrice="100" if product != BinanceProduct.SPOT else "0",
        cummulativeQuoteQty="0.1",
    )
    getattr(binance_client_mock, query_method).return_value = query_response

    result = await adapter.place_order(_order(product, OrderType.MARKET))

    assert result.status is OrderStatus.FILLED
    assert result.average_fill_price == Decimal("100")
    query = getattr(binance_client_mock, query_method)
    if placement_avg_price == "100":
        query.assert_not_awaited()
    else:
        query.assert_awaited_once_with(
            symbol="BTCUSD_PERP" if product == BinanceProduct.COIN_M else "BTCUSDT",
            orderId="987",
        )


@pytest.mark.parametrize(
    ("product", "create_method", "query_method"),
    [
        (BinanceProduct.SPOT, "create_order", "get_order"),
        (BinanceProduct.USD_M, "futures_create_order", "futures_get_order"),
        (BinanceProduct.COIN_M, "futures_coin_create_order", "futures_coin_get_order"),
    ],
    ids=("spot", "usd-m", "coin-m"),
)
async def test_filled_market_order_fails_when_query_has_no_average_price(
    product: BinanceProduct,
    create_method: str,
    query_method: str,
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    placement_response = _order_response(status="FILLED", executedQty="0.001")
    placement_response.pop("avgPrice")
    query_response = _order_response(
        status="FILLED",
        executedQty="0.001",
        avgPrice="0",
        cummulativeQuoteQty="0",
    )
    query_response.pop("avgPrice")
    getattr(binance_client_mock, create_method).return_value = placement_response
    getattr(binance_client_mock, query_method).return_value = query_response

    with pytest.raises(PlatformError, match="placement or order-query response"):
        await adapter.place_order(_order(product, OrderType.MARKET))

    getattr(binance_client_mock, query_method).assert_awaited_once_with(
        symbol="BTCUSD_PERP" if product == BinanceProduct.COIN_M else "BTCUSDT",
        orderId="987",
    )


async def test_filled_market_order_fails_when_query_cannot_find_order(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    placement_response = _order_response(status="FILLED", executedQty="0.001")
    placement_response.pop("avgPrice")
    binance_client_mock.futures_create_order.return_value = placement_response
    binance_client_mock.futures_get_order.side_effect = OrderNotFoundError("not found")
    binance_client_mock.futures_get_all_orders.return_value = []

    with pytest.raises(PlatformError, match="could not be queried"):
        await adapter.place_order(_order(product, OrderType.MARKET))

    binance_client_mock.futures_get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )


async def test_usdm_conditional_order_uses_public_algo_order_lifecycle(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    binance_client_mock.futures_create_order.return_value = {
        "algoId": 45,
        "algoStatus": "NEW",
        "createTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
    }

    result = await adapter.place_order(_order(product, OrderType.STOP, stop_price=Decimal("95")))

    venue_id = to_binance_client_order_id("test-order-1")
    binance_client_mock.futures_create_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        side="BUY",
        type="STOP_MARKET",
        quantity="0.001",
        clientAlgoId=venue_id,
        stopPrice="95",
    )
    assert result.platform_order_id == "45"

    algo = {
        "algoId": 45,
        "algoStatus": "NEW",
        "createTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
    }
    binance_client_mock.futures_get_order.return_value = algo
    assert await adapter.get_order_by_client_id("test-order-1") == result
    binance_client_mock.futures_get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        algoId="45",
    )

    binance_client_mock.futures_cancel_order.return_value = {
        "code": 200,
        "msg": "The algo order is canceled.",
    }
    binance_client_mock.futures_get_order.side_effect = OrderNotFoundError("not found")
    binance_client_mock.futures_get_all_algo_orders.return_value = [
        {
            **algo,
            "algoStatus": "CANCELED",
        }
    ]
    cancelled = await adapter.cancel_order("test-order-1")
    assert cancelled.status is OrderStatus.CANCELLED
    binance_client_mock.futures_cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        algoId="45",
    )
    binance_client_mock.futures_get_order.assert_awaited_with(
        symbol="BTCUSDT",
        algoId="45",
    )
    binance_client_mock.futures_get_all_algo_orders.assert_awaited_once_with(
        symbol="BTCUSDT",
    )


async def test_usdm_conditional_query_falls_back_to_algo_history(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    algo = {
        "algoId": 45,
        "algoStatus": "NEW",
        "createTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
    }
    binance_client_mock.futures_create_order.return_value = algo
    await adapter.place_order(_order(product, OrderType.STOP, stop_price=Decimal("95")))

    binance_client_mock.futures_get_order.side_effect = OrderNotFoundError("not found")
    binance_client_mock.futures_get_all_algo_orders.return_value = [algo]

    result = await adapter.get_order_by_client_id("test-order-1")

    assert result is not None
    assert result.platform_order_id == "45"
    assert result.status is OrderStatus.OPEN
    binance_client_mock.futures_get_all_algo_orders.assert_awaited_once_with(
        symbol="BTCUSDT",
    )


async def test_usdm_finished_conditional_order_follows_actual_order(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    algo = {
        "algoId": 45,
        "algoStatus": "NEW",
        "createTime": _TIMESTAMP,
        "updateTime": _TIMESTAMP,
    }
    binance_client_mock.futures_create_order.return_value = algo
    await adapter.place_order(_order(product, OrderType.STOP, stop_price=Decimal("95")))

    finished = {
        **algo,
        "algoStatus": "FINISHED",
        "actualOrderId": 7001,
        "quantity": "0.001",
        "actualQty": "0.001",
        "actualPrice": "101.5",
    }
    binance_client_mock.futures_get_order.side_effect = [
        finished,
        _order_response(
            order_id="7001",
            status="FILLED",
            executedQty="0.001",
            avgPrice="101.5",
        ),
    ]

    result = await adapter.get_order_by_client_id("test-order-1")

    assert result is not None
    assert result.status is OrderStatus.FILLED
    assert result.filled_quantity == Decimal("0.001")
    assert result.average_fill_price == Decimal("101.5")
    assert result.platform_order_id == "45"
    assert binance_client_mock.futures_get_order.await_args_list == [
        call(symbol="BTCUSDT", algoId="45"),
        call(symbol="BTCUSDT", orderId="7001"),
    ]

    binance_client_mock.futures_get_order.side_effect = [
        finished,
        OrderNotFoundError("missing"),
    ]
    fallback = await adapter.get_order_by_client_id("test-order-1")
    assert fallback is not None
    assert fallback.status is OrderStatus.FILLED
    assert fallback.filled_quantity == Decimal("0.001")
    assert fallback.average_fill_price == Decimal("101.5")
    assert fallback.platform_order_id == "45"


async def test_coinm_limit_order_routes_through_public_coinm_methods(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.COIN_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_qty=Decimal("1"),
                max_qty=Decimal("1000"),
                min_notional=Decimal("0"),
                price_precision=1,
                qty_precision=0,
            )
        ),
    )
    binance_client_mock.futures_coin_create_order.return_value = _order_response()
    await adapter.place_order(_order(product, OrderType.LIMIT, price=Decimal("100")))

    venue_id = to_binance_client_order_id("test-order-1")
    binance_client_mock.futures_coin_create_order.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        side="BUY",
        type="LIMIT",
        quantity="0.001",
        newClientOrderId=venue_id,
        price="100",
        timeInForce="GTC",
        newOrderRespType="RESULT",
    )

    binance_client_mock.futures_coin_get_order.return_value = _order_response()
    assert await adapter.get_order_by_client_id("test-order-1") is not None
    binance_client_mock.futures_coin_get_order.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        orderId="987",
    )

    binance_client_mock.futures_coin_v1_put_order.return_value = _order_response()
    await adapter.modify_order(
        OrderModification(client_order_id="test-order-1", quantity=Decimal("0.002"))
    )
    binance_client_mock.futures_coin_v1_put_order.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        origClientOrderId=venue_id,
        quantity="0.002",
    )

    binance_client_mock.futures_coin_cancel_order.return_value = _order_response(status="CANCELED")
    cancelled = await adapter.cancel_order("test-order-1")
    assert cancelled.status is OrderStatus.CANCELLED
    binance_client_mock.futures_coin_cancel_order.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        orderId="987",
    )


async def test_futures_modify_supports_only_normal_limit_orders(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    binance_client_mock.futures_create_order.return_value = _order_response()
    await adapter.place_order(_order(product, OrderType.LIMIT, price=Decimal("100")))
    binance_client_mock.futures_modify_order.return_value = _order_response(
        order_id="987",
        price="101",
    )

    result = await adapter.modify_order(
        OrderModification(client_order_id="test-order-1", price=Decimal("101"))
    )

    assert result.platform_order_id == "987"
    binance_client_mock.futures_modify_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        origClientOrderId=to_binance_client_order_id("test-order-1"),
        price="101",
    )


async def test_futures_modify_requeries_when_response_omits_average_price(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.USD_M
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("5"),
                price_precision=1,
                qty_precision=3,
            )
        ),
    )
    binance_client_mock.futures_create_order.return_value = _order_response()
    await adapter.place_order(_order(product, OrderType.LIMIT, price=Decimal("100")))

    modify_response = _order_response(
        order_id="987",
        status="PARTIALLY_FILLED",
        executedQty="0.0005",
        cummulativeQuoteQty="0",
    )
    modify_response.pop("avgPrice")
    binance_client_mock.futures_modify_order.return_value = modify_response
    binance_client_mock.futures_get_order.return_value = _order_response(
        order_id="987",
        status="PARTIALLY_FILLED",
        executedQty="0.0005",
        cummulativeQuoteQty="0.05",
    )

    result = await adapter.modify_order(
        OrderModification(client_order_id="test-order-1", price=Decimal("101"))
    )

    assert result.status is OrderStatus.PARTIALLY_FILLED
    assert result.filled_quantity == Decimal("0.0005")
    assert result.average_fill_price == Decimal("100")
    binance_client_mock.futures_get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )


async def test_timeout_preserves_reference_for_ambiguity_lookup(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = BinanceProduct.SPOT
    await adapter.connect()
    adapter._exchange_info_cache[product] = [_metadata(product)]
    monkeypatch.setattr(
        adapter,
        "fetch_instrument_spec",
        AsyncMock(
            return_value=InstrumentSpec(
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("10"),
                min_notional=Decimal("5"),
                price_precision=2,
                qty_precision=3,
            )
        ),
    )
    binance_client_mock.create_order.side_effect = TimeoutError
    order = _order(product, OrderType.LIMIT, price=Decimal("100"))

    with pytest.raises(TimeoutError):
        await adapter.place_order(order)

    assert "test-order-1" in adapter._order_refs
    binance_client_mock.create_order.side_effect = None
    binance_client_mock.get_order.return_value = _order_response()
    result = await adapter.get_order_by_client_id("test-order-1")
    assert result is not None
    assert result.client_order_id == "test-order-1"


async def test_confirmed_absence_returns_none_but_unknown_ids_do_not_probe(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
) -> None:
    await adapter.connect()
    with pytest.raises(PlatformError, match="Cannot resolve Binance product and symbol"):
        await adapter.get_order_by_client_id("unknown")
    binance_client_mock.get_order.assert_not_awaited()

    adapter._order_refs["missing"] = OrderReference(
        product=BinanceProduct.SPOT,
        symbol="BTCUSDT",
        conditional=False,
        order_type=OrderType.LIMIT,
    )
    binance_client_mock.get_order.side_effect = OrderNotFoundError("missing")
    binance_client_mock.get_all_orders.return_value = []
    assert await adapter.get_order_by_client_id("missing") is None


async def test_order_operations_recover_routing_from_state_store_cache_miss(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus,
) -> None:
    instrument = _instrument(BinanceProduct.SPOT)
    timestamp = datetime.fromtimestamp(_TIMESTAMP / 1000, tz=UTC)
    record = OrderRecord(
        instrument=instrument,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.GTC,
        client_order_id="core-order",
        price=Decimal("100"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="987",
        status=OrderStatus.OPEN,
        filled_quantity=Decimal("0"),
        average_fill_price=None,
        correlation_id="core-order",
        created_at=timestamp,
        updated_at=timestamp,
    )
    store = AsyncMock()
    store.get_order.return_value = record
    adapter._state_store = store
    adapter._client = binance_client_mock
    adapter._connected = True
    adapter._exchange_info_cache[BinanceProduct.SPOT] = [_metadata(BinanceProduct.SPOT)]
    binance_client_mock.get_order.return_value = _order_response()
    binance_client_mock.cancel_order.return_value = _order_response(status="CANCELED")
    cancelled_events: list[Event] = []
    event_bus.subscribe(OrderCancelledEvent, cancelled_events.append)

    result = await adapter.get_order_by_client_id("core-order")

    assert result is not None
    assert result.status is OrderStatus.OPEN
    binance_client_mock.get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )
    assert adapter._order_refs["core-order"].symbol == "BTCUSDT"

    adapter._forget_order_reference("core-order")
    cancelled = await adapter.cancel_order("core-order")

    assert cancelled.status is OrderStatus.CANCELLED
    binance_client_mock.cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        orderId="987",
    )
    assert store.get_order.await_count == 2
    assert cancelled_events == []
    assert "core-order" not in adapter._order_refs

"""Unit tests for Bybit order operations — translation (orders.py) + adapter (Section 17.10)."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError, InvalidRequestError

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.bybit.orders import (
    build_amend_payload,
    build_cancel_payload,
    build_place_order_payload,
    map_order_status,
    parse_order_result,
)
from unified_trading_execution.errors import (
    InsufficientBalanceError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    OrderModification,
    TpSlAttachment,
    UnifiedOrder,
)


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def _futures_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="USDT",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _inverse_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="BTC",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _order(
    instrument: Instrument,
    *,
    order_type: OrderType,
    side: OrderSide = OrderSide.BUY,
    quantity: Decimal = Decimal("0.001"),
    time_in_force: TimeInForce = TimeInForce.GTC,
    client_order_id: str | None = "c1",
    **kwargs: Any,
) -> UnifiedOrder:
    return UnifiedOrder(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=quantity,
        time_in_force=time_in_force,
        client_order_id=client_order_id,
        **kwargs,
    )


def _order_entry(
    client_order_id: str = "c1",
    order_id: str = "o1",
    status: str = "New",
    *,
    cum_exec_qty: str = "0",
    avg_price: str = "0",
    symbol: str = "BTCUSDT",
) -> dict[str, str]:
    return {
        "orderId": order_id,
        "orderLinkId": client_order_id,
        "symbol": symbol,
        "orderStatus": status,
        "cumExecQty": cum_exec_qty,
        "avgPrice": avg_price,
        "createdTime": "1684738540559",
        "updatedTime": "1684738540561",
    }


class TestMapOrderStatus:
    def test_maps_open_statuses(self) -> None:
        assert map_order_status("New") is OrderStatus.OPEN
        assert map_order_status("Untriggered") is OrderStatus.OPEN
        assert map_order_status("Triggered") is OrderStatus.OPEN

    def test_maps_partially_filled_and_filled(self) -> None:
        assert map_order_status("PartiallyFilled") is OrderStatus.PARTIALLY_FILLED
        assert map_order_status("Filled") is OrderStatus.FILLED

    def test_maps_cancelled_spellings(self) -> None:
        assert map_order_status("Cancelled") is OrderStatus.CANCELLED
        assert map_order_status("Canceled") is OrderStatus.CANCELLED
        assert map_order_status("Deactivated") is OrderStatus.CANCELLED

    def test_maps_rejected(self) -> None:
        assert map_order_status("Rejected") is OrderStatus.REJECTED

    def test_unknown_status_raises(self) -> None:
        with pytest.raises(PlatformError):
            map_order_status("SomeNewStatus")


class TestBuildPlaceOrderPayload:
    def test_spot_limit(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
        )
        payload = build_place_order_payload(
            order,
            category="spot",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload == {
            "category": "spot",
            "symbol": "BTCUSDT",
            "side": "Buy",
            "orderType": "Limit",
            "qty": "0.001",
            "orderLinkId": "c1",
            "price": "100",
            "timeInForce": "GTC",
        }

    def test_futures_limit_with_reduce_only(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.LIMIT,
            side=OrderSide.SELL,
            price=Decimal("100"),
            reduce_only=True,
        )
        payload = build_place_order_payload(
            order,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["side"] == "Sell"
        assert payload["reduceOnly"] == "true"
        assert payload["timeInForce"] == "GTC"

    def test_market_omits_time_in_force_and_price(self) -> None:
        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        payload = build_place_order_payload(
            order,
            category="spot",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["orderType"] == "Market"
        assert payload["marketUnit"] == "baseCoin"
        assert "price" not in payload
        assert "timeInForce" not in payload

    def test_market_unit_base_coin_only_for_spot_market(self) -> None:
        futures = _order(_futures_instrument(), order_type=OrderType.MARKET)
        futures_payload = build_place_order_payload(
            futures,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert "marketUnit" not in futures_payload

        spot_limit = _order(_spot_instrument(), order_type=OrderType.LIMIT, price=Decimal("100"))
        spot_limit_payload = build_place_order_payload(
            spot_limit,
            category="spot",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert "marketUnit" not in spot_limit_payload

    def test_stop_buy_sets_trigger_and_direction_rise(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.STOP,
            stop_price=Decimal("90"),
        )
        payload = build_place_order_payload(
            order,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["orderType"] == "Market"
        assert payload["triggerPrice"] == "90"
        assert payload["triggerDirection"] == 1
        assert "timeInForce" not in payload

    def test_stop_limit_sell_sets_direction_fall(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.STOP_LIMIT,
            side=OrderSide.SELL,
            price=Decimal("85"),
            stop_price=Decimal("90"),
        )
        payload = build_place_order_payload(
            order,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["orderType"] == "Limit"
        assert payload["price"] == "85"
        assert payload["triggerPrice"] == "90"
        assert payload["triggerDirection"] == 2
        assert payload["timeInForce"] == "GTC"

    def test_inverse_stop(self) -> None:
        order = _order(
            _inverse_instrument(),
            order_type=OrderType.STOP,
            stop_price=Decimal("90"),
        )
        payload = build_place_order_payload(
            order,
            category="inverse",
            symbol="BTCUSD",
            client_order_id="c1",
        )
        assert payload["category"] == "inverse"
        assert payload["symbol"] == "BTCUSD"
        assert payload["orderType"] == "Market"
        assert payload["triggerPrice"] == "90"
        assert payload["triggerDirection"] == 1
        assert "timeInForce" not in payload

    def test_day_time_in_force_raises(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order, category="spot", symbol="BTCUSDT", client_order_id="c1"
            )

    def test_stop_on_spot_raises(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.STOP,
            stop_price=Decimal("90"),
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order, category="spot", symbol="BTCUSDT", client_order_id="c1"
            )

    def test_spot_reduce_only_raises(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            reduce_only=True,
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order, category="spot", symbol="BTCUSDT", client_order_id="c1"
            )

    def test_reduce_only_with_tp_sl_raises(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            reduce_only=True,
            take_profit=TpSlAttachment(trigger_price=Decimal("120")),
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order,
                category="linear",
                symbol="BTCUSDT",
                client_order_id="c1",
            )

    def test_spot_tp_sl_on_market_raises(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.MARKET,
            stop_loss=TpSlAttachment(trigger_price=Decimal("80")),
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order, category="spot", symbol="BTCUSDT", client_order_id="c1"
            )

    def test_spot_limit_tp_sl(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            take_profit=TpSlAttachment(trigger_price=Decimal("120")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("80")),
        )
        payload = build_place_order_payload(
            order,
            category="spot",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["takeProfit"] == "120"
        assert payload["tpOrderType"] == "Market"
        assert payload["stopLoss"] == "80"
        assert payload["slOrderType"] == "Market"
        assert "tpslMode" not in payload

    def test_spot_limit_tp_sl_with_limit_prices(self) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            take_profit=TpSlAttachment(trigger_price=Decimal("120"), limit_price=Decimal("119")),
        )
        payload = build_place_order_payload(
            order,
            category="spot",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["tpOrderType"] == "Limit"
        assert payload["tpLimitPrice"] == "119"

    def test_futures_tp_sl_market_mode(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            take_profit=TpSlAttachment(trigger_price=Decimal("120")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("80")),
        )
        payload = build_place_order_payload(
            order,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["takeProfit"] == "120"
        assert payload["stopLoss"] == "80"
        assert payload["tpslMode"] == "Full"
        assert payload["tpOrderType"] == "Market"
        assert payload["slOrderType"] == "Market"

    def test_futures_tp_sl_limit_mode(self) -> None:
        order = _order(
            _futures_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            take_profit=TpSlAttachment(trigger_price=Decimal("120"), limit_price=Decimal("119")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("80")),
        )
        payload = build_place_order_payload(
            order,
            category="linear",
            symbol="BTCUSDT",
            client_order_id="c1",
        )
        assert payload["tpslMode"] == "Partial"
        assert payload["tpOrderType"] == "Limit"
        assert payload["tpLimitPrice"] == "119"
        assert payload["slOrderType"] == "Market"

    def test_inverse_tp_sl_partial(self) -> None:
        order = _order(
            _inverse_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            take_profit=TpSlAttachment(trigger_price=Decimal("120"), limit_price=Decimal("119")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("80")),
        )
        payload = build_place_order_payload(
            order,
            category="inverse",
            symbol="BTCUSD",
            client_order_id="c1",
        )
        assert payload["category"] == "inverse"
        assert payload["tpslMode"] == "Partial"
        assert payload["tpOrderType"] == "Limit"
        assert payload["tpLimitPrice"] == "119"
        assert payload["slOrderType"] == "Market"

    def test_unsupported_order_type_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "unified_trading_execution.bybit.orders._BYBIT_ORDER_TYPE",
            {},
        )
        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        with pytest.raises(UnsupportedOrderTypeError):
            build_place_order_payload(
                order, category="spot", symbol="BTCUSDT", client_order_id="c1"
            )


class TestBuildAmendPayload:
    def test_price_and_quantity(self) -> None:
        modification = OrderModification(
            client_order_id="c1",
            price=Decimal("101"),
            quantity=Decimal("0.002"),
        )
        payload = build_amend_payload(
            modification,
            category="linear",
            symbol="BTCUSDT",
        )
        assert payload == {
            "category": "linear",
            "symbol": "BTCUSDT",
            "orderLinkId": "c1",
            "price": "101",
            "qty": "0.002",
        }

    def test_stop_price(self) -> None:
        modification = OrderModification(client_order_id="c1", stop_price=Decimal("95"))
        payload = build_amend_payload(
            modification,
            category="linear",
            symbol="BTCUSDT",
        )
        assert payload["triggerPrice"] == "95"

    def test_tp_sl_derivatives(self) -> None:
        modification = OrderModification(
            client_order_id="c1",
            take_profit=TpSlAttachment(trigger_price=Decimal("130")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("70"), limit_price=Decimal("71")),
        )
        payload = build_amend_payload(
            modification,
            category="linear",
            symbol="BTCUSDT",
        )
        assert payload["takeProfit"] == "130"
        assert payload["stopLoss"] == "70"
        assert payload["tpslMode"] == "Partial"
        assert payload["slLimitPrice"] == "71"

    def test_tp_sl_spot_raises(self) -> None:
        modification = OrderModification(
            client_order_id="c1",
            take_profit=TpSlAttachment(trigger_price=Decimal("130")),
        )
        with pytest.raises(UnsupportedOrderTypeError):
            build_amend_payload(modification, category="spot", symbol="BTCUSDT")


class TestBuildCancelPayload:
    def test_cancel_payload(self) -> None:
        payload = build_cancel_payload("c1", category="linear", symbol="BTCUSDT")
        assert payload == {
            "category": "linear",
            "symbol": "BTCUSDT",
            "orderLinkId": "c1",
        }


class TestParseOrderResult:
    def test_parses_filled_order(self) -> None:
        entry = _order_entry(
            status="Filled",
            cum_exec_qty="0.001",
            avg_price="100.5",
        )
        result = parse_order_result(entry, "c1")
        assert result.client_order_id == "c1"
        assert result.platform_order_id == "o1"
        assert result.status is OrderStatus.FILLED
        assert result.filled_quantity == Decimal("0.001")
        assert result.average_fill_price == Decimal("100.5")
        assert result.created_at.tzinfo is not None
        assert result.updated_at.tzinfo is not None

    def test_avg_price_zero_or_empty_is_none(self) -> None:
        for avg_price in ("0", ""):
            result = parse_order_result(_order_entry(avg_price=avg_price), "c1")
            assert result.average_fill_price is None
            assert result.filled_quantity == Decimal("0")

    def test_missing_order_id_raises(self) -> None:
        entry = _order_entry()
        entry.pop("orderId")
        with pytest.raises(PlatformError):
            parse_order_result(entry, "c1")

    def test_missing_status_raises(self) -> None:
        entry = _order_entry()
        entry.pop("orderStatus")
        with pytest.raises(PlatformError):
            parse_order_result(entry, "c1")

    def test_missing_timestamps_raise(self) -> None:
        entry = _order_entry()
        entry.pop("createdTime")
        with pytest.raises(PlatformError):
            parse_order_result(entry, "c1")


class TestPlaceOrder:
    async def test_places_and_requeries(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry()], "category": "spot"}},
            None,
            {},
        )

        order = _order(_spot_instrument(), order_type=OrderType.LIMIT, price=Decimal("100"))
        result = await adapter.place_order(order)

        assert result.platform_order_id == "o1"
        assert result.status is OrderStatus.OPEN
        mock_pybit_http.place_order.assert_called_once_with(
            category="spot",
            symbol="BTCUSDT",
            side="Buy",
            orderType="Limit",
            qty="0.001",
            orderLinkId="c1",
            price="100",
            timeInForce="GTC",
        )
        assert adapter._order_refs["c1"] == ("spot", "BTCUSDT")

    async def test_places_inverse_order(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_positions.return_value = (
            {"retCode": 0, "result": {"list": []}},
            None,
            {},
        )
        mock_pybit_http.get_open_orders.return_value = (
            {
                "retCode": 0,
                "result": {"list": [_order_entry(symbol="BTCUSD")], "category": "inverse"},
            },
            None,
            {},
        )

        order = _order(_inverse_instrument(), order_type=OrderType.MARKET)
        result = await adapter.place_order(order)

        assert result.platform_order_id == "o1"
        assert result.status is OrderStatus.OPEN
        mock_pybit_http.place_order.assert_called_once_with(
            category="inverse",
            symbol="BTCUSD",
            side="Buy",
            orderType="Market",
            qty="0.001",
            orderLinkId="c1",
            positionIdx=0,
        )
        mock_pybit_http.get_open_orders.assert_called_with(
            category="inverse",
            orderLinkId="c1",
            symbol="BTCUSD",
        )
        assert adapter._order_refs["c1"] == ("inverse", "BTCUSD")

    async def test_generates_client_order_id_when_none(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "generated"}},
            None,
            {},
        )

        def open_orders_side_effect(
            **kwargs: Any,
        ) -> tuple[dict[str, Any], None, dict[str, Any]]:
            return (
                {
                    "retCode": 0,
                    "result": {
                        "list": [_order_entry(client_order_id=kwargs["orderLinkId"])],
                        "category": "spot",
                    },
                },
                None,
                {},
            )

        mock_pybit_http.get_open_orders.side_effect = open_orders_side_effect

        order = _order(
            _spot_instrument(),
            order_type=OrderType.MARKET,
            client_order_id=None,
        )
        result = await adapter.place_order(order)

        called = mock_pybit_http.place_order.call_args.kwargs
        assert isinstance(called["orderLinkId"], str) and called["orderLinkId"]
        assert result.client_order_id == called["orderLinkId"]

    async def test_translates_insufficient_balance(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.side_effect = InvalidRequestError(
            request="POST /v5/order/create",
            message="Wallet balance is insufficient",
            status_code=110006,
            time="12:00:00",
            resp_headers=None,
        )

        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        with pytest.raises(InsufficientBalanceError):
            await adapter.place_order(order)

    async def test_place_rejection_invalidates_cached_spec(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        """A REST rejection must drop the cached spec (Side effect, never swallow)."""
        instrument = _spot_instrument()
        adapter._instrument_specs[instrument] = (
            InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("10"),
                min_notional=Decimal("5"),
                price_precision=3,
                qty_precision=3,
            ),
            time.monotonic(),
        )
        mock_pybit_http.place_order.side_effect = InvalidRequestError(
            request="POST /v5/order/create",
            message="Invalid quantity",
            status_code=10002,
            time="12:00:00",
            resp_headers=None,
        )

        order = _order(instrument, order_type=OrderType.LIMIT, price=Decimal("100"))
        with pytest.raises(PlatformError):
            await adapter.place_order(order)
        assert instrument not in adapter._instrument_specs

    async def test_place_success_keeps_cached_spec(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        """A successful placement must NOT invalidate a cached spec."""
        instrument = _spot_instrument()
        spec = InstrumentSpec(
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("10"),
            min_notional=Decimal("5"),
            price_precision=3,
            qty_precision=3,
        )
        adapter._instrument_specs[instrument] = (spec, time.monotonic())
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry()], "category": "spot"}},
            None,
            {},
        )

        order = _order(instrument, order_type=OrderType.MARKET)
        await adapter.place_order(order)
        cached = adapter._instrument_specs.get(instrument)
        assert cached is not None and cached[0] is spec

    async def test_translates_rate_limit(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.side_effect = InvalidRequestError(
            request="POST /v5/order/create",
            message="Too many new orders",
            status_code=170005,
            time="12:00:00",
            resp_headers=None,
        )

        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        with pytest.raises(RateLimitError):
            await adapter.place_order(order)

    async def test_translates_http_error(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.place_order.side_effect = FailedRequestError(
            request="POST /v5/order/create",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        with pytest.raises(PlatformConnectionError):
            await adapter.place_order(order)

    async def test_unsupported_order_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        order = _order(
            _spot_instrument(),
            order_type=OrderType.LIMIT,
            price=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        )
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.place_order(order)
        mock_pybit_http.place_order.assert_not_called()

    async def test_updates_rate_limits(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        headers = {
            "X-Bapi-Limit": "100",
            "X-Bapi-Remaining": "50",
            "X-Bapi-Reset-Timestamp": "1684738540561",
        }
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            {},
            headers,
        )
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry()], "category": "spot"}},
            {},
            headers,
        )

        order = _order(_spot_instrument(), order_type=OrderType.MARKET)
        await adapter.place_order(order)

        limits = await adapter.get_rate_limits()
        assert limits.requests_per_interval == 100
        assert limits.remaining == 50


class TestModifyOrder:
    async def test_modifies_cached_order(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter._order_refs["c1"] = ("linear", "BTCUSDT")
        mock_pybit_http.amend_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry()], "category": "linear"}},
            None,
            {},
        )

        modification = OrderModification(client_order_id="c1", price=Decimal("101"))
        result = await adapter.modify_order(modification)

        assert result.platform_order_id == "o1"
        mock_pybit_http.amend_order.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            orderLinkId="c1",
            price="101",
        )
        mock_pybit_http.get_open_orders.assert_called_with(
            category="linear",
            orderLinkId="c1",
            symbol="BTCUSDT",
        )

    async def test_modify_unknown_order_raises(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )
        mock_pybit_http.get_order_history.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )

        modification = OrderModification(client_order_id="missing", price=Decimal("101"))
        with pytest.raises(OrderNotFoundError):
            await adapter.modify_order(modification)
        mock_pybit_http.amend_order.assert_not_called()

    async def test_modify_rejection_invalidates_cached_spec(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        """A REST rejection on amend must invalidate the resolved instrument's spec."""
        instrument = _futures_instrument()
        adapter._instruments[("linear", "BTCUSDT")] = instrument
        adapter._order_refs["c1"] = ("linear", "BTCUSDT")
        adapter._instrument_specs[instrument] = (
            InstrumentSpec(
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("10"),
                min_notional=Decimal("5"),
                price_precision=3,
                qty_precision=3,
            ),
            time.monotonic(),
        )
        mock_pybit_http.amend_order.side_effect = InvalidRequestError(
            request="POST /v5/order/amend",
            message="Invalid price",
            status_code=10005,
            time="12:00:00",
            resp_headers=None,
        )
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry()], "category": "linear"}},
            None,
            {},
        )

        modification = OrderModification(client_order_id="c1", price=Decimal("101"))
        with pytest.raises(PlatformError):
            await adapter.modify_order(modification)
        assert instrument not in adapter._instrument_specs


class TestCancelOrder:
    async def test_cancels_cached_order(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter._order_refs["c1"] = ("spot", "BTCUSDT")
        mock_pybit_http.cancel_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_open_orders.return_value = (
            {
                "retCode": 0,
                "result": {"list": [_order_entry(status="Cancelled")], "category": "spot"},
            },
            None,
            {},
        )

        result = await adapter.cancel_order("c1")

        assert result.status is OrderStatus.CANCELLED
        mock_pybit_http.cancel_order.assert_called_once_with(
            category="spot",
            symbol="BTCUSDT",
            orderLinkId="c1",
        )

    async def test_cancel_unknown_order_raises(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )
        mock_pybit_http.get_order_history.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )

        with pytest.raises(OrderNotFoundError):
            await adapter.cancel_order("missing")
        mock_pybit_http.cancel_order.assert_not_called()

    async def test_finds_category_by_scan(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.cancel_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_order_history.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )

        def open_orders_side_effect(
            **kwargs: Any,
        ) -> tuple[dict[str, Any], None, dict[str, Any]]:
            category = kwargs["category"]
            if category == "spot":
                return ({"retCode": 0, "result": {"list": [], "category": "spot"}}, {}, {})
            return (
                {"retCode": 0, "result": {"list": [_order_entry()], "category": category}},
                {},
                {},
            )

        mock_pybit_http.get_open_orders.side_effect = open_orders_side_effect

        result = await adapter.cancel_order("c1")

        assert result.platform_order_id == "o1"
        assert adapter._order_refs["c1"] == ("linear", "BTCUSDT")
        calls = [c.kwargs["category"] for c in mock_pybit_http.get_open_orders.call_args_list]
        assert calls[0] == "spot"
        assert calls[1] == "linear"


class TestGetOrderByClientId:
    async def test_returns_none_when_not_found(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )
        mock_pybit_http.get_order_history.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )

        result = await adapter.get_order_by_client_id("missing")
        assert result is None

    async def test_returns_order_via_scan(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [_order_entry(status="Filled")], "category": "spot"}},
            None,
            {},
        )

        result = await adapter.get_order_by_client_id("c1")

        assert result is not None
        assert result.status is OrderStatus.FILLED
        assert adapter._order_refs["c1"] == ("spot", "BTCUSDT")

    async def test_finds_order_in_inverse_via_scan(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        def open_orders_side_effect(
            **kwargs: Any,
        ) -> tuple[dict[str, Any], None, dict[str, Any]]:
            category = kwargs["category"]
            if category in ("spot", "linear"):
                return ({"retCode": 0, "result": {"list": [], "category": category}}, None, {})
            return (
                {"retCode": 0, "result": {"list": [_order_entry()], "category": category}},
                {},
                {},
            )

        mock_pybit_http.get_open_orders.side_effect = open_orders_side_effect
        mock_pybit_http.get_order_history.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )

        result = await adapter.get_order_by_client_id("c1")

        assert result is not None
        assert result.status is OrderStatus.OPEN
        assert adapter._order_refs["c1"] == ("inverse", "BTCUSDT")
        calls = [c.kwargs["category"] for c in mock_pybit_http.get_open_orders.call_args_list]
        assert calls == ["spot", "linear", "inverse"]

    async def test_falls_back_to_history(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter._order_refs["c1"] = ("spot", "BTCUSDT")
        mock_pybit_http.get_open_orders.return_value = (
            {"retCode": 0, "result": {"list": [], "category": "spot"}},
            None,
            {},
        )
        mock_pybit_http.get_order_history.return_value = (
            {
                "retCode": 0,
                "result": {"list": [_order_entry(status="Cancelled")], "category": "spot"},
            },
            None,
            {},
        )

        result = await adapter.get_order_by_client_id("c1")

        assert result is not None
        assert result.status is OrderStatus.CANCELLED

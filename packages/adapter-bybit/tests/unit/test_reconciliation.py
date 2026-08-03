"""Unit tests for Bybit reconciliation REST fetches (Section 6.1 / 6.3)."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest
from pybit.exceptions import FailedRequestError, InvalidRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.symbols import from_bybit_symbol
from unified_trading_execution.errors import InvalidSymbolError, PlatformConnectionError
from unified_trading_execution.types.enums import OrderStatus
from unified_trading_execution.types.instrument import Instrument

_EMPTY: tuple[dict[str, Any], None, dict[str, str]] = ({"result": {"list": []}}, None, {})


def _register(
    adapter: BybitAdapter,
    bybit_symbol: str,
    base: str,
    quote: str,
    category: str,
) -> Instrument:
    """Register a canonical ``Instrument`` in the adapter's reverse registry."""
    instrument = from_bybit_symbol(bybit_symbol, base, quote, category)
    adapter._instruments[(category, bybit_symbol)] = instrument
    return instrument


_BTC = {
    "symbol": "BTCUSDT",
    "category": "linear",
    "side": "Buy",
    "size": "1.5",
    "entryPrice": "90000",
    "updatedTime": "1700000000000",
}
_ETH = {
    "symbol": "ETHUSDT",
    "category": "linear",
    "side": "Sell",
    "size": "2",
    "entryPrice": "3000",
    "updatedTime": "1700000000000",
}
_SOL_FLAT = {
    "symbol": "SOLUSDT",
    "category": "linear",
    "side": "",
    "size": "0",
    "entryPrice": "0",
    "updatedTime": "1700000000000",
}


def _open_order(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "symbol": "BTCUSDT",
        "category": "linear",
        "orderId": "order-1",
        "orderLinkId": "client-1",
        "side": "Buy",
        "orderType": "Limit",
        "stopOrderType": "",
        "orderStatus": "New",
        "price": "100000",
        "qty": "0.5",
        "timeInForce": "GTC",
        "cumExecQty": "0",
        "avgPrice": "",
        "createdTime": "1700000000000",
        "updatedTime": "1700000000000",
    }
    entry.update(overrides)
    return entry


def _execution(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "symbol": "BTCUSDT",
        "category": "linear",
        "execId": "exec-1",
        "orderId": "order-1",
        "orderLinkId": "client-1",
        "execType": "Trade",
        "execPrice": "100000",
        "execQty": "0.2",
        "execTime": "1700000000000",
        "execFee": "0.5",
        "feeCurrency": "USDT",
    }
    entry.update(overrides)
    return entry


class TestFetchPositions:
    async def test_long_short_flat_keyed_by_instrument(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        _register(adapter, "ETHUSDT", "ETH", "USDT", "linear")
        _register(adapter, "SOLUSDT", "SOL", "USDT", "linear")
        mock_pybit_http.get_positions.side_effect = [
            _EMPTY,
            ({"result": {"list": [_BTC, _ETH, _SOL_FLAT]}}, None, {}),
            _EMPTY,
        ]

        result = await adapter.fetch_positions()

        assert len(result) == 3
        btc = result[adapter._instruments[("linear", "BTCUSDT")]]
        eth = result[adapter._instruments[("linear", "ETHUSDT")]]
        sol = result[adapter._instruments[("linear", "SOLUSDT")]]
        assert btc.quantity == Decimal("1.5")
        assert eth.quantity == Decimal("-2")
        assert sol.quantity == Decimal("0")

    async def test_paginates_across_cursor(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        _register(adapter, "ETHUSDT", "ETH", "USDT", "linear")
        page_one = {"result": {"list": [_BTC], "nextPageCursor": "abc"}, "category": "linear"}
        page_two = {"result": {"list": [_ETH], "nextPageCursor": ""}, "category": "linear"}
        mock_pybit_http.get_positions.side_effect = [
            _EMPTY,
            (page_one, None, {}),
            (page_two, None, {}),
            _EMPTY,
        ]

        result = await adapter.fetch_positions()

        assert len(result) == 2
        assert mock_pybit_http.get_positions.call_count == 4

    async def test_unknown_symbol_skipped_with_log(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        unknown = {
            "symbol": "ZZZUSDT",
            "side": "Buy",
            "size": "1",
            "entryPrice": "1",
            "updatedTime": "1700000000000",
        }
        mock_pybit_http.get_positions.side_effect = [
            _EMPTY,
            ({"result": {"list": [_BTC, unknown]}, "category": "linear"}, None, {}),
            _EMPTY,
        ]
        with caplog.at_level(logging.ERROR):
            result = await adapter.fetch_positions()

        assert len(result) == 1
        assert any("position entry" in record.message for record in caplog.records)


class TestFetchBalances:
    async def test_single_member_keyed_by_currency(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        mock_pybit_http.get_wallet_balance.return_value = (
            {
                "result": {
                    "list": [
                        {
                            "coin": [
                                {
                                    "coin": "USDT",
                                    "walletBalance": "100",
                                    "totalOrderIM": "10",
                                    "totalPositionIM": "20",
                                    "locked": "5",
                                    "bonus": "0",
                                },
                                {
                                    "coin": "BTC",
                                    "walletBalance": "0.5",
                                    "totalOrderIM": "0",
                                    "totalPositionIM": "0",
                                    "locked": "0",
                                    "bonus": "0",
                                },
                            ]
                        }
                    ]
                }
            },
            None,
            {},
        )

        result = await adapter.fetch_balances()

        assert set(result) == {"USDT", "BTC"}
        usdt = result["USDT"]
        assert usdt.free == Decimal("65")
        assert usdt.used == Decimal("35")
        assert usdt.total == Decimal("100")
        assert usdt.free + usdt.used == usdt.total
        mock_pybit_http.get_wallet_balance.assert_called_once_with(accountType="UNIFIED")

    async def test_empty_list_returns_empty(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        mock_pybit_http.get_wallet_balance.return_value = (
            {"result": {"list": []}},
            None,
            {},
        )

        assert await adapter.fetch_balances() == {}


class TestFetchOpenOrders:
    async def test_keyed_by_order_link_id(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        entry = _open_order()
        mock_pybit_http.get_open_orders.side_effect = [
            _EMPTY,
            ({"result": {"list": [entry], "nextPageCursor": ""}}, None, {}),
            _EMPTY,
        ]

        result = await adapter.fetch_open_orders()

        assert list(result) == ["client-1"]
        assert result["client-1"].client_order_id == "client-1"
        assert result["client-1"].status == OrderStatus.OPEN

    async def test_missing_order_link_id_falls_back_to_platform_id(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        entry = _open_order(orderLinkId="", orderId="order-7")
        mock_pybit_http.get_open_orders.side_effect = [
            _EMPTY,
            ({"result": {"list": [entry], "nextPageCursor": ""}}, None, {}),
            _EMPTY,
        ]

        result = await adapter.fetch_open_orders()

        assert list(result) == ["order-7"]
        assert result["order-7"].platform_order_id == "order-7"


class TestFetchFills:
    async def test_filters_trade_and_groups_by_client_order_id(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        _register(adapter, "BTCUSDT", "BTC", "USDT", "linear")
        mock_pybit_http.get_executions.side_effect = [
            _EMPTY,
            (
                {
                    "result": {
                        "list": [
                            _execution(execQty="0.2"),
                            _execution(execId="exec-2", execQty="0.3"),
                            _execution(
                                execId="exec-3",
                                orderLinkId="client-9",
                                execType="Funding",
                            ),
                        ],
                        "nextPageCursor": "",
                    }
                },
                None,
                {},
            ),
            _EMPTY,
        ]

        result = await adapter.fetch_fills()

        assert set(result) == {"client-1"}
        fills = result["client-1"]
        assert len(fills) == 2
        assert sum(f.fill_quantity for f in fills) == Decimal("0.5")


class TestErrorTranslation:
    async def test_failed_request_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        mock_pybit_http.get_positions.side_effect = FailedRequestError(
            request="GET /v5/position/list",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_positions()

    async def test_invalid_request_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        mock_pybit_http.get_open_orders.side_effect = InvalidRequestError(
            request="GET /v5/order/realtime",
            message="The requested symbol is invalid",
            status_code=10029,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_open_orders()


class TestRateLimits:
    async def test_reconciliation_call_updates_rate_limits(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: Any,
    ) -> None:
        mock_pybit_http.get_positions.return_value = (
            {"result": {"list": []}},
            None,
            {
                "X-Bapi-Limit": "100",
                "X-Bapi-Remaining": "42",
                "X-Bapi-Reset-Timestamp": "1700000000000",
            },
        )

        await adapter.fetch_positions()

        limits = await adapter.get_rate_limits()
        assert limits.remaining == 42
        assert limits.requests_per_interval == 100

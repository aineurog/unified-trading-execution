"""Unit tests for WebSocket event streams — translation, emission, and wiring."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import patch

import pytest

from unified_trading_execution.bybit import streams
from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.symbols import from_bybit_symbol
from unified_trading_execution.bybit.websocket import BybitWebSocket
from unified_trading_execution.errors import PlatformConnectionError, PlatformError
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    Event,
    EventBus,
    FillEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.types.enums import OrderSide, OrderStatus, OrderType, TimeInForce


class _SyncLoop:
    """A stand-in event loop that invokes ``call_soon_threadsafe`` inline."""

    def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
        callback(*args)


_BTCUSDT = from_bybit_symbol("BTCUSDT", "BTC", "USDT", "linear")
_TS = datetime(2024, 1, 1, tzinfo=UTC)


def _base_order_entry(**overrides: Any) -> dict[str, Any]:
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


class TestStreamsTranslation:
    def test_translate_fill(self) -> None:
        fill = streams.translate_fill(
            {
                "symbol": "BTCUSDT",
                "execId": "exec-1",
                "orderLinkId": "client-1",
                "execQty": "0.5",
                "execPrice": "95900.1",
                "execTime": "1746270400353",
                "feeCurrency": "USDT",
                "execFee": "26.37",
            },
            instrument=_BTCUSDT,
            client_order_id="client-1",
        )
        assert fill.client_order_id == "client-1"
        assert fill.platform_fill_id == "exec-1"
        assert fill.instrument == _BTCUSDT
        assert fill.fill_quantity == Decimal("0.5")
        assert fill.fill_price == Decimal("95900.1")
        assert fill.fill_timestamp.tzinfo is not None
        assert fill.fee_currency == "USDT"
        assert fill.fee_amount == Decimal("26.37")
        assert fill.correlation_id == "client-1"

    def test_translate_position_long(self) -> None:
        pos = streams.translate_position(
            {
                "symbol": "BTCUSDT",
                "category": "linear",
                "side": "Buy",
                "size": "0.5",
                "entryPrice": "95900.1",
                "updatedTime": "1700000000000",
            },
            instrument=_BTCUSDT,
        )
        assert pos.quantity == Decimal("0.5")
        assert pos.average_entry_price == Decimal("95900.1")
        assert pos.updated_at.tzinfo is not None

    def test_translate_position_short_is_negative(self) -> None:
        pos = streams.translate_position(
            {
                "symbol": "BTCUSDT",
                "category": "linear",
                "side": "Sell",
                "size": "0.25",
                "entryPrice": "96000",
                "updatedTime": "1700000000000",
            },
            instrument=_BTCUSDT,
        )
        assert pos.quantity == Decimal("-0.25")

    def test_translate_position_flat_is_zero(self) -> None:
        pos = streams.translate_position(
            {
                "symbol": "BTCUSDT",
                "category": "linear",
                "side": "",
                "size": "0",
                "entryPrice": "0",
                "updatedTime": "1700000000000",
            },
            instrument=_BTCUSDT,
        )
        assert pos.quantity == Decimal("0")

    def test_translate_wallet_preserves_invariant(self) -> None:
        member = {
            "accountType": "UNIFIED",
            "coin": [
                {
                    "coin": "BTC",
                    "walletBalance": "1.0",
                    "totalOrderIM": "0.2",
                    "totalPositionIM": "0.3",
                    "locked": "0.1",
                    "bonus": "0.05",
                },
                {"coin": "USDT", "walletBalance": "100", "totalOrderIM": "0", "locked": "0"},
            ],
        }
        balances = streams.translate_wallet_member(member, timestamp=_TS)
        assert len(balances) == 2
        btc = balances[0]
        assert btc.currency == "BTC"
        assert btc.free + btc.used == btc.total
        assert btc.used == Decimal("0.65")
        assert btc.free == Decimal("0.35")
        assert balances[1].free == Decimal("100")
        assert balances[1].used == Decimal("0")

    def test_translate_order_limit(self) -> None:
        order = streams.translate_order_entry(_base_order_entry(), instrument=_BTCUSDT)
        assert order.order_type == OrderType.LIMIT
        assert order.side == OrderSide.BUY
        assert order.time_in_force == TimeInForce.GTC
        assert order.quantity == Decimal("0.5")
        assert order.price == Decimal("100000")
        assert order.platform_order_id == "order-1"
        assert order.client_order_id == "client-1"
        assert order.status == OrderStatus.OPEN

    def test_translate_order_stop_limit_with_tp_sl(self) -> None:
        order = streams.translate_order_entry(
            _base_order_entry(
                stopOrderType="Stop",
                triggerPrice="99999",
                takeProfit="120000",
                tpLimitPrice="121000",
                stopLoss="90000",
                slLimitPrice="89500",
            ),
            instrument=_BTCUSDT,
        )
        assert order.order_type == OrderType.STOP_LIMIT
        assert order.stop_price == Decimal("99999")
        assert order.take_profit is not None
        assert order.take_profit.trigger_price == Decimal("120000")
        assert order.take_profit.limit_price == Decimal("121000")
        assert order.stop_loss is not None
        assert order.stop_loss.trigger_price == Decimal("90000")

    def test_translate_order_unknown_status_fails_loud(self) -> None:
        with pytest.raises(PlatformError):
            streams.translate_order_entry(
                _base_order_entry(orderStatus="TotallyNewStatus"),
                instrument=_BTCUSDT,
            )

    def test_terminal_status(self) -> None:
        assert streams.is_terminal_order_status(OrderStatus.CANCELLED)
        assert streams.is_terminal_order_status(OrderStatus.FILLED) is False

    def test_final_status(self) -> None:
        assert streams.is_final_order_status(OrderStatus.FILLED)
        assert streams.is_final_order_status(OrderStatus.CANCELLED)
        assert streams.is_final_order_status(OrderStatus.OPEN) is False


class TestStreamEmission:
    def _wired_adapter(self, adapter: BybitAdapter) -> BybitAdapter:
        adapter._instruments = {("linear", "BTCUSDT"): _BTCUSDT}
        adapter._loop = cast(asyncio.AbstractEventLoop, _SyncLoop())
        return adapter

    def test_execution_emits_fill(self, adapter: BybitAdapter, event_bus: EventBus) -> None:
        adapter = self._wired_adapter(adapter)
        captured: list[FillEvent] = []
        event_bus.subscribe(FillEvent, captured.append)
        adapter._on_execution_message(
            {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "category": "linear",
                        "execId": "exec-1",
                        "orderLinkId": "client-1",
                        "execQty": "0.5",
                        "execPrice": "95900.1",
                        "execTime": "1706270400353",
                    }
                ]
            }
        )
        assert len(captured) == 1
        assert captured[0].correlation_id == "client-1"
        assert captured[0].fill.instrument == _BTCUSDT

    def test_position_emits_update(self, adapter: BybitAdapter, event_bus: EventBus) -> None:
        adapter = self._wired_adapter(adapter)
        captured: list[PositionUpdateEvent] = []
        event_bus.subscribe(PositionUpdateEvent, captured.append)
        adapter._on_position_message(
            {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "category": "linear",
                        "side": "Buy",
                        "size": "0.5",
                        "entryPrice": "95900.1",
                        "updatedTime": "1700000000000",
                    }
                ]
            }
        )
        assert len(captured) == 1
        assert captured[0].position.quantity == Decimal("0.5")

    def test_wallet_emits_balance_per_coin(
        self, adapter: BybitAdapter, event_bus: EventBus
    ) -> None:
        adapter = self._wired_adapter(adapter)
        captured: list[BalanceUpdateEvent] = []
        event_bus.subscribe(BalanceUpdateEvent, captured.append)
        adapter._on_wallet_message(
            {
                "creationTime": 1700000000000,
                "data": [
                    {
                        "accountType": "UNIFIED",
                        "coin": [
                            {"coin": "BTC", "walletBalance": "1.0"},
                            {"coin": "USDT", "walletBalance": "100"},
                        ],
                    }
                ],
            }
        )
        assert len(captured) == 2
        assert [e.balance.currency for e in captured] == ["BTC", "USDT"]

    def test_order_placed_then_cancelled(self, adapter: BybitAdapter, event_bus: EventBus) -> None:
        adapter = self._wired_adapter(adapter)
        placed: list[OrderPlacedEvent] = []
        cancelled: list[OrderCancelledEvent] = []
        event_bus.subscribe(OrderPlacedEvent, placed.append)
        event_bus.subscribe(OrderCancelledEvent, cancelled.append)

        adapter._on_order_message({"data": [_base_order_entry()]})
        assert len(placed) == 1
        assert cancelled == []

        adapter._on_order_message({"data": [_base_order_entry(orderStatus="Cancelled")]})
        assert len(placed) == 1
        assert len(cancelled) == 1
        assert cancelled[0].client_order_id == "client-1"

    def test_order_terminal_echo_not_replaced(
        self, adapter: BybitAdapter, event_bus: EventBus
    ) -> None:
        adapter = self._wired_adapter(adapter)
        placed: list[OrderPlacedEvent] = []
        cancelled: list[OrderCancelledEvent] = []
        event_bus.subscribe(OrderPlacedEvent, placed.append)
        event_bus.subscribe(OrderCancelledEvent, cancelled.append)

        adapter._on_order_message({"data": [_base_order_entry()]})
        adapter._on_order_message(
            {"data": [_base_order_entry(orderStatus="Filled", orderId="order-1")]}
        )
        adapter._on_order_message(
            {"data": [_base_order_entry(orderStatus="Filled", orderId="order-1")]}
        )
        assert len(placed) == 1
        assert cancelled == []

    def test_filled_order_pruned_from_open_set(
        self, adapter: BybitAdapter, event_bus: EventBus
    ) -> None:
        adapter = self._wired_adapter(adapter)
        placed: list[OrderPlacedEvent] = []
        event_bus.subscribe(OrderPlacedEvent, placed.append)

        adapter._on_order_message({"data": [_base_order_entry()]})
        adapter._on_order_message(
            {"data": [_base_order_entry(orderStatus="Filled", orderId="order-1")]}
        )
        assert len(placed) == 1
        assert "order-1" not in adapter._open_order_ids
        assert "order-1" in adapter._final_order_ids

    def test_final_order_lru_is_bounded(self, adapter: BybitAdapter, event_bus: EventBus) -> None:
        adapter = self._wired_adapter(adapter)
        cap = 10
        with patch("unified_trading_execution.bybit.adapter._MAX_TRACKED_FINAL_ORDER_IDS", cap):
            for i in range(cap * 2):
                adapter._on_order_message(
                    {"data": [_base_order_entry(orderId=f"order-{i}", orderStatus="Filled")]}
                )
        assert len(adapter._final_order_ids) == cap

    def test_unknown_instrument_skipped_not_emitted(
        self, adapter: BybitAdapter, event_bus: EventBus
    ) -> None:
        adapter = self._wired_adapter(adapter)
        adapter._instruments = {}
        captured: list[Event] = []
        event_bus.subscribe(Event, captured.append)
        adapter._on_execution_message({"data": [{"symbol": "UNKNOWN", "category": "linear"}]})
        assert captured == []

    def test_missing_loop_raises(self, adapter: BybitAdapter) -> None:
        adapter._instruments = {("linear", "BTCUSDT"): _BTCUSDT}
        adapter._loop = None
        with pytest.raises(PlatformError):
            adapter._on_execution_message(
                {
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "category": "linear",
                            "execId": "exec-1",
                            "execQty": "0.5",
                            "execPrice": "95900.1",
                            "execTime": "1706270400353",
                        }
                    ]
                }
            )


class TestStreamWiring:
    async def test_connect_subscribes_four_streams(
        self, adapter: BybitAdapter, mock_bybit_websocket: Any
    ) -> None:
        await adapter.connect()
        mock_bybit_websocket.subscribe_order.assert_called_once_with(adapter._on_order_message)
        mock_bybit_websocket.subscribe_execution.assert_called_once_with(
            adapter._on_execution_message
        )
        mock_bybit_websocket.subscribe_position.assert_called_once_with(
            adapter._on_position_message
        )
        mock_bybit_websocket.subscribe_wallet.assert_called_once_with(adapter._on_wallet_message)

    async def test_registry_populated(self, adapter: BybitAdapter, mock_pybit_http: Any) -> None:
        mock_pybit_http.get_instruments_info.return_value = (
            {
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "baseCoin": "BTC",
                            "quoteCoin": "USDT",
                            "contractType": "Perpetual",
                            "status": "Trading",
                        }
                    ]
                }
            },
            None,
            {},
        )
        await adapter._refresh_instrument_registry()
        assert ("linear", "BTCUSDT") in adapter._instruments


@patch("unified_trading_execution.bybit.websocket.WebSocket")
class TestWebSocketSubscribe:
    def test_subscribe_requires_connect(self, mock_ws_cls: Any, bybit_config: BybitConfig) -> None:
        socket = BybitWebSocket(bybit_config)
        with pytest.raises(PlatformConnectionError):
            socket.subscribe_order(lambda message: None)

    def test_delegates_to_pybit_stream(self, mock_ws_cls: Any, bybit_config: BybitConfig) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()

        def callback(message: Any) -> None:
            del message

        socket.subscribe_order(callback)
        socket.subscribe_execution(callback)
        socket.subscribe_position(callback)
        socket.subscribe_wallet(callback)
        mock_ws_cls.return_value.order_stream.assert_called_once_with(callback)
        mock_ws_cls.return_value.execution_stream.assert_called_once_with(callback)
        mock_ws_cls.return_value.position_stream.assert_called_once_with(callback)
        mock_ws_cls.return_value.wallet_stream.assert_called_once_with(callback)

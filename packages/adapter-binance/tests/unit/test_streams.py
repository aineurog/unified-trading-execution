"""Tests for Binance account-stream translation and event routing."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.streams import (
    balance_currency,
    translate_fill_entry,
    translate_futures_balance,
    translate_order_entry,
    translate_position,
    translate_spot_balance,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    FillEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.types.enums import AssetClass, FillEntry, OrderStatus
from unified_trading_execution.types.instrument import Instrument

_EVENT_TIME = 1_684_738_540_559
_TIMESTAMP = datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)


def _spot_instrument() -> Instrument:
    return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)


def _usd_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        currency="USDT",
        asset_class=AssetClass.FUTURES,
        multiplier=1,
        platform_symbol="BTCUSDT",
    )


def _coin_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        currency="BTC",
        asset_class=AssetClass.FUTURES,
        multiplier=100,
        platform_symbol="BTCUSD_PERP",
    )


def test_balance_translators_keep_wallets_product_qualified() -> None:
    spot = translate_spot_balance(
        {"a": "USDT", "f": "12.5", "l": "2.5"},
        timestamp=_TIMESTAMP,
    )
    usd_m = translate_futures_balance(
        {
            "asset": "USDT",
            "balance": "10",
            "availableBalance": "12",
        },
        product=BinanceProduct.USD_M,
        timestamp=_TIMESTAMP,
    )

    assert spot.currency == "SPOT:USDT"
    assert (spot.free, spot.used, spot.total) == (
        Decimal("12.5"),
        Decimal("2.5"),
        Decimal("15.0"),
    )
    assert usd_m.currency == "USD_M:USDT"
    assert (usd_m.free, usd_m.used, usd_m.total) == (
        Decimal("12"),
        Decimal("-2"),
        Decimal("10"),
    )
    assert balance_currency(BinanceProduct.COIN_M, "BTC") == "COIN_M:BTC"


def test_futures_position_event_fields_map_to_signed_position() -> None:
    position = translate_position(
        {
            "s": "BTCUSDT",
            "pa": "-0.25",
            "ep": "62000.5",
            "ps": "SHORT",
        },
        product=BinanceProduct.USD_M,
        instrument=_usd_m_instrument(),
        timestamp=_TIMESTAMP,
    )

    assert position.quantity == Decimal("-0.25")
    assert position.average_entry_price == Decimal("62000.5")
    assert position.position_id == "usd_m:BTCUSDT:SHORT"
    assert position.updated_at == _TIMESTAMP


def test_order_and_fill_translators_accept_execution_report_shape() -> None:
    entry: dict[str, object] = {
        "s": "BTCUSDT",
        "c": "client-123",
        "i": 456,
        "S": "BUY",
        "o": "MARKET",
        "f": "GTC",
        "q": "0.5",
        "z": "0.5",
        "X": "FILLED",
        "p": "0",
        "Z": "31000",
        "O": _EVENT_TIME,
        "T": _EVENT_TIME,
        "x": "TRADE",
        "l": "0.5",
        "L": "62000",
        "t": 789,
        "n": "0.01",
        "N": "USDT",
        "R": True,
    }

    order = translate_order_entry(
        entry,
        product=BinanceProduct.SPOT,
        instrument=_spot_instrument(),
    )
    fill = translate_fill_entry(
        entry,
        product=BinanceProduct.SPOT,
        instrument=_spot_instrument(),
        client_order_id="client-123",
    )

    assert order.status == OrderStatus.FILLED
    assert order.average_fill_price == Decimal("62000")
    assert fill is not None
    assert fill.platform_fill_id == "spot:BTCUSDT:789"
    assert fill.fill_quantity == Decimal("0.5")
    assert fill.fill_price == Decimal("62000")
    assert fill.fee_currency == "USDT"
    assert fill.fee_amount == Decimal("0.01")
    assert fill.entry == FillEntry.OUT


@pytest.mark.asyncio
async def test_spot_execution_report_publishes_order_and_fill_once(adapter, event_bus) -> None:
    adapter._symbol_instruments[(BinanceProduct.SPOT, "BTCUSDT")] = _spot_instrument()
    published: list[object] = []
    event_bus.subscribe(OrderPlacedEvent, published.append)
    event_bus.subscribe(FillEvent, published.append)
    payload: dict[str, object] = {
        "e": "executionReport",
        "E": _EVENT_TIME,
        "s": "BTCUSDT",
        "c": "client-123",
        "i": 456,
        "S": "BUY",
        "o": "MARKET",
        "f": "GTC",
        "q": "0.5",
        "z": "0.5",
        "X": "FILLED",
        "p": "0",
        "Z": "31000",
        "O": _EVENT_TIME,
        "T": _EVENT_TIME,
        "x": "TRADE",
        "l": "0.5",
        "L": "62000",
        "t": 789,
        "n": "0.01",
        "N": "USDT",
    }

    await adapter._handle_user_stream_message(BinanceProduct.SPOT, payload)
    await adapter._handle_user_stream_message(BinanceProduct.SPOT, payload)

    assert len(published) == 2
    assert isinstance(published[0], OrderPlacedEvent)
    assert isinstance(published[1], FillEvent)
    assert published[1].fill.client_order_id == "client-123"


@pytest.mark.asyncio
async def test_order_stream_updates_live_status_then_deduplicates_terminal_echo(
    adapter,
    event_bus,
) -> None:
    adapter._symbol_instruments[(BinanceProduct.SPOT, "BTCUSDT")] = _spot_instrument()
    published: list[object] = []
    event_bus.subscribe(OrderPlacedEvent, published.append)
    event_bus.subscribe(FillEvent, published.append)
    event_bus.subscribe(OrderStatusEvent, published.append)
    partial: dict[str, object] = {
        "e": "executionReport",
        "E": _EVENT_TIME,
        "s": "BTCUSDT",
        "c": "client-partial",
        "i": 789,
        "S": "BUY",
        "o": "MARKET",
        "f": "GTC",
        "q": "0.5",
        "z": "0.2",
        "X": "PARTIALLY_FILLED",
        "Z": "12400",
        "O": _EVENT_TIME,
        "T": _EVENT_TIME,
        "x": "TRADE",
        "l": "0.2",
        "L": "62000",
        "t": 1001,
        "n": "0",
    }
    filled = {
        **partial,
        "z": "0.5",
        "Z": "31000",
        "l": "0.3",
        "t": 1002,
        "X": "FILLED",
    }

    await adapter._handle_user_stream_message(BinanceProduct.SPOT, partial)
    await adapter._handle_user_stream_message(BinanceProduct.SPOT, filled)
    await adapter._handle_user_stream_message(BinanceProduct.SPOT, filled)

    assert [type(event) for event in published] == [
        OrderPlacedEvent,
        FillEvent,
        FillEvent,
        OrderStatusEvent,
    ]
    assert published[-1].order.status == OrderStatus.FILLED


@pytest.mark.parametrize(
    ("product", "symbol", "instrument", "event_type"),
    [
        (BinanceProduct.SPOT, "BTCUSDT", _spot_instrument(), "executionReport"),
        (BinanceProduct.USD_M, "BTCUSDT", _usd_m_instrument(), "ORDER_TRADE_UPDATE"),
        (BinanceProduct.COIN_M, "BTCUSD_PERP", _coin_m_instrument(), "ORDER_TRADE_UPDATE"),
    ],
)
async def test_cancelled_order_stream_updates_publish_once(
    adapter,
    event_bus,
    product: BinanceProduct,
    symbol: str,
    instrument: Instrument,
    event_type: str,
) -> None:
    adapter._symbol_instruments[(product, symbol)] = instrument
    published: list[OrderCancelledEvent] = []
    event_bus.subscribe(OrderCancelledEvent, published.append)
    order = {
        "s": symbol,
        "c": f"cancel-{product.value}",
        "i": 456,
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "0.001",
        "z": "0",
        "X": "CANCELED",
        "p": "10000",
    }
    if product == BinanceProduct.SPOT:
        payload: dict[str, object] = {
            **order,
            "e": event_type,
            "E": _EVENT_TIME,
        }
    else:
        payload = {
            "e": event_type,
            "E": _EVENT_TIME,
            "o": order,
        }

    await adapter._handle_user_stream_message(product, payload)
    await adapter._handle_user_stream_message(product, payload)

    assert len(published) == 1
    assert published[0].client_order_id == f"cancel-{product.value}"
    assert published[0].instrument == instrument
    assert f"cancel-{product.value}" not in adapter._order_refs
    assert adapter._platform_order_keys == {}


@pytest.mark.asyncio
async def test_usdm_algo_update_stream_schema_is_normalized(
    adapter,
    event_bus,
) -> None:
    adapter._symbol_instruments[(BinanceProduct.USD_M, "BTCUSDT")] = _usd_m_instrument()
    published: list[OrderPlacedEvent] = []
    event_bus.subscribe(OrderPlacedEvent, published.append)

    await adapter._handle_user_stream_message(
        BinanceProduct.USD_M,
        {
            "e": "ALGO_UPDATE",
            "E": _EVENT_TIME,
            "o": {
                "caid": "algo-client",
                "aid": 987,
                "at": "CONDITIONAL",
                "o": "TAKE_PROFIT",
                "s": "BTCUSDT",
                "S": "SELL",
                "ps": "BOTH",
                "f": "GTC",
                "q": "0.25",
                "X": "TRIGGERED",
                "ai": "12345",
                "ap": "0",
                "aq": "0",
                "tp": "65000",
                "p": "65000",
                "cp": False,
                "R": False,
            },
        },
    )

    assert len(published) == 1
    order = published[0].order
    assert order.client_order_id == "algo-client"
    assert order.platform_order_id == "987"
    assert order.order_type.value == "STOP_LIMIT"
    assert order.stop_price == Decimal("65000")
    assert order.status == OrderStatus.OPEN
    assert adapter._order_refs["algo-client"].conditional is True
    assert adapter._platform_order_keys[
        (BinanceProduct.USD_M, "BTCUSDT", "12345")
    ] == "algo-client"


@pytest.mark.asyncio
async def test_spot_balance_event_and_futures_account_update_are_translated(
    adapter,
    event_bus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter._symbol_instruments[(BinanceProduct.USD_M, "BTCUSDT")] = _usd_m_instrument()
    monkeypatch.setattr(adapter, "_schedule_balance_refresh", MagicMock())
    balances: list[BalanceUpdateEvent] = []
    positions: list[PositionUpdateEvent] = []
    event_bus.subscribe(BalanceUpdateEvent, balances.append)
    event_bus.subscribe(PositionUpdateEvent, positions.append)

    await adapter._handle_user_stream_message(
        BinanceProduct.SPOT,
        {
            "e": "outboundAccountPosition",
            "E": _EVENT_TIME,
            "B": [{"a": "USDT", "f": "4", "l": "1"}],
        },
    )
    await adapter._handle_user_stream_message(
        BinanceProduct.USD_M,
        {
            "e": "ACCOUNT_UPDATE",
            "E": _EVENT_TIME,
            "a": {
                "B": [{"a": "USDT", "wb": "5", "cw": "4", "bc": "0"}],
                "P": [{"s": "BTCUSDT", "pa": "0.2", "ep": "61000", "ps": "LONG"}],
            },
        },
    )

    assert balances[0].balance.currency == "SPOT:USDT"
    assert positions[0].position.quantity == Decimal("0.2")
    assert positions[0].position.position_id == "usd_m:BTCUSDT:LONG"


@pytest.mark.asyncio
async def test_coin_m_private_stream_uses_sdk_coin_futures_socket(
    adapter,
    binance_client_mock,
    binance_socket_manager_mock,
) -> None:
    adapter._client = binance_client_mock
    adapter._connected = True
    adapter._stream_states = dict.fromkeys(BinanceProduct, False)
    ready = asyncio.Event()
    worker = asyncio.create_task(adapter._run_private_stream(BinanceProduct.COIN_M, ready))
    try:
        await asyncio.wait_for(ready.wait(), timeout=1)
        binance_socket_manager_mock.coin_futures_socket.assert_called_once_with()
        binance_socket_manager_mock.user_socket.assert_not_called()
        binance_socket_manager_mock.futures_user_socket.assert_not_called()
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

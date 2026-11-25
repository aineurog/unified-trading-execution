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
from unified_trading_execution.types.enums import (
    AssetClass,
    FillEntry,
    FillReason,
    OrderStatus,
    OrderType,
)
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


@pytest.mark.parametrize(
    ("product", "instrument", "symbol"),
    [
        (BinanceProduct.SPOT, _spot_instrument(), "BTCUSDT"),
        (BinanceProduct.USD_M, _usd_m_instrument(), "BTCUSDT"),
        (BinanceProduct.COIN_M, _coin_m_instrument(), "BTCUSD_PERP"),
    ],
)
def test_rest_trade_and_execution_report_share_fill_identity(
    product: BinanceProduct,
    instrument: Instrument,
    symbol: str,
) -> None:
    rest_fill = translate_fill_entry(
        {
            "symbol": symbol,
            "id": 789,
            "price": "62000",
            "qty": "0.5",
            "time": _EVENT_TIME,
        },
        product=product,
        instrument=instrument,
        client_order_id="rest-client",
    )
    stream_fill = translate_fill_entry(
        {
            "s": symbol,
            "t": 789,
            "L": "62000",
            "l": "0.5",
            "T": _EVENT_TIME,
            "x": "TRADE",
        },
        product=product,
        instrument=instrument,
        client_order_id="stream-client",
    )

    assert rest_fill is not None
    assert stream_fill is not None
    assert rest_fill.platform_fill_id == stream_fill.platform_fill_id


def test_take_profit_market_maps_to_stop_until_core_distinguishes_tp_sl() -> None:
    order = translate_order_entry(
        {
            "orderId": 123,
            "clientOrderId": "take-profit-order",
            "side": "SELL",
            "type": "TAKE_PROFIT_MARKET",
            "origQty": "0.5",
            "executedQty": "0",
            "status": "NEW",
            "stopPrice": "65000",
        },
        product=BinanceProduct.USD_M,
        instrument=_usd_m_instrument(),
    )

    assert order.order_type is OrderType.STOP
    assert order.stop_price == Decimal("65000")


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
@pytest.mark.parametrize(
    ("product", "symbol", "instrument"),
    [
        (BinanceProduct.USD_M, "BTCUSDT", _usd_m_instrument()),
        (BinanceProduct.COIN_M, "BTCUSD_PERP", _coin_m_instrument()),
    ],
)
async def test_futures_algo_update_stream_schema_is_normalized(
    adapter,
    event_bus,
    product: BinanceProduct,
    symbol: str,
    instrument: Instrument,
) -> None:
    adapter._symbol_instruments[(product, symbol)] = instrument
    published: list[OrderPlacedEvent] = []
    event_bus.subscribe(OrderPlacedEvent, published.append)

    await adapter._handle_user_stream_message(
        product,
        {
            "e": "ALGO_UPDATE",
            "E": _EVENT_TIME,
            "o": {
                "caid": "algo-client",
                "aid": 987,
                "at": "CONDITIONAL",
                "o": "TAKE_PROFIT",
                "s": symbol,
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
    assert adapter._platform_order_keys[(product, symbol, "12345")] == "algo-client"


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
async def test_zero_balances_publish_only_on_nonzero_transition(
    adapter,
    event_bus,
) -> None:
    # Spot enumerates every account asset; steady-state zero rows must not
    # become balance events, but a real nonzero→zero transition still must.
    balances: list[BalanceUpdateEvent] = []
    event_bus.subscribe(BalanceUpdateEvent, balances.append)

    def _spot_balance(asset: str, free: str) -> None:
        adapter._publish_balance(
            translate_spot_balance({"a": asset, "f": free, "l": "0"}, timestamp=_TIMESTAMP)
        )

    _spot_balance("LTC", "0")
    assert balances == []

    _spot_balance("LTC", "1")
    _spot_balance("LTC", "0")
    _spot_balance("LTC", "0")

    assert [event.balance.total for event in balances] == [Decimal("1"), Decimal("0")]
    assert all(event.balance.currency == "SPOT:LTC" for event in balances)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("product", "symbol", "instrument", "fee_asset"),
    [
        (BinanceProduct.USD_M, "BTCUSDT", _usd_m_instrument(), "USDT"),
        (BinanceProduct.COIN_M, "BTCUSD_PERP", _coin_m_instrument(), "BTC"),
    ],
)
async def test_triggered_algo_child_trade_reuses_core_client_id(
    adapter,
    event_bus,
    product: BinanceProduct,
    symbol: str,
    instrument: Instrument,
    fee_asset: str,
) -> None:
    adapter._symbol_instruments[(product, symbol)] = instrument
    fills: list[FillEvent] = []
    event_bus.subscribe(FillEvent, fills.append)

    await adapter._handle_user_stream_message(
        product,
        {
            "e": "ALGO_UPDATE",
            "E": _EVENT_TIME,
            "o": {
                "caid": "algo-client",
                "aid": 987,
                "at": "CONDITIONAL",
                "o": "TAKE_PROFIT_MARKET",
                "s": symbol,
                "S": "SELL",
                "ps": "BOTH",
                "f": "GTC",
                "q": "0.25",
                "X": "TRIGGERED",
                "ai": "12345",
                "ap": "0",
                "aq": "0",
                "tp": "65000",
                "p": "0",
                "R": True,
            },
        },
    )
    await adapter._handle_user_stream_message(
        product,
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": _EVENT_TIME,
            "o": {
                "s": symbol,
                "c": "venue-generated-child",
                "i": 12345,
                "S": "SELL",
                "o": "TAKE_PROFIT_MARKET",
                "f": "GTC",
                "q": "0.25",
                "z": "0.25",
                "X": "FILLED",
                "p": "0",
                "ap": "65000",
                "l": "0.25",
                "L": "65000",
                "T": _EVENT_TIME,
                "t": 555,
                "n": "0.01",
                "N": fee_asset,
                "R": True,
                "x": "TRADE",
            },
        },
    )

    assert len(fills) == 1
    assert fills[0].fill.client_order_id == "algo-client"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("product", "symbol", "instrument", "client_algo_id", "order_type", "expected_reason"),
    [
        (
            BinanceProduct.USD_M,
            "BTCUSDT",
            _usd_m_instrument(),
            "tpsl-u-BTCUSDT-BOTH-sl",
            "STOP_MARKET",
            FillReason.STOP_LOSS,
        ),
        (
            BinanceProduct.COIN_M,
            "BTCUSD_PERP",
            _coin_m_instrument(),
            "tpsl-c-BTCUSD_PERP-BOTH-tp",
            "TAKE_PROFIT_MARKET",
            FillReason.TAKE_PROFIT,
        ),
        (
            BinanceProduct.USD_M,
            "BTCUSDT",
            _usd_m_instrument(),
            "trl-u-BTCUSDT-BOTH",
            "TRAILING_STOP_MARKET",
            FillReason.TRAILING_STOP,
        ),
    ],
)
async def test_triggered_protection_child_trade_reuses_core_client_id(
    adapter,
    event_bus,
    product: BinanceProduct,
    symbol: str,
    instrument: Instrument,
    client_algo_id: str,
    order_type: str,
    expected_reason: FillReason,
) -> None:
    adapter._symbol_instruments[(product, symbol)] = instrument
    published: list[object] = []
    event_bus.subscribe(FillEvent, published.append)
    event_bus.subscribe(OrderPlacedEvent, published.append)
    event_bus.subscribe(OrderStatusEvent, published.append)
    event_bus.subscribe(OrderCancelledEvent, published.append)
    algo_entry: dict[str, object] = {
        "caid": client_algo_id,
        "aid": 987,
        "at": "CONDITIONAL",
        "o": order_type,
        "s": symbol,
        "S": "SELL",
        "ps": "BOTH",
        "f": "GTE_GTC",
        "q": "0.25",
        "X": "TRIGGERED",
        "ai": "12345",
        "ap": "0",
        "aq": "0",
        "tp": "65000",
        "p": "0",
        "R": True,
    }

    await adapter._handle_user_stream_message(
        product,
        {"e": "ALGO_UPDATE", "E": _EVENT_TIME, "o": algo_entry},
    )

    assert adapter._platform_order_keys[(product, symbol, "12345")] == client_algo_id

    await adapter._handle_user_stream_message(
        product,
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": _EVENT_TIME,
            "o": {
                "s": symbol,
                "c": "venue-generated-child",
                "i": 12345,
                "S": "SELL",
                "o": order_type,
                "f": "GTE_GTC",
                "q": "0.25",
                "z": "0.25",
                "X": "FILLED",
                "p": "0",
                "ap": "65000",
                "l": "0.25",
                "L": "65000",
                "T": _EVENT_TIME,
                "t": 555,
                "n": "0.01",
                "N": "USDT",
                "R": True,
                "x": "TRADE",
            },
        },
    )

    assert len(published) == 1
    assert isinstance(published[0], FillEvent)
    assert published[0].fill.client_order_id == client_algo_id
    assert published[0].fill.reason == expected_reason
    assert client_algo_id not in adapter._order_refs
    assert "venue-generated-child" not in adapter._order_refs

    await adapter._handle_user_stream_message(
        product,
        {
            "e": "ALGO_UPDATE",
            "E": _EVENT_TIME,
            "o": {**algo_entry, "X": "FINISHED"},
        },
    )

    assert (product, symbol, "12345") not in adapter._platform_order_keys
    assert len(published) == 1


def _futures_fill_entry(client_order_id: str) -> dict[str, object]:
    return {
        "e": "ORDER_TRADE_UPDATE",
        "E": _EVENT_TIME,
        "s": "BTCUSDT",
        "c": client_order_id,
        "i": 987,
        "S": "SELL",
        "o": "MARKET",
        "f": "GTC",
        "q": "0.001",
        "l": "0.001",
        "L": "62000",
        "x": "TRADE",
        "X": "FILLED",
        "R": True,
        "ps": "BOTH",
        "T": _EVENT_TIME,
        "t": 789,
        "n": "0.01",
        "N": "USDT",
    }


@pytest.mark.parametrize(
    ("client_order_id", "expected_reason"),
    [
        ("tpsl-u-BTCUSDT-BOTH-tp", FillReason.TAKE_PROFIT),
        ("tpsl-c-BTCUSD_PERP-BOTH-sl", FillReason.STOP_LOSS),
        ("phase-entry-1", None),
    ],
)
def test_fill_reason_maps_protection_client_ids(
    client_order_id: str, expected_reason: FillReason | None
) -> None:
    fill = translate_fill_entry(
        _futures_fill_entry(client_order_id),
        product=BinanceProduct.USD_M,
        instrument=_usd_m_instrument(),
        client_order_id=client_order_id,
    )

    assert fill is not None
    assert fill.entry == FillEntry.OUT
    assert fill.reason == expected_reason

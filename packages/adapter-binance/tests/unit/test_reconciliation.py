"""Tests for Binance account reconciliation snapshots."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderRecord

_EVENT_TIME = 1_684_738_540_559


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


def _spot_metadata() -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "status": "TRADING",
    }


def _futures_metadata(product: BinanceProduct) -> dict[str, object]:
    if product == BinanceProduct.COIN_M:
        return {
            "symbol": "BTCUSD_PERP",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "contractType": "PERPETUAL",
            "contractSize": "100",
            "status": "TRADING",
        }
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
    }


def _connect_fake_client(adapter, client: AsyncMock) -> None:
    adapter._client = client
    adapter._connected = True


@pytest.mark.asyncio
async def test_fetch_balances_qualifies_spot_and_futures_wallets(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    _connect_fake_client(adapter, binance_client_mock)
    binance_client_mock.get_account.return_value = {
        "balances": [{"asset": "USDT", "free": "10", "locked": "2"}]
    }
    binance_client_mock.futures_account_balance.return_value = [
        {"asset": "USDT", "balance": "20", "availableBalance": "15"}
    ]
    binance_client_mock.futures_coin_account_balance.return_value = [
        {"asset": "BTC", "balance": "2", "availableBalance": "1.5"}
    ]

    balances = await adapter.fetch_balances()

    assert balances["SPOT:USDT"].total == Decimal("12")
    assert balances["USD_M:USDT"].used == Decimal("5")
    assert balances["COIN_M:BTC"].free == Decimal("1.5")


@pytest.mark.asyncio
async def test_fetch_positions_reads_usd_m_and_coin_m_legs(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    _connect_fake_client(adapter, binance_client_mock)
    adapter._exchange_info_cache = {
        BinanceProduct.USD_M: [_futures_metadata(BinanceProduct.USD_M)],
        BinanceProduct.COIN_M: [_futures_metadata(BinanceProduct.COIN_M)],
    }
    binance_client_mock.futures_position_information.return_value = [
        {
            "symbol": "BTCUSDT",
            "positionAmt": "0.25",
            "entryPrice": "60000",
            "positionSide": "LONG",
            "updateTime": _EVENT_TIME,
        }
    ]
    binance_client_mock.futures_coin_position_information.return_value = [
        {
            "symbol": "BTCUSD_PERP",
            "positionAmt": "2",
            "entryPrice": "59000",
            "positionSide": "SHORT",
            "updateTime": _EVENT_TIME,
        }
    ]

    positions = await adapter.fetch_positions()

    assert len(positions) == 2
    assert positions[0].quantity == Decimal("0.25")
    assert positions[0].position_id == "usd_m:BTCUSDT:LONG"
    assert positions[1].quantity == Decimal("-2")
    assert positions[1].position_id == "coin_m:BTCUSD_PERP:SHORT"
    assert positions[0].updated_at == datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)


@pytest.mark.asyncio
async def test_fetch_open_orders_includes_usd_m_algo_orders(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    _connect_fake_client(adapter, binance_client_mock)
    adapter._exchange_info_cache = {
        BinanceProduct.SPOT: [_spot_metadata()],
        BinanceProduct.USD_M: [_futures_metadata(BinanceProduct.USD_M)],
        BinanceProduct.COIN_M: [_futures_metadata(BinanceProduct.COIN_M)],
    }
    binance_client_mock.get_open_orders.return_value = [
        {
            "symbol": "BTCUSDT",
            "orderId": 10,
            "clientOrderId": "spot-order",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "origQty": "0.1",
            "executedQty": "0",
            "status": "NEW",
            "price": "60000",
        }
    ]
    binance_client_mock.futures_get_open_orders.return_value = []
    binance_client_mock.futures_coin_get_open_orders.return_value = []
    binance_client_mock.futures_get_open_algo_orders.return_value = [
        {
            "symbol": "BTCUSDT",
            "algoId": 20,
            "clientAlgoId": "algo-order",
            "side": "SELL",
            "type": "STOP_MARKET",
            "timeInForce": "GTC",
            "quantity": "0.2",
            "triggerPrice": "55000",
            "algoStatus": "NEW",
        }
    ]

    orders = await adapter.fetch_open_orders()

    assert set(orders) == {"spot-order", "algo-order"}
    assert orders["spot-order"].status == OrderStatus.OPEN
    assert orders["algo-order"].stop_price == Decimal("55000")
    assert adapter._order_refs["algo-order"].conditional is True
    assert adapter._open_order_ids == {"spot:BTCUSDT:10", "usd_m:BTCUSDT:20"}


@pytest.mark.asyncio
async def test_fetch_fills_uses_known_symbol_and_rest_trade_fields(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    _connect_fake_client(adapter, binance_client_mock)
    instrument = _spot_instrument()
    adapter._known_instruments.add(instrument)
    adapter._exchange_info_cache[BinanceProduct.SPOT] = [_spot_metadata()]
    timestamp = datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)
    store = AsyncMock()
    store.query_orders.return_value = [
        OrderRecord(
            instrument=instrument,
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.5"),
            time_in_force=TimeInForce.GTC,
            client_order_id="core-order",
            price=Decimal("61000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="10",
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.5"),
            average_fill_price=Decimal("62000"),
            correlation_id="core-order",
            created_at=timestamp,
            updated_at=timestamp,
        )
    ]
    adapter._state_store = store
    binance_client_mock.get_my_trades.return_value = [
        {
            "symbol": "BTCUSDT",
            "id": 99,
            "orderId": 10,
            "price": "62000",
            "qty": "0.5",
            "commission": "0.01",
            "commissionAsset": "USDT",
            "time": _EVENT_TIME,
            "isBuyer": True,
        }
    ]

    fills = await adapter.fetch_fills(
        since=datetime.fromtimestamp((_EVENT_TIME - 1000) / 1000, tz=UTC)
    )

    assert len(fills["core-order"]) == 1
    fill = fills["core-order"][0]
    assert fill.platform_fill_id == "spot:BTCUSDT:99"
    assert fill.fill_quantity == Decimal("0.5")
    assert fill.fill_price == Decimal("62000")
    assert adapter._platform_order_keys == {}
    store.query_orders.assert_awaited_once_with(instrument=instrument, limit=100_000)
    binance_client_mock.get_my_trades.assert_awaited_once_with(
        symbol="BTCUSDT",
        limit=1000,
        startTime=_EVENT_TIME - 1000,
    )


@pytest.mark.asyncio
async def test_connect_recovery_restores_persisted_order_routing(adapter) -> None:
    instrument = _spot_instrument()
    timestamp = datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)
    record = OrderRecord(
        instrument=instrument,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        time_in_force=TimeInForce.GTC,
        client_order_id="core-order",
        price=Decimal("61000"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="10",
        status=OrderStatus.PARTIALLY_FILLED,
        filled_quantity=Decimal("0.1"),
        average_fill_price=Decimal("60000"),
        correlation_id="core-order",
        created_at=timestamp,
        updated_at=timestamp,
    )
    store = AsyncMock()
    store.query_orders.return_value = [record]
    store.query_open_orders.return_value = [record]
    store.query_positions.return_value = []
    adapter._state_store = store
    adapter._exchange_info_cache[BinanceProduct.SPOT] = [_spot_metadata()]

    await adapter._restore_order_references()

    reference = adapter._order_refs["core-order"]
    assert reference.product == BinanceProduct.SPOT
    assert reference.symbol == "BTCUSDT"
    assert reference.platform_order_id == "10"
    assert adapter._open_order_ids == {"spot:BTCUSDT:10"}
    assert instrument in adapter._known_instruments


@pytest.mark.asyncio
async def test_connect_restores_only_live_order_references(adapter) -> None:
    instrument = _spot_instrument()
    timestamp = datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)
    live_order = OrderRecord(
        instrument=instrument,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        time_in_force=TimeInForce.GTC,
        client_order_id="live-order",
        price=Decimal("61000"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="10",
        status=OrderStatus.PARTIALLY_FILLED,
        filled_quantity=Decimal("0.1"),
        average_fill_price=Decimal("60000"),
        correlation_id="live-order",
        created_at=timestamp,
        updated_at=timestamp,
    )
    historical_instrument = Instrument(
        symbol="ETH",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
    )
    terminal_order = replace(
        live_order,
        instrument=historical_instrument,
        client_order_id="historical-order",
        platform_order_id="11",
        status=OrderStatus.CANCELLED,
    )
    store = AsyncMock()
    store.query_orders.return_value = [live_order, terminal_order]
    store.query_open_orders.return_value = [live_order]
    store.query_positions.return_value = []
    adapter._state_store = store
    adapter._exchange_info_cache[BinanceProduct.SPOT] = [_spot_metadata()]

    await adapter._restore_order_references()

    assert set(adapter._order_refs) == {"live-order"}
    assert adapter._platform_order_keys == {(BinanceProduct.SPOT, "BTCUSDT", "10"): "live-order"}
    assert adapter._open_order_ids == {"spot:BTCUSDT:10"}
    assert adapter._known_instruments == {instrument, historical_instrument}
    store.query_open_orders.assert_awaited_once_with(limit=100_000)


@pytest.mark.asyncio
async def test_order_reference_cache_miss_recovers_one_persisted_order(adapter) -> None:
    instrument = _spot_instrument()
    timestamp = datetime.fromtimestamp(_EVENT_TIME / 1000, tz=UTC)
    record = OrderRecord(
        instrument=instrument,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        time_in_force=TimeInForce.GTC,
        client_order_id="core-order",
        price=Decimal("61000"),
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id="10",
        status=OrderStatus.PARTIALLY_FILLED,
        filled_quantity=Decimal("0.1"),
        average_fill_price=Decimal("60000"),
        correlation_id="core-order",
        created_at=timestamp,
        updated_at=timestamp,
    )
    store = AsyncMock()
    store.get_order.return_value = record
    adapter._state_store = store
    adapter._exchange_info_cache[BinanceProduct.SPOT] = [_spot_metadata()]

    reference = await adapter._restore_order_reference("core-order")

    assert reference is not None
    assert reference.product == BinanceProduct.SPOT
    assert reference.symbol == "BTCUSDT"
    assert reference.order_type == OrderType.LIMIT
    assert reference.platform_order_id == "10"
    assert adapter._order_refs["core-order"] == reference
    assert adapter._platform_order_keys[(BinanceProduct.SPOT, "BTCUSDT", "10")] == ("core-order")
    assert adapter._open_order_ids == {"spot:BTCUSDT:10"}
    store.get_order.assert_awaited_once_with("core-order")

"""Opt-in integration coverage for Binance private account streams."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    BinanceProduct,
)
from unified_trading_execution.events import (
    Event,
    EventBus,
    OrderCancelledEvent,
    OrderPlacedEvent,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderResult, UnifiedOrder

_PRODUCT_CASES = (
    ("spot", BinanceProduct.SPOT, "BTCUSDT"),
    ("usd_m", BinanceProduct.USD_M, "BTCUSDT"),
    ("coin_m", BinanceProduct.COIN_M, "BTCUSD_PERP"),
)
_TICKER_METHODS = {
    BinanceProduct.SPOT: "get_orderbook_ticker",
    BinanceProduct.USD_M: "futures_orderbook_ticker",
    BinanceProduct.COIN_M: "futures_coin_orderbook_ticker",
}
_MAX_TEST_NOTIONAL_USD = Decimal("150")
_OPEN_STATUSES = frozenset({OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED})


class _EventCollector:
    def __init__(self, event_bus: EventBus) -> None:
        self.events: list[Event] = []
        self._changed = asyncio.Event()
        self._subscribers: list[tuple[type[Event], Callable[[Event], None]]] = []
        for event_type in (OrderPlacedEvent, OrderCancelledEvent):
            callback = self._record
            event_bus.subscribe(event_type, callback)
            self._subscribers.append((event_type, callback))

    def _record(self, event: Event) -> None:
        self.events.append(event)
        self._changed.set()

    def close(self, event_bus: EventBus) -> None:
        for event_type, callback in self._subscribers:
            event_bus.unsubscribe(event_type, callback)

    async def wait_for(
        self,
        event_type: type[Event],
        *,
        predicate: Callable[[Event], bool],
        timeout: float = 30,
    ) -> Event:
        async with asyncio.timeout(timeout):
            while True:
                for event in self.events:
                    if isinstance(event, event_type) and predicate(event):
                        return event
                self._changed.clear()
                await self._changed.wait()


def _instrument(product: BinanceProduct, symbol: str) -> Instrument:
    if product == BinanceProduct.SPOT:
        return Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            platform_symbol=symbol,
        )
    if product == BinanceProduct.USD_M:
        return Instrument(
            symbol="BTC",
            quote_currency="USDT",
            currency="USDT",
            asset_class=AssetClass.FUTURES,
            multiplier=1,
            platform_symbol=symbol,
        )
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        currency="BTC",
        asset_class=AssetClass.FUTURES,
        multiplier=100,
        platform_symbol=symbol,
    )


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ticker_row(response: object, *, symbol: str) -> dict[str, object]:
    if isinstance(response, dict):
        rows = [response]
    elif isinstance(response, list) and all(isinstance(row, dict) for row in response):
        rows = response
    else:
        raise AssertionError(f"Binance returned an invalid book ticker: {response!r}")
    matching_rows = [row for row in rows if row.get("symbol") == symbol]
    if len(matching_rows) != 1:
        raise AssertionError(
            f"Expected one Binance book ticker for {symbol}, received {response!r}"
        )
    return matching_rows[0]


def _limit_order(
    *,
    product: BinanceProduct,
    instrument: Instrument,
    client_order_id: str,
    bid: Decimal,
    tick_size: Decimal,
    lot_size: Decimal,
    min_qty: Decimal,
    max_qty: Decimal,
    min_notional: Decimal,
) -> UnifiedOrder:
    price = _floor_to_step(bid * Decimal("0.90"), tick_size)
    if price <= 0 or price >= bid:
        pytest.skip(f"Could not select a passive limit price for {instrument.platform_symbol}")

    target_notional = min_notional * Decimal("1.1")
    if product == BinanceProduct.COIN_M:
        quantity = _ceil_to_step(
            target_notional / Decimal(str(instrument.multiplier)),
            lot_size,
        )
        notional = quantity * Decimal(str(instrument.multiplier))
    else:
        quantity = _ceil_to_step(target_notional / price, lot_size)
        notional = quantity * price
    quantity = max(quantity, min_qty)
    if product != BinanceProduct.COIN_M:
        notional = quantity * price
    if quantity > max_qty:
        pytest.skip(f"Minimum valid quantity exceeds the {product.value} maximum")
    if notional > _MAX_TEST_NOTIONAL_USD:
        pytest.skip(
            f"Minimum valid {product.value} order exceeds the {_MAX_TEST_NOTIONAL_USD} USD test cap"
        )

    return UnifiedOrder(
        instrument=instrument,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=quantity,
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
        price=price,
    )


def _assert_sandbox_environment(adapter: BinanceAdapter, config: BinanceConfig) -> None:
    client = adapter._client
    if client is None or client.testnet != config.testnet or client.demo != config.demo:
        raise AssertionError("Refusing stream order test: SDK client does not match sandbox config")
    if not client.testnet and not client.demo:
        raise AssertionError("Refusing stream order test: production environment is not allowed")


async def _cancel_if_still_open(
    adapter: BinanceAdapter,
    client_order_id: str,
) -> None:
    if not adapter.is_connected or client_order_id not in adapter._order_refs:
        return
    current: OrderResult | None = None
    for attempt in range(8):
        current = await adapter.get_order_by_client_id(client_order_id)
        if current is not None:
            break
        if attempt < 7:
            await asyncio.sleep(0.25)
    if current is not None and current.status in _OPEN_STATUSES:
        await adapter.cancel_order(client_order_id)


async def test_all_private_streams_and_reconciliation_snapshots(
    connected_adapter: BinanceAdapter,
) -> None:
    """Verify all private sockets reach ready state and all snapshot calls work."""
    assert set(connected_adapter._stream_states) == set(BinanceProduct)
    assert all(connected_adapter._stream_states.values())
    assert all(not task.done() for task in connected_adapter._stream_tasks.values())

    balances = await connected_adapter.fetch_balances()
    assert balances
    assert all(
        balance.currency.split(":", maxsplit=1)[0] in {"SPOT", "USD_M", "COIN_M"}
        for balance in balances.values()
    )
    assert all(balance.free + balance.used == balance.total for balance in balances.values())

    positions = await connected_adapter.fetch_positions()
    assert all(position.quantity != 0 for position in positions)
    assert all(position.updated_at.tzinfo is not None for position in positions)

    open_orders = await connected_adapter.fetch_open_orders()
    assert isinstance(open_orders, dict)

    fills = await connected_adapter.fetch_fills()
    assert isinstance(fills, dict)
    assert all(isinstance(order_fills, list) for order_fills in fills.values())


@pytest.mark.parametrize(
    ("product_key", "product", "symbol", "binance_order_config"),
    [pytest.param(*case, case[0], id=case[0]) for case in _PRODUCT_CASES],
    indirect=["binance_order_config"],
)
async def test_private_stream_order_and_cancel_events(
    product_key: str,
    product: BinanceProduct,
    symbol: str,
    binance_order_config: BinanceConfig,
    binance_account_stream_order_opt_in: None,
    event_bus: EventBus,
) -> None:
    """Create and cancel one passive sandbox order; verify lifecycle events."""
    adapter = BinanceAdapter(binance_order_config, event_bus=event_bus)
    collector = _EventCollector(event_bus)
    client_order_id = f"ws-{product_key[0]}-{uuid4().hex[:20]}"
    try:
        await adapter.connect()
        _assert_sandbox_environment(adapter, binance_order_config)

        instrument = _instrument(product, symbol)
        spec = await adapter.fetch_instrument_spec(instrument)
        client = adapter._client
        if client is None:
            raise AssertionError("Connected Binance adapter has no REST client")
        raw_ticker = await getattr(client, _TICKER_METHODS[product])(symbol=symbol)
        ticker = _ticker_row(raw_ticker, symbol=symbol)
        bid = Decimal(str(ticker["bidPrice"]))
        if bid <= spec.tick_size:
            raise AssertionError(f"Binance {symbol} returned invalid bid price {bid}")

        order = _limit_order(
            product=product,
            instrument=instrument,
            client_order_id=client_order_id,
            bid=bid,
            tick_size=spec.tick_size,
            lot_size=spec.lot_size,
            min_qty=spec.min_qty,
            max_qty=spec.max_qty,
            min_notional=spec.min_notional,
        )
        result = await adapter.place_order(order)
        assert result.client_order_id == client_order_id
        assert result.platform_order_id is not None

        placed = await collector.wait_for(
            OrderPlacedEvent,
            predicate=lambda event: (
                isinstance(event, OrderPlacedEvent)
                and event.order.client_order_id == client_order_id
            ),
        )
        assert isinstance(placed, OrderPlacedEvent)
        assert placed.order.instrument == instrument
        assert placed.order.platform_order_id == result.platform_order_id
        assert placed.order.quantity == order.quantity
        assert placed.timestamp.tzinfo is not None
        assert all(adapter._stream_states.values()), (
            "all product streams should remain available while handling the order event"
        )

        cancel_result = await adapter.cancel_order(client_order_id)
        assert cancel_result.status is OrderStatus.CANCELLED, (
            f"REST cancellation returned {cancel_result.status.value}"
        )
        cancelled = await collector.wait_for(
            OrderCancelledEvent,
            predicate=lambda event: (
                isinstance(event, OrderCancelledEvent) and event.client_order_id == client_order_id
            ),
        )
        assert isinstance(cancelled, OrderCancelledEvent)
        assert cancelled.instrument == instrument
        assert all(adapter._stream_states.values()), (
            "all product streams should remain available after cancellation"
        )

        open_orders = await adapter.fetch_open_orders()
        assert client_order_id not in open_orders
        assert (
            sum(
                isinstance(event, OrderPlacedEvent)
                and event.order.client_order_id == client_order_id
                for event in collector.events
            )
            == 1
        ), "an order stream update must not duplicate the initial order event"
    finally:
        try:
            await _cancel_if_still_open(adapter, client_order_id)
        finally:
            collector.close(event_bus)
            await adapter.disconnect()

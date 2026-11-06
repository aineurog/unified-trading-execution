"""Opt-in Spot and Futures Testnet/Demo order place/query/cancel coverage."""

from __future__ import annotations

import asyncio
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig, BinanceProduct
from unified_trading_execution.binance.orders import parse_order_result
from unified_trading_execution.errors import (
    InvalidOrderError,
    OrderNotFoundError,
    UnsupportedOrderTypeError,
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
_BOOK_TICKER_METHODS = {
    BinanceProduct.SPOT: "get_orderbook_ticker",
    BinanceProduct.USD_M: "futures_orderbook_ticker",
    BinanceProduct.COIN_M: "futures_coin_orderbook_ticker",
}
_ORDER_TYPE_TIF_CASES = tuple(
    (order_type, time_in_force)
    for order_type in (
        OrderType.MARKET,
        OrderType.LIMIT,
        OrderType.STOP,
        OrderType.STOP_LIMIT,
    )
    for time_in_force in (TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK)
)
_MAX_TEST_NOTIONAL_USD = Decimal("150")
_TERMINAL_STATUSES = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
)


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


def _ticker_row(response: object, *, product: BinanceProduct, symbol: str) -> dict[str, object]:
    if isinstance(response, dict):
        rows = [response]
    elif isinstance(response, list) and all(isinstance(row, dict) for row in response):
        rows = response
    else:
        raise AssertionError(
            f"Binance {product.value} returned an invalid book ticker payload: {response!r}"
        )
    matching_rows = [row for row in rows if row.get("symbol") == symbol]
    if len(matching_rows) != 1:
        raise AssertionError(
            f"Expected one {product.value} book ticker for {symbol}, received {response!r}"
        )
    return matching_rows[0]


async def _exchange_order_history(
    client: object,
    *,
    product: BinanceProduct,
    symbol: str,
    conditional: bool,
) -> list[dict[str, object]]:
    if product == BinanceProduct.SPOT:
        method_name = "get_all_orders"
    elif product == BinanceProduct.USD_M and conditional:
        method_name = "futures_get_all_algo_orders"
    elif product == BinanceProduct.USD_M:
        method_name = "futures_get_all_orders"
    else:
        method_name = "futures_coin_get_all_orders"
    response = await getattr(client, method_name)(symbol=symbol)
    if not isinstance(response, list) or not all(isinstance(row, dict) for row in response):
        raise AssertionError(
            f"Binance {product.value} returned invalid {method_name} history: {response!r}"
        )
    return response


async def _wait_for_history_entry(
    client: object,
    *,
    product: BinanceProduct,
    symbol: str,
    conditional: bool,
    client_order_id: str,
    platform_order_id: str,
) -> dict[str, object] | None:
    for attempt in range(8):
        history = await _exchange_order_history(
            client,
            product=product,
            symbol=symbol,
            conditional=conditional,
        )
        entry = _history_entry(
            history,
            client_order_id=client_order_id,
            platform_order_id=platform_order_id,
            conditional=conditional,
        )
        if entry is not None:
            return entry
        if attempt < 7:
            await asyncio.sleep(0.25)
    return None


async def _wait_for_order(
    adapter: BinanceAdapter,
    client_order_id: str,
) -> OrderResult | None:
    for attempt in range(8):
        result = await adapter.get_order_by_client_id(client_order_id)
        if result is not None:
            return result
        if attempt < 7:
            await asyncio.sleep(0.25)
    return None


def _history_entry(
    history: list[dict[str, object]],
    *,
    client_order_id: str,
    platform_order_id: str,
    conditional: bool,
) -> dict[str, object] | None:
    id_field = "algoId" if conditional else "orderId"
    client_id_field = "clientAlgoId" if conditional else "clientOrderId"
    return next(
        (
            row
            for row in history
            if str(row.get(id_field)) == platform_order_id
            or row.get(client_id_field) == client_order_id
        ),
        None,
    )


def test_coinm_book_ticker_list_response_is_normalized() -> None:
    ticker = {
        "symbol": "BTCUSD_PERP",
        "bidPrice": "75898.3",
        "askPrice": "75912.9",
    }

    assert (
        _ticker_row(
            [ticker],
            product=BinanceProduct.COIN_M,
            symbol="BTCUSD_PERP",
        )
        == ticker
    )


def test_book_ticker_payload_must_contain_one_matching_symbol() -> None:
    with pytest.raises(AssertionError, match="Expected one coin_m book ticker"):
        _ticker_row(
            [{"symbol": "ETHUSD_PERP"}],
            product=BinanceProduct.COIN_M,
            symbol="BTCUSD_PERP",
        )


def _is_expected_unsupported(
    product: BinanceProduct,
    order_type: OrderType,
    time_in_force: TimeInForce,
) -> bool:
    if product == BinanceProduct.COIN_M and order_type in (
        OrderType.STOP,
        OrderType.STOP_LIMIT,
    ):
        return True
    return order_type in (OrderType.MARKET, OrderType.STOP) and (time_in_force != TimeInForce.GTC)


@pytest.mark.parametrize(
    ("product_key", "product", "symbol", "binance_order_config"),
    [pytest.param(*case, case[0], id=case[0]) for case in _PRODUCT_CASES],
    indirect=["binance_order_config"],
)
@pytest.mark.parametrize(
    ("order_type", "time_in_force"),
    _ORDER_TYPE_TIF_CASES,
    ids=[
        f"{order_type.value.lower()}-{time_in_force.value.lower()}"
        for order_type, time_in_force in _ORDER_TYPE_TIF_CASES
    ],
)
async def test_order_type_and_time_in_force_matrix(
    product_key: str,
    product: BinanceProduct,
    symbol: str,
    binance_order_config: BinanceConfig,
    order_type: OrderType,
    time_in_force: TimeInForce,
) -> None:
    """Exercise the type/TIF matrix, history, and test-owned order cleanup."""
    adapter = BinanceAdapter(binance_order_config)
    client_order_id = f"{product_key[0]}-{uuid4().hex}"
    instrument = _instrument(product, symbol)
    attempted = False
    cancel_requested = False
    placement_result = None
    try:
        await adapter.connect()
        client = adapter._client
        if (
            client is None
            or client.testnet != binance_order_config.testnet
            or client.demo != binance_order_config.demo
        ):
            raise AssertionError(
                "Refusing order test: SDK client environment does not match config"
            )
        if binance_order_config.demo:
            expected_urls = (
                "https://demo-api.binance.com/api/v3/ping",
                "https://demo-fapi.binance.com/fapi/v1/ping",
                "https://demo-dapi.binance.com/dapi/v1/ping",
            )
        else:
            expected_urls = (
                "https://testnet.binance.vision/api/v3/ping",
                "https://testnet.binancefuture.com/fapi/v1/ping",
                "https://testnet.binancefuture.com/dapi/v1/ping",
            )
        actual_urls = (
            client._create_api_uri("ping", signed=False),
            client._create_futures_api_uri("ping"),
            client._create_futures_coin_api_url("ping"),
        )
        if actual_urls != expected_urls:
            raise AssertionError(f"Refusing order test: SDK routes are {actual_urls!r}")

        if _is_expected_unsupported(product, order_type, time_in_force):
            rejected_order = UnifiedOrder(
                instrument=instrument,
                order_type=order_type,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=time_in_force,
                client_order_id=client_order_id,
                price=Decimal("101"),
                stop_price=Decimal("100"),
            )
            with pytest.raises(UnsupportedOrderTypeError):
                await adapter.place_order(rejected_order)
            return

        spec = await adapter.fetch_instrument_spec(instrument)
        raw_book = await getattr(client, _BOOK_TICKER_METHODS[product])(symbol=symbol)
        book = _ticker_row(raw_book, product=product, symbol=symbol)

        bid = Decimal(str(book["bidPrice"]))
        ask = Decimal(str(book["askPrice"]))
        if bid <= spec.tick_size or ask <= bid:
            raise AssertionError(
                f"Binance {product.value} returned unusable bid/ask values: {bid}/{ask}"
            )
        passive_price = bid - spec.tick_size
        if passive_price <= 0 or passive_price >= ask:
            raise AssertionError(
                f"Could not select a passive {product.value} limit price: {passive_price}"
            )
        stop_price = _ceil_to_step(ask * Decimal("1.02"), spec.tick_size)
        if order_type == OrderType.LIMIT and time_in_force == TimeInForce.FOK:
            order_price = _floor_to_step(bid * Decimal("0.95"), spec.tick_size)
        else:
            order_price = (
                passive_price
                if order_type in (OrderType.LIMIT, OrderType.STOP)
                else stop_price + spec.tick_size
            )

        required_notional = spec.min_notional
        sizing_price = max(
            order_price if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) else ask, ask
        )
        if product == BinanceProduct.COIN_M and required_notional > 0:
            contract_value = Decimal(str(instrument.multiplier))
            quantity = max(
                spec.min_qty,
                _ceil_to_step(required_notional / contract_value, spec.lot_size),
            )
        elif required_notional > 0:
            quantity = max(
                spec.min_qty,
                _ceil_to_step(required_notional * Decimal("1.1") / sizing_price, spec.lot_size),
            )
        else:
            quantity = max(spec.min_qty, spec.lot_size)
        if quantity > spec.max_qty:
            raise AssertionError(
                f"Minimum valid order quantity {quantity} exceeds {product.value} maximum"
            )
        order_notional = (
            quantity * Decimal(str(instrument.multiplier))
            if product == BinanceProduct.COIN_M
            else quantity * sizing_price
        )
        if order_notional > _MAX_TEST_NOTIONAL_USD:
            pytest.skip(
                f"Minimum valid {product.value} order exceeds the "
                f"{_MAX_TEST_NOTIONAL_USD} USD safety cap"
            )

        order = UnifiedOrder(
            instrument=instrument,
            order_type=order_type,
            side=OrderSide.BUY,
            quantity=quantity,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            price=order_price if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) else None,
            stop_price=stop_price if order_type in (OrderType.STOP, OrderType.STOP_LIMIT) else None,
        )
        if (
            product in (BinanceProduct.USD_M, BinanceProduct.COIN_M)
            and order_type == OrderType.LIMIT
            and time_in_force == TimeInForce.FOK
        ):
            attempted = True
            with pytest.raises(InvalidOrderError):
                await adapter.place_order(order)
            history = await _exchange_order_history(
                client,
                product=product,
                symbol=symbol,
                conditional=False,
            )
            assert (
                _history_entry(
                    history,
                    client_order_id=client_order_id,
                    platform_order_id="",
                    conditional=False,
                )
                is None
            )
            return

        attempted = True
        result = await adapter.place_order(order)
        placement_result = result
        assert result.client_order_id == client_order_id
        assert result.platform_order_id is not None
        assert result.status != OrderStatus.REJECTED

        conditional = product == BinanceProduct.USD_M and order_type in (
            OrderType.STOP,
            OrderType.STOP_LIMIT,
        )
        queried = await _wait_for_order(adapter, client_order_id)
        history_entry = await _wait_for_history_entry(
            client,
            product=product,
            symbol=symbol,
            conditional=conditional,
            client_order_id=client_order_id,
            platform_order_id=result.platform_order_id,
        )
        assert history_entry is not None, (
            f"Accepted {product.value} order {client_order_id} "
            "was not found in Binance order history"
        )
        assert queried is not None
        assert queried.platform_order_id == result.platform_order_id
        assert queried.status != OrderStatus.REJECTED
        if order_type == OrderType.LIMIT and time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
            assert queried.status in (OrderStatus.FILLED, OrderStatus.EXPIRED)

        if queried.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED):
            cancel_requested = True
            cancelled = await adapter.cancel_order(client_order_id)
            assert cancelled.status in _TERMINAL_STATUSES
    finally:
        try:
            if attempted and adapter.is_connected:
                current = await _wait_for_order(adapter, client_order_id)
                if current is None and placement_result is not None:
                    cleanup_client = adapter._client
                    if cleanup_client is None:
                        raise AssertionError("Connected adapter has no Binance REST client")
                    conditional = product == BinanceProduct.USD_M and order_type in (
                        OrderType.STOP,
                        OrderType.STOP_LIMIT,
                    )
                    history_entry = await _wait_for_history_entry(
                        cleanup_client,
                        product=product,
                        symbol=symbol,
                        conditional=conditional,
                        client_order_id=client_order_id,
                        platform_order_id=placement_result.platform_order_id or "",
                    )
                    if history_entry is not None:
                        current = parse_order_result(
                            history_entry,
                            client_order_id=client_order_id,
                            conditional=conditional,
                        )
                if current is None and placement_result is not None and not cancel_requested:
                    cancel_requested = True
                    await adapter.cancel_order(client_order_id)
                elif current is not None and current.status in (
                    OrderStatus.OPEN,
                    OrderStatus.PARTIALLY_FILLED,
                ):
                    if cancel_requested:
                        raise AssertionError(
                            f"Test-owned order {client_order_id} remains open after cancellation"
                        )
                    cancel_requested = True
                    try:
                        await adapter.cancel_order(client_order_id)
                    except OrderNotFoundError:
                        latest = await adapter.get_order_by_client_id(client_order_id)
                        if latest is None or latest.status not in _TERMINAL_STATUSES:
                            raise
        finally:
            await adapter.disconnect()

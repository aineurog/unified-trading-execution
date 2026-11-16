"""Opt-in Futures Demo/Testnet coverage for position TP/SL protection.

Places one tiny test-owned position, attaches market-on-trigger TP/SL
through the adapter's product-specific conditional-order routes, verifies
read-back, repricing, side-preserving clears, and mirror exclusion, then
flattens the position and removes every protection order it created.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig, BinanceProduct
from unified_trading_execution.binance.enums import PositionMode
from unified_trading_execution.binance.protection import (
    ProtectionSide,
    is_protection_client_id,
    parse_position_id,
    protection_client_id,
)
from unified_trading_execution.errors import OrderNotFoundError
from unified_trading_execution.trailing.types import TrailingBasis
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder
from unified_trading_execution.types.position import Position

_FUTURES_CASES = (
    pytest.param(
        "usd_m",
        BinanceProduct.USD_M,
        "BTCUSDT",
        "usd_m:BTCUSDT:BOTH",
        "usd_m",
        id="usd-m",
    ),
    pytest.param(
        "coin_m",
        BinanceProduct.COIN_M,
        "BTCUSD_PERP",
        "coin_m:BTCUSD_PERP:BOTH",
        "coin_m",
        id="coin-m",
    ),
)
_USD_M_TEST_NOTIONAL_TARGET = Decimal("60")
_USD_M_TEST_NOTIONAL_CAP = Decimal("75")
_COIN_M_TEST_NOTIONAL_CAP = Decimal("150")


def _instrument(product: BinanceProduct, symbol: str) -> Instrument:
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


async def _wait_for_position(adapter: BinanceAdapter, position_id: str) -> Position:
    for _ in range(20):
        positions = await adapter.fetch_positions()
        for position in positions:
            if position.position_id == position_id and position.quantity != 0:
                return position
        await asyncio.sleep(0.5)
    raise AssertionError(f"No open one-way {position_id} position appeared after order placement")


async def _wait_for_flat(adapter: BinanceAdapter, position_id: str) -> None:
    for _ in range(20):
        positions = await adapter.fetch_positions()
        if all(
            position.position_id != position_id or position.quantity == 0 for position in positions
        ):
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"Position {position_id} did not flatten in time")


@pytest.mark.parametrize(
    ("product_key", "product", "symbol", "position_id", "binance_order_config"),
    _FUTURES_CASES,
    indirect=["binance_order_config"],
)
async def test_position_tpsl_lifecycle(
    product_key: str,
    product: BinanceProduct,
    symbol: str,
    position_id: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: Callable[[BinanceAdapter, BinanceProduct, str], Awaitable[None]],
) -> None:
    adapter = BinanceAdapter(binance_order_config)
    instrument = _instrument(product, symbol)
    entry_client_order_id: str | None = None
    entry_order_attempted = False
    entry_filled_quantity = Decimal("0")
    protection_write_attempted = False
    try:
        await adapter.connect()
        client = adapter._client
        if client is None:
            raise AssertionError("Connected adapter has no Binance REST client")
        await require_clean_futures_symbol(adapter, product, symbol)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip(
                "Position protection integration test requires one-way mode for safe cleanup"
            )

        spec = await adapter.fetch_instrument_spec(instrument)
        ticker = await adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.mark is not None
        mark = ticker.mark
        if product == BinanceProduct.COIN_M:
            contract_value = Decimal(str(instrument.multiplier))
            minimum_notional_quantity = (
                spec.min_notional / contract_value if spec.min_notional > 0 else Decimal("0")
            )
            quantity = _ceil_to_step(
                max(spec.min_qty, minimum_notional_quantity),
                spec.lot_size,
            )
            test_notional = quantity * contract_value
            notional_cap = _COIN_M_TEST_NOTIONAL_CAP
        else:
            quantity = max(
                spec.min_qty,
                _ceil_to_step(
                    max(_USD_M_TEST_NOTIONAL_TARGET, spec.min_notional) / mark,
                    spec.lot_size,
                ),
            )
            test_notional = quantity * mark
            notional_cap = _USD_M_TEST_NOTIONAL_CAP
        if test_notional > notional_cap:
            pytest.skip(
                f"Minimum {product.value} position exceeds the {notional_cap} USD test notional cap"
            )

        entry_client_order_id = f"pptp-{product_key[0]}-entry-{uuid4().hex[:16]}"
        entry_order_attempted = True
        entry_result = await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=entry_client_order_id,
            )
        )
        entry_filled_quantity = entry_result.filled_quantity
        leg = await _wait_for_position(adapter, position_id)
        leg_side = parse_position_id(product, position_id)
        assert leg_side.symbol == symbol
        long_leg = leg.quantity > 0

        # Position read reports no protection before attachment.
        assert await adapter.get_position_tpsl(instrument, position_id) in (None, (None, None))

        # Attach market-on-trigger protection far from mark.
        raw_stop_trigger = mark * (Decimal("0.5") if long_leg else Decimal("2"))
        raw_tp_trigger = mark * (Decimal("2") if long_leg else Decimal("0.5"))
        stop_trigger = (
            _floor_to_step(raw_stop_trigger, spec.tick_size)
            if long_leg
            else _ceil_to_step(raw_stop_trigger, spec.tick_size)
        )
        tp_trigger = (
            _ceil_to_step(raw_tp_trigger, spec.tick_size)
            if long_leg
            else _floor_to_step(raw_tp_trigger, spec.tick_size)
        )
        protection_write_attempted = True
        await adapter.modify_position_tpsl(
            instrument,
            position_id,
            stop_loss=TpSlAttachment(trigger_price=stop_trigger),
            take_profit=TpSlAttachment(trigger_price=tp_trigger),
            trigger_basis=TrailingBasis.MARK,
        )

        if product == BinanceProduct.COIN_M:
            regular_orders = await client.futures_coin_get_open_orders(symbol=symbol)
            assert isinstance(regular_orders, list)
            expected_orders = {
                protection_client_id(leg_side, ProtectionSide.TAKE_PROFIT): (
                    "TAKE_PROFIT_MARKET",
                    tp_trigger,
                ),
                protection_client_id(leg_side, ProtectionSide.STOP_LOSS): (
                    "STOP_MARKET",
                    stop_trigger,
                ),
            }
            found = {
                row.get("clientOrderId"): row
                for row in regular_orders
                if isinstance(row, dict) and row.get("clientOrderId") in expected_orders
            }
            assert set(found) == set(expected_orders)
            for client_order_id, (expected_type, expected_trigger) in expected_orders.items():
                order = found[client_order_id]
                assert order.get("type") == expected_type
                assert Decimal(str(order.get("stopPrice"))) == expected_trigger
                assert order.get("closePosition") in (True, "true", "TRUE")
                assert order.get("workingType") == "MARK_PRICE"
                assert order.get("priceProtect") in (True, "true", "TRUE")

        confirmed = await adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert tp is not None and tp.trigger_price == tp_trigger
        assert sl is not None and sl.trigger_price == stop_trigger
        assert tp.limit_price is None and sl.limit_price is None

        # Reprice the stop; take profit must be preserved untouched.
        raw_new_stop = stop_trigger * Decimal("0.9") if long_leg else stop_trigger * Decimal("1.1")
        new_stop = (
            _floor_to_step(raw_new_stop, spec.tick_size)
            if long_leg
            else _ceil_to_step(raw_new_stop, spec.tick_size)
        )
        await adapter.modify_position_tpsl(
            instrument,
            position_id,
            stop_loss=TpSlAttachment(trigger_price=new_stop),
        )
        confirmed = await adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert sl is not None and sl.trigger_price == new_stop
        assert tp is not None and tp.trigger_price == tp_trigger

        # Clearing one side preserves the other.
        await adapter.clear_position_tpsl(instrument, position_id, stop_loss=True)
        confirmed = await adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert sl is None
        assert tp is not None and tp.trigger_price == tp_trigger

        # A limit-trigger attachment round-trips both prices. Values stay
        # inside the venue price band and far enough from mark to never fill.
        round_step = _floor_to_step if long_leg else _ceil_to_step
        limit_trigger = round_step(mark * Decimal("0.8" if long_leg else "1.25"), spec.tick_size)
        limit_price = round_step(mark * Decimal("0.78" if long_leg else "1.28"), spec.tick_size)
        await adapter.modify_position_tpsl(
            instrument,
            position_id,
            stop_loss=TpSlAttachment(
                trigger_price=limit_trigger,
                limit_price=limit_price,
            ),
        )
        confirmed = await adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert sl is not None
        assert sl.trigger_price == limit_trigger
        assert sl.limit_price == limit_price
        assert tp is not None and tp.trigger_price == tp_trigger
        await adapter.clear_position_tpsl(instrument, position_id, stop_loss=True)
        confirmed = await adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None
        assert confirmed[1] is None

        # Protection orders stay out of the ordinary order mirror.
        open_orders = await adapter.fetch_open_orders()
        protection_leaks = [
            record.client_order_id
            for record in open_orders.values()
            if is_protection_client_id(record.client_order_id)
        ]
        assert protection_leaks == []

        await adapter.clear_position_tpsl(instrument, position_id, take_profit=True)
        assert await adapter.get_position_tpsl(instrument, position_id) == (None, None)
    finally:
        try:
            if adapter.is_connected and entry_order_attempted and entry_client_order_id is not None:
                if entry_client_order_id in adapter._order_refs:
                    with contextlib.suppress(OrderNotFoundError):
                        entry_result = await adapter.get_order_by_client_id(entry_client_order_id)
                        if entry_result is not None:
                            entry_filled_quantity = max(
                                entry_filled_quantity,
                                entry_result.filled_quantity,
                            )
                try:
                    if protection_write_attempted:
                        await adapter.clear_position_tpsl(
                            instrument, position_id, take_profit=True, stop_loss=True
                        )
                        remaining_protections = await adapter.get_position_tpsl(
                            instrument, position_id
                        )
                        if remaining_protections not in (None, (None, None)):
                            raise AssertionError(
                                f"{product.value} protection cleanup left open orders: "
                                f"{remaining_protections!r}"
                            )
                finally:
                    positions = await adapter.fetch_positions()
                    open_leg = next(
                        (
                            position
                            for position in positions
                            if position.position_id == position_id and position.quantity != 0
                        ),
                        None,
                    )
                    if open_leg is not None:
                        if abs(open_leg.quantity) > entry_filled_quantity:
                            raise AssertionError(
                                f"Refusing to close sandbox exposure {abs(open_leg.quantity)} "
                                f"beyond the test entry fill {entry_filled_quantity}"
                            )
                        close_quantity = abs(open_leg.quantity)
                        if close_quantity == 0:
                            raise AssertionError(
                                "Found an open sandbox position but could not confirm any fill "
                                "for the test entry; refusing to close unowned exposure"
                            )
                        await adapter.place_order(
                            UnifiedOrder(
                                instrument=instrument,
                                order_type=OrderType.MARKET,
                                side=(OrderSide.SELL if open_leg.quantity > 0 else OrderSide.BUY),
                                quantity=close_quantity,
                                time_in_force=TimeInForce.GTC,
                                reduce_only=True,
                                client_order_id=f"pptp-close-{uuid4().hex[:20]}",
                            )
                        )
                        await _wait_for_flat(adapter, position_id)
        finally:
            await adapter.disconnect()

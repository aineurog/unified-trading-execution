"""Opt-in USDⓈ-M Demo/Testnet coverage for position TP/SL protection.

Places one tiny test-owned position, attaches market-on-trigger TP/SL
through the adapter's conditional algo orders, verifies read-back,
repricing, side-preserving clears, and mirror exclusion, then flattens the
position and removes every protection order it created.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from decimal import ROUND_CEILING, Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig, BinanceProduct
from unified_trading_execution.binance.enums import PositionMode
from unified_trading_execution.binance.protection import (
    is_protection_client_id,
    parse_position_id,
)
from unified_trading_execution.errors import OrderNotFoundError, UnsupportedOrderTypeError
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

_SYMBOL = "BTCUSDT"
_INSTRUMENT = Instrument(
    symbol="BTC",
    quote_currency="USDT",
    currency="USDT",
    asset_class=AssetClass.FUTURES,
    multiplier=1,
    platform_symbol=_SYMBOL,
)
_MAX_TEST_NOTIONAL_USD = Decimal("25")


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


async def _wait_for_position(adapter: BinanceAdapter) -> Position:
    for _ in range(20):
        positions = await adapter.fetch_positions()
        for position in positions:
            if position.position_id == "usd_m:BTCUSDT:BOTH" and position.quantity != 0:
                return position
        await asyncio.sleep(0.5)
    raise AssertionError(f"No open one-way {_SYMBOL} position appeared after order placement")


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
    ("binance_order_config"),
    ["usd_m"],
    indirect=["binance_order_config"],
)
async def test_position_tpsl_lifecycle(
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: Callable[[BinanceAdapter, BinanceProduct, str], Awaitable[None]],
) -> None:
    adapter = BinanceAdapter(binance_order_config)
    position_id: str | None = None
    entry_client_order_id: str | None = None
    entry_order_attempted = False
    entry_filled_quantity = Decimal("0")
    try:
        await adapter.connect()
        client = adapter._client
        if client is None:
            raise AssertionError("Connected adapter has no Binance REST client")
        await require_clean_futures_symbol(adapter, BinanceProduct.USD_M, _SYMBOL)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip(
                "Position protection integration test requires one-way mode for safe cleanup"
            )
        position_id = "usd_m:BTCUSDT:BOTH"

        spec = await adapter.fetch_instrument_spec(_INSTRUMENT)
        ticker = await adapter.fetch_ticker(_INSTRUMENT)
        assert ticker is not None and ticker.mark is not None
        mark = ticker.mark
        quantity = max(
            spec.min_qty,
            _ceil_to_step(_MAX_TEST_NOTIONAL_USD / mark, spec.lot_size),
        )
        if quantity * mark > _MAX_TEST_NOTIONAL_USD * Decimal("1.5"):
            pytest.skip("Minimum USDⓈ-M quantity exceeds the test notional cap")

        entry_client_order_id = f"pptp-entry-{uuid4().hex[:20]}"
        entry_order_attempted = True
        entry_result = await adapter.place_order(
            UnifiedOrder(
                instrument=_INSTRUMENT,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=entry_client_order_id,
            )
        )
        entry_filled_quantity = entry_result.filled_quantity
        leg = await _wait_for_position(adapter)
        leg_side = parse_position_id(BinanceProduct.USD_M, position_id)
        assert leg_side.symbol == _SYMBOL
        long_leg = leg.quantity > 0

        # Flat-leg read reports no position.
        assert await adapter.get_position_tpsl(_INSTRUMENT, "usd_m:BTCUSDT:BOTH") in (
            None,
            (None, None),
        )

        # Attach market-on-trigger protection far from mark.
        stop_trigger = mark * (Decimal("0.5") if long_leg else Decimal("2"))
        tp_trigger = mark * (Decimal("2") if long_leg else Decimal("0.5"))
        await adapter.modify_position_tpsl(
            _INSTRUMENT,
            position_id,
            stop_loss=TpSlAttachment(trigger_price=stop_trigger),
            take_profit=TpSlAttachment(trigger_price=tp_trigger),
            trigger_basis=TrailingBasis.MARK,
        )

        confirmed = await adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert tp is not None and tp.trigger_price == tp_trigger
        assert sl is not None and sl.trigger_price == stop_trigger
        assert tp.limit_price is None and sl.limit_price is None

        # Reprice the stop; take profit must be preserved untouched.
        new_stop = stop_trigger * Decimal("0.9") if long_leg else stop_trigger * Decimal("1.1")
        await adapter.modify_position_tpsl(
            _INSTRUMENT,
            position_id,
            stop_loss=TpSlAttachment(trigger_price=new_stop),
        )
        confirmed = await adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert sl is not None and sl.trigger_price == new_stop
        assert tp is not None and tp.trigger_price == tp_trigger

        # Clearing one side preserves the other.
        await adapter.clear_position_tpsl(_INSTRUMENT, position_id, stop_loss=True)
        confirmed = await adapter.get_position_tpsl(_INSTRUMENT, position_id)
        assert confirmed is not None
        tp, sl = confirmed
        assert sl is None
        assert tp is not None and tp.trigger_price == tp_trigger

        # Protection orders stay out of the ordinary order mirror.
        open_orders = await adapter.fetch_open_orders()
        protection_leaks = [
            record.client_order_id
            for record in open_orders.values()
            if is_protection_client_id(record.client_order_id)
        ]
        assert protection_leaks == []

        # Unsupported surfaces stay explicit.
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                _INSTRUMENT,
                "coin_m:BTCUSD_PERP:BOTH",
                stop_loss=TpSlAttachment(trigger_price=Decimal("1")),
            )
        assert (
            await adapter.get_position_tpsl(
                Instrument(
                    symbol="BTC",
                    quote_currency="USD",
                    currency="BTC",
                    asset_class=AssetClass.FUTURES,
                    multiplier=100,
                    platform_symbol="BTCUSD_PERP",
                ),
                "coin_m:BTCUSD_PERP:BOTH",
            )
            is None
        )
    finally:
        try:
            if (
                adapter.is_connected
                and position_id is not None
                and entry_order_attempted
                and entry_client_order_id is not None
            ):
                if entry_client_order_id in adapter._order_refs:
                    with contextlib.suppress(OrderNotFoundError):
                        entry_result = await adapter.get_order_by_client_id(entry_client_order_id)
                        if entry_result is not None:
                            entry_filled_quantity = max(
                                entry_filled_quantity,
                                entry_result.filled_quantity,
                            )
                await adapter.clear_position_tpsl(
                    _INSTRUMENT, position_id, take_profit=True, stop_loss=True
                )
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
                    close_quantity = min(abs(open_leg.quantity), entry_filled_quantity)
                    if close_quantity == 0:
                        raise AssertionError(
                            "Found an open sandbox position but could not confirm any fill "
                            "for the test entry; refusing to close unowned exposure"
                        )
                    await adapter.place_order(
                        UnifiedOrder(
                            instrument=_INSTRUMENT,
                            order_type=OrderType.MARKET,
                            side=(OrderSide.SELL if open_leg.quantity > 0 else OrderSide.BUY),
                            quantity=close_quantity,
                            time_in_force=TimeInForce.GTC,
                            reduce_only=True,
                            client_order_id=f"pptp-close-{uuid4().hex[:20]}",
                        )
                    )
                    if close_quantity < abs(open_leg.quantity):
                        raise AssertionError(
                            f"Refusing to close sandbox exposure {abs(open_leg.quantity)} "
                            f"beyond the test entry fill {entry_filled_quantity}"
                        )
                    await _wait_for_flat(adapter, position_id)
        finally:
            await adapter.disconnect()

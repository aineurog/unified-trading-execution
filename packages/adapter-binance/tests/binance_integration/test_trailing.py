"""Opt-in USDⓈ-M and COIN-M Demo/Testnet coverage for native trailing stops."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from decimal import ROUND_CEILING, Decimal
from uuid import uuid4

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig, BinanceProduct
from unified_trading_execution.binance.enums import PositionMode
from unified_trading_execution.binance.trailing import (
    TRAILING_CLIENT_ID_PREFIX,
    is_trailing_client_id,
)
from unified_trading_execution.errors import OrderNotFoundError
from unified_trading_execution.trailing.types import (
    NativeTrailingRequest,
    TrailingBasis,
    TrailingOffsetType,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

_USD_M_POSITION_ID = "usd_m:BTCUSDT:BOTH"
_USD_M_SYMBOL = "BTCUSDT"
_USD_M_TEST_NOTIONAL_TARGET = Decimal("60")
_USD_M_TEST_NOTIONAL_CAP = Decimal("75")
_COIN_M_POSITION_ID = "coin_m:BTCUSD_PERP:BOTH"
_COIN_M_SYMBOL = "BTCUSD_PERP"
_COIN_M_TEST_NOTIONAL_CAP = Decimal("150")
_TRAILING_OFFSET_PERCENT = Decimal("5")


def _usd_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        currency="USDT",
        asset_class=AssetClass.FUTURES,
        multiplier=1,
        platform_symbol=_USD_M_SYMBOL,
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


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


async def _wait_for_leg_quantity(adapter: BinanceAdapter, position_id: str) -> Decimal:
    for _ in range(20):
        for position in await adapter.fetch_positions():
            if position.position_id == position_id and position.quantity != 0:
                return abs(position.quantity)
        await asyncio.sleep(0.5)
    raise AssertionError(f"No open one-way {position_id} position appeared")


async def _wait_for_flat(adapter: BinanceAdapter, position_id: str) -> None:
    for _ in range(20):
        if all(
            position.position_id != position_id or position.quantity == 0
            for position in await adapter.fetch_positions()
        ):
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"Position {position_id} did not flatten in time")


def _request(
    instrument: Instrument,
    position_id: str,
    quantity: Decimal,
    offset: Decimal,
    client_id: str,
) -> NativeTrailingRequest:
    return NativeTrailingRequest(
        instrument=instrument,
        position_id=position_id,
        side=OrderSide.BUY,
        quantity=quantity,
        offset=offset,
        offset_type=TrailingOffsetType.PERCENT,
        basis=TrailingBasis.MARK,
        activation_price=None,
        client_id=client_id,
    )


def test_trailing_capability_matrix() -> None:
    adapter = BinanceAdapter.__new__(BinanceAdapter)

    usd_m = adapter.trailing_capability(_usd_m_instrument())
    assert usd_m.supported is True
    assert usd_m.offset_types == frozenset({TrailingOffsetType.PERCENT})
    assert usd_m.position_scoped is True
    assert usd_m.supports_take_profit is False

    coin_m = adapter.trailing_capability(_coin_m_instrument())
    assert coin_m.supported is True
    assert coin_m.offset_types == frozenset({TrailingOffsetType.PERCENT})
    assert coin_m.position_scoped is True
    assert coin_m.supports_take_profit is False


async def test_usd_m_native_trailing_lifecycle(
    binance_config: BinanceConfig,
    require_clean_futures_symbol: Callable[[BinanceAdapter, BinanceProduct, str], Awaitable[None]],
) -> None:
    adapter = BinanceAdapter(binance_config)
    instrument = _usd_m_instrument()
    trail_native_id: str | None = None
    position_open = False
    try:
        await adapter.connect()
        await require_clean_futures_symbol(adapter, BinanceProduct.USD_M, _USD_M_SYMBOL)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip("Native trailing integration test requires one-way mode")

        spec = await adapter.fetch_instrument_spec(instrument)
        ticker = await adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.mark is not None
        quantity = max(
            spec.min_qty,
            _ceil_to_step(
                max(_USD_M_TEST_NOTIONAL_TARGET, spec.min_notional) / ticker.mark,
                spec.lot_size,
            ),
        )
        if quantity * ticker.mark > _USD_M_TEST_NOTIONAL_CAP:
            pytest.skip("Minimum USD-M position exceeds the test notional cap")

        result = await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=f"trace-{uuid4().hex[:16]}",
            )
        )
        assert result.filled_quantity != 0
        leg_quantity = await _wait_for_leg_quantity(adapter, _USD_M_POSITION_ID)
        position_open = True

        placed = await adapter.place_trailing_stop(
            _request(
                instrument,
                _USD_M_POSITION_ID,
                leg_quantity,
                _TRAILING_OFFSET_PERCENT,
                f"trail-it-{uuid4().hex[:12]}",
            )
        )
        trail_native_id = placed.native_id
        assert trail_native_id.startswith(TRAILING_CLIENT_ID_PREFIX)
        assert is_trailing_client_id(trail_native_id)
        assert placed.confirmed_offset == _TRAILING_OFFSET_PERCENT
        assert placed.position_scoped is True

        by_position = await adapter.get_position_trailing_stop(instrument, _USD_M_POSITION_ID)
        assert by_position is not None
        assert by_position.native_id == trail_native_id
        by_id = await adapter.get_trailing_stop(trail_native_id, instrument)
        assert by_id is not None
        assert by_id.native_id == trail_native_id

        request = _request(
            instrument,
            _USD_M_POSITION_ID,
            leg_quantity,
            Decimal("7"),
            f"trail-it-{uuid4().hex[:12]}",
        )
        repriced = await adapter.place_trailing_stop(request)
        assert repriced.native_id == trail_native_id

        await adapter.cancel_trailing_stop(trail_native_id, instrument)
        trail_native_id = None
        assert await adapter.get_position_trailing_stop(instrument, _USD_M_POSITION_ID) is None
    finally:
        if trail_native_id is not None:
            with contextlib.suppress(Exception):
                await adapter.cancel_trailing_stop(trail_native_id, instrument)
        if position_open:
            try:
                for position in await adapter.fetch_positions():
                    if position.position_id == _USD_M_POSITION_ID and position.quantity != 0:
                        await adapter.place_order(
                            UnifiedOrder(
                                instrument=instrument,
                                order_type=OrderType.MARKET,
                                side=(OrderSide.SELL if position.quantity > 0 else OrderSide.BUY),
                                quantity=abs(position.quantity),
                                time_in_force=TimeInForce.GTC,
                                reduce_only=True,
                                client_order_id=f"trcl-{uuid4().hex[:16]}",
                            )
                        )
                await _wait_for_flat(adapter, _USD_M_POSITION_ID)
            except Exception:  # cleanup must not mask failures
                pass
        await adapter.disconnect()


@pytest.mark.parametrize("binance_order_config", ["coin_m"], indirect=True)
async def test_coin_m_native_trailing_lifecycle(
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: Callable[[BinanceAdapter, BinanceProduct, str], Awaitable[None]],
) -> None:
    adapter = BinanceAdapter(binance_order_config)
    instrument = _coin_m_instrument()
    entry_client_order_id = f"trace-c-{uuid4().hex[:16]}"
    entry_order_attempted = False
    entry_filled_quantity = Decimal("0")
    trail_native_id: str | None = None
    try:
        await adapter.connect()
        await require_clean_futures_symbol(adapter, BinanceProduct.COIN_M, _COIN_M_SYMBOL)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip("Native trailing integration test requires one-way mode")

        spec = await adapter.fetch_instrument_spec(instrument)
        contract_value = Decimal(str(instrument.multiplier))
        quantity = max(
            spec.min_qty,
            _ceil_to_step(
                max(spec.min_notional, contract_value) / contract_value,
                spec.lot_size,
            ),
        )
        test_notional = quantity * contract_value
        if test_notional > _COIN_M_TEST_NOTIONAL_CAP:
            pytest.skip(
                f"Minimum COIN-M position exceeds the {_COIN_M_TEST_NOTIONAL_CAP} USD test cap"
            )

        entry_order_attempted = True
        result = await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=entry_client_order_id,
            )
        )
        entry_filled_quantity = result.filled_quantity
        leg_quantity = await _wait_for_leg_quantity(adapter, _COIN_M_POSITION_ID)

        trail_native_id = f"{TRAILING_CLIENT_ID_PREFIX}c-{_COIN_M_SYMBOL}-BOTH"
        placed = await adapter.place_trailing_stop(
            _request(
                instrument,
                _COIN_M_POSITION_ID,
                leg_quantity,
                _TRAILING_OFFSET_PERCENT,
                f"trail-it-{uuid4().hex[:12]}",
            )
        )
        assert placed.native_id == trail_native_id
        assert is_trailing_client_id(placed.native_id)
        assert placed.confirmed_offset == _TRAILING_OFFSET_PERCENT
        assert placed.position_scoped is True

        by_position = await adapter.get_position_trailing_stop(instrument, _COIN_M_POSITION_ID)
        assert by_position is not None
        assert by_position.native_id == trail_native_id
        assert by_position.confirmed_offset == _TRAILING_OFFSET_PERCENT
        by_id = await adapter.get_trailing_stop(trail_native_id, instrument)
        assert by_id is not None
        assert by_id.native_id == trail_native_id

        repriced = await adapter.place_trailing_stop(
            _request(
                instrument,
                _COIN_M_POSITION_ID,
                leg_quantity,
                Decimal("7"),
                f"trail-it-{uuid4().hex[:12]}",
            )
        )
        assert repriced.native_id == trail_native_id
        by_position = await adapter.get_position_trailing_stop(instrument, _COIN_M_POSITION_ID)
        assert by_position is not None
        assert by_position.confirmed_offset == Decimal("7")

        await adapter.cancel_trailing_stop(trail_native_id, instrument)
        trail_native_id = None
        assert await adapter.get_position_trailing_stop(instrument, _COIN_M_POSITION_ID) is None
    finally:
        try:
            if adapter.is_connected:
                if trail_native_id is not None:
                    await adapter.cancel_trailing_stop(trail_native_id, instrument)
                if entry_order_attempted:
                    if entry_client_order_id in adapter._order_refs:
                        with contextlib.suppress(OrderNotFoundError):
                            entry_result = await adapter.get_order_by_client_id(
                                entry_client_order_id
                            )
                            if entry_result is not None:
                                entry_filled_quantity = max(
                                    entry_filled_quantity,
                                    entry_result.filled_quantity,
                                )
                    positions = await adapter.fetch_positions()
                    open_leg = next(
                        (
                            position
                            for position in positions
                            if position.position_id == _COIN_M_POSITION_ID
                            and position.quantity != 0
                        ),
                        None,
                    )
                    if open_leg is not None:
                        if (
                            abs(open_leg.quantity) > entry_filled_quantity
                            or entry_filled_quantity == 0
                        ):
                            raise AssertionError(
                                f"Refusing to close COIN-M exposure {abs(open_leg.quantity)} "
                                f"without confirming the test entry fill {entry_filled_quantity}"
                            )
                        await adapter.place_order(
                            UnifiedOrder(
                                instrument=instrument,
                                order_type=OrderType.MARKET,
                                side=OrderSide.SELL if open_leg.quantity > 0 else OrderSide.BUY,
                                quantity=abs(open_leg.quantity),
                                time_in_force=TimeInForce.GTC,
                                reduce_only=True,
                                client_order_id=f"trcl-c-{uuid4().hex[:16]}",
                            )
                        )
                        await _wait_for_flat(adapter, _COIN_M_POSITION_ID)
        finally:
            await adapter.disconnect()

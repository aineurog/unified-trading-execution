"""Opt-in sandbox integration coverage for persisted Futures configuration."""

from __future__ import annotations

import os
import sys
from decimal import Decimal, InvalidOperation
from functools import partial

import pytest

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    BinanceProduct,
    MarginMode,
)
from unified_trading_execution.binance.events import FuturesSettingDriftEvent
from unified_trading_execution.events import EventBus
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_FUTURES_CASES = (
    (BinanceProduct.USD_M, "BTCUSDT"),
    (BinanceProduct.COIN_M, "BTCUSD_PERP"),
)


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


@pytest.fixture
def futures_config_mutation_opt_in(binance_config: BinanceConfig) -> None:
    """Require a second explicit opt-in for authenticated sandbox mutations."""
    if os.getenv("BINANCE_FUTURES_CONFIG_INTEGRATION", "true").strip().lower() != "true":
        pytest.skip(
            "Set BINANCE_FUTURES_CONFIG_INTEGRATION=true to opt in to "
            "Futures configuration mutation tests"
        )
    if not (binance_config.testnet or binance_config.demo):
        pytest.fail("Futures configuration integration tests must never use production")


async def _require_clean_symbol(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    symbol: str,
) -> None:
    client = adapter._require_client()
    open_orders_method = (
        "futures_get_open_orders"
        if product == BinanceProduct.USD_M
        else "futures_coin_get_open_orders"
    )
    open_orders = await adapter._run_order_request(
        product,
        partial(getattr(client, open_orders_method), symbol=symbol),
    )
    if not isinstance(open_orders, list):
        raise AssertionError(f"Unexpected open-orders response: {open_orders!r}")
    if open_orders:
        pytest.skip(f"Refusing to mutate {symbol}: sandbox has open Futures orders")

    if product == BinanceProduct.USD_M:
        algo_orders = await adapter._run_order_request(
            product,
            partial(client.futures_get_open_algo_orders, symbol=symbol),
        )
        if not isinstance(algo_orders, list):
            raise AssertionError(f"Unexpected open algo-orders response: {algo_orders!r}")
        if algo_orders:
            pytest.skip(f"Refusing to mutate {symbol}: sandbox has open algo orders")

    positions = await adapter._futures_position_risk(product, symbol=symbol)
    for row in positions:
        try:
            position_amount = Decimal(str(row.get("positionAmt")))
        except InvalidOperation as exc:
            raise AssertionError(f"Malformed position amount in Binance response: {row!r}") from exc
        if not position_amount.is_finite():
            raise AssertionError(f"Non-finite position amount in Binance response: {row!r}")
        if position_amount != 0:
            pytest.skip(f"Refusing to mutate {symbol}: sandbox has a non-flat Futures position")


async def _change_remote_leverage(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    symbol: str,
    leverage: int,
) -> None:
    client = adapter._require_client()
    method_name = (
        "futures_change_leverage"
        if product == BinanceProduct.USD_M
        else "futures_coin_change_leverage"
    )
    await adapter._run_order_request(
        product,
        partial(getattr(client, method_name), symbol=symbol, leverage=leverage),
    )


async def _change_remote_margin_mode(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    symbol: str,
    mode: MarginMode,
) -> None:
    client = adapter._require_client()
    method_name = (
        "futures_change_margin_type"
        if product == BinanceProduct.USD_M
        else "futures_coin_change_margin_type"
    )
    await adapter._run_order_request(
        product,
        partial(
            getattr(client, method_name),
            symbol=symbol,
            marginType="CROSSED" if mode is MarginMode.CROSS else "ISOLATED",
        ),
    )


@pytest.mark.parametrize(("product", "symbol"), _FUTURES_CASES)
async def test_futures_config_mutations_reconcile_drift_and_reapply_on_connect(
    product: BinanceProduct,
    symbol: str,
    binance_config: BinanceConfig,
    event_bus: EventBus,
    futures_config_mutation_opt_in: None,
) -> None:
    """Exercise real Binance sandbox changes, recovery, and state persistence."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    instrument = _instrument(product, symbol)
    adapter = BinanceAdapter(binance_config, event_bus=event_bus)
    adapter.attach_state_store(store)
    leverage_storage_key = BinanceAdapter._leverage_key(product, symbol)
    margin_storage_key = BinanceAdapter._margin_mode_key(product, symbol)
    drift_events: list[FuturesSettingDriftEvent] = []
    event_bus.subscribe(FuturesSettingDriftEvent, drift_events.append)
    original_leverage: int | None = None
    original_margin_mode: MarginMode | None = None
    cleanup_errors: list[Exception] = []

    try:
        await adapter.connect()
        client = adapter._require_client()
        if client.testnet != binance_config.testnet or client.demo != binance_config.demo:
            raise AssertionError("Connected SDK client does not match the selected sandbox")
        if not client.testnet and not client.demo:
            raise AssertionError("Refusing to change Futures settings on production")

        await _require_clean_symbol(adapter, product, symbol)
        original_leverage = await adapter.get_leverage(instrument)
        original_margin_mode = await adapter.get_margin_mode(instrument)
        if original_leverage is None or original_margin_mode is None:
            pytest.skip(
                f"Binance does not report restorable leverage and margin mode for {symbol}; "
                "refusing to change sandbox settings without known original values"
            )

        target_leverage = 2 if original_leverage != 2 else 3
        drift_leverage = 3 if target_leverage == 2 else 2
        target_margin_mode = (
            MarginMode.ISOLATED if original_margin_mode is MarginMode.CROSS else MarginMode.CROSS
        )
        drift_margin_mode = original_margin_mode

        await adapter.set_leverage(instrument, target_leverage, on_drift="reapply")
        await adapter.set_margin_mode(instrument, target_margin_mode, on_drift="reapply")
        assert await adapter.get_leverage(instrument) == target_leverage
        assert await adapter.get_margin_mode(instrument) is target_margin_mode
        assert await store.get_adapter_config(leverage_storage_key) == str(target_leverage)
        assert await store.get_adapter_config(margin_storage_key) == target_margin_mode.value
        assert (
            await store.get_adapter_config(
                BinanceAdapter._leverage_policy_key("on_drift", product, symbol)
            )
            == "reapply"
        )
        assert (
            await store.get_adapter_config(
                BinanceAdapter._margin_mode_policy_key("on_drift", product, symbol)
            )
            == "reapply"
        )

        await _change_remote_leverage(adapter, product, symbol, drift_leverage)
        await _change_remote_margin_mode(adapter, product, symbol, drift_margin_mode)
        assert await adapter.get_leverage(instrument) == drift_leverage
        assert await adapter.get_margin_mode(instrument) is drift_margin_mode

        await adapter.reconcile_user_intent()
        assert await adapter.get_leverage(instrument) == target_leverage
        assert await adapter.get_margin_mode(instrument) is target_margin_mode
        assert {
            event.setting: event.action_taken
            for event in drift_events
            if event.instrument == instrument
        } == {"leverage": "reapplied", "margin_mode": "reapplied"}

        await _change_remote_leverage(adapter, product, symbol, drift_leverage)
        await _change_remote_margin_mode(adapter, product, symbol, drift_margin_mode)
        assert await adapter.get_leverage(instrument) == drift_leverage
        assert await adapter.get_margin_mode(instrument) is drift_margin_mode

        await adapter.disconnect()
        adapter = BinanceAdapter(binance_config, event_bus=event_bus)
        adapter.attach_state_store(store)
        await adapter.connect()
        assert await adapter.get_leverage(instrument) == target_leverage
        assert await adapter.get_margin_mode(instrument) is target_margin_mode
    finally:
        if original_leverage is not None and original_margin_mode is not None:
            if not adapter.is_connected:
                try:
                    await adapter.connect()
                except Exception as exc:
                    cleanup_errors.append(exc)
            if adapter.is_connected:
                for restore in (
                    partial(adapter.set_leverage, instrument, original_leverage),
                    partial(adapter.set_margin_mode, instrument, original_margin_mode),
                ):
                    try:
                        await restore()
                    except Exception as exc:
                        cleanup_errors.append(exc)

        for key in (
            leverage_storage_key,
            margin_storage_key,
            BinanceAdapter._leverage_policy_key("on_drift", product, symbol),
            BinanceAdapter._leverage_policy_key("auto_apply_on_connect", product, symbol),
            BinanceAdapter._margin_mode_policy_key("on_drift", product, symbol),
            BinanceAdapter._margin_mode_policy_key("auto_apply_on_connect", product, symbol),
        ):
            try:
                await store.delete_adapter_config(key)
            except Exception as exc:
                cleanup_errors.append(exc)

        try:
            await adapter.disconnect()
        except Exception as exc:
            cleanup_errors.append(exc)
        try:
            await store.close()
        except Exception as exc:
            cleanup_errors.append(exc)
        event_bus.unsubscribe(FuturesSettingDriftEvent, drift_events.append)

        if cleanup_errors:
            message = f"Failed to restore Binance sandbox Futures configuration: {cleanup_errors!r}"
            active_error = sys.exception()
            if active_error is not None:
                active_error.add_note(message)
            else:
                raise AssertionError(message) from cleanup_errors[0]

"""Tests for persisted Binance USDⓈ-M and COIN-M configuration."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceProduct,
    MarginMode,
    PositionMode,
)
from unified_trading_execution.binance.events import (
    FuturesMarginModeChangedEvent,
    FuturesSettingAppliedEvent,
    FuturesSettingApplyFailedEvent,
    FuturesSettingDriftEvent,
)
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _instrument(*, spot: bool = False) -> Instrument:
    if spot:
        return Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            platform_symbol="BTCUSDT",
        )
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        platform_symbol="BTCUSDT",
        multiplier=1,
    )


def _coin_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        platform_symbol="BTCUSD_PERP",
        multiplier=1,
    )


@pytest.fixture
async def config_adapter(
    adapter: Any,
    binance_client_mock: Any,
) -> AsyncIterator[tuple[Any, SQLiteStateStore, Instrument]]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    instrument = _instrument()
    adapter.attach_state_store(store)
    adapter._client = binance_client_mock
    adapter._connected = True
    adapter._exchange_info_cache[BinanceProduct.USD_M] = [
        {
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
        }
    ]
    adapter._symbol_instruments[(BinanceProduct.USD_M, "BTCUSDT")] = instrument
    adapter.fetch_instrument_spec = AsyncMock()
    yield adapter, store, instrument
    adapter._client = None
    adapter._connected = False
    await store.close()


def _risk(
    leverage: str = "10",
    *,
    position_amt: str = "0",
    isolated: bool = False,
) -> list[dict[str, object]]:
    return [
        {
            "symbol": "BTCUSDT",
            "leverage": leverage,
            "positionAmt": position_amt,
            "isolated": isolated,
        }
    ]


def _coin_risk(
    leverage: str = "10",
    *,
    position_amt: str = "0",
    isolated: bool = False,
) -> list[dict[str, object]]:
    entries = _risk(leverage, position_amt=position_amt, isolated=isolated)
    entries[0]["symbol"] = "BTCUSD_PERP"
    return entries


def _allow_empty_config_changes(client: Any) -> None:
    client.futures_get_open_orders.return_value = []
    client.futures_get_open_algo_orders.return_value = []
    client.futures_position_information.return_value = _risk()
    client.futures_coin_get_open_orders.return_value = []
    client.futures_coin_position_information.return_value = []


async def test_set_leverage_persists_product_and_symbol_scoped_intent(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, instrument = config_adapter
    _allow_empty_config_changes(binance_client_mock)
    binance_client_mock.futures_position_information.return_value = _risk("10")
    binance_client_mock.futures_change_leverage.return_value = {
        "symbol": "BTCUSDT",
        "leverage": 20,
        "maxNotionalValue": "1000000",
    }

    await adapter.set_leverage(instrument, 20, on_drift="notify")

    binance_client_mock.futures_change_leverage.assert_awaited_once_with(
        symbol="BTCUSDT",
        leverage=20,
    )
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT")
        )
        == "20"
    )
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_policy_key("on_drift", BinanceProduct.USD_M, "BTCUSDT")
        )
        == "notify"
    )


async def test_coin_m_leverage_uses_coin_product_methods_and_key(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, _ = config_adapter
    instrument = _coin_instrument()
    adapter._exchange_info_cache[BinanceProduct.COIN_M] = [
        {
            "symbol": "BTCUSD_PERP",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "contractSize": "1",
            "contractType": "PERPETUAL",
            "status": "TRADING",
        }
    ]
    adapter._symbol_instruments[(BinanceProduct.COIN_M, "BTCUSD_PERP")] = instrument
    binance_client_mock.futures_coin_position_information.return_value = _coin_risk("10")
    binance_client_mock.futures_coin_get_open_orders.return_value = []
    binance_client_mock.futures_coin_change_leverage.return_value = {
        "symbol": "BTCUSD_PERP",
        "leverage": 20,
        "maxQty": "1000",
    }

    await adapter.set_leverage(instrument, 20)

    binance_client_mock.futures_coin_change_leverage.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        leverage=20,
    )
    binance_client_mock.futures_change_leverage.assert_not_awaited()
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_key(BinanceProduct.COIN_M, "BTCUSD_PERP")
        )
        == "20"
    )


async def test_get_symbol_config_ignores_other_symbols_in_position_risk_response(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, _, instrument = config_adapter
    binance_client_mock.futures_position_information.return_value = [
        {
            "symbol": "ETHUSDT",
            "leverage": "5",
            "marginType": "cross",
            "positionAmt": "0",
        },
        {
            "symbol": "BTCUSDT",
            "leverage": "10",
            "marginType": "isolated",
            "positionAmt": "0",
        },
    ]

    assert await adapter.get_leverage(instrument) == 10
    assert await adapter.get_margin_mode(instrument) is MarginMode.ISOLATED


async def test_leverage_change_is_blocked_by_open_order_without_persisting(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, instrument = config_adapter
    binance_client_mock.futures_position_information.return_value = _risk("10")
    binance_client_mock.futures_get_open_orders.return_value = [{"orderId": 1}]
    binance_client_mock.futures_get_open_algo_orders.return_value = []

    with pytest.raises(PlatformError, match="open orders"):
        await adapter.set_leverage(instrument, 20)

    binance_client_mock.futures_change_leverage.assert_not_awaited()
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT")
        )
        is None
    )


async def test_leverage_change_is_blocked_by_non_flat_position(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, instrument = config_adapter
    binance_client_mock.futures_position_information.return_value = _risk(
        "10",
        position_amt="0.01",
    )
    binance_client_mock.futures_get_open_orders.return_value = []
    binance_client_mock.futures_get_open_algo_orders.return_value = []

    with pytest.raises(PlatformError, match="position is open"):
        await adapter.set_leverage(instrument, 20)

    binance_client_mock.futures_change_leverage.assert_not_awaited()
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT")
        )
        is None
    )


async def test_same_leverage_is_persisted_without_changing_live_state_with_open_order(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, instrument = config_adapter
    binance_client_mock.futures_position_information.return_value = _risk("20")
    binance_client_mock.futures_get_open_orders.return_value = [{"orderId": 1}]

    await adapter.set_leverage(instrument, 20)

    binance_client_mock.futures_change_leverage.assert_not_awaited()
    assert (
        await store.get_adapter_config(
            BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT")
        )
        == "20"
    )


async def test_set_margin_mode_guards_changes_and_confirms_readback(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
    event_bus: Any,
) -> None:
    adapter, store, instrument = config_adapter
    _allow_empty_config_changes(binance_client_mock)
    binance_client_mock.futures_position_information.side_effect = [
        _risk("10", isolated=False),
        _risk("10", isolated=False),
        _risk("10", isolated=True),
    ]
    binance_client_mock.futures_change_margin_type.return_value = {
        "code": 200,
        "msg": "success",
    }
    changed: list[Any] = []
    event_bus.subscribe(FuturesMarginModeChangedEvent, changed.append)

    await adapter.set_margin_mode(instrument, MarginMode.ISOLATED)

    binance_client_mock.futures_change_margin_type.assert_awaited_once_with(
        symbol="BTCUSDT",
        marginType="ISOLATED",
    )
    assert await store.get_adapter_config(
        BinanceAdapter._margin_mode_key(BinanceProduct.USD_M, "BTCUSDT")
    ) == ("isolated")
    assert len(changed) == 1


async def test_coin_m_margin_mode_uses_product_specific_endpoint(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, _ = config_adapter
    instrument = _coin_instrument()
    adapter._exchange_info_cache[BinanceProduct.COIN_M] = [
        {
            "symbol": "BTCUSD_PERP",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "contractSize": "1",
            "contractType": "PERPETUAL",
            "status": "TRADING",
        }
    ]
    adapter._symbol_instruments[(BinanceProduct.COIN_M, "BTCUSD_PERP")] = instrument
    binance_client_mock.futures_coin_position_information.side_effect = [
        _coin_risk("10", isolated=False),
        _coin_risk("10", isolated=False),
        _coin_risk("10", isolated=True),
    ]
    binance_client_mock.futures_coin_get_open_orders.return_value = []
    binance_client_mock.futures_coin_change_margin_type.return_value = {
        "code": 200,
        "msg": "success",
    }

    await adapter.set_margin_mode(instrument, MarginMode.ISOLATED)

    binance_client_mock.futures_coin_change_margin_type.assert_awaited_once_with(
        symbol="BTCUSD_PERP",
        marginType="ISOLATED",
    )
    binance_client_mock.futures_change_margin_type.assert_not_awaited()
    assert (
        await store.get_adapter_config(
            BinanceAdapter._margin_mode_key(BinanceProduct.COIN_M, "BTCUSD_PERP")
        )
        == "isolated"
    )


async def test_margin_mode_no_change_error_is_accepted_only_after_readback(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, instrument = config_adapter
    binance_client_mock.futures_position_information.side_effect = [
        [],
        [],
        _risk("10", isolated=False),
    ]
    binance_client_mock.futures_get_open_orders.return_value = []
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    binance_client_mock.futures_change_margin_type.side_effect = PlatformError(
        "No need to change margin type",
        platform_error={"code": -4046},
    )

    await adapter.set_margin_mode(instrument, MarginMode.CROSS)

    assert await store.get_adapter_config(
        BinanceAdapter._margin_mode_key(BinanceProduct.USD_M, "BTCUSDT")
    ) == ("cross")


async def test_account_position_mode_persists_once_after_safe_change(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, _ = config_adapter
    _allow_empty_config_changes(binance_client_mock)
    binance_client_mock.futures_get_position_mode.side_effect = [
        {"dualSidePosition": False},
        {"dualSidePosition": True},
    ]
    binance_client_mock.futures_coin_get_position_mode.side_effect = [
        {"dualSidePosition": False},
        {"dualSidePosition": True},
    ]
    binance_client_mock.futures_change_position_mode.return_value = {
        "code": 200,
        "msg": "success",
    }

    await adapter.set_position_mode(PositionMode.HEDGE)

    binance_client_mock.futures_change_position_mode.assert_awaited_once_with(
        dualSidePosition=True,
    )
    assert await store.get_adapter_config(BinanceAdapter._position_mode_key()) == "hedge"
    binance_client_mock.futures_get_open_orders.assert_awaited_once_with()
    binance_client_mock.futures_coin_get_open_orders.assert_awaited_once_with()


async def test_position_mode_no_change_error_requires_confirming_readback(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, _ = config_adapter
    binance_client_mock.futures_get_position_mode.side_effect = [
        {"dualSidePosition": True},
        {"dualSidePosition": False},
    ]
    binance_client_mock.futures_coin_get_position_mode.side_effect = [
        {"dualSidePosition": True},
        {"dualSidePosition": False},
    ]
    _allow_empty_config_changes(binance_client_mock)
    binance_client_mock.futures_change_position_mode.side_effect = PlatformError(
        "No need to change position side",
        platform_error={"code": -4059},
    )

    await adapter.set_position_mode(PositionMode.ONE_WAY)

    assert await store.get_adapter_config(BinanceAdapter._position_mode_key()) == "one_way"


async def test_account_position_mode_change_is_blocked_by_coin_m_open_order(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, store, _ = config_adapter
    binance_client_mock.futures_get_position_mode.return_value = {"dualSidePosition": False}
    binance_client_mock.futures_coin_get_position_mode.return_value = {"dualSidePosition": False}
    binance_client_mock.futures_get_open_orders.return_value = []
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    binance_client_mock.futures_position_information.return_value = []
    binance_client_mock.futures_coin_get_open_orders.return_value = [{"orderId": 2}]

    with pytest.raises(PlatformError, match="COIN-M open orders"):
        await adapter.set_position_mode(PositionMode.HEDGE)

    binance_client_mock.futures_change_position_mode.assert_not_awaited()
    assert await store.get_adapter_config(BinanceAdapter._position_mode_key()) is None


async def test_get_position_mode_rejects_usd_m_coin_m_divergence(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
) -> None:
    adapter, _, _ = config_adapter
    binance_client_mock.futures_get_position_mode.return_value = {"dualSidePosition": False}
    binance_client_mock.futures_coin_get_position_mode.return_value = {"dualSidePosition": True}

    with pytest.raises(PlatformError, match="position modes disagree"):
        await adapter.get_position_mode()


async def test_spot_configuration_is_rejected(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
) -> None:
    adapter, _, _ = config_adapter

    with pytest.raises(InvalidSymbolError, match="does not support Spot"):
        await adapter.set_leverage(_instrument(spot=True), 5)


async def test_startup_reapply_halts_when_open_orders_block_stored_leverage(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
    event_bus: Any,
) -> None:
    adapter, store, instrument = config_adapter
    halt_machine = HaltStateMachine()
    adapter.attach_halt_machine(halt_machine)
    await store.set_adapter_config(
        BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT"), "20"
    )
    binance_client_mock.futures_position_information.return_value = _risk("10")
    binance_client_mock.futures_get_open_orders.return_value = [{"orderId": 1}]
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    failures: list[Any] = []
    event_bus.subscribe(FuturesSettingApplyFailedEvent, failures.append)

    await adapter._reapply_stored_intent()

    assert halt_machine.is_instrument_halted(instrument)
    assert len(failures) == 1
    assert failures[0].setting == "leverage"
    binance_client_mock.futures_change_leverage.assert_not_awaited()


async def test_reconcile_halts_on_leverage_drift_by_default(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
    event_bus: Any,
) -> None:
    adapter, store, instrument = config_adapter
    halt_machine = HaltStateMachine()
    adapter.attach_halt_machine(halt_machine)
    await store.set_adapter_config(
        BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT"), "20"
    )
    binance_client_mock.futures_position_information.return_value = _risk("10")
    drifts: list[Any] = []
    event_bus.subscribe(FuturesSettingDriftEvent, drifts.append)

    await adapter.reconcile_user_intent()

    assert halt_machine.is_instrument_halted(instrument)
    assert len(drifts) == 1
    assert drifts[0].action_taken == "halted"
    binance_client_mock.futures_change_leverage.assert_not_awaited()


async def test_reconcile_reapplies_leverage_only_after_safe_guard(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
    event_bus: Any,
) -> None:
    adapter, store, _ = config_adapter
    await store.set_adapter_config(
        BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT"), "20"
    )
    await store.set_adapter_config(
        BinanceAdapter._leverage_policy_key("on_drift", BinanceProduct.USD_M, "BTCUSDT"),
        "reapply",
    )
    binance_client_mock.futures_position_information.side_effect = [
        _risk("10"),
        _risk("10"),
        _risk("10"),
        _risk("20"),
    ]
    binance_client_mock.futures_get_open_orders.return_value = []
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    binance_client_mock.futures_change_leverage.return_value = {
        "symbol": "BTCUSDT",
        "leverage": 20,
        "maxNotionalValue": "1000000",
    }
    drifts: list[Any] = []
    event_bus.subscribe(FuturesSettingDriftEvent, drifts.append)

    await adapter.reconcile_user_intent()

    binance_client_mock.futures_change_leverage.assert_awaited_once_with(
        symbol="BTCUSDT",
        leverage=20,
    )
    assert len(drifts) == 1
    assert drifts[0].action_taken == "reapplied"


async def test_startup_reapply_publishes_applied_event_when_value_matches(
    config_adapter: tuple[Any, SQLiteStateStore, Instrument],
    binance_client_mock: Any,
    event_bus: Any,
) -> None:
    adapter, store, instrument = config_adapter
    await store.set_adapter_config(
        BinanceAdapter._leverage_key(BinanceProduct.USD_M, "BTCUSDT"), "10"
    )
    binance_client_mock.futures_position_information.return_value = _risk("10")
    applied: list[Any] = []
    event_bus.subscribe(FuturesSettingAppliedEvent, applied.append)

    await adapter._reapply_stored_intent()

    assert len(applied) == 1
    assert applied[0].setting == "leverage"
    assert applied[0].instrument == instrument
    binance_client_mock.futures_change_leverage.assert_not_awaited()

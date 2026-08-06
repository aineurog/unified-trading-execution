"""Unit tests for adapter-owned user-intent reconciliation (Phase 6, Step 11).

``reconcile_user_intent`` enumerates every instrument with stored leverage /
margin-mode intent, queries the platform, and applies the configured
``on_drift`` behavior.  Recovery clears any residual drift halt.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.events import LeverageDriftEvent
from unified_trading_execution.bybit.margin import LeverageConfig
from unified_trading_execution.events import EventBus, HaltClearedEvent, HaltEnteredEvent
from unified_trading_execution.state.halt import HaltConfig, HaltStateMachine
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _futures_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="USDT",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _spec_response() -> tuple[dict[str, Any], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "status": "Trading",
                        "lotSizeFilter": {
                            "qtyStep": "0.001",
                            "minOrderQty": "0.001",
                            "maxOrderQty": "1000",
                            "minNotionalValue": "5",
                        },
                        "priceFilter": {
                            "tickSize": "0.10",
                            "minPrice": "0.10",
                            "maxPrice": "1999999.80",
                        },
                        "leverageFilter": {
                            "minLeverage": "1",
                            "maxLeverage": "100",
                            "leverageStep": "0.01",
                        },
                    }
                ]
            },
        },
        None,
        {},
    )


def _position(
    leverage: str = "10",
    trade_mode: str = "0",
) -> tuple[dict[str, Any], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "size": "0",
                        "leverage": leverage,
                        "tradeMode": trade_mode,
                    }
                ]
            },
        },
        None,
        {},
    )


def _ok() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {}}, None, {})


def _registry_refresh_side_effect(**kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
    if kwargs.get("symbol"):
        return _spec_response()
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": []}}, None, {})


class _Collector:
    def __init__(self, bus: EventBus) -> None:
        self.events: list[Any] = []
        for event_type in (LeverageDriftEvent, HaltEnteredEvent, HaltClearedEvent):
            bus.subscribe(event_type, self.events.append)

    def of_type(self, event_type: type[Any]) -> list[Any]:
        return [e for e in self.events if isinstance(e, event_type)]


async def _make_adapter(
    *,
    on_drift: str = "reapply",
    auto_halt_enabled: bool = True,
    seed_leverage: str | None = None,
    seed_margin: str | None = None,
) -> tuple[BybitAdapter, SQLiteStateStore, HaltStateMachine, _Collector]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    bus = EventBus()
    config = BybitConfig(
        testnet=True,
        api_key="k",
        api_secret="s",
        leverage=LeverageConfig(on_drift=on_drift),
    )
    adapter = BybitAdapter(config, event_bus=bus, state_store=store)
    adapter._instruments = {("linear", "BTCUSDT"): _futures_instrument()}
    halt_machine = HaltStateMachine(HaltConfig(auto_halt_enabled=auto_halt_enabled))
    adapter.attach_halt_machine(halt_machine)
    if seed_leverage is not None:
        await store.set_adapter_config("leverage.BTCUSDT", seed_leverage)
    if seed_margin is not None:
        await store.set_adapter_config("margin_mode.BTCUSDT", seed_margin)
    return adapter, store, halt_machine, _Collector(bus)


class TestReconcileLeverageDrift:
    async def test_match_is_noop(self, mock_pybit_http: MagicMock) -> None:
        adapter, store, halt_machine, collector = await _make_adapter(seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="10")
            await adapter.reconcile_user_intent()
            assert collector.of_type(LeverageDriftEvent) == []
            assert halt_machine.active_halts() == []
        finally:
            await store.close()

    async def test_reapply_restores_stored_leverage(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _, collector = await _make_adapter(seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="50")
            mock_pybit_http.get_instruments_info.return_value = _spec_response()
            mock_pybit_http.set_leverage.return_value = _ok()

            await adapter.reconcile_user_intent()

            mock_pybit_http.set_leverage.assert_called_once_with(
                category="linear",
                symbol="BTCUSDT",
                buyLeverage="10",
                sellLeverage="10",
            )
            drift = collector.of_type(LeverageDriftEvent)
            assert len(drift) == 1
            assert drift[0].action_taken == "reapplied"
            assert drift[0].stored_leverage == 10
            assert drift[0].platform_leverage == 50
        finally:
            await store.close()

    async def test_notify_does_not_touch_platform(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, halt_machine, collector = await _make_adapter(
            on_drift="notify", seed_leverage="10"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="50")
            await adapter.reconcile_user_intent()
            mock_pybit_http.set_leverage.assert_not_called()
            drift = collector.of_type(LeverageDriftEvent)
            assert len(drift) == 1
            assert drift[0].action_taken == "notified"
            assert halt_machine.active_halts() == []
        finally:
            await store.close()

    async def test_halt_enters_instrument_halt(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, halt_machine, collector = await _make_adapter(
            on_drift="halt", seed_leverage="10"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="50")
            await adapter.reconcile_user_intent()
            assert len(halt_machine.active_halts()) == 1
            drift = collector.of_type(LeverageDriftEvent)
            assert len(drift) == 1
            assert drift[0].action_taken == "halted"
            entered = collector.of_type(HaltEnteredEvent)
            assert len(entered) == 1
            assert entered[0].scope == "instrument"
        finally:
            await store.close()

    async def test_recovery_clears_drift_halt(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, halt_machine, collector = await _make_adapter(
            on_drift="halt", seed_leverage="10"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="50")
            await adapter.reconcile_user_intent()
            assert len(halt_machine.active_halts()) == 1

            # Platform now matches stored intent.
            mock_pybit_http.get_positions.return_value = _position(leverage="10")
            await adapter.reconcile_user_intent()

            assert halt_machine.active_halts() == []
            cleared = collector.of_type(HaltClearedEvent)
            assert len(cleared) == 1
            assert cleared[0].scope == "instrument"
        finally:
            await store.close()


class TestReconcileMarginModeDrift:
    async def test_margin_mode_drift_reapplies(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _, _ = await _make_adapter(seed_leverage="20", seed_margin="isolated")
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="20", trade_mode="0")
            mock_pybit_http.switch_margin_mode.return_value = _ok()
            await adapter.reconcile_user_intent()
            mock_pybit_http.switch_margin_mode.assert_called_once_with(
                category="linear",
                symbol="BTCUSDT",
                tradeMode=1,
                buyLeverage="20",
                sellLeverage="20",
            )
        finally:
            await store.close()


class TestReconcileNoIntent:
    async def test_no_intent_is_noop(self, mock_pybit_http: MagicMock) -> None:
        adapter, store, _, _ = await _make_adapter()
        try:
            await adapter.reconcile_user_intent()
            mock_pybit_http.get_positions.assert_not_called()
        finally:
            await store.close()

    async def test_attach_halt_machine_stores_machine(self) -> None:
        adapter, store, _, _ = await _make_adapter()
        try:
            machine = HaltStateMachine()
            adapter.attach_halt_machine(machine)
            assert adapter._halt_machine is machine
        finally:
            await store.close()

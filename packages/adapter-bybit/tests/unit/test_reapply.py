"""Unit tests for connect-time leverage / margin-mode reapply (Phase 4, Step 7).

Seeds stored intent in a real in-memory SQLiteStateStore, connects the adapter
(with HTTP and WebSocket mocked), and verifies the platform calls and emitted
events.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.events import (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    MarginModeChangedEvent,
)
from unified_trading_execution.bybit.margin import LeverageConfig, MarginMode
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _linear_instrument() -> Instrument:
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


def _flat_position() -> tuple[dict[str, Any], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [{"symbol": "BTCUSDT", "size": "0", "tradeMode": "0"}]},
        },
        None,
        {},
    )


def _ok() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {}}, None, {})


def _registry_refresh_side_effect(**kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
    """Return a spec only for symbol-scoped queries; empty for registry scans.

    ``_refresh_instrument_registry`` polls every category without a symbol, so
    returning the same BTCUSDT listing for all three would make the last
    (inverse) translation win the symbol->instrument map.  The registry scan
    therefore sees empty listings (the test pre-seeds ``_instruments``), and
    only the symbol-scoped ``fetch_instrument_spec`` gets the spec.
    """
    if kwargs.get("symbol"):
        return _spec_response()
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": []}}, None, {})


class _Collector:
    def __init__(self, event_bus: EventBus) -> None:
        self.events: list[Any] = []
        for event_type in (
            LeverageAppliedEvent,
            LeverageApplyFailedEvent,
            MarginModeChangedEvent,
            ConnectionStateEvent,
        ):
            event_bus.subscribe(event_type, self.events.append)

    def of_type(self, event_type: type[Any]) -> list[Any]:
        return [event for event in self.events if isinstance(event, event_type)]


@pytest.fixture
async def reapply_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
) -> tuple[BybitAdapter, SQLiteStateStore, _Collector]:
    """Connected adapter + store + event collector for one reapply scenario.

    The store is pre-initialized; the test seeds intent keys before connecting.
    The instrument registry is seeded directly so reapply can resolve the
    stored Bybit symbol to a canonical Instrument.
    """
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    config = BybitConfig(
        api_key=bybit_config.api_key,
        api_secret=bybit_config.api_secret,
        testnet=bybit_config.testnet,
        leverage=LeverageConfig(),
    )
    adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
    adapter._instruments = {("linear", "BTCUSDT"): _linear_instrument()}
    collector = _Collector(event_bus)
    yield adapter, store, collector
    await store.close()


class TestReapplyStoredIntent:
    async def test_reapply_leverage_only_on_connect(
        self,
        reapply_adapter: tuple[BybitAdapter, SQLiteStateStore, _Collector],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = reapply_adapter
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        mock_pybit_http.get_instruments_info.side_effect = _registry_refresh_side_effect
        mock_pybit_http.get_positions.return_value = _flat_position()
        mock_pybit_http.set_leverage.return_value = _ok()

        await adapter.connect()

        mock_pybit_http.set_leverage.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            buyLeverage="10",
            sellLeverage="10",
        )
        applied = collector.of_type(LeverageAppliedEvent)
        assert len(applied) == 1
        assert applied[0].leverage == 10
        assert collector.of_type(LeverageApplyFailedEvent) == []
        await adapter.disconnect()

    async def test_reapply_margin_mode_and_leverage_on_connect(
        self,
        reapply_adapter: tuple[BybitAdapter, SQLiteStateStore, _Collector],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = reapply_adapter
        await store.set_adapter_config("leverage.BTCUSDT", "20")
        await store.set_adapter_config("margin_mode.BTCUSDT", "isolated")
        mock_pybit_http.get_positions.return_value = _flat_position()
        mock_pybit_http.switch_margin_mode.return_value = _ok()

        await adapter.connect()

        mock_pybit_http.switch_margin_mode.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            tradeMode=1,
            buyLeverage="20",
            sellLeverage="20",
        )
        assert len(collector.of_type(LeverageAppliedEvent)) == 1
        changed = collector.of_type(MarginModeChangedEvent)
        assert len(changed) == 1
        assert changed[0].current is MarginMode.ISOLATED
        assert collector.of_type(LeverageApplyFailedEvent) == []
        await adapter.disconnect()

    async def test_reapply_failure_emits_event_without_crashing(
        self,
        reapply_adapter: tuple[BybitAdapter, SQLiteStateStore, _Collector],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = reapply_adapter
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        mock_pybit_http.get_instruments_info.side_effect = _registry_refresh_side_effect
        mock_pybit_http.get_positions.return_value = _flat_position()
        mock_pybit_http.set_leverage.side_effect = FailedRequestError(
            request="POST /v5/position/set-leverage",
            message="Invalid leverage",
            status_code=12222,
            time="12:00:00",
            resp_headers=None,
        )

        # Connect must succeed and the failure surfaces as an event.
        await adapter.connect()

        assert adapter.is_connected is True
        failed = collector.of_type(LeverageApplyFailedEvent)
        assert len(failed) == 1
        assert failed[0].leverage == 10
        assert "Invalid leverage" in failed[0].reason
        assert collector.of_type(LeverageAppliedEvent) == []
        await adapter.disconnect()

    async def test_reapply_skips_unknown_symbol(
        self,
        reapply_adapter: tuple[BybitAdapter, SQLiteStateStore, _Collector],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = reapply_adapter
        # Stored intent for a symbol not in the registry (delisted / never listed).
        await store.set_adapter_config("leverage.NOPEUSDT", "10")

        await adapter.connect()

        assert adapter.is_connected is True
        mock_pybit_http.set_leverage.assert_not_called()
        assert collector.of_type(LeverageAppliedEvent) == []
        assert collector.of_type(LeverageApplyFailedEvent) == []
        await adapter.disconnect()

    async def test_reapply_skipped_when_auto_apply_disabled(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        store = SQLiteStateStore(":memory:")
        await store.initialize()
        await store.set_adapter_config("leverage.BTCUSDT", "10")
        try:
            config = BybitConfig(
                api_key=bybit_config.api_key,
                api_secret=bybit_config.api_secret,
                testnet=bybit_config.testnet,
                leverage=LeverageConfig(auto_apply_on_connect=False),
            )
            adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
            adapter._instruments = {("linear", "BTCUSDT"): _linear_instrument()}

            await adapter.connect()
        finally:
            await store.close()

        mock_pybit_http.set_leverage.assert_not_called()
        await adapter.disconnect()

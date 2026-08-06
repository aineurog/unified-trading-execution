"""Unit tests for pre-order leverage verification (Phase 5, Step 9).

``strict_check`` queries the platform's current leverage before every order
dispatch and rejects the order (``LeverageDriftError``) when it differs from
stored intent.  With ``on_drift="reapply"`` the stored leverage is restored and
the order proceeds; with no stored intent the check is skipped entirely.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import LeverageDriftError
from unified_trading_execution.bybit.events import LeverageDriftEvent
from unified_trading_execution.bybit.margin import LeverageConfig
from unified_trading_execution.events import EventBus
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder


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


def _position(leverage: str) -> tuple[dict[str, Any], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [{"symbol": "BTCUSDT", "size": "0", "leverage": leverage}]},
        },
        None,
        {},
    )


def _no_position() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": []}}, None, {})


def _ok() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {}}, None, {})


def _registry_refresh_side_effect(**kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
    """Return a spec only for symbol-scoped queries; empty for registry scans."""
    if kwargs.get("symbol"):
        return _spec_response()
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": []}}, None, {})


def _make_order() -> UnifiedOrder:
    return UnifiedOrder(
        instrument=_futures_instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        price=Decimal("100"),
        time_in_force=TimeInForce.GTC,
        client_order_id="c1",
    )


def _order_entry() -> dict[str, str]:
    return {
        "orderId": "o1",
        "orderLinkId": "c1",
        "symbol": "BTCUSDT",
        "price": "100",
        "qty": "0.001",
        "orderStatus": "New",
        "orderType": "Limit",
        "side": "Buy",
    }


async def _new_adapter(
    *,
    strict_check: bool,
    on_drift: Literal["reapply", "notify", "halt"] = "reapply",
    seed_leverage: str | None = "10",
) -> tuple[BybitAdapter, SQLiteStateStore, EventBus]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    config = BybitConfig(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=True,
        leverage=LeverageConfig(strict_check=strict_check, on_drift=on_drift),
    )
    event_bus = EventBus()
    adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
    adapter._instruments = {("linear", "BTCUSDT"): _futures_instrument()}
    if seed_leverage is not None:
        await store.set_adapter_config("leverage.BTCUSDT", str(seed_leverage))
    return adapter, store, event_bus


class TestStrictCheckDriftAction:
    async def test_skipped_when_no_stored_intent(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _ = await _new_adapter(strict_check=True, seed_leverage=None)
        try:
            await store.delete_adapter_config("leverage.BTCUSDT")
            mock_pybit_http.get_positions.return_value = _position("50")
            await adapter._strict_check_leverage(_futures_instrument())
            mock_pybit_http.get_positions.assert_not_called()
            mock_pybit_http.set_leverage.assert_not_called()
        finally:
            await store.close()

    async def test_no_drift_proceeds(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _ = await _new_adapter(strict_check=True, seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position("10")
            await adapter._strict_check_leverage(_futures_instrument())
            mock_pybit_http.set_leverage.assert_not_called()
        finally:
            await store.close()

    async def test_drift_reapply_restores_and_proceeds(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _ = await _new_adapter(strict_check=True, on_drift="reapply")
        try:
            mock_pybit_http.get_positions.return_value = _position("50")
            mock_pybit_http.get_instruments_info.return_value = _spec_response()
            mock_pybit_http.set_leverage.return_value = _ok()
            await adapter._strict_check_leverage(_futures_instrument())
            mock_pybit_http.set_leverage.assert_called_once_with(
                category="linear",
                symbol="BTCUSDT",
                buyLeverage="10",
                sellLeverage="10",
            )
        finally:
            await store.close()

    async def test_drift_notify_rejects_with_event(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, bus = await _new_adapter(strict_check=True, on_drift="notify")
        try:
            drift_events: list[LeverageDriftEvent] = []
            bus.subscribe(LeverageDriftEvent, drift_events.append)
            mock_pybit_http.get_positions.return_value = _position("50")
            with pytest.raises(LeverageDriftError):
                await adapter._strict_check_leverage(_futures_instrument())
            mock_pybit_http.set_leverage.assert_not_called()
            assert len(drift_events) == 1
            assert drift_events[0].action_taken == "notified"
            assert drift_events[0].stored_leverage == 10
            assert drift_events[0].platform_leverage == 50
        finally:
            await store.close()

    async def test_strict_check_disabled_skips(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _ = await _new_adapter(strict_check=False, seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position("50")
            await adapter._strict_check_leverage(_futures_instrument())
            mock_pybit_http.get_positions.assert_not_called()
            mock_pybit_http.set_leverage.assert_not_called()
        finally:
            await store.close()


class TestPlaceOrderStrictCheck:
    async def test_place_order_raises_on_drift(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = _spec_response()
        mock_pybit_http.get_positions.return_value = _position("50")
        store = SQLiteStateStore(":memory:")
        await store.initialize()
        try:
            config = BybitConfig(
                testnet=True,
                api_key="k",
                api_secret="s",
                leverage=LeverageConfig(strict_check=True, on_drift="notify"),
            )
            adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
            adapter._instruments = {("linear", "BTCUSDT"): _futures_instrument()}
            await store.set_adapter_config("leverage.BTCUSDT", "10")
            with pytest.raises(LeverageDriftError):
                await adapter.place_order(_make_order())
            mock_pybit_http.place_order.assert_not_called()
        finally:
            await store.close()

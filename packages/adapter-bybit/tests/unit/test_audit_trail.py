"""Unit tests for Phase 7 — leverage events written to the audit trail (§6.1).

Every adapter-published leverage/margin-mode event must also be appended to
the state store as an ``AuditEvent`` with the ``bybit.*`` event_type — the
adapter writes the record itself (Section 17.12 pattern for adapter-owned
events), so no emit is ever missing from the trail.
"""

from __future__ import annotations

from typing import Any, Literal
from unittest.mock import MagicMock

from pybit.exceptions import FailedRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.margin import LeverageConfig
from unified_trading_execution.events import EventBus
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


async def _make_adapter(
    bybit_config: BybitConfig,
    *,
    on_drift: Literal["reapply", "notify", "halt"] = "reapply",
    seed_leverage: str | None = None,
    seed_margin: str | None = None,
) -> tuple[BybitAdapter, SQLiteStateStore]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    config = BybitConfig(
        api_key=bybit_config.api_key,
        api_secret=bybit_config.api_secret,
        testnet=bybit_config.testnet,
        leverage=LeverageConfig(on_drift=on_drift),
    )
    adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
    adapter._instruments = {("linear", "BTCUSDT"): _linear_instrument()}
    if seed_leverage is not None:
        await store.set_adapter_config("leverage.BTCUSDT", seed_leverage)
    if seed_margin is not None:
        await store.set_adapter_config("margin_mode.BTCUSDT", seed_margin)
    return adapter, store


async def _audit_event_types(store: SQLiteStateStore) -> list[str]:
    events = await store.query_audit_events()
    return [event.event_type for event in events]


class TestReapplyAuditTrail:
    async def test_leverage_applied_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(bybit_config, seed_leverage="10")
        try:
            mock_pybit_http.get_instruments_info.side_effect = _registry_refresh_side_effect
            mock_pybit_http.get_positions.return_value = _flat_position()
            mock_pybit_http.set_leverage.return_value = _ok()

            await adapter.connect()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.leverage.applied"]
            assert len(matches) == 1
            assert matches[0].payload["symbol"] == "BTCUSDT"
            assert matches[0].payload["leverage"] == 10
            assert matches[0].adapter_name == "bybit"
        finally:
            await adapter.disconnect()
            await store.close()

    async def test_margin_mode_changed_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(
            bybit_config,
            seed_leverage="20",
            seed_margin="isolated",
        )
        try:
            mock_pybit_http.get_positions.return_value = _flat_position()
            mock_pybit_http.switch_margin_mode.return_value = _ok()

            await adapter.connect()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.margin_mode.changed"]
            assert len(matches) == 1
            assert matches[0].payload["symbol"] == "BTCUSDT"
            assert matches[0].payload["current"] == "isolated"
            assert matches[0].payload["leverage"] == 20
        finally:
            await adapter.disconnect()
            await store.close()

    async def test_apply_failed_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(bybit_config, seed_leverage="10")
        try:
            mock_pybit_http.get_instruments_info.side_effect = _registry_refresh_side_effect
            mock_pybit_http.get_positions.return_value = _flat_position()
            mock_pybit_http.set_leverage.side_effect = FailedRequestError(
                request="POST /v5/position/set-leverage",
                message="Invalid leverage",
                status_code=12222,
                time="12:00:00",
                resp_headers=None,
            )

            await adapter.connect()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.leverage.apply_failed"]
            assert len(matches) == 1
            assert matches[0].payload["symbol"] == "BTCUSDT"
            assert matches[0].payload["leverage"] == 10
            reason = matches[0].payload["reason"]
            assert isinstance(reason, str)
            assert "Invalid leverage" in reason
        finally:
            await adapter.disconnect()
            await store.close()


class TestDriftAuditTrail:
    async def test_leverage_drift_notify_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(bybit_config, on_drift="notify", seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="50")

            await adapter.reconcile_user_intent()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.leverage.drift"]
            assert len(matches) == 1
            assert matches[0].payload["symbol"] == "BTCUSDT"
            assert matches[0].payload["stored_leverage"] == 10
            assert matches[0].payload["platform_leverage"] == 50
            assert matches[0].payload["action_taken"] == "notified"
        finally:
            await store.close()

    async def test_leverage_drift_reapply_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(bybit_config, seed_leverage="10")
        try:
            mock_pybit_http.get_positions.side_effect = [
                _position(leverage="50"),  # reconcile query
                _position(leverage="50"),  # set_leverage open-position guard
            ]
            mock_pybit_http.get_instruments_info.return_value = _spec_response()
            mock_pybit_http.set_leverage.return_value = _ok()

            await adapter.reconcile_user_intent()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.leverage.drift"]
            assert len(matches) == 1
            assert matches[0].payload["action_taken"] == "reapplied"
        finally:
            await store.close()

    async def test_margin_mode_drift_reapply_written_to_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(
            bybit_config,
            seed_leverage="10",
            seed_margin="isolated",
        )
        try:
            mock_pybit_http.get_positions.side_effect = [
                _position(leverage="10", trade_mode="0"),  # get_leverage (reconcile)
                _position(leverage="10", trade_mode="0"),  # get_margin_mode (reconcile)
            ]
            mock_pybit_http.get_instruments_info.return_value = _spec_response()
            mock_pybit_http.switch_margin_mode.return_value = _ok()

            await adapter.reconcile_user_intent()

            events = await store.query_audit_events()
            matches = [e for e in events if e.event_type == "bybit.margin_mode.changed"]
            assert len(matches) == 1
            assert matches[0].payload["current"] == "isolated"
            assert matches[0].payload["previous"] == "cross"
        finally:
            await store.close()

    async def test_no_events_means_empty_audit(
        self,
        bybit_config: BybitConfig,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = await _make_adapter(bybit_config, seed_leverage="10")
        try:
            mock_pybit_http.get_positions.return_value = _position(leverage="10")

            await adapter.reconcile_user_intent()

            assert await _audit_event_types(store) == []
        finally:
            await store.close()

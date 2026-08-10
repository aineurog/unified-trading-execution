"""Unit tests for BybitAdapter position-mode operations (PLAN_feat_bybit-position-mode).

Covers set/get/remove position mode, spot rejection, the 110025 idempotent
no-op, coin-batch switching, connect-time reapply, and drift reconciliation.
HTTP is mocked via the shared ``mock_pybit_http`` fixture; intent persistence
is verified against a real in-memory SQLiteStateStore.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import InvalidRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.enums import PositionMode
from unified_trading_execution.bybit.events import (
    PositionModeAppliedEvent,
    PositionModeApplyFailedEvent,
    PositionModeDriftEvent,
)
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.events import EventBus, HaltClearedEvent, HaltEnteredEvent
from unified_trading_execution.state.halt import HaltConfig, HaltStateMachine
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder


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


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def _position_response(position_idx: str = "0") -> tuple[dict[str, Any], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{"symbol": "BTCUSDT", "size": "0", "positionIdx": position_idx}]
            },
        },
        None,
        {},
    )


def _no_position() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": []}}, None, {})


def _ok() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {}}, None, {})


@pytest.fixture
async def store_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
    mock_pybit_http: MagicMock,
) -> AsyncIterator[tuple[BybitAdapter, SQLiteStateStore]]:
    """A BybitAdapter wired to a real in-memory state store (and mocked HTTP)."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    config = BybitConfig(
        api_key=bybit_config.api_key,
        api_secret=bybit_config.api_secret,
        testnet=bybit_config.testnet,
    )
    adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
    yield adapter, store
    await store.close()


class TestSetPositionMode:
    async def test_set_one_way_calls_switch_and_persists(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode(_linear_instrument(), PositionMode.ONE_WAY)

        mock_pybit_http.switch_position_mode.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            mode=0,
        )
        assert await store.get_adapter_config("position_mode.BTCUSDT") == "one_way"

    async def test_set_accepts_raw_string(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode(_linear_instrument(), "hedge")

        mock_pybit_http.switch_position_mode.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            mode=3,
        )
        assert await store.get_adapter_config("position_mode.BTCUSDT") == "hedge"

    async def test_set_rejects_invalid_string(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter

        with pytest.raises(ValueError, match="Invalid position mode"):
            await adapter.set_position_mode(_linear_instrument(), "dual")

        mock_pybit_http.switch_position_mode.assert_not_called()
        assert await store.get_adapter_config("position_mode.BTCUSDT") is None

    async def test_set_hedge_calls_switch_and_persists(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode(_linear_instrument(), PositionMode.HEDGE)

        mock_pybit_http.switch_position_mode.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            mode=3,
        )
        assert await store.get_adapter_config("position_mode.BTCUSDT") == "hedge"

    async def test_set_persists_policy_knobs(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode(
            _linear_instrument(),
            PositionMode.HEDGE,
            on_drift="halt",
            auto_apply_on_connect=False,
        )

        assert await store.get_adapter_config("position_mode.policy.on_drift:BTCUSDT") == "halt"
        assert (
            await store.get_adapter_config("position_mode.policy.auto_apply:BTCUSDT") == "0"
        )

    async def test_set_spot_raises(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter

        with pytest.raises(InvalidSymbolError):
            await adapter.set_position_mode(_spot_instrument(), PositionMode.HEDGE)

        mock_pybit_http.switch_position_mode.assert_not_called()
        assert await store.get_adapter_config("position_mode.BTCUSDT") is None

    async def test_set_platform_rejection_propagates(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.side_effect = InvalidRequestError(
            request="POST /v5/position/switch-position-mode",
            message="Position mode not modified",
            status_code=110025,
            time="12:00:00",
            resp_headers=None,
        )

        # 110025 "not modified" is an idempotent no-op — treated as applied.
        await adapter.set_position_mode(_linear_instrument(), PositionMode.HEDGE)

        mock_pybit_http.switch_position_mode.assert_called_once()
        assert await store.get_adapter_config("position_mode.BTCUSDT") == "hedge"

    async def test_set_open_position_guard_error_propagates(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        """110030/110031 are platform-enforced guards surfaced via PlatformError."""
        adapter, store = store_adapter
        mock_pybit_http.switch_position_mode.side_effect = InvalidRequestError(
            request="POST /v5/position/switch-position-mode",
            message="You have existing position, so position mode cannot be switched",
            status_code=110030,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformError):
            await adapter.set_position_mode(_linear_instrument(), PositionMode.HEDGE)

        assert await store.get_adapter_config("position_mode.BTCUSDT") is None


class TestGetPositionMode:
    async def test_position_idx_zero_is_one_way(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = _position_response("0")

        assert await adapter.get_position_mode(_linear_instrument()) is PositionMode.ONE_WAY

    async def test_position_idx_one_is_hedge(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = _position_response("1")

        assert await adapter.get_position_mode(_linear_instrument()) is PositionMode.HEDGE

    async def test_position_idx_two_is_hedge(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = _position_response("2")

        assert await adapter.get_position_mode(_linear_instrument()) is PositionMode.HEDGE

    async def test_no_open_position_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = _no_position()

        assert await adapter.get_position_mode(_linear_instrument()) is None

    async def test_spot_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter

        assert await adapter.get_position_mode(_spot_instrument()) is None
        mock_pybit_http.get_positions.assert_not_called()


class TestRemovePositionMode:
    async def test_remove_drops_intent_and_knobs_only(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        await store.set_adapter_config("position_mode.BTCUSDT", "hedge")
        await store.set_adapter_config("position_mode.policy.on_drift:BTCUSDT", "reapply")
        await store.set_adapter_config("position_mode.policy.auto_apply:BTCUSDT", "1")

        await adapter.remove_position_mode(_linear_instrument())

        assert await store.get_adapter_config("position_mode.BTCUSDT") is None
        assert await store.get_adapter_config("position_mode.policy.on_drift:BTCUSDT") is None
        assert await store.get_adapter_config("position_mode.policy.auto_apply:BTCUSDT") is None
        mock_pybit_http.switch_position_mode.assert_not_called()


class TestSetPositionModeForCoin:
    async def test_batch_switch_passes_coin(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode_for_coin("USDT", "linear", PositionMode.HEDGE)

        mock_pybit_http.switch_position_mode.assert_called_once_with(
            category="linear",
            coin="USDT",
            mode=3,
        )

    async def test_batch_switch_accepts_raw_string(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.switch_position_mode.return_value = _ok()

        await adapter.set_position_mode_for_coin("USDT", "linear", "one_way")

        mock_pybit_http.switch_position_mode.assert_called_once_with(
            category="linear",
            coin="USDT",
            mode=0,
        )


class _Collector:
    def __init__(self, bus: EventBus) -> None:
        self.events: list[Any] = []
        for event_type in (
            PositionModeDriftEvent,
            PositionModeAppliedEvent,
            PositionModeApplyFailedEvent,
            HaltEnteredEvent,
            HaltClearedEvent,
        ):
            bus.subscribe(event_type, self.events.append)

    def of_type(self, event_type: type[Any]) -> list[Any]:
        return [e for e in self.events if isinstance(e, event_type)]


async def _make_reconcile_adapter(
    *,
    on_drift: str = "reapply",
    auto_halt_enabled: bool = True,
    seed_mode: str | None = None,
) -> tuple[BybitAdapter, SQLiteStateStore, HaltStateMachine, _Collector]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    bus = EventBus()
    config = BybitConfig(testnet=True, api_key="k", api_secret="s")
    adapter = BybitAdapter(config, event_bus=bus, state_store=store)
    adapter._instruments = {("linear", "BTCUSDT"): _linear_instrument()}
    halt_machine = HaltStateMachine(HaltConfig(auto_halt_enabled=auto_halt_enabled))
    adapter.attach_halt_machine(halt_machine)
    await store.set_adapter_config("position_mode.policy.on_drift:BTCUSDT", on_drift)
    if seed_mode is not None:
        await store.set_adapter_config("position_mode.BTCUSDT", seed_mode)
    return adapter, store, halt_machine, _Collector(bus)


class TestReconcilePositionModeDrift:
    async def test_match_is_noop(self, mock_pybit_http: MagicMock) -> None:
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("1")
            await adapter.reconcile_user_intent()
            assert collector.of_type(PositionModeDriftEvent) == []
            assert halt_machine.active_halts() == []
        finally:
            await store.close()

    async def test_reapply_restores_stored_mode(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, _, collector = await _make_reconcile_adapter(seed_mode="hedge")
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            mock_pybit_http.switch_position_mode.return_value = _ok()

            await adapter.reconcile_user_intent()

            mock_pybit_http.switch_position_mode.assert_called_once_with(
                category="linear",
                symbol="BTCUSDT",
                mode=3,
            )
            drift = collector.of_type(PositionModeDriftEvent)
            assert len(drift) == 1
            assert drift[0].stored is PositionMode.HEDGE
            assert drift[0].platform is PositionMode.ONE_WAY
            assert drift[0].action_taken == "reapplied"
        finally:
            await store.close()

    async def test_notify_does_not_touch_platform(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            on_drift="notify", seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            await adapter.reconcile_user_intent()
            mock_pybit_http.switch_position_mode.assert_not_called()
            drift = collector.of_type(PositionModeDriftEvent)
            assert len(drift) == 1
            assert drift[0].action_taken == "notified"
            assert halt_machine.active_halts() == []
        finally:
            await store.close()

    async def test_halt_enters_instrument_halt(
        self,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            on_drift="halt", seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            await adapter.reconcile_user_intent()
            assert len(halt_machine.active_halts()) == 1
            drift = collector.of_type(PositionModeDriftEvent)
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
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            on_drift="halt", seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            await adapter.reconcile_user_intent()
            assert len(halt_machine.active_halts()) == 1

            mock_pybit_http.get_positions.return_value = _position_response("1")
            await adapter.reconcile_user_intent()

            assert halt_machine.active_halts() == []
            cleared = collector.of_type(HaltClearedEvent)
            assert len(cleared) == 1
            assert cleared[0].scope == "instrument"
        finally:
            await store.close()

    async def test_no_open_position_is_skipped(self, mock_pybit_http: MagicMock) -> None:
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            on_drift="halt", seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _no_position()
            await adapter.reconcile_user_intent()
            mock_pybit_http.switch_position_mode.assert_not_called()
            assert collector.of_type(PositionModeDriftEvent) == []
            assert halt_machine.active_halts() == []
        finally:
            await store.close()

    async def test_policy_knob_rows_are_skipped(self, mock_pybit_http: MagicMock) -> None:
        adapter, store, halt_machine, collector = await _make_reconcile_adapter(
            on_drift="reapply"
        )
        try:
            await store.set_adapter_config("position_mode.policy.on_drift:BTCUSDT", "halt")
            await adapter.reconcile_user_intent()
            mock_pybit_http.get_positions.assert_not_called()
            assert collector.of_type(PositionModeDriftEvent) == []
            assert halt_machine.active_halts() == []
        finally:
            await store.close()


class TestReapplyPositionModeOnConnect:
    async def _make_connect_adapter(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        *,
        seed_mode: str | None,
        auto_apply: str | None = None,
    ) -> tuple[BybitAdapter, SQLiteStateStore, _Collector]:
        store = SQLiteStateStore(":memory:")
        await store.initialize()
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=bybit_config.testnet,
        )
        adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
        adapter._instruments = {("linear", "BTCUSDT"): _linear_instrument()}
        if seed_mode is not None:
            await store.set_adapter_config("position_mode.BTCUSDT", seed_mode)
        if auto_apply is not None:
            await store.set_adapter_config(
                "position_mode.policy.auto_apply:BTCUSDT",
                auto_apply,
            )
        return adapter, store, _Collector(event_bus)

    async def test_reapply_hedge_on_connect(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = await self._make_connect_adapter(
            bybit_config, event_bus, seed_mode="hedge"
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            mock_pybit_http.switch_position_mode.return_value = _ok()

            await adapter.connect()

            mock_pybit_http.switch_position_mode.assert_called_once_with(
                category="linear",
                symbol="BTCUSDT",
                mode=3,
            )
            applied = collector.of_type(PositionModeAppliedEvent)
            assert len(applied) == 1
            assert applied[0].mode is PositionMode.HEDGE
            assert collector.of_type(PositionModeApplyFailedEvent) == []
        finally:
            await store.close()
            await adapter.disconnect()

    async def test_reapply_skipped_when_auto_apply_false(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = await self._make_connect_adapter(
            bybit_config,
            event_bus,
            seed_mode="hedge",
            auto_apply="0",
        )
        try:
            mock_pybit_http.get_positions.return_value = _position_response("0")
            await adapter.connect()
            mock_pybit_http.switch_position_mode.assert_not_called()
            assert collector.of_type(PositionModeAppliedEvent) == []
        finally:
            await store.close()
            await adapter.disconnect()

    async def test_unknown_symbol_skipped(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = await self._make_connect_adapter(
            bybit_config, event_bus, seed_mode="hedge"
        )
        try:
            adapter._instruments = {}
            await adapter.connect()
            mock_pybit_http.switch_position_mode.assert_not_called()
            assert collector.of_type(PositionModeAppliedEvent) == []
            assert collector.of_type(PositionModeApplyFailedEvent) == []
        finally:
            await store.close()
            await adapter.disconnect()

    async def test_apply_failure_emits_failed_event(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = await self._make_connect_adapter(
            bybit_config, event_bus, seed_mode="hedge"
        )
        try:
            mock_pybit_http.switch_position_mode.side_effect = InvalidRequestError(
                request="POST /v5/position/switch-position-mode",
                message="You have existing position, so position mode cannot be switched",
                status_code=110030,
                time="12:00:00",
                resp_headers=None,
            )

            await adapter.connect()

            failed = collector.of_type(PositionModeApplyFailedEvent)
            assert len(failed) == 1
            assert failed[0].mode is PositionMode.HEDGE
            assert collector.of_type(PositionModeAppliedEvent) == []
        finally:
            await store.close()
            await adapter.disconnect()

    async def test_no_intent_is_noop(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store, collector = await self._make_connect_adapter(
            bybit_config, event_bus, seed_mode=None
        )
        try:
            await adapter.connect()
            mock_pybit_http.switch_position_mode.assert_not_called()
            assert collector.of_type(PositionModeAppliedEvent) == []
        finally:
            await store.close()
            await adapter.disconnect()


class TestResolvePositionIdx:
    """_resolve_position_idx maps stored mode + order side to positionIdx.

    Source of truth is the stored position-mode intent: hedge buys place the
    long leg (1), hedge sells the short leg (2), one-way/no intent uses 0, and
    spot instruments carry no positionIdx at all (None).
    """

    async def _seed(self, store: SQLiteStateStore, mode: str | None) -> None:
        if mode is not None:
            await store.set_adapter_config("position_mode.BTCUSDT", mode)

    async def test_hedge_buy_is_long_leg(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, store = store_adapter
        await self._seed(store, "hedge")
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.BUY) == 1

    async def test_hedge_sell_is_short_leg(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, store = store_adapter
        await self._seed(store, "hedge")
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.SELL) == 2

    async def test_one_way_uses_zero(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, store = store_adapter
        await self._seed(store, "one_way")
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.BUY) == 0

    async def test_missing_intent_defaults_to_zero(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, _ = store_adapter
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.SELL) == 0

    async def test_invalid_stored_value_defaults_to_zero(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, store = store_adapter
        await self._seed(store, "not-a-mode")
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.BUY) == 0

    async def test_spot_has_no_position_idx(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
    ) -> None:
        adapter, store = store_adapter
        await self._seed(store, "hedge")
        assert await adapter._resolve_position_idx(_spot_instrument(), OrderSide.BUY) is None

    async def test_no_state_store_defaults_to_zero(
        self,
        adapter: BybitAdapter,
    ) -> None:
        assert await adapter._resolve_position_idx(_linear_instrument(), OrderSide.BUY) == 0


class TestPlaceOrderPositionIdx:
    """place_order forwards the resolved positionIdx to the platform.

    The linear leg must be addressed correctly in hedge mode or Bybit rejects
    with ``position idx not match position mode`` (10001).
    """

    async def _place(
        self,
        adapter: BybitAdapter,
        store: SQLiteStateStore | None,
        *,
        mode: str | None,
        side: OrderSide,
        instrument: Instrument,
        mock_pybit_http: MagicMock,
    ) -> dict[str, Any]:
        if store is not None and mode is not None:
            await store.set_adapter_config("position_mode.BTCUSDT", mode)
        mock_pybit_http.place_order.return_value = (
            {"retCode": 0, "result": {"orderId": "o1", "orderLinkId": "c1"}},
            None,
            {},
        )
        mock_pybit_http.get_positions.return_value = _no_position()
        mock_pybit_http.get_open_orders.return_value = (
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "orderId": "o1",
                            "orderLinkId": "c1",
                            "symbol": "BTCUSDT",
                            "orderStatus": "New",
                            "cumExecQty": "0",
                            "avgPrice": "0",
                            "createdTime": "1684738540559",
                            "updatedTime": "1684738540561",
                        }
                    ],
                    "category": "linear",
                },
            },
            None,
            {},
        )
        order = UnifiedOrder(
            instrument=instrument,
            order_type=OrderType.MARKET,
            side=side,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.GTC,
            client_order_id="c1",
        )
        await adapter.place_order(order)
        call = mock_pybit_http.place_order.call_args
        assert call is not None and call.kwargs is not None
        return dict(call.kwargs)

    async def test_hedge_buy_payload_carries_long_leg(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        payload = await self._place(
            adapter,
            store,
            mode="hedge",
            side=OrderSide.BUY,
            instrument=_linear_instrument(),
            mock_pybit_http=mock_pybit_http,
        )
        assert payload["positionIdx"] == 1

    async def test_hedge_sell_payload_carries_short_leg(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        payload = await self._place(
            adapter,
            store,
            mode="hedge",
            side=OrderSide.SELL,
            instrument=_linear_instrument(),
            mock_pybit_http=mock_pybit_http,
        )
        assert payload["positionIdx"] == 2

    async def test_one_way_payload_carries_zero(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        payload = await self._place(
            adapter,
            store,
            mode="one_way",
            side=OrderSide.BUY,
            instrument=_linear_instrument(),
            mock_pybit_http=mock_pybit_http,
        )
        assert payload["positionIdx"] == 0

    async def test_spot_payload_omits_position_idx(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        payload = await self._place(
            adapter,
            store,
            mode="hedge",
            side=OrderSide.BUY,
            instrument=_spot_instrument(),
            mock_pybit_http=mock_pybit_http,
        )
        assert "positionIdx" not in payload

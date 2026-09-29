"""Unit tests for leverage/margin lifecycle events (transport mocked, no network)."""

from __future__ import annotations

from datetime import UTC
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.events import Event, EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.events import (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    LeverageDriftEvent,
    MarginModeApplyFailedEvent,
    MarginModeChangedEvent,
    MarginModeDriftEvent,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32

_ALL_EVENT_TYPES = (
    LeverageAppliedEvent,
    LeverageApplyFailedEvent,
    LeverageDriftEvent,
    MarginModeChangedEvent,
    MarginModeDriftEvent,
    MarginModeApplyFailedEvent,
)


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _store_mock(data: dict[str, str] | None = None) -> MagicMock:
    backing = dict(data or {})
    store = MagicMock()
    store.get_adapter_config = AsyncMock(side_effect=lambda key: backing.get(key))

    async def _set(key: str, value: str) -> None:
        backing[key] = value

    async def _delete(key: str) -> None:
        backing.pop(key, None)

    async def _list(prefix: str) -> dict[str, str]:
        return {k: v for k, v in backing.items() if k.startswith(prefix)}

    store.set_adapter_config = AsyncMock(side_effect=_set)
    store.delete_adapter_config = AsyncMock(side_effect=_delete)
    store.list_adapter_config = AsyncMock(side_effect=_list)
    store.backing = backing
    return store


def _exchange_mock(
    *,
    legs: list[dict[str, Any]] | None = None,
    fail_submit: bool = False,
    pair_coins: dict[str, str] | None = None,
) -> MagicMock:
    exchange = MagicMock()
    if fail_submit:
        exchange.update_leverage.side_effect = PlatformError("venue rejected updateLeverage")
    else:
        exchange.update_leverage.return_value = {"status": "ok", "response": {"type": "default"}}
    exchange.info.user_state.return_value = {"assetPositions": legs or []}
    if pair_coins is not None:
        exchange.info.name_to_coin = pair_coins
    return exchange


def _leg(coin: str = "BTC", value: int = 20, kind: str = "isolated") -> dict[str, Any]:
    return {
        "type": "oneWay",
        "position": {
            "coin": coin,
            "szi": "1",
            "entryPx": "100",
            "leverage": {"type": kind, "value": value},
        },
    }


def _adapter(
    store_data: dict[str, str] | None = None,
    **kwargs: Any,
) -> tuple[HyperliquidAdapter, EventBus, list[Event]]:
    bus = EventBus()
    seen: list[Event] = []
    for event_type in _ALL_EVENT_TYPES:
        bus.subscribe(event_type, seen.append)
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=bus)
    adapter.attach_state_store(_store_mock(store_data))
    adapter._exchange = _exchange_mock(**kwargs)
    adapter._connected = True
    return adapter, bus, seen


def _of_type(seen: list[Event], event_type: type[Event]) -> list[Event]:
    return [e for e in seen if isinstance(e, event_type)]


def _exchange_of(adapter: HyperliquidAdapter) -> MagicMock:
    """Narrow the mocked exchange — fails loudly if no mock is attached."""
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    return exchange


def _assert_identity(event: Event) -> None:
    assert event.adapter_name == "hyperliquid"
    assert event.account_id == _TEST_ADDRESS
    UUID(event.event_id)
    assert event.timestamp.tzinfo is UTC


async def test_set_margin_mode_emits_changed_with_stored_previous() -> None:
    adapter, _, seen = _adapter({"margin.mode:BTC": "cross"})
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)
    (changed,) = _of_type(seen, MarginModeChangedEvent)
    assert isinstance(changed, MarginModeChangedEvent)
    _assert_identity(changed)
    assert changed.previous is MarginMode.CROSS
    assert changed.current is MarginMode.ISOLATED
    assert changed.instrument == _perp()


async def test_set_margin_mode_emits_changed_with_venue_previous() -> None:
    adapter, _, seen = _adapter(legs=[_leg(value=10, kind="cross")])
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED, block_on_open_position=False)
    (changed,) = _of_type(seen, MarginModeChangedEvent)
    assert isinstance(changed, MarginModeChangedEvent)
    assert changed.previous is MarginMode.CROSS
    # Leverage preserved from the leg, not reset to the default.
    _, args, _ = _exchange_of_update(adapter)
    assert args[0] == 10


async def test_set_margin_mode_emits_changed_with_none_previous() -> None:
    adapter, _, seen = _adapter()
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)
    (changed,) = _of_type(seen, MarginModeChangedEvent)
    assert isinstance(changed, MarginModeChangedEvent)
    assert changed.previous is None
    assert changed.current is MarginMode.ISOLATED


async def test_set_margin_mode_no_event_when_unchanged() -> None:
    adapter, _, seen = _adapter({"margin.mode:BTC": "isolated"})
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)
    assert _of_type(seen, MarginModeChangedEvent) == []


async def test_set_leverage_emits_nothing() -> None:
    adapter, _, seen = _adapter()
    _exchange_of(adapter).info.meta.return_value = {
        "universe": [{"name": "BTC", "maxLeverage": 40, "marginTableId": 1}],
        "marginTables": [],
    }
    await adapter.set_leverage(_perp(), leverage=5)
    assert seen == []


def _exchange_of_update(adapter: HyperliquidAdapter) -> Any:
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    return exchange.update_leverage.mock_calls[0]


async def test_reconcile_lev_reapply_emits_drift_only() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "reapply"}, legs=[_leg(value=5)]
    )
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, LeverageDriftEvent)
    assert isinstance(drift, LeverageDriftEvent)
    _assert_identity(drift)
    assert (drift.stored, drift.platform, drift.action_taken) == (10, 5, "reapplied")
    assert _of_type(seen, LeverageAppliedEvent) == []
    assert _of_type(seen, LeverageApplyFailedEvent) == []


async def test_reconcile_lev_notify_emits_drift_without_submit() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "notify"}, legs=[_leg(value=5)]
    )
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, LeverageDriftEvent)
    assert isinstance(drift, LeverageDriftEvent)
    assert drift.action_taken == "notified"
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    exchange.update_leverage.assert_not_called()


async def test_reconcile_lev_halt_emits_drift_then_halts() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "halt"}, legs=[_leg(value=5)]
    )
    halt = MagicMock()
    adapter.attach_halt_machine(halt)
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, LeverageDriftEvent)
    assert isinstance(drift, LeverageDriftEvent)
    assert drift.action_taken == "halted"
    halt.enter_halt.assert_called_once()


async def test_reconcile_lev_submit_failure_emits_failed() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "reapply"},
        legs=[_leg(value=5)],
        fail_submit=True,
    )
    await adapter.reconcile_user_intent()  # must not raise
    (failed,) = _of_type(seen, LeverageApplyFailedEvent)
    assert isinstance(failed, LeverageApplyFailedEvent)
    _assert_identity(failed)
    assert failed.leverage == 10
    assert "venue rejected" in failed.reason
    assert _of_type(seen, LeverageDriftEvent) == []


async def test_reconcile_mode_reapply_emits_drift() -> None:
    adapter, _, seen = _adapter(
        {"margin.mode:BTC": "cross", "margin.mode.on_drift:BTC": "reapply"},
        legs=[_leg(value=10, kind="isolated")],
    )
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, MarginModeDriftEvent)
    assert isinstance(drift, MarginModeDriftEvent)
    _assert_identity(drift)
    assert drift.stored is MarginMode.CROSS
    assert drift.platform is MarginMode.ISOLATED
    assert drift.action_taken == "reapplied"


async def test_reconcile_mode_notify_and_halt() -> None:
    adapter, _, seen = _adapter(
        {"margin.mode:BTC": "cross", "margin.mode.on_drift:BTC": "notify"},
        legs=[_leg(value=10, kind="isolated")],
    )
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, MarginModeDriftEvent)
    assert isinstance(drift, MarginModeDriftEvent)
    assert drift.action_taken == "notified"

    adapter2, _, seen2 = _adapter(
        {"margin.mode:BTC": "cross", "margin.mode.on_drift:BTC": "halt"},
        legs=[_leg(value=10, kind="isolated")],
    )
    halt = MagicMock()
    adapter2.attach_halt_machine(halt)
    await adapter2.reconcile_user_intent()
    (drift2,) = _of_type(seen2, MarginModeDriftEvent)
    assert isinstance(drift2, MarginModeDriftEvent)
    assert drift2.action_taken == "halted"
    halt.enter_halt.assert_called_once()


async def test_reconcile_mode_submit_failure_emits_failed() -> None:
    adapter, _, seen = _adapter(
        {"margin.mode:BTC": "cross", "margin.mode.on_drift:BTC": "reapply"},
        legs=[_leg(value=10, kind="isolated")],
        fail_submit=True,
    )
    await adapter.reconcile_user_intent()
    (failed,) = _of_type(seen, MarginModeApplyFailedEvent)
    assert isinstance(failed, MarginModeApplyFailedEvent)
    assert failed.mode is MarginMode.CROSS
    assert "venue rejected" in failed.reason
    assert _of_type(seen, MarginModeDriftEvent) == []


async def test_reconcile_spot_child_resolves_through_pair_table() -> None:
    adapter, _, seen = _adapter(
        {"margin.mode:@107": "cross", "margin.mode.on_drift:@107": "reapply"},
        legs=[_leg(coin="@107", value=10, kind="isolated")],
        pair_coins={"HYPE/USDC": "@107"},
    )
    await adapter.reconcile_user_intent()
    (drift,) = _of_type(seen, MarginModeDriftEvent)
    assert isinstance(drift, MarginModeDriftEvent)
    assert drift.instrument.symbol == "HYPE"


async def test_reconcile_skips_unresolvable_coin_without_submit() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BAD:COIN": "10"},
        legs=[_leg(coin="BAD:COIN", value=5)],
    )
    await adapter.reconcile_user_intent()
    assert seen == []
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    exchange.update_leverage.assert_not_called()


async def test_reapply_on_connect_emits_applied() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "9", "leverage.auto_apply:BTC": "1"}, legs=[_leg(value=9)]
    )
    await adapter._reapply_stored_intent()
    (applied,) = _of_type(seen, LeverageAppliedEvent)
    assert isinstance(applied, LeverageAppliedEvent)
    _assert_identity(applied)
    assert applied.leverage == 9


async def test_reapply_on_connect_emits_failed() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "9", "leverage.auto_apply:BTC": "1"},
        legs=[_leg(value=5)],
        fail_submit=True,
    )
    await adapter._reapply_stored_intent()  # must not raise
    (failed,) = _of_type(seen, LeverageApplyFailedEvent)
    assert isinstance(failed, LeverageApplyFailedEvent)
    assert failed.leverage == 9
    assert _of_type(seen, LeverageAppliedEvent) == []


async def test_reapply_mode_emits_changed_only_on_change() -> None:
    adapter, _, seen = _adapter(
        {"margin.mode:BTC": "cross"}, legs=[_leg(value=10, kind="isolated")]
    )
    await adapter._reapply_stored_intent()
    (changed,) = _of_type(seen, MarginModeChangedEvent)
    assert isinstance(changed, MarginModeChangedEvent)
    assert changed.previous is MarginMode.ISOLATED
    assert changed.current is MarginMode.CROSS

    adapter2, _, seen2 = _adapter({"margin.mode:BTC": "cross"}, legs=[_leg(value=10, kind="cross")])
    await adapter2._reapply_stored_intent()
    assert _of_type(seen2, MarginModeChangedEvent) == []


async def test_empty_store_makes_no_venue_calls() -> None:
    """First run (no intent): reconcile and reapply cost zero venue calls."""
    adapter, _, seen = _adapter({})
    await adapter.reconcile_user_intent()
    await adapter._reapply_stored_intent()
    exchange = _exchange_of(adapter)
    exchange.info.user_state.assert_not_called()
    assert seen == []


async def test_event_without_bus_raises_documenting_bus_required() -> None:
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config)  # no bus
    adapter.attach_state_store(_store_mock())
    adapter._exchange = _exchange_mock()
    adapter._connected = True
    with pytest.raises(RuntimeError, match="event_bus not wired"):
        await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)

"""Unit tests for block_on_open_position and strict_check (transport mocked)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.events import Event, EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.errors import LeverageDriftError
from unified_trading_execution.hyperliquid.events import LeverageDriftEvent
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32

_META = {
    "universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40, "marginTableId": 56}],
    "marginTables": [
        [56, {"description": "", "marginTiers": [{"lowerBound": "0.0", "maxLeverage": 40}]}]
    ],
}


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _spot() -> Instrument:
    return Instrument(symbol="HYPE", quote_currency="USDC", asset_class=AssetClass.SPOT)


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


def _exchange_mock(*, legs: list[dict[str, Any]] | None = None) -> MagicMock:
    exchange = MagicMock()
    exchange.update_leverage.return_value = {"status": "ok", "response": {"type": "default"}}
    exchange.info.meta.return_value = _META
    exchange.info.user_state.return_value = {"assetPositions": legs or []}
    return exchange


def _leg(
    coin: str = "BTC", value: int = 20, kind: str = "isolated", szi: str = "1"
) -> dict[str, Any]:
    return {
        "type": "oneWay",
        "position": {
            "coin": coin,
            "szi": szi,
            "entryPx": "100",
            "leverage": {"type": kind, "value": value},
        },
    }


def _adapter(
    store_data: dict[str, str] | None = None, **kwargs: Any
) -> tuple[HyperliquidAdapter, EventBus, list[Event]]:
    bus = EventBus()
    seen: list[Event] = []
    bus.subscribe(LeverageDriftEvent, seen.append)
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=bus)
    adapter.attach_state_store(_store_mock(store_data))
    adapter._exchange = _exchange_mock(**kwargs)
    adapter._connected = True
    return adapter, bus, seen


def _exchange_of(adapter: HyperliquidAdapter) -> MagicMock:
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    return exchange


def _store_of(adapter: HyperliquidAdapter) -> MagicMock:
    store = adapter._state_store
    assert isinstance(store, MagicMock)
    return store


# ---- block_on_open_position ----


async def test_set_leverage_blocked_with_open_leg() -> None:
    adapter, _, seen = _adapter(legs=[_leg(value=5)])
    with pytest.raises(PlatformError, match="open position"):
        await adapter.set_leverage(_perp(), leverage=10)
    _exchange_of(adapter).update_leverage.assert_not_called()
    assert _store_of(adapter).backing.get("leverage.value:BTC") is None
    assert seen == []


async def test_set_leverage_passes_when_flat() -> None:
    adapter, _, _ = _adapter()
    await adapter.set_leverage(_perp(), leverage=10)
    _exchange_of(adapter).update_leverage.assert_called_once()


async def test_set_leverage_block_disabled() -> None:
    adapter, _, _ = _adapter(legs=[_leg(value=5)])
    await adapter.set_leverage(_perp(), leverage=10, block_on_open_position=False)
    _exchange_of(adapter).update_leverage.assert_called_once()
    assert _store_of(adapter).backing["leverage.block_on_open:BTC"] == "0"


async def test_set_leverage_block_enabled_by_default() -> None:
    adapter, _, _ = _adapter()
    await adapter.set_leverage(_perp(), leverage=10)
    assert _store_of(adapter).backing["leverage.block_on_open:BTC"] == "1"


async def test_set_margin_mode_blocked_with_open_leg() -> None:
    adapter, _, seen = _adapter(legs=[_leg(value=10, kind="cross")])
    with pytest.raises(PlatformError, match="open position"):
        await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)
    _exchange_of(adapter).update_leverage.assert_not_called()
    assert seen == []


async def test_set_margin_mode_block_disabled() -> None:
    adapter, _, _ = _adapter(legs=[_leg(value=10, kind="cross")])
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED, block_on_open_position=False)
    _exchange_of(adapter).update_leverage.assert_called_once()


async def test_margin_block_knob_persists_under_family_prefix() -> None:
    """The guard must read the same key the margin family writes."""
    adapter, _, _ = _adapter()
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED, block_on_open_position=False)
    backing = _store_of(adapter).backing
    assert backing["margin.mode.block_on_open:BTC"] == "0"
    assert await adapter._policy_knob("margin.mode", "block_on_open", "BTC") == "0"


async def test_margin_block_knob_honoured_on_later_call() -> None:
    """A stored unblocked intent lets a later mode change through the guard."""
    adapter, _, _ = _adapter()
    await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED, block_on_open_position=False)
    _exchange_of(adapter).info.user_state.return_value = {
        "assetPositions": [_leg(value=10, kind="isolated")]
    }
    await adapter.set_margin_mode(_perp(), MarginMode.CROSS)
    assert _exchange_of(adapter).update_leverage.call_count == 2


async def test_block_never_applies_to_spot() -> None:
    adapter, _, _ = _adapter()
    await adapter._block_on_open_position(_spot(), action="change leverage", kind="leverage")


async def test_zero_size_leg_does_not_block() -> None:
    adapter, _, _ = _adapter(legs=[_leg(value=5, szi="0.0")])
    await adapter.set_leverage(_perp(), leverage=10)
    _exchange_of(adapter).update_leverage.assert_called_once()


async def test_remove_clears_knob_rows() -> None:
    adapter, _, _ = _adapter()
    await adapter.set_leverage(_perp(), leverage=10, strict_check=False)
    await adapter.remove_leverage(_perp())
    backing = _store_of(adapter).backing
    assert backing.get("leverage.strict_check:BTC") is None
    assert backing.get("leverage.block_on_open:BTC") is None


# ---- strict_check ----


async def test_strict_disabled_makes_no_venue_call() -> None:
    adapter, _, _ = _adapter({"leverage.strict_check:BTC": "0"}, legs=[_leg(value=5)])
    await adapter._strict_check_leverage(_perp())
    _exchange_of(adapter).info.user_state.assert_not_called()


async def test_strict_passes_when_flat() -> None:
    adapter, _, _ = _adapter({"leverage.value:BTC": "10"})
    await adapter._strict_check_leverage(_perp())  # no leg: nothing contradicts intent
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_strict_passes_when_matching() -> None:
    adapter, _, _ = _adapter({"leverage.value:BTC": "10"}, legs=[_leg(value=10, kind="cross")])
    await adapter._strict_check_leverage(_perp())
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_strict_reapplies_drift_and_proceeds() -> None:
    adapter, _, seen = _adapter({"leverage.value:BTC": "10"}, legs=[_leg(value=5)])
    await adapter._strict_check_leverage(_perp())  # reapply policy: no raise
    _exchange_of(adapter).update_leverage.assert_called_once()
    (drift,) = [e for e in seen if isinstance(e, LeverageDriftEvent)]
    assert isinstance(drift, LeverageDriftEvent)
    assert drift.action_taken == "reapplied"


async def test_strict_notify_rejects_order() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "notify"}, legs=[_leg(value=5)]
    )
    with pytest.raises(LeverageDriftError, match="differs from intent"):
        await adapter._strict_check_leverage(_perp())
    _exchange_of(adapter).update_leverage.assert_not_called()
    (drift,) = [e for e in seen if isinstance(e, LeverageDriftEvent)]
    assert isinstance(drift, LeverageDriftEvent)
    assert drift.action_taken == "notified"


async def test_strict_halt_rejects_and_halts() -> None:
    adapter, _, seen = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "halt"}, legs=[_leg(value=5)]
    )
    halt = MagicMock()
    adapter.attach_halt_machine(halt)
    with pytest.raises(LeverageDriftError, match="differs from intent"):
        await adapter._strict_check_leverage(_perp())
    halt.enter_halt.assert_called_once()
    (drift,) = [e for e in seen if isinstance(e, LeverageDriftEvent)]
    assert isinstance(drift, LeverageDriftEvent)
    assert drift.action_taken == "halted"


async def test_strict_failed_reapply_rejects_order() -> None:
    """A reapply that itself raises leaves drift unrepaired — reject the order."""
    adapter, _, _ = _adapter({"leverage.value:BTC": "10"}, legs=[_leg(value=5)])
    _exchange_of(adapter).update_leverage.side_effect = PlatformError("venue down")
    with pytest.raises(LeverageDriftError, match="differs from intent"):
        await adapter._strict_check_leverage(_perp())


async def test_strict_unconfigured_coin_enforces_default() -> None:
    """Never-configured coins verify against default 1x (Bybit parity)."""
    adapter, _, _ = _adapter(legs=[_leg(value=5)])
    await adapter._strict_check_leverage(_perp())  # reapplies to 1, no raise
    _, args, _ = _exchange_of(adapter).update_leverage.mock_calls[0]
    assert args[0] == 1


async def test_place_order_hook_rejects_on_unrepaired_drift() -> None:
    adapter, _, _ = _adapter(
        {"leverage.value:BTC": "10", "leverage.on_drift:BTC": "notify"}, legs=[_leg(value=5)]
    )
    order = UnifiedOrder(
        instrument=_perp(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        price=Decimal("50000"),
        time_in_force=TimeInForce.GTC,
        client_order_id="strict-hook-1",
    )
    with pytest.raises(LeverageDriftError, match="differs from intent"):
        await adapter.place_order(order)
    _exchange_of(adapter).bulk_orders.assert_not_called()


async def test_place_order_hook_rejects_when_reapply_fails() -> None:
    """End to end: a failed strict reapply must keep the order off the venue."""
    adapter, _, _ = _adapter({"leverage.value:BTC": "10"}, legs=[_leg(value=5)])
    _exchange_of(adapter).update_leverage.side_effect = PlatformError("venue down")
    order = UnifiedOrder(
        instrument=_perp(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        price=Decimal("50000"),
        time_in_force=TimeInForce.GTC,
        client_order_id="strict-hook-2",
    )
    with pytest.raises(LeverageDriftError, match="differs from intent"):
        await adapter.place_order(order)
    _exchange_of(adapter).bulk_orders.assert_not_called()

"""Unit tests for leverage/margin intent (transport mocked, no network)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.errors import (
    InvalidOrderError,
    InvalidSymbolError,
    PlatformError,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

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
    exchange.update_isolated_margin.return_value = {"status": "ok", "response": {"type": "default"}}
    exchange.info.meta.return_value = _META
    exchange.info.user_state.return_value = {"assetPositions": legs or []}
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


def _exchange_of(adapter: HyperliquidAdapter) -> MagicMock:
    """Narrow the mocked exchange — fails loudly if no mock is attached."""
    exchange = adapter._exchange
    assert isinstance(exchange, MagicMock)
    return exchange


def _store_of(adapter: HyperliquidAdapter) -> MagicMock:
    """Narrow the mocked store — fails loudly if no mock is attached."""
    store = adapter._state_store
    assert isinstance(store, MagicMock)
    return store


def _adapter(store_data: dict[str, str] | None = None, **kwargs: Any) -> HyperliquidAdapter:
    from unified_trading_execution.events import EventBus

    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    adapter.attach_state_store(_store_mock(store_data))
    adapter._exchange = _exchange_mock(**kwargs)
    adapter._connected = True
    return adapter


async def test_set_leverage_submits_and_persists() -> None:
    adapter = _adapter()
    await adapter.set_leverage(_perp(), leverage=10)
    _, args, kwargs = _exchange_of(adapter).update_leverage.mock_calls[0]
    assert (args[0], args[2] if len(args) > 2 else kwargs.get("is_cross")) == (10, True)
    backing = _store_of(adapter).backing
    assert backing["leverage:BTC"] == "10"
    assert backing["leverage.on_drift:BTC"] == "reapply"


async def test_set_leverage_preserves_stored_isolated_mode() -> None:
    adapter = _adapter({"margin_mode:BTC": "isolated"})
    await adapter.set_leverage(_perp(), leverage=5)
    _, args, kwargs = _exchange_of(adapter).update_leverage.mock_calls[0]
    is_cross = args[2] if len(args) > 2 else kwargs.get("is_cross")
    assert is_cross is False


@pytest.mark.parametrize("leverage", [0, -3, True, "10"])
async def test_set_leverage_rejects_bad_values(leverage: Any) -> None:
    adapter = _adapter()
    with pytest.raises(InvalidOrderError):
        await adapter.set_leverage(_perp(), leverage=leverage)
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_set_leverage_rejects_above_tier_cap() -> None:
    adapter = _adapter()
    with pytest.raises(InvalidOrderError, match="exceeds max"):
        await adapter.set_leverage(_perp(), leverage=41)
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_set_leverage_rejects_spot() -> None:
    adapter = _adapter()
    with pytest.raises(InvalidSymbolError):
        await adapter.set_leverage(_spot(), leverage=2)


async def test_set_leverage_rejects_bad_policy() -> None:
    adapter = _adapter()
    with pytest.raises(ValueError, match="on_drift"):
        await adapter.set_leverage(_perp(), leverage=2, on_drift="explode")  # type: ignore[arg-type]


async def test_get_leverage_reads_leg() -> None:
    adapter = _adapter(legs=[_leg()])
    assert await adapter.get_leverage(_perp()) == (20, False)


async def test_get_leverage_none_without_leg_or_spot() -> None:
    adapter = _adapter()
    assert await adapter.get_leverage(_perp()) is None
    assert await adapter.get_leverage(_spot()) is None


async def test_remove_leverage() -> None:
    adapter = _adapter({"leverage:BTC": "5", "leverage.on_drift:BTC": "halt"})
    await adapter.remove_leverage(_perp())
    assert _store_of(adapter).backing == {}
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_top_up_submits_delta() -> None:
    adapter = _adapter()
    await adapter.top_up_isolated_margin(_perp(), amount_usdc=Decimal("25.5"))
    _, args, _ = _exchange_of(adapter).update_isolated_margin.mock_calls[0]
    assert args[0] == 25.5
    with pytest.raises(InvalidSymbolError):
        await adapter.top_up_isolated_margin(_spot(), amount_usdc=Decimal("1"))


async def test_set_margin_mode_preserves_leverage() -> None:
    adapter = _adapter({"leverage:BTC": "7"})
    await adapter.set_margin_mode(_perp(), "isolated")
    _, args, kwargs = _exchange_of(adapter).update_leverage.mock_calls[0]
    assert args[0] == 7
    is_cross = args[2] if len(args) > 2 else kwargs.get("is_cross")
    assert is_cross is False
    assert _store_of(adapter).backing["margin_mode:BTC"] == "isolated"


async def test_set_margin_mode_rejects() -> None:
    adapter = _adapter()
    with pytest.raises(ValueError, match="mode"):
        await adapter.set_margin_mode(_perp(), "portfolio")  # type: ignore[arg-type]
    with pytest.raises(InvalidSymbolError):
        await adapter.set_margin_mode(_spot(), MarginMode.CROSS)
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_get_remove_margin_mode() -> None:
    adapter = _adapter(legs=[_leg(kind="cross", value=3)])
    assert await adapter.get_margin_mode(_perp()) is MarginMode.CROSS
    await adapter.remove_margin_mode(_perp())
    assert _store_of(adapter).backing == {}
    empty = _adapter()
    assert await empty.get_margin_mode(_perp()) is None


async def test_reconcile_reapplies_drift() -> None:
    adapter = _adapter(
        {"leverage:BTC": "10", "leverage.on_drift:BTC": "reapply"}, legs=[_leg(value=5)]
    )
    await adapter.reconcile_user_intent()
    _, args, _ = _exchange_of(adapter).update_leverage.mock_calls[0]
    assert args[0] == 10


async def test_reconcile_notify_does_not_submit() -> None:
    adapter = _adapter(
        {"leverage:BTC": "10", "leverage.on_drift:BTC": "notify"}, legs=[_leg(value=5)]
    )
    await adapter.reconcile_user_intent()
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_reconcile_halt_without_machine_logs() -> None:
    adapter = _adapter(
        {"leverage:BTC": "10", "leverage.on_drift:BTC": "halt"}, legs=[_leg(value=5)]
    )
    await adapter.reconcile_user_intent()  # no halt machine: logs, no submit, no raise
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_reconcile_halt_enters_halt() -> None:
    adapter = _adapter(
        {"margin_mode:BTC": "cross", "margin_mode.on_drift:BTC": "halt"},
        legs=[_leg(kind="isolated")],
    )
    halt = MagicMock()
    halt.enter_halt.return_value = True
    adapter.attach_halt_machine(halt)
    await adapter.reconcile_user_intent()
    halt.enter_halt.assert_called_once()
    assert halt.enter_halt.call_args[1]["reason"] == "margin_mode_drift"
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_reconcile_matching_does_nothing() -> None:
    adapter = _adapter({"leverage:BTC": "20"}, legs=[_leg(value=20, kind="cross")])
    await adapter.reconcile_user_intent()
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_reconcile_fetches_venue_state_once_per_pass() -> None:
    """Three drifted coins share two user_state fetches, not six."""
    adapter = _adapter(
        {
            "leverage:BTC": "10",
            "leverage:ETH": "10",
            "leverage:SOL": "10",
            "margin_mode:BTC": "cross",
            "margin_mode:ETH": "cross",
            "margin_mode:SOL": "cross",
        },
        legs=[
            _leg(coin="BTC", value=5, kind="isolated"),
            _leg(coin="ETH", value=5, kind="isolated"),
            _leg(coin="SOL", value=5, kind="isolated"),
        ],
    )
    await adapter.reconcile_user_intent()
    assert _exchange_of(adapter).info.user_state.call_count == 2
    assert _exchange_of(adapter).update_leverage.call_count == 6


async def test_reconcile_without_store_returns() -> None:
    from unified_trading_execution.events import EventBus

    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    await adapter.reconcile_user_intent()


async def test_intent_requires_store() -> None:
    from unified_trading_execution.events import EventBus

    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    adapter._exchange = _exchange_mock()
    adapter._connected = True
    with pytest.raises(PlatformError, match="state_store"):
        await adapter.set_leverage(_perp(), leverage=2)
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_margin_mode_requires_store_without_mutating() -> None:
    """Without a store the venue must not be mutated before the raise."""
    from unified_trading_execution.events import EventBus

    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    adapter._exchange = _exchange_mock()
    adapter._connected = True
    with pytest.raises(PlatformError, match="state_store"):
        await adapter.set_margin_mode(_perp(), MarginMode.ISOLATED)
    _exchange_of(adapter).update_leverage.assert_not_called()


async def test_connect_reapplies_auto_apply() -> None:
    from unittest.mock import patch

    from unified_trading_execution.events import EventBus

    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    event_bus = EventBus()
    adapter = HyperliquidAdapter(config, event_bus=event_bus)
    adapter.attach_state_store(_store_mock({"leverage:BTC": "9", "margin_mode:BTC": "cross"}))
    exchange = _exchange_mock(legs=[_leg(value=9, kind="cross")])
    with patch("unified_trading_execution.hyperliquid.adapter.Exchange", return_value=exchange):
        exchange.info.user_role.return_value = {"role": "agent"}
        exchange.info.query_user_abstraction_state.return_value = "unifiedAccount"
        await adapter.connect()
    submitted = [call[1][0] for call in exchange.update_leverage.mock_calls]
    assert 9 in submitted
    await adapter.disconnect()

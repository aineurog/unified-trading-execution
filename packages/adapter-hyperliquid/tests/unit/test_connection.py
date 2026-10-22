"""Unit tests for connect/disconnect (transport mocked, no network)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from unified_trading_execution.errors import PlatformConnectionError, PlatformError
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)


def _exchange_mock(*, role: str = "agent", abstraction: str = "unifiedAccount") -> MagicMock:
    exchange = MagicMock()
    exchange.info.user_role.return_value = {"role": role}
    exchange.info.query_user_abstraction_state.return_value = abstraction
    return exchange


def _events(event_bus: EventBus) -> list[Any]:
    captured: list[Any] = []
    event_bus.subscribe(ConnectionStateEvent, captured.append)
    return captured


async def test_connect_publishes_and_flags() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ):
        await adapter.connect()
    assert adapter.is_connected is True
    assert len(captured) == 1
    assert captured[0].connected is True
    assert captured[0].account_id == _TEST_ADDRESS


async def test_connect_idempotent() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ) as factory:
        await adapter.connect()
        await adapter.connect()
    assert factory.call_count == 1
    assert len(captured) == 1


async def test_connect_rejects_missing_role() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            return_value=_exchange_mock(role="missing"),
        ),
        pytest.raises(PlatformError, match="approveAgent"),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_connect_rejects_non_unified_abstraction() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            return_value=_exchange_mock(abstraction="portfolioMargin"),
        ),
        pytest.raises(PlatformError, match="unified"),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_connect_maps_transport_failure() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            side_effect=requests.exceptions.ConnectionError("down"),
        ),
        pytest.raises(PlatformConnectionError),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_disconnect_publishes_and_clears() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ):
        await adapter.connect()
        await adapter.disconnect()
    assert adapter.is_connected is False
    assert [e.connected for e in captured] == [True, False]


async def test_disconnect_when_never_connected_is_quiet() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    await adapter.disconnect()
    assert captured == []


async def test_exchange_required_when_disconnected() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with pytest.raises(PlatformConnectionError):
        adapter._require_exchange()

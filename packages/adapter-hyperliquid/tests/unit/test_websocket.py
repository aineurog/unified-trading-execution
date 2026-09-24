"""Unit tests for the WebSocket wrapper (manager mocked, no network)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.hyperliquid import HyperliquidConfig
from unified_trading_execution.hyperliquid.websocket import HyperliquidWebSocket

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(
        wallet_address=_TEST_ADDRESS,
        private_key=_TEST_KEY,
        testnet=True,
        request_timeout_seconds=0.2,
    )


def _manager_mock(*, ready: bool = True, alive: bool = True) -> MagicMock:
    manager = MagicMock()
    manager.ws_ready = ready
    manager.is_alive.return_value = alive
    manager.subscribe.side_effect = lambda subscription, callback: 7
    return manager


def _connect_recorded(ws: HyperliquidWebSocket, manager: MagicMock) -> None:
    with patch(
        "unified_trading_execution.hyperliquid.websocket.WebsocketManager",
        return_value=manager,
    ):
        ws.connect()


def test_connect_uses_testnet_url_and_reports_ready() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock()
    with patch(
        "unified_trading_execution.hyperliquid.websocket.WebsocketManager",
        return_value=manager,
    ) as factory:
        ws.connect()
    assert factory.call_args[0][0] == "https://api.hyperliquid-testnet.xyz"
    assert ws.is_connected() is True


def test_connect_timeout_stops_manager() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock(ready=False, alive=True)
    with (
        patch(
            "unified_trading_execution.hyperliquid.websocket.WebsocketManager",
            return_value=manager,
        ),
        pytest.raises(PlatformConnectionError, match="ready in time"),
    ):
        ws.connect()
    manager.stop.assert_called_once()
    assert ws.is_connected() is False


def test_connect_dead_thread() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock(ready=False, alive=False)
    with (
        patch(
            "unified_trading_execution.hyperliquid.websocket.WebsocketManager",
            return_value=manager,
        ),
        pytest.raises(PlatformConnectionError, match="died"),
    ):
        ws.connect()


def test_subscriptions_send_venue_shapes() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock()
    _connect_recorded(ws, manager)
    seen: list[Any] = []

    def _cb(message: dict[str, Any]) -> None:
        seen.append(message)

    assert ws.subscribe_user_events(_cb) == 7
    assert ws.subscribe_order_updates(_cb) == 7
    assert ws.subscribe_user_fills(_cb) == 7
    sent = [call[0][0] for call in manager.subscribe.call_args_list]
    assert {"type": "userEvents", "user": _TEST_ADDRESS} in sent
    assert {"type": "orderUpdates", "user": _TEST_ADDRESS} in sent
    assert {"type": "userFills", "user": _TEST_ADDRESS} in sent


def test_subscribe_without_connect_raises() -> None:
    ws = HyperliquidWebSocket(_config())
    with pytest.raises(PlatformConnectionError, match="not connected"):
        ws.subscribe_user_events(lambda message: None)


def test_disconnect_unsubscribes_and_stops() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock()
    _connect_recorded(ws, manager)
    ws.subscribe_user_events(lambda message: None)
    ws.disconnect()
    assert ws.is_connected() is False
    manager.unsubscribe.assert_called_once()
    manager.stop.assert_called_once()
    ws.disconnect()  # second call is quiet

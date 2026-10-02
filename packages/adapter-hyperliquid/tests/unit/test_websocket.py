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
        "unified_trading_execution.hyperliquid.websocket._AccountStateWebsocketManager",
        return_value=manager,
    ):
        ws.connect()


def test_connect_uses_testnet_url_and_reports_ready() -> None:
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock()
    with patch(
        "unified_trading_execution.hyperliquid.websocket._AccountStateWebsocketManager",
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
            "unified_trading_execution.hyperliquid.websocket._AccountStateWebsocketManager",
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
            "unified_trading_execution.hyperliquid.websocket._AccountStateWebsocketManager",
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


# ---- account-state routing shim (SDK 0.24.0 lacks these branches) ----


def _shim_manager() -> Any:
    from unified_trading_execution.hyperliquid.websocket import (
        _AccountStateWebsocketManager,
    )

    manager = _AccountStateWebsocketManager("https://api.hyperliquid-testnet.xyz")
    manager.ws_ready = True
    manager.ws = MagicMock()
    return manager


def _account_message(channel: str, user: str, body: dict[str, Any]) -> str:
    import json

    return json.dumps({"channel": channel, "data": {"user": user, **body}})


def test_shim_routes_clearinghouse_state_to_callback() -> None:
    import json

    manager = _shim_manager()
    seen: list[Any] = []
    sub_id = manager.subscribe({"type": "clearinghouseState", "user": _TEST_ADDRESS}, seen.append)
    sent = json.loads(manager.ws.send.call_args[0][0])
    assert sent == {
        "method": "subscribe",
        "subscription": {"type": "clearinghouseState", "user": _TEST_ADDRESS},
    }
    manager.on_message(
        None,
        _account_message("clearinghouseState", _TEST_ADDRESS, {"clearinghouseState": {}}),
    )
    assert len(seen) == 1
    assert (
        manager.unsubscribe({"type": "clearinghouseState", "user": _TEST_ADDRESS}, sub_id) is True
    )
    manager.on_message(
        None,
        _account_message("clearinghouseState", _TEST_ADDRESS, {"clearinghouseState": {}}),
    )
    assert len(seen) == 1  # unsubscribed — no second delivery


def test_shim_routes_spot_state_to_callback() -> None:
    manager = _shim_manager()
    seen: list[Any] = []
    manager.subscribe({"type": "spotState", "user": _TEST_ADDRESS}, seen.append)
    manager.on_message(None, _account_message("spotState", _TEST_ADDRESS, {"spotState": {}}))
    assert len(seen) == 1


def test_shim_ignores_other_users() -> None:
    manager = _shim_manager()
    seen: list[Any] = []
    manager.subscribe({"type": "spotState", "user": _TEST_ADDRESS}, seen.append)
    manager.on_message(
        None,
        _account_message(
            "spotState", "0x0000000000000000000000000000000000000002", {"spotState": {}}
        ),
    )
    assert seen == []


def test_shim_delegates_sdk_known_channels() -> None:
    import json

    manager = _shim_manager()
    seen: list[Any] = []
    manager.subscribe({"type": "userFills", "user": _TEST_ADDRESS}, seen.append)
    manager.on_message(
        None,
        json.dumps(
            {
                "channel": "userFills",
                "data": {"user": _TEST_ADDRESS, "fills": []},
            }
        ),
    )
    assert len(seen) == 1


def test_connect_daemonizes_manager_thread() -> None:
    """A wedged socket must never block process exit — graceful stop first,
    daemon bit as the backstop."""
    ws = HyperliquidWebSocket(_config())
    manager = _manager_mock()
    _connect_recorded(ws, manager)
    assert manager.daemon is True

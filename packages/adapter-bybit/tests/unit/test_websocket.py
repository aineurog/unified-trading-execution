"""Unit tests for BybitWebSocket — the pybit WebSocket wrapper."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import websocket  # type: ignore[import-untyped]
from pybit.exceptions import InvalidChannelTypeError, UnauthorizedExceptionError

from unified_trading_execution.bybit.websocket import BybitWebSocket
from unified_trading_execution.errors import PlatformConnectionError, PlatformError


@patch("unified_trading_execution.bybit.websocket.WebSocket")
class TestConnect:
    def test_creates_private_websocket(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()

        mock_ws_cls.assert_called_once_with(
            channel_type="private",
            testnet=bybit_config.testnet,
            demo=bybit_config.demo,
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            ping_interval=20,
            ping_timeout=10,
            retries=10,
            restart_on_error=True,
        )

    def test_connect_is_idempotent(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()
        socket.connect()
        mock_ws_cls.assert_called_once()

    def test_is_connected_after_connect(self, mock_ws_cls, bybit_config) -> None:
        mock_ws_cls.return_value.is_connected.return_value = True
        socket = BybitWebSocket(bybit_config)
        socket.connect()
        assert socket.is_connected() is True

    def test_translates_timeout_to_platform_connection_error(
        self,
        mock_ws_cls,
        bybit_config,
    ) -> None:
        mock_ws_cls.side_effect = websocket.WebSocketTimeoutException("connection timeout")
        socket = BybitWebSocket(bybit_config)

        with pytest.raises(PlatformConnectionError):
            socket.connect()

    def test_translates_connection_error_to_platform_connection_error(
        self,
        mock_ws_cls,
        bybit_config,
    ) -> None:
        mock_ws_cls.side_effect = ConnectionError("connection refused")
        socket = BybitWebSocket(bybit_config)

        with pytest.raises(PlatformConnectionError):
            socket.connect()

    def test_translates_unauthorized_to_platform_error(
        self,
        mock_ws_cls,
        bybit_config,
    ) -> None:
        mock_ws_cls.side_effect = UnauthorizedExceptionError("missing API keys")
        socket = BybitWebSocket(bybit_config)

        with pytest.raises(PlatformError):
            socket.connect()

    def test_translates_invalid_channel_to_platform_error(
        self,
        mock_ws_cls,
        bybit_config,
    ) -> None:
        mock_ws_cls.side_effect = InvalidChannelTypeError("bad channel")
        socket = BybitWebSocket(bybit_config)

        with pytest.raises(PlatformError):
            socket.connect()


@patch("unified_trading_execution.bybit.websocket.WebSocket")
class TestDisconnect:
    def test_disconnect_closes_socket(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()
        socket.disconnect()

        mock_ws_cls.return_value.exit.assert_called_once_with()
        assert socket.is_connected() is False

    def test_disconnect_without_connect_is_noop(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.disconnect()

        mock_ws_cls.return_value.exit.assert_not_called()


@patch("unified_trading_execution.bybit.websocket.WebSocket")
class TestIsConnected:
    def test_false_before_connect(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        assert socket.is_connected() is False

    def test_false_after_disconnect(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()
        socket.disconnect()
        assert socket.is_connected() is False

    def test_swallows_underlying_errors(self, mock_ws_cls, bybit_config) -> None:
        socket = BybitWebSocket(bybit_config)
        socket.connect()
        mock_ws_cls.return_value.is_connected.side_effect = RuntimeError("boom")
        assert socket.is_connected() is False

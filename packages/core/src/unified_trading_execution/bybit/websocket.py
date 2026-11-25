"""Bybit WebSocket connection wrapper.

pybit's ``WebSocket`` is a threaded websocket-client wrapper that blocks
during construction while it connects and reconnects automatically, and
invokes stream callbacks on a background daemon thread.  This module wraps
it behind a small, testable surface, translates pybit/websocket-client
failures into the unified exception hierarchy before they cross the adapter
boundary (Section 17.10), and exposes a private-topic subscription API
(``order`` / ``execution`` / ``position`` / ``wallet``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import websocket
from pybit.exceptions import InvalidChannelTypeError, UnauthorizedExceptionError
from pybit.unified_trading import WebSocket

from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.errors import PlatformConnectionError, PlatformError


class BybitWebSocket:
    """Thin wrapper around pybit's WebSocket client.

    The underlying client connects in its constructor and runs in a daemon
    thread.  Call :meth:`connect` from a worker thread (e.g. via
    ``asyncio.to_thread``) because it blocks until the socket is connected.
    Stream callbacks are invoked by pybit on its background thread — the
    adapter is responsible for marshalling any resulting work back onto the
    event loop.
    """

    def __init__(self, config: BybitConfig) -> None:
        self._config = config
        self._ws: WebSocket | None = None

    def connect(self) -> None:
        """Establish the private WebSocket connection (blocking).

        Raises:
            PlatformConnectionError: if the connection could not be established.
            PlatformError: if the configuration is invalid (e.g. missing keys).
        """
        if self._ws is not None:
            return
        try:
            self._ws = WebSocket(
                channel_type="private",
                testnet=self._config.testnet,
                demo=self._config.demo,
                api_key=self._config.api_key,
                api_secret=self._config.api_secret,
                ping_interval=20,
                ping_timeout=10,
                retries=10,
                restart_on_error=True,
            )
        except UnauthorizedExceptionError as exc:
            raise PlatformError(
                f"Bybit WebSocket auth not configured: {exc}",
            ) from exc
        except InvalidChannelTypeError as exc:
            raise PlatformError(
                f"Bybit WebSocket channel type invalid: {exc}",
            ) from exc
        except websocket.WebSocketException as exc:
            raise PlatformConnectionError(
                f"Bybit WebSocket connection failed: {exc}",
            ) from exc
        except (ConnectionError, OSError) as exc:
            raise PlatformConnectionError(
                f"Bybit WebSocket connection failed: {exc}",
            ) from exc

    def disconnect(self) -> None:
        """Close the WebSocket connection (blocking)."""
        if self._ws is None:
            return
        self._ws.exit()
        self._ws = None

    def is_connected(self) -> bool:
        """Return True if the underlying socket is currently connected."""
        if self._ws is None:
            return False
        try:
            return bool(self._ws.is_connected())
        except Exception:
            return False

    def subscribe_order(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the private ``order`` stream.

        Args:
            callback: Receives each complete message ``{"topic": ..., "data": [...]}``
                on pybit's background thread.  Conditional on an open connection.
        """
        self._require_connected().order_stream(callback)

    def subscribe_execution(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the private ``execution`` (fills) stream.

        Args:
            callback: Receives each complete message on pybit's background thread.
        """
        self._require_connected().execution_stream(callback)

    def subscribe_position(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the private ``position`` stream.

        Args:
            callback: Receives each complete message on pybit's background thread.
        """
        self._require_connected().position_stream(callback)

    def subscribe_wallet(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the private ``wallet`` stream.

        Args:
            callback: Receives each complete message on pybit's background thread.
        """
        self._require_connected().wallet_stream(callback)

    def _require_connected(self) -> WebSocket:
        if self._ws is None:
            raise PlatformConnectionError("Bybit WebSocket is not connected")
        return self._ws

"""Bybit WebSocket connection wrapper.

pybit's ``WebSocket`` is a threaded websocket-client wrapper that blocks
during construction while it connects and reconnects automatically.  This
module wraps it behind a small, testable surface and translates
pybit/websocket-client failures into the unified exception hierarchy before
they cross the adapter boundary (Section 17.10).
"""

from __future__ import annotations

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

"""Hyperliquid WebSocket connection wrapper.

Wraps the SDK ``WebsocketManager`` (a ``threading.Thread`` running a
``websocket-client`` app with a 50s ping) behind a small, testable surface.
Subscriptions need no auth (address string only) — note the privacy
implication: anyone can subscribe to anyone's fills.  Callbacks fire on the
manager's network thread, so the adapter marshals them onto the event loop
(``loop.call_soon_threadsafe``) at the handlers step.  The manager does not
auto-reconnect — drops are detected and rebuilt adapter-side.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, cast

import websocket
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from hyperliquid.utils.types import Subscription
from hyperliquid.websocket_manager import WebsocketManager

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.hyperliquid.config import HyperliquidConfig

logger = logging.getLogger(__name__)

_CONNECT_POLL_SECONDS = 0.05


class HyperliquidWebSocket:
    """Thin wrapper around the SDK websocket manager.

    One instance per adapter.  Call :meth:`connect` from a worker thread
    (e.g. via ``asyncio.to_thread``): it starts the manager thread and
    blocks until the socket reports ready (or the configured timeout
    expires).  Tracked subscription ids allow a best-effort
    :meth:`unsubscribe_all` on disconnect.
    """

    def __init__(self, config: HyperliquidConfig) -> None:
        self._config = config
        self._manager: WebsocketManager | None = None
        self._subscriptions: list[tuple[dict[str, Any], int]] = []

    @property
    def _base_url(self) -> str:
        url: str = TESTNET_API_URL if self._config.testnet else MAINNET_API_URL
        return url

    def connect(self) -> None:
        """Start the manager thread and wait for the socket to go ready.

        Raises:
            PlatformConnectionError: if the socket never reports ready
                within ``request_timeout_seconds`` or the thread dies.
        """
        if self._manager is not None:
            return
        try:
            manager = WebsocketManager(self._base_url)
            manager.start()
        except websocket.WebSocketException as exc:
            raise PlatformConnectionError(f"Hyperliquid WebSocket failed to start: {exc}") from exc
        except (ConnectionError, OSError) as exc:
            raise PlatformConnectionError(f"Hyperliquid WebSocket failed to start: {exc}") from exc
        deadline = time.monotonic() + self._config.request_timeout_seconds
        while not manager.ws_ready:
            if not manager.is_alive():
                raise PlatformConnectionError("Hyperliquid WebSocket thread died before ready")
            if time.monotonic() >= deadline:
                manager.stop()
                raise PlatformConnectionError("Hyperliquid WebSocket did not become ready in time")
            time.sleep(_CONNECT_POLL_SECONDS)
        self._manager = manager

    def disconnect(self) -> None:
        """Unsubscribe everything best-effort and stop the manager thread (blocking)."""
        manager, self._manager = self._manager, None
        subscriptions, self._subscriptions = self._subscriptions, []
        if manager is None:
            return
        for subscription, subscription_id in subscriptions:
            try:
                manager.unsubscribe(_as_subscription(subscription), subscription_id)
            except Exception:
                logger.exception("Hyperliquid WS unsubscribe failed for %s", subscription)
        manager.stop()

    def is_connected(self) -> bool:
        """Return True if the underlying socket thread is alive and ready."""
        manager = self._manager
        if manager is None:
            return False
        try:
            return bool(manager.is_alive() and manager.ws_ready)
        except Exception:
            return False

    def subscribe_user_events(self, callback: Callable[[dict[str, Any]], None]) -> int:
        """Subscribe to ``userEvents`` (fills/funding/liquidation/non-user cancels).

        Arrives on channel ``"user"``.  Single subscription per connection
        (venue limit — a second call raises through from the manager).
        """
        return self._subscribe(
            {"type": "userEvents", "user": self._config.wallet_address}, callback
        )

    def subscribe_order_updates(self, callback: Callable[[dict[str, Any]], None]) -> int:
        """Subscribe to ``orderUpdates`` (single subscription per connection)."""
        return self._subscribe(
            {"type": "orderUpdates", "user": self._config.wallet_address}, callback
        )

    def subscribe_user_fills(
        self, callback: Callable[[dict[str, Any]], None], *, aggregate_by_time: bool = False
    ) -> int:
        """Subscribe to ``userFills`` (snapshot open, then streaming)."""
        subscription: dict[str, Any] = {"type": "userFills", "user": self._config.wallet_address}
        if aggregate_by_time:
            subscription["aggregateByTime"] = True
        return self._subscribe(subscription, callback)

    def _subscribe(
        self, subscription: dict[str, Any], callback: Callable[[dict[str, Any]], None]
    ) -> int:
        manager = self._require_connected()
        try:
            subscription_id: int = manager.subscribe(_as_subscription(subscription), callback)
        except websocket.WebSocketException as exc:
            raise PlatformConnectionError(f"Hyperliquid WS subscribe failed: {exc}") from exc
        self._subscriptions.append((subscription, subscription_id))
        return subscription_id

    def _require_connected(self) -> WebsocketManager:
        if self._manager is None:
            raise PlatformConnectionError("Hyperliquid WebSocket is not connected")
        return self._manager


def _as_subscription(subscription: dict[str, Any]) -> Subscription:
    """Narrow a locally built dict to the SDK ``Subscription`` union.

    The cast is deliberate: our dicts are venue-verified shapes (including
    the documented ``aggregateByTime`` key the SDK's own TypedDict omits),
    so exact-typing would be less truthful than this auditable boundary.
    """
    return cast(Subscription, subscription)

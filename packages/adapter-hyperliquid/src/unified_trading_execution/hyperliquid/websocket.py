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

import json
import logging
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any, cast

import websocket
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from hyperliquid.utils.types import Subscription
from hyperliquid.websocket_manager import ActiveSubscription, WebsocketManager

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.hyperliquid.config import HyperliquidConfig

logger = logging.getLogger(__name__)

#: Server-initiated close signatures the venue sends on routine recycles
#: (roughly 10-minute TTL). The adapter detects these via its liveness
#: monitor and rebuilds by itself — the library's ERROR-level goodbye adds
#: noise, not information, so it is demoted to DEBUG. Anything else the
#: library reports (handshake failures, abnormal drops) keeps its level:
#: if rebuilding itself starts failing, the adapter says so loudly via
#: ``ConnectionStateEvent(False)`` and its own rebuild logs.
_SERVER_CLOSE_SNIPPETS = ("Expired", "was lost.")


class _ServerCloseFilter(logging.Filter):
    """Demote routine server recycles; keep genuine failures loud."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        if record.levelno == logging.ERROR and any(
            snippet in message for snippet in _SERVER_CLOSE_SNIPPETS
        ):
            record.levelno = logging.DEBUG
            record.levelname = "DEBUG"
        return True


def _quiet_server_recycles() -> None:
    """Install the recycle filter on the ``websocket-client`` logger, once."""
    lib_logger = logging.getLogger("websocket")
    if not any(isinstance(f, _ServerCloseFilter) for f in lib_logger.filters):
        lib_logger.addFilter(_ServerCloseFilter())


class _AccountStateWebsocketManager(WebsocketManager):  # type: ignore[misc]
    """SDK manager plus ``clearinghouseState`` / ``spotState`` routing.

    The pinned SDK (0.24.0, and upstream master) has no routing branches for
    these two account-state channels: ``subscription_to_identifier`` and
    ``ws_msg_to_identifier`` both return ``None``, so subscribing succeeds
    while every arrival drops as "not handling empty message" (probe-proven).
    This subclass routes exactly those two channels itself — same identifier
    discipline (``{type}:{user}``), same fan-out and queue-while-connecting
    semantics as the SDK — and delegates everything else to ``super()``. If
    a future SDK adds the branches, these overrides stop matching first and
    stay harmless.
    """

    def __init__(self, base_url: str) -> None:
        super().__init__(base_url)
        self._account_subs: dict[str, list[ActiveSubscription]] = defaultdict(list)

    @staticmethod
    def _account_identifier(subscription: dict[str, Any]) -> str | None:
        channel = subscription.get("type")
        if channel not in ("clearinghouseState", "spotState"):
            return None
        user = subscription.get("user")
        if not isinstance(user, str) or not user:
            return None
        return f"{channel}:{user.lower()}"

    def subscribe(
        self,
        subscription: Subscription,
        callback: Callable[[Any], None],
        subscription_id: int | None = None,
    ) -> int:
        identifier = self._account_identifier(dict(subscription))
        if identifier is None:
            delegated = super().subscribe(subscription, callback, subscription_id)
            assert isinstance(delegated, int)
            return delegated
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        if not self.ws_ready:
            self.queued_subscriptions.append(
                (subscription, ActiveSubscription(callback, subscription_id))
            )
        else:
            self._account_subs[identifier].append(ActiveSubscription(callback, subscription_id))
            self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        identifier = self._account_identifier(dict(subscription))
        if identifier is None:
            delegated = super().unsubscribe(subscription, subscription_id)
            assert isinstance(delegated, bool)
            return delegated
        current = self._account_subs[identifier]
        kept = [s for s in current if s.subscription_id != subscription_id]
        if len(kept) == len(current):
            return False
        if kept:
            self._account_subs[identifier] = kept
        else:
            del self._account_subs[identifier]
            self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
        return True

    def on_message(self, _ws: Any, message: str) -> None:
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        try:
            ws_msg: dict[str, Any] = json.loads(message)
        except Exception:
            logging.debug("Websocket received non-JSON message")
            return
        channel = ws_msg.get("channel")
        if channel not in ("clearinghouseState", "spotState"):
            super().on_message(_ws, message)
            return
        data = ws_msg.get("data")
        user = data.get("user") if isinstance(data, dict) else None
        identifier = f"{channel}:{str(user).lower()}" if user else None
        subs = self._account_subs.get(identifier, []) if identifier else []
        if not subs:
            logging.debug("Websocket message from an unexpected subscription: %s", identifier)
            return
        for sub in subs:
            sub.callback(cast(Any, ws_msg))


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
        self._manager: _AccountStateWebsocketManager | None = None
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
        _quiet_server_recycles()
        try:
            manager = _AccountStateWebsocketManager(self._base_url)
            # Daemon: graceful stop is always attempted first (disconnect),
            # but a wedged socket must never block process exit.
            manager.daemon = True
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
        if manager.is_alive():
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

    def subscribe_clearinghouse_state(self, callback: Callable[[dict[str, Any]], None]) -> int:
        """Subscribe to ``clearinghouseState`` (perp legs + margin, ~5s heartbeat)."""
        return self._subscribe(
            {"type": "clearinghouseState", "user": self._config.wallet_address}, callback
        )

    def subscribe_spot_state(self, callback: Callable[[dict[str, Any]], None]) -> int:
        """Subscribe to ``spotState`` (spot balances, ~5s heartbeat)."""
        return self._subscribe({"type": "spotState", "user": self._config.wallet_address}, callback)

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

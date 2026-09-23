"""Hyperliquid WebSocket connection wrapper.

Wraps the SDK websocket manager behind a small, testable surface and
exposes the push subscriptions (``user`` fills/funding/liquidation,
``orderUpdates``, ``userFills``) plus snapshot channels
(``allMids``/``l2Book``/``trades``).  Subscriptions need no auth (address
string only) — note the privacy implication: anyone can subscribe to
anyone's fills.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from unified_trading_execution.hyperliquid.config import HyperliquidConfig


class HyperliquidWebSocket:
    """Thin wrapper around the SDK websocket manager.

    One instance per adapter; the adapter marshals callbacks back onto the
    event loop.  No signing happens here — only address-keyed subscriptions.
    """

    def __init__(self, config: HyperliquidConfig) -> None:
        """Capture config; no network is touched until ``connect``."""
        raise NotImplementedError

    def connect(self) -> None:
        """Establish the WS connection (blocking; call off the loop)."""
        raise NotImplementedError

    def disconnect(self) -> None:
        """Close the WS connection (blocking)."""
        raise NotImplementedError

    def is_connected(self) -> bool:
        """Return True if the underlying socket is currently connected."""
        raise NotImplementedError

    def subscribe_user(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the ``user`` channel (fills/funding/liquidation)."""
        raise NotImplementedError

    def subscribe_order_updates(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the ``orderUpdates`` channel."""
        raise NotImplementedError

    def subscribe_fills(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the ``userFills`` channel."""
        raise NotImplementedError

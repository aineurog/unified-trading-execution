"""Bybit native error → unified exception hierarchy translation goes here.

Every Bybit-specific error code (HTTP status, WebSocket error, Bybit
business-logic error) must be translated into one of the common exception
types from ``unified_trading_execution.errors`` before it crosses the
adapter boundary.  Core must never receive a raw Bybit error.

TODO: map Bybit error codes to:
  - PlatformConnectionError   (network timeouts, 5xx)
  - InvalidSymbolError        (unknown symbol)
  - OrderNotFoundError        (order does not exist)
  - InsufficientBalanceError  (margin / balance too low)
  - RateLimitError            (HTTP 429 or Bybit rate-limit response)
  - PlatformError             (catch-all for unexpected Bybit errors)
"""

from __future__ import annotations

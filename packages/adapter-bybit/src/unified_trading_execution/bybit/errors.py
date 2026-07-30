"""Bybit native error → unified exception hierarchy translation.

Every Bybit-specific error code (HTTP status, WebSocket error, Bybit
business-logic error) must be translated into one of the common exception
types from ``unified_trading_execution.errors`` before it crosses the
adapter boundary.  Core must never receive a raw Bybit error.
"""

from __future__ import annotations

from unified_trading_execution.errors import (
    InsufficientBalanceError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UteError,
)

_RET_CODE_MAP: dict[int, type[UteError]] = {
    # ---- Rate limiting ----
    10006: RateLimitError,         # "Too many visits. Exceeded the API Rate Limit"
    10018: RateLimitError,         # "Exceeded the IP Rate Limit"
    10429: RateLimitError,         # WS OE: "System level frequency protection"
    20003: RateLimitError,         # WS OE: "Too frequent requests under the same session"
    30035: RateLimitError,         # "Option: Too fast to cancel, Try it later"
    170005: RateLimitError,        # "Too many new orders; current limit is %s orders per %s"
    170222: RateLimitError,        # "Too many requests in this time frame"
    # ---- Invalid symbol / coin ----
    10029: InvalidSymbolError,     # "The requested symbol is invalid"
    110050: InvalidSymbolError,    # "Invalid coin"
    170121: InvalidSymbolError,    # "Invalid symbol"
    170221: InvalidSymbolError,    # "This coin does not exist"
    # ---- Insufficient balance ----
    110004: InsufficientBalanceError,   # "Wallet balance is insufficient"
    110006: InsufficientBalanceError,   # "assets cannot cover position margin"
    110007: InsufficientBalanceError,   # "Available balance is insufficient"
    110012: InsufficientBalanceError,   # "Insufficient available balance"
    110044: InsufficientBalanceError,   # "Available margin is insufficient"
    110045: InsufficientBalanceError,   # "Wallet balance is insufficient"
    110051: InsufficientBalanceError,   # "balance cannot cover the lowest price of the current market"
    110052: InsufficientBalanceError,   # "insufficient balance to set the price"
    110053: InsufficientBalanceError,   # "balance cannot cover the current market price and upper limit price"
     110131: InsufficientBalanceError,   # "Margin limit exceeded (Perps)"
    30256: InsufficientBalanceError,    # "Margin limit exceeded (Spot)"
    170033: InsufficientBalanceError,   # "margin Insufficient account balance"
    170131: InsufficientBalanceError,   # "Balance insufficient"
    # ---- Order not found ----
    110001: OrderNotFoundError,    # "Order does not exist"
    170143: OrderNotFoundError,    # "Cannot be found on order book"
    170213: OrderNotFoundError,    # "Order does not exist"
    # ---- Connection / retryable ----
    10000: PlatformConnectionError,  # "Server Timeout"
    10016: PlatformConnectionError,  # "Server error"
    10019: PlatformConnectionError,  # WS OE: "ws trade service is restarting"
    110079: PlatformConnectionError, # "order is processing, try again later"
    110118: PlatformConnectionError, # "low liquidity, unable to retrieve price"
    170001: PlatformConnectionError, # "Internal error"
    170007: PlatformConnectionError, # "Timeout waiting for response from backend server"
    170032: PlatformConnectionError, # "Network error. Please try again later"
    170146: PlatformConnectionError, # "Order creation timeout"
    170147: PlatformConnectionError, # "Order cancellation timeout"
    170191: PlatformConnectionError, # "Can not cancel order, please try again later"
    170234: PlatformConnectionError, # "System Error"
    170310: PlatformConnectionError, # "Order modification timeout"
    3400214: PlatformConnectionError, # "Server error, please try again later"
}


def _platform_error_context(
    *,
    ret_code: int | None = None,
    http_status: int | None = None,
) -> dict[str, int | None] | None:
    ctx: dict[str, int | None] = {}
    if ret_code is not None:
        ctx["ret_code"] = ret_code
    if http_status is not None:
        ctx["http_status"] = http_status
    return ctx or None


def map_bybit_error(
    *,
    ret_code: int | None = None,
    ret_msg: str = "",
    http_status: int | None = None,
) -> UteError:
    if http_status is not None:
        if http_status == 429:
            return RateLimitError(ret_msg or "rate limit exceeded")
        if http_status == 403:
            return PlatformError(
                ret_msg or "permission denied",
                platform_error=_platform_error_context(http_status=http_status),
            )
        if 500 <= http_status < 600:
            return PlatformConnectionError(ret_msg or f"HTTP {http_status} server error")
        if http_status >= 400:
            return PlatformError(
                ret_msg or f"HTTP {http_status} error",
                platform_error=_platform_error_context(http_status=http_status),
            )

    if ret_code is not None:
        exc_type = _RET_CODE_MAP.get(ret_code)
        if exc_type is not None:
            return exc_type(ret_msg or f"Bybit error {ret_code}")
        return PlatformError(
            ret_msg or f"unmapped Bybit error {ret_code}",
            platform_error=_platform_error_context(ret_code=ret_code),
        )

    return PlatformError(ret_msg or "unknown Bybit error")

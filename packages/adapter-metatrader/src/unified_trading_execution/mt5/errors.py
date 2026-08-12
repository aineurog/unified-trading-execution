"""MT5 native error → unified exception hierarchy translation.

MT5's error model: ``mt5.last_error()`` returns ``(error_code: int,
description: str)``.  Every code must be translated into an exception from
``unified_trading_execution.errors`` before it crosses the adapter boundary.

Non-error return codes (``TRADE_RETCODE_PLACED``, ``TRADE_RETCODE_DONE``,
``TRADE_RETCODE_DONE_PARTIAL``, ``TRADE_RETCODE_NO_CHANGES``) are not mapped
— they indicate success and should never reach this function.
"""

from __future__ import annotations

from typing import Any

from unified_trading_execution.errors import (
    InstrumentHaltedError,
    InsufficientBalanceError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UteError,
)

# Maps MT5 trade return codes → unified exception types.
# Codes not in this dict fall through to PlatformError with full context.
_TRADE_RETCODE_MAP: dict[int, type[UteError]] = {
    # ---- Retryable / connection ----
    10004: PlatformConnectionError,  # TRADE_RETCODE_REQUOTE — price moved
    10007: PlatformConnectionError,  # TRADE_RETCODE_CANCEL — server-canceled
    10012: PlatformConnectionError,  # TRADE_RETCODE_TIMEOUT
    10020: PlatformConnectionError,  # TRADE_RETCODE_PRICE_CHANGED
    10021: PlatformConnectionError,  # TRADE_RETCODE_PRICE_OFF — no quotes
    10028: PlatformConnectionError,  # TRADE_RETCODE_LOCKED — order locked
    10031: PlatformConnectionError,  # TRADE_RETCODE_CONNECTION — no connection
    # ---- Invalid symbol / params ----
    10013: InvalidSymbolError,       # TRADE_RETCODE_INVALID — invalid request
    10014: InvalidSymbolError,       # TRADE_RETCODE_INVALID_VOLUME
    10015: InvalidSymbolError,       # TRADE_RETCODE_INVALID_PRICE
    10016: InvalidSymbolError,       # TRADE_RETCODE_INVALID_STOPS
    10018: InvalidSymbolError,       # TRADE_RETCODE_MARKET_CLOSED
    10022: InvalidSymbolError,       # TRADE_RETCODE_INVALID_EXPIRATION
    10034: InvalidSymbolError,       # TRADE_RETCODE_LIMIT_VOLUME
    # ---- Insufficient balance ----
    10019: InsufficientBalanceError, # TRADE_RETCODE_NO_MONEY
    # ---- Halted / frozen ----
    10017: InstrumentHaltedError,    # TRADE_RETCODE_TRADE_DISABLED
    10029: InstrumentHaltedError,    # TRADE_RETCODE_FROZEN
    # ---- Rate limiting ----
    10024: RateLimitError,           # TRADE_RETCODE_TOO_MANY_REQUESTS
    10033: RateLimitError,           # TRADE_RETCODE_LIMIT_ORDERS
    # ---- Order not found ----
    10035: OrderNotFoundError,       # TRADE_RETCODE_INVALID_ORDER
    # ---- Generic / catch-all ----
    10006: PlatformError,            # TRADE_RETCODE_REJECT — generic reject
    10011: PlatformError,            # TRADE_RETCODE_ERROR — processing error
    10023: PlatformError,            # TRADE_RETCODE_ORDER_CHANGED
    10026: PlatformError,            # TRADE_RETCODE_SERVER_DISABLES_AT
    10027: PlatformError,            # TRADE_RETCODE_CLIENT_DISABLES_AT
    10030: PlatformError,            # TRADE_RETCODE_INVALID_FILL
    10032: PlatformError,            # TRADE_RETCODE_ONLY_REAL
}

# Non-trade error codes returned by mt5.last_error() after
# non-order-send calls (initialize, symbol_info, account_info, etc.).
_NON_TRADE_ERROR_MAP: dict[int, type[UteError]] = {
    -1: PlatformError,               # Generic / unknown
    32768: PlatformConnectionError,  # Internal error, copy data failed
    32769: PlatformConnectionError,  # Not initialized
}


def map_mt5_error(error_code: int, description: str = "") -> UteError:
    """Translate an MT5 error code into a unified exception.

    Call after any MT5 function that sets ``mt5.last_error()``.
    *error_code* and *description* come from ``mt5.last_error()``.

    Returns an instance of the appropriate ``UteError`` subclass.
    Codes not in the map become ``PlatformError`` with the raw
    ``mt5_error_code`` and ``mt5_description`` carried as context.
    """
    exc_type = _TRADE_RETCODE_MAP.get(error_code) or _NON_TRADE_ERROR_MAP.get(error_code)
    if exc_type is not None:
        return exc_type(description or f"MT5 error {error_code}")
    return PlatformError(
        description or f"unmapped MT5 error {error_code}",
        platform_error={"mt5_error_code": error_code, "mt5_description": description},
    )


def check_mt5_result(result: Any, description: str = "") -> None:
    """Check *result* from an MT5 function call and raise if it indicates failure.

    Many MT5 functions return ``None`` or ``()`` on failure and set
    ``mt5.last_error()``.  This helper calls ``mt5.last_error()`` when
    *result* indicates failure and raises the mapped exception.
    """
    raise NotImplementedError

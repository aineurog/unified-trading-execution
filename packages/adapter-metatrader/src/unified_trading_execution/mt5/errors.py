"""MT5 native error → unified exception hierarchy translation.

MT5's error model: ``mt5.last_error()`` returns ``(error_code: int,
description: str)``.  Every code must be translated into an exception from
``unified_trading_execution.errors`` before it crosses the adapter boundary.

Non-error return codes (``TRADE_RETCODE_PLACED``, ``TRADE_RETCODE_DONE``,
``TRADE_RETCODE_DONE_PARTIAL``, ``TRADE_RETCODE_NO_CHANGES``) are not mapped
— they indicate success and should never reach this function.

The Python wrapper reports success as ``RES_S_OK`` (=1), not the MQL-native
0, and errors as the negative ``RES_E_*`` codes.  The non-trade mapping is
built from the wrapper's named constants (``_build_non_trade_error_map``).
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
    10013: InvalidSymbolError,  # TRADE_RETCODE_INVALID — invalid request
    10014: InvalidSymbolError,  # TRADE_RETCODE_INVALID_VOLUME
    10015: InvalidSymbolError,  # TRADE_RETCODE_INVALID_PRICE
    10016: InvalidSymbolError,  # TRADE_RETCODE_INVALID_STOPS
    10018: InvalidSymbolError,  # TRADE_RETCODE_MARKET_CLOSED
    10022: InvalidSymbolError,  # TRADE_RETCODE_INVALID_EXPIRATION
    10034: InvalidSymbolError,  # TRADE_RETCODE_LIMIT_VOLUME
    # ---- Insufficient balance ----
    10019: InsufficientBalanceError,  # TRADE_RETCODE_NO_MONEY
    # ---- Halted / frozen ----
    10017: InstrumentHaltedError,  # TRADE_RETCODE_TRADE_DISABLED
    10029: InstrumentHaltedError,  # TRADE_RETCODE_FROZEN
    # ---- Rate limiting ----
    10024: RateLimitError,  # TRADE_RETCODE_TOO_MANY_REQUESTS
    10033: RateLimitError,  # TRADE_RETCODE_LIMIT_ORDERS
    # ---- Order not found ----
    10035: OrderNotFoundError,  # TRADE_RETCODE_INVALID_ORDER
    # ---- Generic / catch-all ----
    10006: PlatformError,  # TRADE_RETCODE_REJECT — generic reject
    10011: PlatformError,  # TRADE_RETCODE_ERROR — processing error
    10023: PlatformError,  # TRADE_RETCODE_ORDER_CHANGED
    10026: PlatformConnectionError,  # TRADE_RETCODE_SERVER_DISABLES_AT — auto-trading off
    10027: PlatformConnectionError,  # TRADE_RETCODE_CLIENT_DISABLES_AT — auto-trading off
    10030: PlatformError,  # TRADE_RETCODE_INVALID_FILL
    10032: PlatformError,  # TRADE_RETCODE_ONLY_REAL
}


def _build_non_trade_error_map(mt5: Any) -> dict[int, type[UteError]]:
    """Map ``last_error()`` codes (the wrapper's negative ``RES_E_*`` space).

    Built from the wrapper's named constants so the mapping stays in sync
    with the installed MetaTrader5 module instead of raw ints.  Codes not
    listed fall through to ``PlatformError`` with full context.
    """
    return {
        mt5.RES_E_FAIL: PlatformError,  # generic failure
        mt5.RES_E_INVALID_PARAMS: PlatformError,  # invalid arguments
        mt5.RES_E_NO_MEMORY: PlatformError,  # no memory condition
        mt5.RES_E_NOT_FOUND: PlatformError,  # no history
        mt5.RES_E_INVALID_VERSION: PlatformError,  # invalid version
        mt5.RES_E_AUTH_FAILED: PlatformConnectionError,  # authorization failed
        mt5.RES_E_UNSUPPORTED: PlatformError,  # unsupported method
        mt5.RES_E_AUTO_TRADING_DISABLED: PlatformConnectionError,  # auto-trading off
        mt5.RES_E_INTERNAL_FAIL: PlatformConnectionError,  # IPC general failure
        mt5.RES_E_INTERNAL_FAIL_SEND: PlatformConnectionError,  # IPC send failed
        mt5.RES_E_INTERNAL_FAIL_RECEIVE: PlatformConnectionError,  # IPC recv failed
        mt5.RES_E_INTERNAL_FAIL_INIT: PlatformConnectionError,  # IPC initialization
        mt5.RES_E_INTERNAL_FAIL_CONNECT: PlatformConnectionError,  # IPC no ipc
        mt5.RES_E_INTERNAL_FAIL_TIMEOUT: PlatformConnectionError,  # IPC timeout
    }


def map_mt5_error(error_code: int, description: str = "") -> UteError:
    """Translate an MT5 error code into a unified exception.

    Call after any MT5 function that sets ``mt5.last_error()``.
    *error_code* and *description* come from ``mt5.last_error()``.

    Returns an instance of the appropriate ``UteError`` subclass.
    Codes not in the map become ``PlatformError`` with the raw
    ``mt5_error_code`` and ``mt5_description`` carried as context.
    """
    exc_type = _TRADE_RETCODE_MAP.get(error_code)
    if exc_type is None:
        import unified_trading_execution.mt5.adapter as _mt5_adapter

        exc_type = _build_non_trade_error_map(_mt5_adapter._get_mt5()).get(error_code)
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
    import unified_trading_execution.mt5.adapter as _mt5_adapter

    mt5 = _mt5_adapter._get_mt5()
    if result is None or (hasattr(result, "__len__") and len(result) == 0):
        code, desc = mt5.last_error()
        # Success is RES_S_OK (1) in the wrapper, not the MQL-native 0.
        if code != 0 and code != mt5.RES_S_OK:
            raise map_mt5_error(code, desc or description)

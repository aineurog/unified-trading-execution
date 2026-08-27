"""IBKR native error → unified exception hierarchy translation.

IBKR's error model: The API returns errors via integer error codes
(e.g., 100, 110, 200) and string messages. Every code must be translated
into an exception from ``unified_trading_execution.errors`` before it
crosses the adapter boundary.

Informational and warning codes (such as connectivity restored or
historical data farm messages) are not mapped to exceptions — they
indicate state changes or warnings rather than execution failures.
"""

from __future__ import annotations

from typing import Any

from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UnsupportedOrderTypeError,
    UteError,
)

# Maps IBKR error codes → unified exception types.
# Codes not in this dict fall through to PlatformError with full context.
# We intentionally omit code 201 ("Order Rejected") as it acts as a generic
# catch-all for margin, shorting, and compliance errors; it correctly falls
# through to PlatformError where the specific string context is preserved.
_IBKR_ERROR_CODE_MAP: dict[int, type[UteError]] = {
    # ---- Rate limiting ----
    100: RateLimitError,  # Max rate of messages per second has been exceeded.
    101: RateLimitError,  # Max number of tickers has been reached.
    # ---- Duplicate Order ----
    103: DuplicateOrderIdError,  # Duplicate order ID.
    # ---- Unsupported / Invalid Parameters ----
    109: UnsupportedOrderTypeError,  # Price out of range defined by precautionary settings.
    110: UnsupportedOrderTypeError,  # Price does not conform to the minimum price variation.
    111: UnsupportedOrderTypeError,  # The TIF and the order type are incompatible.
    113: UnsupportedOrderTypeError,  # The TIF option should be set to DAY for MOC and LOC orders.
    # ---- Invalid Symbol ----
    116: InvalidSymbolError,  # The order cannot be transmitted to a dead exchange.
    162: InvalidSymbolError,  # Historical Market Data Service error (invalid symbol/permissions).
    200: InvalidSymbolError,  # No security definition has been found for the request.
    # ---- Order Not Found ----
    104: OrderNotFoundError,  # Can't modify a filled order (no longer active).
    105: OrderNotFoundError,  # Order being modified does not match original order.
    135: OrderNotFoundError,  # Can't find order with ID.
    136: OrderNotFoundError,  # This order cannot be cancelled (usually terminal already).
    10147: OrderNotFoundError,  # Order to be canceled was not found.
    # ---- Connection Errors ----
    326: PlatformConnectionError,  # Client id already in use — connect with a unique id.
    501: PlatformConnectionError,  # Already connected.
    502: PlatformConnectionError,  # Couldn't connect to TWS.
    503: PlatformConnectionError,  # The TWS is out of date and must be upgraded.
    504: PlatformConnectionError,  # Not connected.
    509: PlatformConnectionError,  # Exception caught while reading socket.
    1100: PlatformConnectionError,  # Connectivity between IB and TWS has been lost.
    2103: PlatformConnectionError,  # Market data farm connection is broken.
    2105: PlatformConnectionError,  # HMDS data farm connection is broken.
}


def map_ibkr_error(error_code: int, error_string: str = "") -> UteError:
    """Translate an IBKR error code into a unified exception.

    *error_code* and *error_string* come from IBKR's errorEvent or API response.

    Returns an instance of the appropriate ``UteError`` subclass.
    Codes not in the map become ``PlatformError`` with the raw
    ``ibkr_error_code`` and ``ibkr_error_string`` carried as context.
    """
    exc_type = _IBKR_ERROR_CODE_MAP.get(error_code)
    if exc_type is not None:
        return exc_type(error_string or f"IBKR error {error_code}")

    return PlatformError(
        error_string or f"unmapped IBKR error {error_code}",
        platform_error={"ibkr_error_code": error_code, "ibkr_error_string": error_string},
    )


def check_ibkr_result(result: Any, description: str = "") -> None:
    """Check *result* from an IBKR function call and raise if it indicates failure.

    In ib_async, some failures are returned as empty lists, ``None``, or raise
    built-in Python exceptions (e.g. asyncio.TimeoutError). This helper standardizes
    result validation.
    """
    if result is None:
        raise PlatformError(f"{description} failed: returned None.")

    if isinstance(result, list) and len(result) == 0:
        raise PlatformError(f"{description} failed: returned empty list.")

    if isinstance(result, Exception):
        raise PlatformError(f"{description} failed: {result}")

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
    PlatformError,
    UteError,
)

# Maps IBKR error codes → unified exception types.
# Codes not in this dict fall through to PlatformError with full context.
_IBKR_ERROR_CODE_MAP: dict[int, type[UteError]] = {
    # Scaffold: To be populated with specific IBKR integer codes
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
    built-in Python exceptions. This helper standardizes result validation.
    """
    raise NotImplementedError

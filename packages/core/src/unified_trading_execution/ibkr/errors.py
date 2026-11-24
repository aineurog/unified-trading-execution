"""IBKR native error → unified exception hierarchy translation.

IBKR's error model: errors arrive via integer error codes (e.g., 100, 110,
200) with string messages, delivered asynchronously through
``IB.errorEvent(reqId, errorCode, errorString, contract)`` (see
``ib_async.ib.IB`` / ``ib_async.wrapper.Wrapper.error``). Every code must be
translated into an exception from ``unified_trading_execution.errors``
before it crosses the adapter boundary.

Code reference: IBKR TWS API "Error Codes" table
(``docs/tws-api/doc/error-handling/error-codes``) and "System Message Codes"
(``docs/tws-api/doc/error-handling/system-message-codes``). Only codes whose
TWS message text was verified against those tables are mapped below.

Informational codes (farm-OK / restored notifications) are not mapped to
exceptions — see ``IGNORED_IBKR_CODES``. They indicate state changes rather
than execution failures and are filtered by the adapter's ``_on_error``
handler before translation.
"""

from __future__ import annotations

from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InstrumentHaltedError,
    InvalidOrderError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UnsupportedOrderTypeError,
    UteError,
)

# Notification codes — verified "not a true error condition" / "safely
# ignore" in the official Error Codes table, or connectivity-restored on the
# System Message Codes page. The adapter's ``_on_error`` handler filters
# these to a debug log and never translates them into exceptions.
IGNORED_IBKR_CODES: frozenset[int] = frozenset(
    {
        2104,  # Market data farm connection is OK.
        2106,  # A historical data farm is connected.
        2107,  # A historical data farm connection has become inactive.
        2108,  # A market data farm connection has become inactive.
        2119,  # Market data farm is connecting.
        2158,  # Sec-def data farm connection is OK.
        1101,  # Connectivity between IB and TWS restored — data lost.
        1102,  # Connectivity between IB and TWS restored — data maintained.
    }
)

# Maps IBKR error codes → unified exception types.
# Codes not in this dict fall through to PlatformError with full context.
# Code 201 ("Order rejected - Reason:") and 202 ("Order cancelled -
# Reason:") are intentionally omitted: the docs table gives only the prefix
# and the distinguishing reason is appended at runtime (funds, short, halt,
# closed, ...), with no enumerated suffix list to verify against. Sniffing
# the free-text suffix would be guesswork, so they fall through to
# PlatformError where the raw string context is preserved. Rejections still
# surface authoritatively: _on_error logs the mapped type and the order's
# terminal status is read on the next reconcile pass via fetch_open_orders.
_IBKR_ERROR_CODE_MAP: dict[int, type[UteError]] = {
    # ---- Rate limiting ----
    100: RateLimitError,  # Max rate of messages per second has been exceeded.
    101: RateLimitError,  # Max number of tickers has been reached.
    # ---- Duplicate Order ----
    103: DuplicateOrderIdError,  # Duplicate order ID.
    # ---- Unsupported / Invalid Parameters ----
    106: UnsupportedOrderTypeError,  # Can't transmit order ID: invalid type/formatting.
    111: UnsupportedOrderTypeError,  # The TIF and the order type are incompatible.
    113: UnsupportedOrderTypeError,  # The TIF option should be set to DAY for MOC and LOC orders.
    # ---- Invalid Order (price validation) ----
    109: InvalidOrderError,  # Price out of range defined by precautionary settings.
    110: InvalidOrderError,  # Price does not conform to the minimum price variation.
    # ---- Invalid Symbol ----
    116: InvalidSymbolError,  # The order cannot be transmitted to a dead exchange.
    124: InvalidSymbolError,  # No market rule for conid: non-tradeable instrument e.g. Index.
    138: InvalidSymbolError,  # Could not parse ticker request: invalid symbols.
    162: InvalidSymbolError,  # Historical Market Data Service error (invalid symbol/permissions).
    200: InvalidSymbolError,  # No security definition has been found for the request.
    203: InvalidSymbolError,  # Security not available or allowed for this account.
    # ---- Halted ----
    154: InstrumentHaltedError,  # Orders cannot be transmitted for a halted security.
    # ---- Order Not Found ----
    104: OrderNotFoundError,  # Can't modify a filled order (no longer active).
    105: OrderNotFoundError,  # Order being modified does not match original order.
    134: OrderNotFoundError,  # Modify order failed: already executed or cancelled.
    135: OrderNotFoundError,  # Can't find order with ID.
    136: OrderNotFoundError,  # This order cannot be cancelled (usually terminal already).
    161: OrderNotFoundError,  # Cancel attempted when order is not in a cancellable state.
    10147: OrderNotFoundError,  # Order to be canceled was not found.
    # ---- Connection Errors ----
    326: PlatformConnectionError,  # Client id already in use — connect with a unique id.
    501: PlatformConnectionError,  # Already connected.
    502: PlatformConnectionError,  # Couldn't connect to TWS.
    503: PlatformConnectionError,  # The TWS is out of date and must be upgraded.
    504: PlatformConnectionError,  # Not connected.
    509: PlatformConnectionError,  # Exception caught while reading socket.
    1100: PlatformConnectionError,  # Connectivity between IB and TWS has been lost.
    1300: PlatformConnectionError,  # TWS socket port reset — reconnect on the new port.
    2102: PlatformConnectionError,  # Unable to modify: order still being processed (transient).
    2103: PlatformConnectionError,  # Market data farm connection is broken.
    2105: PlatformConnectionError,  # HMDS data farm connection is broken.
    2110: PlatformConnectionError,  # Connectivity between TWS and server is broken.
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

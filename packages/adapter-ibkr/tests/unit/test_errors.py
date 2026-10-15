"""Unit tests for IBKR error translation (errors.py).

Tests cases:
    - Every mapped IBKR error code produces the correct exception type
    - Unmapped codes fall through to PlatformError with raw context
    - map_ibkr_error returns exception instances (not classes)
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
from unified_trading_execution.ibkr.errors import (
    IGNORED_IBKR_CODES,
    map_ibkr_error,
)


class TestMapIBKRError:
    """Test map_ibkr_error translation."""

    def test_known_error_code_maps_correctly(self) -> None:
        """Each mapped code → correct exception subclass."""
        cases: list[tuple[int, type[UteError]]] = [
            # ---- Rate limiting ----
            (100, RateLimitError),  # Max message rate exceeded
            (101, RateLimitError),  # Max tickers reached
            # ---- Duplicate Order ----
            (103, DuplicateOrderIdError),  # Duplicate order ID
            # ---- Unsupported / Invalid Parameters ----
            (106, UnsupportedOrderTypeError),  # Can't transmit order ID
            (111, UnsupportedOrderTypeError),  # TIF and order type incompatible
            (113, UnsupportedOrderTypeError),  # TIF must be DAY for MOC/LOC
            # ---- Invalid Order (price validation) ----
            (109, InvalidOrderError),  # Price out of precautionary range
            (110, InvalidOrderError),  # Minimum price variation mismatch
            # ---- Invalid Symbol ----
            (116, InvalidSymbolError),  # Dead exchange
            (124, InvalidSymbolError),  # No market rule for conid (non-tradeable)
            (138, InvalidSymbolError),  # Could not parse ticker request
            (162, InvalidSymbolError),  # HMDS error / invalid symbol
            (200, InvalidSymbolError),  # Security definition not found
            (203, InvalidSymbolError),  # Security not available for account
            # ---- Halted ----
            (154, InstrumentHaltedError),  # Halted security
            # ---- Order Not Found ----
            (104, OrderNotFoundError),  # Can't modify filled order
            (105, OrderNotFoundError),  # Modified order mismatch
            (134, OrderNotFoundError),  # Modify failed: already done
            (135, OrderNotFoundError),  # Order ID not found
            (136, OrderNotFoundError),  # Order cannot be cancelled
            (161, OrderNotFoundError),  # Cancel when not cancellable
            (10147, OrderNotFoundError),  # Order to be canceled was not found
            # ---- Connection Errors ----
            (326, PlatformConnectionError),  # Client ID in use
            (501, PlatformConnectionError),  # Already connected
            (502, PlatformConnectionError),  # Couldn't connect to TWS
            (503, PlatformConnectionError),  # TWS out of date
            (504, PlatformConnectionError),  # Not connected
            (509, PlatformConnectionError),  # Socket exception
            (1100, PlatformConnectionError),  # Connectivity lost
            (1300, PlatformConnectionError),  # Socket port reset
            (2102, PlatformConnectionError),  # Modify while still processing
            (2103, PlatformConnectionError),  # Market data farm broken
            (2105, PlatformConnectionError),  # HMDS farm broken
            (2110, PlatformConnectionError),  # TWS-server connectivity broken
        ]
        for code, expected in cases:
            err = map_ibkr_error(code, "test error description")
            assert isinstance(err, expected)
            assert "test error description" in str(err)

    def test_overloaded_reject_stays_generic(self) -> None:
        """201/202 carry runtime reasons — preserved as PlatformError context."""
        for code in (201, 202):
            err = map_ibkr_error(code, "Order rejected - Reason: funds exceeded")
            assert type(err) is PlatformError
            ctx = err.platform_error
            assert isinstance(ctx, dict)
            assert ctx.get("ibkr_error_code") == code
            assert "funds exceeded" in str(ctx.get("ibkr_error_string"))

    def test_ignored_codes_are_filtered(self) -> None:
        """Farm-OK / restored notifications never become exceptions."""
        assert 2104 in IGNORED_IBKR_CODES
        assert 2106 in IGNORED_IBKR_CODES
        assert 2119 in IGNORED_IBKR_CODES
        assert 2158 in IGNORED_IBKR_CODES
        assert 1101 in IGNORED_IBKR_CODES
        assert 1102 in IGNORED_IBKR_CODES

    def test_unmapped_code_becomes_platform_error(self) -> None:
        """Unknown codes fall through to PlatformError with raw context."""
        err = map_ibkr_error(99999, "unmapped error")
        assert isinstance(err, PlatformError)
        ctx = err.platform_error
        assert isinstance(ctx, dict)
        assert ctx.get("ibkr_error_code") == 99999
        assert ctx.get("ibkr_error_string") == "unmapped error"

    def test_returns_instance_not_class(self) -> None:
        """map_ibkr_error returns an exception instance."""
        err = map_ibkr_error(100)
        assert isinstance(err, RateLimitError)
        assert not isinstance(err, type)

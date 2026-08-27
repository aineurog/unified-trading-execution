"""Unit tests for IBKR error translation (errors.py).

Tests cases:
    - Every mapped IBKR error code produces the correct exception type
    - Unmapped codes fall through to PlatformError with raw context
    - map_ibkr_error returns exception instances (not classes)
    - check_ibkr_result raises on None / empty list / exception objects
    - check_ibkr_result passes through on valid successful results
"""

from __future__ import annotations

import pytest

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
from unified_trading_execution.ibkr.errors import check_ibkr_result, map_ibkr_error


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
            (109, UnsupportedOrderTypeError),  # Price out of precautionary range
            (110, UnsupportedOrderTypeError),  # Minimum price variation mismatch
            (111, UnsupportedOrderTypeError),  # TIF and order type incompatible
            (113, UnsupportedOrderTypeError),  # TIF must be DAY for MOC/LOC
            # ---- Invalid Symbol ----
            (116, InvalidSymbolError),  # Dead exchange
            (162, InvalidSymbolError),  # HMDS error / invalid symbol
            (200, InvalidSymbolError),  # Security definition not found
            # ---- Order Not Found ----
            (104, OrderNotFoundError),  # Can't modify filled order
            (105, OrderNotFoundError),  # Modified order mismatch
            (135, OrderNotFoundError),  # Order ID not found
            (136, OrderNotFoundError),  # Order cannot be cancelled
            (10147, OrderNotFoundError),  # Order to be canceled was not found
            # ---- Connection Errors ----
            (326, PlatformConnectionError),  # Client ID in use
            (501, PlatformConnectionError),  # Already connected
            (502, PlatformConnectionError),  # Couldn't connect to TWS
            (503, PlatformConnectionError),  # TWS out of date
            (504, PlatformConnectionError),  # Not connected
            (509, PlatformConnectionError),  # Socket exception
            (1100, PlatformConnectionError),  # Connectivity lost
            (2103, PlatformConnectionError),  # Market data farm broken
            (2105, PlatformConnectionError),  # HMDS farm broken
        ]
        for code, expected in cases:
            err = map_ibkr_error(code, "test error description")
            assert isinstance(err, expected)
            assert "test error description" in str(err)

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


class TestCheckIBKRResult:
    """Test check_ibkr_result guard."""

    def test_none_result_raises(self) -> None:
        """None result triggers failure check and raises PlatformError."""
        with pytest.raises(PlatformError, match="reqContractDetails failed: returned None."):
            check_ibkr_result(None, "reqContractDetails")

    def test_empty_list_result_raises(self) -> None:
        """Empty list result triggers failure check and raises PlatformError."""
        with pytest.raises(PlatformError, match="reqContractDetails failed: returned empty list."):
            check_ibkr_result([], "reqContractDetails")

    def test_exception_result_raises(self) -> None:
        """Exception object returned in place of result wraps in PlatformError."""
        exc = TimeoutError("Connection timed out")
        with pytest.raises(PlatformError, match="reqContractDetails failed: Connection timed out"):
            check_ibkr_result(exc, "reqContractDetails")

    def test_success_result_passes_through(self) -> None:
        """Valid result returns without raising."""
        check_ibkr_result([1, 2, 3])
        check_ibkr_result({"status": "Filled"})
        check_ibkr_result(True)
        check_ibkr_result("Success")
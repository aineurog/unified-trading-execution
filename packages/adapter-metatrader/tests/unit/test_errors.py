"""Unit tests for MT5 error translation (errors.py).

Tests cases:
    - Every mapped TRADE_RETCODE_* produces the correct exception type
    - Unmapped codes fall through to PlatformError with raw context
    - Non-trade error codes (initialize, symbol_info, etc.) map correctly
    - map_mt5_error returns exception instances (not classes)
    - check_mt5_result raises on None / empty tuple / failure codes
    - check_mt5_result passes through on success return codes
"""

from __future__ import annotations

import pytest

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
from unified_trading_execution.mt5.errors import check_mt5_result, map_mt5_error


class TestMapMT5Error:
    """Test map_mt5_error translation."""

    def test_known_trade_code_maps_correctly(self) -> None:
        """Each mapped code → correct exception subclass."""
        cases: list[tuple[int, type[UteError]]] = [
            (10004, PlatformConnectionError),  # TRADE_RETCODE_REQUOTE
            (10006, PlatformError),  # TRADE_RETCODE_REJECT
            (10013, InvalidSymbolError),  # TRADE_RETCODE_INVALID
            (10015, InvalidSymbolError),  # TRADE_RETCODE_INVALID_PRICE
            (10017, InstrumentHaltedError),  # TRADE_RETCODE_TRADE_DISABLED
            (10019, InsufficientBalanceError),  # TRADE_RETCODE_NO_MONEY
            (10024, RateLimitError),  # TRADE_RETCODE_TOO_MANY_REQUESTS
            (10035, OrderNotFoundError),  # TRADE_RETCODE_INVALID_ORDER
        ]
        for code, expected in cases:
            assert isinstance(map_mt5_error(code, "desc"), expected)

    def test_unmapped_code_becomes_platform_error(self) -> None:
        """Unknown codes fall through to PlatformError."""
        err = map_mt5_error(99999, "weird")
        assert isinstance(err, PlatformError)
        ctx = err.platform_error
        assert isinstance(ctx, dict)
        assert ctx.get("mt5_error_code") == 99999

    def test_non_trade_code_initialization_error(self) -> None:
        """Non-trade codes (-1, 32768, 32769) map correctly."""
        assert isinstance(map_mt5_error(-1), PlatformError)
        assert isinstance(map_mt5_error(32768), PlatformConnectionError)
        assert isinstance(map_mt5_error(32769), PlatformConnectionError)

    def test_returns_instance_not_class(self) -> None:
        """map_mt5_error returns an exception instance."""
        err = map_mt5_error(10024)
        assert isinstance(err, RateLimitError)
        assert not isinstance(err, type)


class TestCheckMT5Result:
    """Test check_mt5_result guard."""

    def test_none_result_raises(self, mock_mt5_module) -> None:
        """None result triggers last_error check and raises."""
        mock_mt5_module.last_error.return_value = (10015, "invalid price")
        with pytest.raises(InvalidSymbolError, match="invalid price"):
            check_mt5_result(None, "order_send")

    def test_empty_tuple_result_raises(self, mock_mt5_module) -> None:
        """Empty tuple from orders_get triggers last_error check and raises."""
        mock_mt5_module.last_error.return_value = (10024, "too many requests")
        with pytest.raises(RateLimitError, match="too many requests"):
            check_mt5_result(())

    def test_success_result_passes_through(self) -> None:
        """Valid result returns without raising."""
        check_mt5_result(3.14)
        check_mt5_result([1, 2, 3])

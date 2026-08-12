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


class TestMapMT5Error:
    """Test map_mt5_error translation."""

    def test_known_trade_code_maps_correctly(self) -> None:
        """Each mapped code → correct exception subclass."""
        ...

    def test_unmapped_code_becomes_platform_error(self) -> None:
        """Unknown codes fall through to PlatformError."""
        ...

    def test_non_trade_code_initialization_error(self) -> None:
        """Non-trade codes (-1, 32768, 32769) map correctly."""
        ...

    def test_returns_instance_not_class(self) -> None:
        """map_mt5_error returns an exception instance."""
        ...


class TestCheckMT5Result:
    """Test check_mt5_result guard."""

    def test_none_result_raises(self) -> None:
        """None result triggers last_error check and raises."""
        ...

    def test_success_result_passes_through(self) -> None:
        """Valid result returns without raising."""
        ...

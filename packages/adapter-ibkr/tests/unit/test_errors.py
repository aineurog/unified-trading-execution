"""Unit tests for IBKR error translation (errors.py).

Tests cases:
    - Every mapped IBKR error code produces the correct exception type
    - Unmapped error codes fall through to PlatformError with raw context
    - map_ibkr_error returns exception instances (not classes)
    - check_ibkr_result raises on None / empty results or error events
    - check_ibkr_result passes through on valid successful results
"""

from __future__ import annotations


class TestMapIBKRError:
    """Test map_ibkr_error translation."""

    def test_known_error_code_maps_correctly(self) -> None:
        """Each mapped code → correct exception subclass."""
        ...

    def test_unmapped_code_becomes_platform_error(self) -> None:
        """Unknown codes fall through to PlatformError."""
        ...

    def test_returns_instance_not_class(self) -> None:
        """map_ibkr_error returns an exception instance."""
        ...


class TestCheckIBKRResult:
    """Test check_ibkr_result guard."""

    def test_none_or_empty_result_raises(self) -> None:
        """None or empty result triggers error check and raises."""
        ...

    def test_success_result_passes_through(self) -> None:
        """Valid result returns without raising."""
        ...

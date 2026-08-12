"""Unit tests for MT5Config validation.

Tests cases:
    - Valid minimal config (login + password + server)
    - Valid full config with aliases and custom intervals
    - Rejects alias keys without "/" separator
    - Rejects empty broker symbol in alias table
    - Rejects non-positive poll_interval_seconds
    - Rejects non-positive instrument_spec_cache_ttl (allows None)
    - Immutability: frozen dataclass hash/equality
"""

from __future__ import annotations


class TestMT5ConfigValidation:
    """Test MT5Config.__post_init__ validation rules."""

    def test_minimal_config(self) -> None:
        """A login + password + server is sufficient."""
        ...

    def test_full_config(self) -> None:
        """All optional fields accepted."""
        ...

    def test_alias_key_must_have_slash(self) -> None:
        """Keys without / are rejected."""
        ...

    def test_alias_value_must_be_non_empty(self) -> None:
        """Empty broker symbol strings are rejected."""
        ...

    def test_poll_interval_must_be_positive(self) -> None:
        """poll_interval_seconds can't be zero or negative."""
        ...

    def test_cache_ttl_must_be_positive_or_none(self) -> None:
        """instrument_spec_cache_ttl must be > 0 or None."""
        ...


class TestMT5ConfigImmutability:
    """Frozen dataclass: hashable, eq works."""

    def test_hashable(self) -> None:
        """Config can be used as a dict key."""
        ...

    def test_equality(self) -> None:
        """Same fields → equal configs."""
        ...

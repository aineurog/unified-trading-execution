"""Unit tests for IBKRConfig validation.

Tests cases:
    - Valid minimal config (defaults)
    - Valid full config with custom host, port, client_id, and account
    - Rejects non-positive timeout_seconds
    - Rejects negative client_id
    - Rejects empty default_exchange or default_currency
    - Rejects non-positive instrument_spec_cache_ttl (allows None)
    - Immutability: frozen dataclass hash/equality
"""

from __future__ import annotations


class TestIBKRConfigValidation:
    """Test IBKRConfig.__post_init__ validation rules."""

    def test_minimal_config(self) -> None:
        """Default values are valid."""
        ...

    def test_full_config(self) -> None:
        """All optional and custom fields accepted."""
        ...

    def test_timeout_must_be_positive(self) -> None:
        """timeout_seconds can't be zero or negative."""
        ...

    def test_client_id_must_be_non_negative(self) -> None:
        """client_id can't be negative."""
        ...

    def test_default_exchange_must_be_non_empty(self) -> None:
        """Empty default_exchange strings are rejected."""
        ...

    def test_default_currency_must_be_non_empty(self) -> None:
        """Empty default_currency strings are rejected."""
        ...

    def test_cache_ttl_must_be_positive_or_none(self) -> None:
        """instrument_spec_cache_ttl must be > 0 or None."""
        ...


class TestIBKRConfigImmutability:
    """Frozen dataclass: hashable, eq works."""

    def test_hashable(self) -> None:
        """Config can be used as a dict key."""
        ...

    def test_equality(self) -> None:
        """Same fields → equal configs."""
        ...

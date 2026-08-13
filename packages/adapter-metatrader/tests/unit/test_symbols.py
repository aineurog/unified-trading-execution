"""Unit tests for MT5 symbol translation (symbols.py).

Tests cases:
    - to_mt5_symbol: concatenates symbol + quote currency
    - to_mt5_symbol: returns broker_symbol_override directly when set
    - to_mt5_symbol: handles 3-letter and non-standard currency codes
    - from_mt5_symbol: splits canonical shorthand from reverse alias table
    - from_mt5_symbol: returns None quote when shorthand has no quote
    - from_mt5_symbol: raises ValueError when symbol not in reverse table
    - build_reverse_alias_table: swaps keys/values
    - build_reverse_alias_table: empty dict → empty dict
"""

from __future__ import annotations


class TestToMT5Symbol:
    """Canonical Instrument → MT5 broker symbol string."""

    def test_simple_concatenation(self) -> None:
        """Instrument("EUR", "USD") → "EURUSD"."""
        ...

    def test_uses_broker_override_when_set(self) -> None:
        """broker_symbol_override is returned directly."""
        ...

    def test_non_standard_currencies(self) -> None:
        """3-letter and longer currency codes."""
        ...


class TestFromMT5Symbol:
    """MT5 broker symbol string → canonical (symbol, quote) pair."""

    def test_splits_canonical_shorthand(self) -> None:
        """Reverse alias "EURUSD.m" → "EUR/USD" → ("EUR", "USD")."""
        ...

    def test_quote_none_when_no_separator(self) -> None:
        """Canonical value without "/" → ("SYMBOL", None)."""
        ...

    def test_raises_when_symbol_not_in_table(self) -> None:
        """Symbol missing from reverse table raises ValueError (no raw parsing)."""
        ...


class TestBuildReverseAliasTable:
    """Reverse alias table construction."""

    def test_swaps_keys_and_values(self) -> None:
        """{"EUR/USD": "EURUSD.m"} → {"EURUSD.m": "EUR/USD"}."""
        ...

    def test_empty_dict(self) -> None:
        """Empty forward → empty reverse."""
        ...

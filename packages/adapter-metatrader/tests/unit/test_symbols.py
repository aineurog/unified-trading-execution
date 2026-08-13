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

import pytest

from unified_trading_execution.mt5.symbols import (
    build_reverse_alias_table,
    from_mt5_symbol,
    to_mt5_symbol,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument, _with_broker_override


class TestToMT5Symbol:
    """Canonical Instrument → MT5 broker symbol string."""

    def test_simple_concatenation(self) -> None:
        """Instrument("EUR", "USD") → "EURUSD"."""
        instrument = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
        )
        assert to_mt5_symbol(instrument) == "EURUSD"

    def test_uses_broker_override_when_set(self) -> None:
        """broker_symbol_override is returned directly."""
        instrument = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
        )
        overridden = _with_broker_override(instrument, "EURUSD.m")
        assert to_mt5_symbol(overridden) == "EURUSD.m"

    def test_non_standard_currencies(self) -> None:
        """3-letter and longer currency codes."""
        instrument = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.FUTURES,
            multiplier=1,
        )
        assert to_mt5_symbol(instrument) == "BTCUSDT"

    def test_missing_quote_raises(self) -> None:
        """Instrument without quote_currency and no override → ValueError."""
        instrument = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK)
        with pytest.raises(ValueError):
            to_mt5_symbol(instrument)


class TestFromMT5Symbol:
    """MT5 broker symbol string → canonical (symbol, quote) pair."""

    def test_splits_canonical_shorthand(self) -> None:
        """Reverse alias "EURUSD.m" → "EUR/USD" → ("EUR", "USD")."""
        reverse = build_reverse_alias_table({"EUR/USD": "EURUSD.m"})
        assert from_mt5_symbol("EURUSD.m", reverse) == ("EUR", "USD")

    def test_quote_none_when_no_separator(self) -> None:
        """Canonical value without "/" → ("SYMBOL", None)."""
        reverse = build_reverse_alias_table({"AAPL": "AAPL"})
        assert from_mt5_symbol("AAPL", reverse) == ("AAPL", None)

    def test_raises_when_symbol_not_in_table(self) -> None:
        """Symbol missing from reverse table raises ValueError (no raw parsing)."""
        reverse = build_reverse_alias_table({"EUR/USD": "EURUSD.m"})
        with pytest.raises(ValueError):
            from_mt5_symbol("US500", reverse)

    def test_raises_when_no_table(self) -> None:
        """No reverse table → any symbol raises ValueError."""
        with pytest.raises(ValueError):
            from_mt5_symbol("EURUSD.m")


class TestBuildReverseAliasTable:
    """Reverse alias table construction."""

    def test_swaps_keys_and_values(self) -> None:
        """{"EUR/USD": "EURUSD.m"} → {"EURUSD.m": "EUR/USD"}."""
        assert build_reverse_alias_table({"EUR/USD": "EURUSD.m"}) == {"EURUSD.m": "EUR/USD"}

    def test_empty_dict(self) -> None:
        """Empty forward → empty reverse."""
        assert build_reverse_alias_table({}) == {}

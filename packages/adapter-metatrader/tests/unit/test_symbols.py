"""Unit tests for MT5 symbol translation (symbols.py).

Tests cases:
    - to_mt5_symbol: returns platform_symbol verbatim
    - to_mt5_symbol: preserves case / non-standard suffixes
    - to_mt5_symbol: raises ValueError when platform_symbol is missing
"""

from __future__ import annotations

import pytest

from unified_trading_execution.mt5.symbols import to_mt5_symbol
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


class TestToMT5Symbol:
    """Canonical Instrument → MT5 broker symbol string."""

    def test_returns_platform_symbol_verbatim(self) -> None:
        """platform_symbol is returned unchanged."""
        instrument = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
            platform_symbol="EURUSD.m",
        )
        assert to_mt5_symbol(instrument) == "EURUSD.m"

    def test_preserves_case_and_suffix(self) -> None:
        """Broker symbol casing/suffix is never normalised or guessed."""
        instrument = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
            platform_symbol="eurusd.pro",
        )
        assert to_mt5_symbol(instrument) == "eurusd.pro"

    def test_non_pair_stock_symbol(self) -> None:
        """A single-name symbol (stock) resolves via platform_symbol."""
        instrument = Instrument(
            symbol="AAPL",
            asset_class=AssetClass.STOCK,
            platform_symbol="AAPL.US",
        )
        assert to_mt5_symbol(instrument) == "AAPL.US"

    def test_missing_platform_symbol_raises(self) -> None:
        """An instrument without platform_symbol has no usable MT5 symbol."""
        instrument = Instrument(
            symbol="EUR", quote_currency="USD", asset_class=AssetClass.MARGIN_FX
        )
        with pytest.raises(ValueError):
            to_mt5_symbol(instrument)

    def test_missing_platform_symbol_raises_for_stock(self) -> None:
        """A stock without platform_symbol has no usable MT5 symbol either."""
        instrument = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK)
        with pytest.raises(ValueError):
            to_mt5_symbol(instrument)

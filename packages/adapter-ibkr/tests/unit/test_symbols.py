"""Unit tests for IBKR symbol translation (symbols.py).

Tests cases:
    - to_ibkr_contract: maps SPOT/MARGIN_FX to Forex contracts (CASH)
    - to_ibkr_contract: maps STOCK to Stock contracts (STK)
    - to_ibkr_contract: maps OPTION and FUTURES with expiry/strike/right fields
    - to_ibkr_contract: applies default_exchange and default_currency fallbacks
    - from_ibkr_contract: parses IBKR Contract back to a canonical Instrument
    - from_ibkr_contract: handles options and futures parameters correctly
    - raises ValueError on unmapped security types or missing parameters
"""

from __future__ import annotations


class TestToIBKRContract:
    """Canonical Instrument → ib_async.Contract translation."""

    def test_forex_mapping(self) -> None:
        """MARGIN_FX instrument maps to Forex/CASH contract."""
        ...

    def test_stock_mapping(self) -> None:
        """STOCK instrument maps to Stock/STK contract."""
        ...

    def test_option_mapping(self) -> None:
        """OPTION instrument maps to Option/OPT contract with strike, right, and expiry."""
        ...

    def test_futures_mapping(self) -> None:
        """FUTURES instrument maps to Future/FUT contract with expiry and multiplier."""
        ...

    def test_uses_config_defaults(self) -> None:
        """Missing exchange or currency fall back to IBKRConfig defaults."""
        ...


class TestFromIBKRContract:
    """ib_async.Contract → canonical Instrument translation."""

    def test_parses_forex_contract(self) -> None:
        """CASH contract maps back to canonical Instrument."""
        ...

    def test_parses_option_contract(self) -> None:
        """OPT contract maps back to fully populated Option Instrument."""
        ...

    def test_raises_on_unmapped_sec_type(self) -> None:
        """Unknown or unmapped secType raises ValueError."""
        ...

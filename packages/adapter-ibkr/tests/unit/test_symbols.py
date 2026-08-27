"""Unit tests for IBKR symbol translation (symbols.py).

Tests cases:
    - to_ibkr_contract: maps MARGIN_FX to Forex contracts (CASH)
    - to_ibkr_contract: maps SPOT to Crypto contracts (CRYPTO)
    - to_ibkr_contract: maps STOCK to Stock contracts (STK)
    - to_ibkr_contract: maps OPTION and FUTURES with expiry/strike/right fields
    - to_ibkr_contract: applies default_exchange and default_currency fallbacks
    - to_ibkr_contract: raises InvalidSymbolError for unsupported asset classes
    - from_ibkr_contract: parses IBKR Contracts back to canonical Instruments
    - from_ibkr_contract: handles options and futures parameters correctly
    - from_ibkr_contract: raises ValueError on unmapped security types
    - round-trip: contract -> Instrument -> contract preserves the wire fields
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from ib_async import CFD, Contract, Crypto, Forex, Future, Option, Stock

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.ibkr.symbols import from_ibkr_contract, to_ibkr_contract
from unified_trading_execution.types.enums import AssetClass, OptionRight
from unified_trading_execution.types.instrument import Instrument

EURUSD = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
)
BTCUSDT = Instrument(
    symbol="BTC",
    quote_currency="USDT",
    asset_class=AssetClass.SPOT,
)
AAPL = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
AAPL_CALL = Instrument(
    symbol="AAPL",
    asset_class=AssetClass.OPTION,
    currency="USD",
    expiry=date(2026, 12, 18),
    strike=Decimal("200"),
    option_right=OptionRight.CALL,
    multiplier=100,
)
ES_FUT = Instrument(
    symbol="ES",
    asset_class=AssetClass.FUTURES,
    currency="USD",
    expiry=date(2026, 9, 18),
    multiplier=50,
)


class TestToIBKRContract:
    """Canonical Instrument → ib_async.Contract translation."""

    def test_forex_mapping(self) -> None:
        """MARGIN_FX instrument maps to Forex/CASH contract."""
        contract = to_ibkr_contract(EURUSD)
        assert isinstance(contract, Forex)
        assert contract.secType == "CASH"
        assert contract.symbol == "EUR"
        assert contract.currency == "USD"

    def test_spot_maps_to_crypto(self) -> None:
        """SPOT instrument maps to Crypto/CRYPTO contract (not Forex)."""
        contract = to_ibkr_contract(BTCUSDT)
        assert isinstance(contract, Crypto)
        assert contract.secType == "CRYPTO"
        assert contract.symbol == "BTC"
        assert contract.currency == "USDT"

    def test_stock_mapping(self) -> None:
        """STOCK instrument maps to Stock/STK contract."""
        contract = to_ibkr_contract(AAPL)
        assert isinstance(contract, Stock)
        assert contract.secType == "STK"
        assert contract.symbol == "AAPL"
        assert contract.currency == "USD"

    def test_option_mapping(self) -> None:
        """OPTION instrument maps to Option/OPT contract with strike, right, and expiry."""
        contract = to_ibkr_contract(AAPL_CALL)
        assert isinstance(contract, Option)
        assert contract.secType == "OPT"
        assert contract.symbol == "AAPL"
        assert contract.lastTradeDateOrContractMonth == "20261218"
        assert contract.strike == 200.0
        assert contract.right == "C"
        assert contract.multiplier == "100"
        assert contract.currency == "USD"

    def test_futures_mapping(self) -> None:
        """FUTURES instrument maps to Future/FUT contract with expiry and multiplier."""
        contract = to_ibkr_contract(ES_FUT)
        assert isinstance(contract, Future)
        assert contract.secType == "FUT"
        assert contract.symbol == "ES"
        assert contract.lastTradeDateOrContractMonth == "20260918"
        assert contract.multiplier == "50"
        assert contract.currency == "USD"

    def test_cfd_mapping(self) -> None:
        """CFD instrument maps to CFD contract."""
        instrument = Instrument(symbol="IBUS30", asset_class=AssetClass.CFD, currency="USD")
        contract = to_ibkr_contract(instrument)
        assert isinstance(contract, CFD)
        assert contract.secType == "CFD"
        assert contract.symbol == "IBUS30"

    def test_uses_config_defaults(self) -> None:
        """Missing exchange or currency fall back to IBKRConfig defaults."""
        bare_stock = Instrument(symbol="MSFT", asset_class=AssetClass.STOCK)
        config = type(ibkr_config := None)  # placeholder to satisfy linters
        del config, ibkr_config

        from unified_trading_execution.ibkr.config import IBKRConfig

        cfg = IBKRConfig(default_exchange="SMART", default_currency="USD")
        contract = to_ibkr_contract(bare_stock, cfg)
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_forex_without_config_keeps_idealpro_default(self) -> None:
        """No exchange anywhere → Forex keeps its IDEALPRO default (not '')."""
        contract = to_ibkr_contract(EURUSD, config=None)
        assert contract.exchange == "IDEALPRO"

    def test_instrument_exchange_wins_over_config(self) -> None:
        """An explicit instrument exchange is never overridden by config."""
        instrument = Instrument(
            symbol="ES",
            asset_class=AssetClass.FUTURES,
            exchange="GLOBEX",
            currency="USD",
            expiry=date(2026, 9, 18),
            multiplier=50,
        )
        from unified_trading_execution.ibkr.config import IBKRConfig

        contract = to_ibkr_contract(instrument, IBKRConfig(default_exchange="SMART"))
        assert contract.exchange == "GLOBEX"

    def test_platform_symbol_becomes_local_symbol(self) -> None:
        """platform_symbol is forwarded verbatim as localSymbol."""
        instrument = Instrument(
            symbol="AAPL",
            asset_class=AssetClass.STOCK,
            currency="USD",
            platform_symbol="AAPL",
        )
        contract = to_ibkr_contract(instrument)
        assert contract.localSymbol == "AAPL"

    def test_raises_on_unsupported_asset_class(self) -> None:
        """BOND / FUND / etc. raise InvalidSymbolError — never approximated."""
        bond = Instrument(symbol="US10Y", asset_class=AssetClass.BOND, currency="USD")
        with pytest.raises(InvalidSymbolError):
            to_ibkr_contract(bond)

    def test_option_requires_expiry(self) -> None:
        """A malformed option (no expiry — bypassing core validation) is rejected."""
        broken = object.__new__(Instrument)
        object.__setattr__(broken, "symbol", "AAPL")
        object.__setattr__(broken, "asset_class", AssetClass.OPTION)
        object.__setattr__(broken, "quote_currency", None)
        object.__setattr__(broken, "exchange", None)
        object.__setattr__(broken, "currency", "USD")
        object.__setattr__(broken, "expiry", None)
        object.__setattr__(broken, "strike", Decimal("200"))
        object.__setattr__(broken, "option_right", OptionRight.CALL)
        object.__setattr__(broken, "multiplier", 100)
        object.__setattr__(broken, "platform_symbol", None)
        with pytest.raises(ValueError, match="expiry"):
            to_ibkr_contract(broken)


class TestFromIBKRContract:
    """ib_async.Contract → canonical Instrument translation."""

    def test_parses_forex_contract(self) -> None:
        """CASH contract maps back to a MARGIN_FX pair Instrument."""
        contract = Forex("EURUSD", "IDEALPRO")
        instrument = from_ibkr_contract(contract)
        assert instrument.symbol == "EUR"
        assert instrument.quote_currency == "USD"
        assert instrument.asset_class is AssetClass.MARGIN_FX
        assert instrument.exchange == "IDEALPRO"

    def test_parses_crypto_contract(self) -> None:
        """CRYPTO contract maps back to a SPOT pair Instrument."""
        contract = Crypto("BTC", "PAXOS", "USD")
        instrument = from_ibkr_contract(contract)
        assert instrument.symbol == "BTC"
        assert instrument.quote_currency == "USD"
        assert instrument.asset_class is AssetClass.SPOT

    def test_parses_option_contract(self) -> None:
        """OPT contract maps back to fully populated Option Instrument."""
        contract = Option("AAPL", "20261218", 200.0, "C", "SMART", "100", "USD")
        instrument = from_ibkr_contract(contract)
        assert instrument.asset_class is AssetClass.OPTION
        assert instrument.expiry == date(2026, 12, 18)
        assert instrument.strike == Decimal("200")
        assert instrument.option_right is OptionRight.CALL
        assert instrument.multiplier == 100
        assert instrument.currency == "USD"

    def test_parses_put_right_long_form(self) -> None:
        """'PUT'/'CALL' spellings are accepted for right."""
        contract = Option("AAPL", "20261218", 200.0, "PUT", "SMART", "100", "USD")
        assert from_ibkr_contract(contract).option_right is OptionRight.PUT

    def test_parses_contract_month_expiry(self) -> None:
        """YYYYMM expiry maps to the first of that month."""
        contract = Future("ES", "202609", "GLOBEX", multiplier="50", currency="USD")
        assert from_ibkr_contract(contract).expiry == date(2026, 9, 1)

    def test_parses_futures_contract(self) -> None:
        """FUT contract maps back to FUTURES Instrument with settlement currency."""
        contract = Future("ES", "20260918", "GLOBEX", multiplier="50", currency="USD")
        instrument = from_ibkr_contract(contract)
        assert instrument.asset_class is AssetClass.FUTURES
        assert instrument.symbol == "ES"
        assert instrument.currency == "USD"
        assert instrument.multiplier == 50
        assert instrument.quote_currency is None

    def test_local_symbol_preserved_as_platform_symbol(self) -> None:
        """Non-empty localSymbol round-trips into platform_symbol."""
        contract = Stock("AAPL", "SMART", "USD", localSymbol="AAPL")
        assert from_ibkr_contract(contract).platform_symbol == "AAPL"

    def test_raises_on_unmapped_sec_type(self) -> None:
        """Unknown or unmapped secType raises ValueError."""
        contract = Contract(secType="BOND", symbol="US10Y", currency="USD")
        with pytest.raises(ValueError, match="Unmapped IBKR secType"):
            from_ibkr_contract(contract)

    def test_raises_on_empty_symbol(self) -> None:
        """A contract without a symbol cannot become an Instrument."""
        contract = Stock("", "SMART", "USD")
        with pytest.raises(ValueError, match="empty symbol"):
            from_ibkr_contract(contract)

    def test_option_missing_strike_raises(self) -> None:
        """An option without a strike is rejected loudly."""
        contract = Option("AAPL", "20261218", 0.0, "C", "SMART", "100", "USD")
        with pytest.raises(ValueError, match="strike"):
            from_ibkr_contract(contract)

    def test_future_missing_multiplier_raises(self) -> None:
        """A future without a multiplier is rejected loudly."""
        contract = Future("ES", "20260918", "GLOBEX", multiplier="", currency="USD")
        with pytest.raises(ValueError, match="multiplier"):
            from_ibkr_contract(contract)

    def test_forex_without_currency_raises(self) -> None:
        """A CASH contract without currency cannot form a pair."""
        contract = Forex()
        contract.symbol = "EUR"
        contract.currency = ""
        with pytest.raises(ValueError, match="no currency"):
            from_ibkr_contract(contract)


class TestRoundTrip:
    """Wire-field stability through contract → Instrument → contract."""

    @pytest.mark.parametrize(
        ("instrument", "default_exchange"),
        [
            (EURUSD, ""),
            (BTCUSDT, "PAXOS"),
            (AAPL, "SMART"),
            (AAPL_CALL, "SMART"),
            (ES_FUT, "GLOBEX"),
        ],
    )
    def test_round_trip_preserves_wire_fields(
        self, instrument: Instrument, default_exchange: str
    ) -> None:
        """Instrument → contract → Instrument → contract yields identical contracts."""
        from unified_trading_execution.ibkr.config import IBKRConfig

        config = IBKRConfig(default_exchange=default_exchange) if default_exchange else None
        expected_exchange = (
            instrument.exchange
            or default_exchange
            or ("IDEALPRO" if instrument.asset_class is AssetClass.MARGIN_FX else "")
        )

        first = to_ibkr_contract(instrument, config)
        rebuilt = from_ibkr_contract(first)
        second = to_ibkr_contract(rebuilt)

        assert second.secType == first.secType
        assert second.symbol == first.symbol
        if expected_exchange:
            assert second.exchange == first.exchange == expected_exchange

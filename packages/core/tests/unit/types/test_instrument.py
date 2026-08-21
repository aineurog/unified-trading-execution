"""Unit tests for Instrument and InstrumentSpec — Sections 17.2–17.3.

Every construction invariant is tested for both the valid and invalid case.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from unified_trading_execution.types.enums import AssetClass, OptionRight
from unified_trading_execution.types.instrument import (
    Instrument,
    InstrumentSpec,
)

# ---- Helper ----


def make_spot(symbol="BTC", quote="USDT"):
    return Instrument(
        symbol=symbol,
        quote_currency=quote,
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_future(symbol="BTC", quote="USDT"):
    return Instrument(
        symbol=symbol,
        quote_currency=quote,
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency=None,
        expiry=None,  # perpetual — no expiry
        strike=None,
        option_right=None,
        multiplier=1,
    )


def make_dated_future(symbol="ES", quote="USD"):
    return Instrument(
        symbol=symbol,
        quote_currency=quote,
        asset_class=AssetClass.FUTURES,
        exchange="CME",
        currency="USD",
        expiry=date(2026, 12, 18),
        strike=None,
        option_right=None,
        multiplier=50,
    )


def make_option(symbol="AAPL"):
    return Instrument(
        symbol=symbol,
        quote_currency=None,
        asset_class=AssetClass.OPTION,
        exchange="SMART",
        currency="USD",
        expiry=date(2026, 12, 18),
        strike=Decimal("200"),
        option_right=OptionRight.CALL,
        multiplier=100,
    )


# ---- Instrument: valid construction ----


def test_spot_instrument_constructs():
    inst = make_spot()
    assert inst.symbol == "BTC"
    assert inst.asset_class == AssetClass.SPOT


def test_perpetual_future_constructs_with_expiry_none():
    """Fix #1: perpetual futures must allow expiry=None."""
    inst = make_future()
    assert inst.expiry is None
    assert inst.asset_class == AssetClass.FUTURES


def test_dated_future_constructs_with_expiry():
    inst = make_dated_future()
    assert inst.expiry == date(2026, 12, 18)


def test_option_constructs_with_all_fields():
    inst = make_option()
    assert inst.strike == Decimal("200")
    assert inst.option_right == OptionRight.CALL


def test_stock_constructs_with_minimal_fields():
    inst = Instrument(
        symbol="AAPL",
        quote_currency=None,
        asset_class=AssetClass.STOCK,
        exchange=None,
        currency="USD",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )
    assert inst.currency == "USD"


# ---- Instrument: platform_symbol (venue-specific spelling, not identity) ----


def test_platform_symbol_defaults_to_none():
    inst = make_spot()
    assert inst.platform_symbol is None


def test_platform_symbol_is_accepted_by_constructor_and_case_preserved():
    inst = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="btcusdt.p",
    )
    assert inst.platform_symbol == "btcusdt.p"  # NOT uppercased


def test_platform_symbol_excluded_from_equality():
    a = make_spot(symbol="BTC")
    b = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT.P",
    )
    assert a == b
    assert hash(a) == hash(b)


def test_platform_symbol_excluded_from_dict_identity():
    a = make_spot(symbol="BTC")
    b = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT.P",
    )
    d = {a: "value"}
    assert d[b] == "value"


# ---- Instrument: symbol invariant ----


def test_lowercase_identifiers_are_normalized_to_uppercase():
    inst = Instrument(
        symbol="btc",
        quote_currency="usdt",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency="bytes",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )
    assert inst.symbol == "BTC"
    assert inst.quote_currency == "USDT"
    assert inst.currency == "BYTES"


def test_numeric_symbol_lowercase_quote_is_normalized():
    # Symbol "4" has no letters; .upper() leaves it unchanged, quote is uppercased.
    inst = Instrument(
        symbol="4",
        quote_currency="usdt",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )
    assert inst.symbol == "4"
    assert inst.quote_currency == "USDT"


def test_symbol_must_not_be_empty():
    with pytest.raises(ValueError, match="symbol must be non-empty"):
        Instrument(
            symbol="",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_symbol_must_not_be_whitespace_only():
    with pytest.raises(ValueError, match="symbol must be non-empty"):
        Instrument(
            symbol="   ",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_quote_currency_must_not_be_whitespace_only():
    with pytest.raises(ValueError, match="quote_currency must be non-empty"):
        Instrument(
            symbol="BTC",
            quote_currency="  ",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_currency_must_not_be_whitespace_only():
    with pytest.raises(ValueError, match="currency must be non-empty"):
        Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.STOCK,
            exchange=None,
            currency="  ",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


# ---- Instrument: quote_currency invariant ----
#
# Pairs (SPOT, MARGIN_FX) and perpetual futures (FUTURES with expiry=None)
# require a counter currency.  Dated futures, options, and single-name
# instruments (stock, CFD, bond, fund) carry their settlement currency in
# ``currency`` instead and must not be forced to provide a quote_currency.


def test_spot_requires_quote_currency():
    with pytest.raises(ValueError, match="quote_currency is required for SPOT"):
        Instrument(
            symbol="BTC",
            quote_currency=None,
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_margin_fx_requires_quote_currency():
    with pytest.raises(ValueError, match="quote_currency is required for MARGIN_FX"):
        Instrument(
            symbol="EUR",
            quote_currency=None,
            asset_class=AssetClass.MARGIN_FX,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_perpetual_future_requires_quote_currency():
    with pytest.raises(ValueError, match="quote_currency is required for perpetual FUTURES"):
        Instrument(
            symbol="BTC",
            quote_currency=None,
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency=None,
            expiry=None,  # perpetual
            strike=None,
            option_right=None,
            multiplier=1,
        )


def test_dated_future_does_not_require_quote_currency():
    # Dated futures carry their settlement currency in ``currency``, so a
    # missing quote_currency is legitimate here.
    inst = Instrument(
        symbol="ES",
        quote_currency=None,
        asset_class=AssetClass.FUTURES,
        exchange="CME",
        currency="USD",
        expiry=date(2026, 12, 18),
        strike=None,
        option_right=None,
        multiplier=50,
    )
    assert inst.currency == "USD"
    assert inst.quote_currency is None


# ---- Instrument: OPTION invariants ----


def test_option_requires_expiry():
    with pytest.raises(ValueError, match="expiry is required for OPTION"):
        Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.OPTION,
            exchange="SMART",
            currency="USD",
            expiry=None,
            strike=Decimal("200"),
            option_right=OptionRight.CALL,
            multiplier=100,
        )


def test_option_requires_strike():
    with pytest.raises(ValueError, match="strike is required for OPTION"):
        Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.OPTION,
            exchange="SMART",
            currency="USD",
            expiry=date(2026, 12, 18),
            strike=None,
            option_right=OptionRight.CALL,
            multiplier=100,
        )


def test_option_requires_option_right():
    with pytest.raises(ValueError, match="option_right is required for OPTION"):
        Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.OPTION,
            exchange="SMART",
            currency="USD",
            expiry=date(2026, 12, 18),
            strike=Decimal("200"),
            option_right=None,
            multiplier=100,
        )


# ---- Instrument: multiplier invariant ----


def test_futures_requires_multiplier():
    with pytest.raises(ValueError, match="multiplier is required"):
        Instrument(
            symbol="ES",
            quote_currency="USD",
            asset_class=AssetClass.FUTURES,
            exchange="CME",
            currency="USD",
            expiry=date(2026, 12, 18),
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_option_requires_multiplier():
    with pytest.raises(ValueError, match="multiplier is required"):
        Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.OPTION,
            exchange="SMART",
            currency="USD",
            expiry=date(2026, 12, 18),
            strike=Decimal("200"),
            option_right=OptionRight.CALL,
            multiplier=None,
        )


# ---- Instrument: hashable and equality ----


def test_instrument_is_hashable():
    inst = make_spot()
    d = {inst: "value"}
    assert d[inst] == "value"


def test_instrument_equality_considers_all_fields():
    a = make_spot(symbol="BTC")
    b = make_spot(symbol="BTC")
    c = make_spot(symbol="ETH")
    assert a == b
    assert a != c


# ---- Instrument: str() shorthand ----


def test_spot_str_returns_base_quote():
    assert str(make_spot("BTC", "USDT")) == "BTC/USDT"


def test_forex_str_returns_base_quote():
    inst = Instrument(
        symbol="EUR",
        quote_currency="USD",
        asset_class=AssetClass.MARGIN_FX,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )
    assert str(inst) == "EUR/USD"


def test_perpetual_future_str_returns_base_quote():
    assert str(make_future("BTC", "USDT")) == "BTC/USDT"


def test_option_str_raises():
    with pytest.raises(ValueError, match="cannot be represented as a shorthand string"):
        str(make_option())


def test_dated_future_str_raises():
    with pytest.raises(ValueError, match="cannot be represented as a shorthand string"):
        str(make_dated_future())


def test_stock_str_raises():
    inst = Instrument(
        symbol="AAPL",
        quote_currency=None,
        asset_class=AssetClass.STOCK,
        exchange=None,
        currency="USD",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )
    with pytest.raises(ValueError, match="cannot be represented as a shorthand string"):
        str(inst)


# ---- InstrumentSpec ----


def test_instrument_spec_valid():
    spec = InstrumentSpec(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("1"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("100"),
        min_notional=Decimal("10"),
        price_precision=2,
        qty_precision=3,
    )
    assert spec.tick_size == Decimal("0.01")
    assert spec.price_precision == 2
    assert spec.max_leverage is None


def test_instrument_spec_is_frozen():
    spec = InstrumentSpec(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("1"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("100"),
        min_notional=Decimal("10"),
        price_precision=2,
        qty_precision=3,
    )
    with pytest.raises(Exception):
        spec.price_precision = 5  # type: ignore[misc]

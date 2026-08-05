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
    _with_broker_override,
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


# ---- Instrument: broker_symbol_override is not a constructor param ----


def test_broker_symbol_override_defaults_to_none():
    inst = make_spot()
    assert inst.broker_symbol_override is None


def test_broker_symbol_override_not_accepted_by_constructor():
    with pytest.raises(TypeError):
        Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
            broker_symbol_override="BTCUSDT.P",
        )


def test_with_broker_override_creates_copy_with_override_set():
    inst = make_spot()
    copy = _with_broker_override(inst, "BTCUSDT.P")
    assert copy.broker_symbol_override == "BTCUSDT.P"
    assert inst.broker_symbol_override is None  # original unchanged


def test_with_broker_override_preserves_all_other_fields():
    inst = make_spot(symbol="ETH", quote="USDC")
    copy = _with_broker_override(inst, "ETHUSDC.M")
    assert copy.symbol == "ETH"
    assert copy.quote_currency == "USDC"
    assert copy.asset_class == AssetClass.SPOT
    assert copy.multiplier is None


# ---- Instrument: symbol invariant ----


def test_symbol_must_be_non_empty_uppercase():
    with pytest.raises(ValueError, match="symbol must be non-empty and uppercase"):
        Instrument(
            symbol="btc",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )


def test_symbol_must_not_be_empty():
    with pytest.raises(ValueError, match="symbol must be non-empty and uppercase"):
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

"""Unit tests for ``unified_trading_execution.bybit.symbols``.

Tests cover:
- ``to_bybit_symbol`` for spot, linear perpetual, inverse perpetual
- ``from_bybit_symbol`` for each category
- Round-trip consistency
- Error paths (unsupported asset class, missing quote_currency, bad category)
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from unified_trading_execution.bybit.symbols import (
    from_bybit_symbol,
    to_bybit_symbol,
)
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

# ── to_bybit_symbol ────────────────────────────────────────────────────


class TestToBybitSymbol:
    def test_spot(self) -> None:
        inst = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )
        assert to_bybit_symbol(inst) == "BTCUSDT"

    def test_linear_perpetual(self) -> None:
        inst = Instrument(
            symbol="ETH",
            quote_currency="USDT",
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency="USDT",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=1,
        )
        assert to_bybit_symbol(inst) == "ETHUSDT"

    def test_inverse_perpetual(self) -> None:
        inst = Instrument(
            symbol="BTC",
            quote_currency="USD",
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency="BTC",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=1,
        )
        assert to_bybit_symbol(inst) == "BTCUSD"

    def test_raises_on_unsupported_asset_class(self) -> None:
        inst = Instrument(
            symbol="AAPL",
            quote_currency="USD",
            asset_class=AssetClass.STOCK,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )
        with pytest.raises(InvalidSymbolError, match="not supported"):
            to_bybit_symbol(inst)

    def test_raises_on_missing_quote_currency(self) -> None:
        inst = Instrument(
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
        with pytest.raises(InvalidSymbolError, match="no quote_currency"):
            to_bybit_symbol(inst)

    def test_option_not_supported(self) -> None:
        inst = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.OPTION,
            exchange=None,
            currency="USDT",
            expiry=__import__("datetime").date(2026, 12, 31),
            strike=Decimal("50000"),
            option_right=__import__("unified_trading_execution").OptionRight.CALL,
            multiplier=1,
        )
        with pytest.raises(InvalidSymbolError, match="not supported"):
            to_bybit_symbol(inst)


# ── from_bybit_symbol ──────────────────────────────────────────────────


class TestFromBybitSymbol:
    def test_spot(self) -> None:
        inst = from_bybit_symbol("BTCUSDT", "BTC", "USDT", "spot")
        assert inst.symbol == "BTC"
        assert inst.quote_currency == "USDT"
        assert inst.asset_class == AssetClass.SPOT
        assert inst.currency is None
        assert inst.multiplier is None

    def test_linear_perpetual(self) -> None:
        inst = from_bybit_symbol("ETHUSDT", "ETH", "USDT", "linear")
        assert inst.symbol == "ETH"
        assert inst.quote_currency == "USDT"
        assert inst.asset_class == AssetClass.FUTURES
        assert inst.currency == "USDT"
        assert inst.multiplier == 1
        assert inst.expiry is None

    def test_inverse_perpetual(self) -> None:
        inst = from_bybit_symbol("BTCUSD", "BTC", "USD", "inverse")
        assert inst.symbol == "BTC"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.FUTURES
        assert inst.currency == "BTC"
        assert inst.multiplier == 1
        assert inst.expiry is None

    def test_symbol_mismatch_raises(self) -> None:
        with pytest.raises(InvalidSymbolError, match="does not match"):
            from_bybit_symbol("BTCUSDT", "ETH", "USDT", "spot")

    def test_unknown_category_raises(self) -> None:
        with pytest.raises(InvalidSymbolError, match="Unknown"):
            from_bybit_symbol("BTCUSDT", "BTC", "USDT", "option")


# ── Round-trip ─────────────────────────────────────────────────────────


class TestRoundTrip:
    @pytest.mark.parametrize(
        "inst",
        [
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
            ),
            Instrument(
                symbol="ETH",
                quote_currency="USDT",
                asset_class=AssetClass.FUTURES,
                exchange=None,
                currency="USDT",
                expiry=None,
                strike=None,
                option_right=None,
                multiplier=1,
            ),
            Instrument(
                symbol="BTC",
                quote_currency="USD",
                asset_class=AssetClass.FUTURES,
                exchange=None,
                currency="BTC",
                expiry=None,
                strike=None,
                option_right=None,
                multiplier=1,
            ),
        ],
        ids=["spot", "linear-perp", "inverse-perp"],
    )
    def test_round_trip(self, inst: Instrument) -> None:
        bybit_sym = to_bybit_symbol(inst)
        base = inst.symbol
        quote = inst.quote_currency or ""
        if inst.asset_class == AssetClass.SPOT:
            category = "spot"
        else:
            category = "linear" if inst.currency == quote else "inverse"
        restored = from_bybit_symbol(bybit_sym, base, quote, category)
        assert restored == inst

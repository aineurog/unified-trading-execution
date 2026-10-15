"""Unit tests for coin translation (no network).

Integer asset-id resolution is the SDK ``Info`` index's job
(``name_to_asset``/``name_to_coin``) and is not duplicated here; the pair
table below mirrors that index's shape.
"""

from __future__ import annotations

from datetime import date

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.hyperliquid.symbols import (
    from_hyperliquid_coin,
    to_hyperliquid_coin,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

# Mirrors the SDK Info index shape: pair spelling -> venue coin name, plus
# the identity alias entries the SDK also carries (alias -> alias).
_PAIR_COINS = {
    "PURR/USDC": "PURR/USDC",
    "@107": "@107",
    "HYPE/USDC": "@107",
}


def _perp(symbol: str = "BTC") -> Instrument:
    return Instrument(
        symbol=symbol,
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _spot(symbol: str = "HYPE", quote: str = "USDC") -> Instrument:
    return Instrument(symbol=symbol, quote_currency=quote, asset_class=AssetClass.SPOT)


def test_to_hyperliquid_coin() -> None:
    # Spot returns the pair spelling — the SDK normalizes it to the venue
    # alias internally on reads and writes (verified live against Info).
    assert to_hyperliquid_coin(_perp()) == "BTC"
    assert to_hyperliquid_coin(_spot()) == "HYPE/USDC"
    assert to_hyperliquid_coin(_spot("PURR")) == "PURR/USDC"


def test_to_hyperliquid_coin_rejects_dated_and_out_of_scope() -> None:
    dated = Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        expiry=date(2026, 12, 25),
        multiplier=1,
    )
    with pytest.raises(InvalidSymbolError):
        to_hyperliquid_coin(dated)
    with pytest.raises(InvalidSymbolError):
        to_hyperliquid_coin(_perp("xyz:ABC"))


def test_from_hyperliquid_coin() -> None:
    perp = from_hyperliquid_coin("BTC", is_spot=False)
    assert perp.asset_class == AssetClass.FUTURES
    assert perp.expiry is None
    assert perp.multiplier == 1
    spot = from_hyperliquid_coin("@107", is_spot=True, spot_pair_coins=_PAIR_COINS)
    assert (spot.symbol, spot.quote_currency, spot.asset_class) == (
        "HYPE",
        "USDC",
        AssetClass.SPOT,
    )
    named = from_hyperliquid_coin("PURR/USDC", is_spot=True)
    assert (named.symbol, named.quote_currency) == ("PURR", "USDC")


def test_from_hyperliquid_coin_rejects() -> None:
    with pytest.raises(InvalidSymbolError):
        from_hyperliquid_coin("xyz:ABC", is_spot=False)
    with pytest.raises(InvalidSymbolError):
        from_hyperliquid_coin("#10", is_spot=True, spot_pair_coins=_PAIR_COINS)
    with pytest.raises(InvalidSymbolError):
        from_hyperliquid_coin("@107", is_spot=True)
    with pytest.raises(InvalidSymbolError):
        from_hyperliquid_coin("@107", is_spot=False)
    with pytest.raises(InvalidSymbolError):
        from_hyperliquid_coin("A/B/C", is_spot=True)


@pytest.mark.parametrize("symbol", ["X@Y", "X/Y", "X:Y"])
def test_to_hyperliquid_coin_rejects_marked_spot_symbols(symbol: str) -> None:
    with pytest.raises(InvalidSymbolError):
        to_hyperliquid_coin(_spot(symbol))

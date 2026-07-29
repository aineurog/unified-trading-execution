"""Canonical Instrument <-> Bybit symbol translation.

The engine uses ``Instrument`` (symbol, quote_currency, asset_class, ...)
everywhere.  Bybit uses strings like "BTCUSDT" for spot and linear perpetuals,
and "BTCUSD" for inverse perpetuals.  This module provides bidirectional
translation so the adapter can convert back and forth without leaking Bybit
naming conventions into core.
"""

from __future__ import annotations

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_BYBIT_CATEGORY_TO_ASSET_CLASS: dict[str, AssetClass] = {
    "spot": AssetClass.SPOT,
    "linear": AssetClass.FUTURES,
    "inverse": AssetClass.FUTURES,
}

_SUPPORTED_ASSET_CLASSES: frozenset[AssetClass] = frozenset(
    {
        AssetClass.SPOT,
        AssetClass.FUTURES,
    }
)


def to_bybit_symbol(instrument: Instrument) -> str:
    """Convert a canonical ``Instrument`` to a Bybit symbol string.

    For spot and perpetual futures the symbol is simply
    ``{symbol}{quote_currency}``, e.g. ``BTC`` + ``USDT`` = ``BTCUSDT``.

    Raises ``InvalidSymbolError`` if the instrument's asset class is not
    supported or if ``quote_currency`` is missing.
    """
    if instrument.asset_class not in _SUPPORTED_ASSET_CLASSES:
        raise InvalidSymbolError(f"Asset class {instrument.asset_class} is not supported by Bybit")
    if instrument.quote_currency is None:
        raise InvalidSymbolError(
            f"Instrument {instrument.symbol} has no quote_currency — " f"cannot build Bybit symbol"
        )
    return f"{instrument.symbol}{instrument.quote_currency}"


def from_bybit_symbol(
    bybit_symbol: str,
    base_coin: str,
    quote_coin: str,
    category: str,
    *,
    is_perpetual: bool = True,
) -> Instrument:
    """Convert Bybit response fields back to a canonical ``Instrument``.

    Parameters
    ----------
    bybit_symbol :
        Raw Bybit symbol string (e.g. ``"BTCUSDT"``) — used for validation
        only; the actual fields are *base_coin* and *quote_coin*.
    base_coin :
        Base currency from the API response (``baseCoin``).
    quote_coin :
        Quote currency from the API response (``quoteCoin``).
    category :
        Bybit product category: ``"spot"``, ``"linear"``, or ``"inverse"``.
    is_perpetual :
        ``True`` for perpetual contracts (no expiry), ``False`` for dated
        futures (not yet supported — reserved for future use).

    Raises
    ------
    InvalidSymbolError
        If *category* is not recognised, if the constructed symbol does not
        match *bybit_symbol*, or if the instrument type is not supported.
    """
    asset_class = _BYBIT_CATEGORY_TO_ASSET_CLASS.get(category)
    if asset_class is None:
        raise InvalidSymbolError(f"Unknown Bybit category {category!r} for symbol {bybit_symbol}")

    expected_symbol = f"{base_coin}{quote_coin}"
    if bybit_symbol != expected_symbol:
        raise InvalidSymbolError(
            f"Bybit symbol {bybit_symbol!r} does not match "
            f"base_coin={base_coin!r} + quote_coin={quote_coin!r} "
            f"(expected {expected_symbol!r})"
        )

    if asset_class == AssetClass.FUTURES:
        if not is_perpetual:
            raise InvalidSymbolError(f"Dated futures ({bybit_symbol}) are not yet supported")
        currency = base_coin if category == "inverse" else quote_coin

        return Instrument(
            symbol=base_coin,
            quote_currency=quote_coin,
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency=currency,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=1,
        )

    if asset_class == AssetClass.SPOT:
        return Instrument(
            symbol=base_coin,
            quote_currency=quote_coin,
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

    raise InvalidSymbolError(f"Unsupported asset class {asset_class} for symbol {bybit_symbol}")

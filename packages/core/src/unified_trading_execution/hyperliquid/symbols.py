"""Canonical Instrument ↔ Hyperliquid coin translation.

The engine uses ``Instrument`` everywhere.  Perps map to the bare coin name
(``BTC``); spot maps to the pair spelling (``HYPE/USDC``), which the SDK
normalizes to venue aliases internally on reads and writes — so no pair
table is needed in this direction.  Integer asset ids for actions resolve
through the SDK ``Info`` index (``name_to_asset``), which the adapter owns;
this module never duplicates that registry and never hardcodes an id.  It
owns what the SDK cannot: canonical↔coin translation with scope guards.
Decoding venue aliases (``@107``) back to pairs needs the pair table — see
``from_hyperliquid_coin``.  Numeric order shaping (quantization, notional
floors/tiers) lives in ``orders.py``, which consumes it.

Builder-deployed (``{dex}:{coin}``) and outcome (``#``/``+``) names are out
of scope and rejected — the SDK would otherwise resolve them silently.
"""

from __future__ import annotations

from collections.abc import Mapping

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

# Quote currency for perpetuals (USDC-margined; oracle may be USDT-quoted).
PERP_QUOTE_CURRENCY = "USDC"


def to_hyperliquid_coin(instrument: Instrument) -> str:
    """Convert a canonical ``Instrument`` to an SDK-acceptable coin spelling.

    Perps map to the base coin name (``symbol``); spot maps to the pair
    spelling (``HYPE/USDC``) — ``symbol`` plus ``quote_currency`` only.  The
    SDK normalizes pair spellings to venue aliases internally on reads and
    writes (verified live: ``name_to_asset`` and ``l2_snapshot`` both accept
    ``HYPE/USDC`` for ``@107``), so no pair table is needed here.  Do not
    feed the result to raw HTTP — the venue itself only recognizes aliases.
    The venue lists perpetuals only, so a ``FUTURES`` instrument carrying an
    expiry is not tradable here; HIP-3/outcome names raise
    ``InvalidSymbolError``.  Listing existence (unknown pair) surfaces at the
    SDK boundary, not here.
    """
    if instrument.asset_class == AssetClass.FUTURES:
        if instrument.expiry is not None:
            raise InvalidSymbolError(
                f"Hyperliquid lists perpetuals only ({instrument.symbol} has an expiry)"
            )
        if any(mark in instrument.symbol for mark in (":", "#", "+", "@")):
            raise InvalidSymbolError(f"Instrument {instrument.symbol!r} is out of scope")
        return instrument.symbol
    if instrument.asset_class == AssetClass.SPOT:
        if instrument.quote_currency is None:
            raise InvalidSymbolError(
                f"Instrument {instrument.symbol} has no quote_currency — cannot map to a spot coin"
            )
        if any(mark in instrument.symbol for mark in (":", "#", "+", "@", "/")):
            raise InvalidSymbolError(f"Instrument {instrument.symbol!r} is out of scope")
        return f"{instrument.symbol}/{instrument.quote_currency}"
    raise InvalidSymbolError(f"Asset class {instrument.asset_class} is not supported")


def from_hyperliquid_coin(
    coin: str,
    *,
    is_spot: bool,
    spot_pair_coins: Mapping[str, str] | None = None,
) -> Instrument:
    """Convert a Hyperliquid coin name back to a canonical ``Instrument``.

    Perps → ``AssetClass.FUTURES`` (perpetual: ``expiry=None``,
    ``quote_currency="USDC"``, ``multiplier=1``); spot → ``AssetClass.SPOT``
    (``BASE/USDC``), decoding index aliases (``@107``) through
    ``spot_pair_coins``.  HIP-3 (``dex:coin``) and outcome (``#``/``+``)
    names raise ``InvalidSymbolError`` as out of scope.
    """
    if ":" in coin or coin.startswith(("#", "+")):
        raise InvalidSymbolError(f"Coin {coin!r} is out of scope")
    if is_spot:
        if "/" in coin:
            if coin.count("/") != 1:
                raise InvalidSymbolError(f"Malformed spot coin {coin!r}")
            base, _, quote = coin.partition("/")
            if not base or not quote:
                raise InvalidSymbolError(f"Malformed spot coin {coin!r}")
        else:
            if spot_pair_coins is None:
                raise InvalidSymbolError(
                    f"Spot coin {coin!r} needs the pair index to decode — none supplied"
                )
            # Prefer true pair spellings (contain "/"): the index also
            # carries identity alias entries (alias -> alias) which sort
            # first and carry no base/quote to split.
            pair = next(
                (p for p, c in spot_pair_coins.items() if c == coin and "/" in p),
                None,
            )
            if pair is None:
                raise InvalidSymbolError(f"Unknown spot coin {coin!r}")
            base, _, quote = pair.partition("/")
            if not base or not quote:
                raise InvalidSymbolError(f"Malformed spot pair {pair!r}")
        return Instrument(
            symbol=base,
            quote_currency=quote,
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )
    if coin.startswith("@") or "/" in coin:
        raise InvalidSymbolError(f"Coin {coin!r} is not a perpetual")
    return Instrument(
        symbol=coin,
        quote_currency=PERP_QUOTE_CURRENCY,
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency=PERP_QUOTE_CURRENCY,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )

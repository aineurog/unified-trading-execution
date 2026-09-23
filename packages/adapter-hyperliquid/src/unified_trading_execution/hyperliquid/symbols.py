"""Canonical Instrument ↔ Hyperliquid coin / asset-id translation.

The engine uses ``Instrument`` everywhere.  Hyperliquid addresses assets by
integer asset id: perps resolve by name into the ``meta.universe`` index,
spot resolves to ``10000 +`` the ``spotMeta`` universe index.  This module
owns id resolution plus ``szDecimals`` quantization (size rounding, the
5-significant-figure price rule, min-notional / max-notional tiers).
"""

from __future__ import annotations

from decimal import Decimal

from unified_trading_execution.types.instrument import Instrument


def to_hyperliquid_coin(instrument: Instrument) -> str:
    """Convert a canonical ``Instrument`` to the Hyperliquid coin name.

    If ``instrument.platform_symbol`` is set it is returned verbatim (the
    venue's exact spelling).  Otherwise perps map to the base coin name and
    spot maps to ``BASE/USDC``.  HIP-3 dexes, HIP-1/HIP-2 deployments and
    outcomes raise ``InvalidSymbolError``.
    """
    raise NotImplementedError


def from_hyperliquid_coin(coin: str, *, is_spot: bool) -> Instrument:
    """Convert a Hyperliquid coin name back to a canonical ``Instrument``.

    Perps → ``AssetClass.FUTURES`` (perpetual: ``expiry=None``,
    ``quote_currency="USDC"``, ``multiplier=1``); spot → ``AssetClass.SPOT``
    (``BASE/USDC``).
    """
    raise NotImplementedError


def resolve_asset_id(coin: str, *, is_spot: bool) -> int:
    """Resolve a coin name to its integer asset id for action payloads.

    Perps: ``meta.universe`` index resolved by name at connect and cached
    with meta (never hardcoded).  Spot: ``10000 +`` the ``spotMeta``
    universe index.
    """
    raise NotImplementedError


def quantize_size(quantity: Decimal, sz_decimals: int) -> Decimal:
    """Round a size to the asset's ``szDecimals``."""
    raise NotImplementedError


def quantize_price(price: Decimal, sz_decimals: int, *, is_spot: bool) -> Decimal:
    """Validate/round a price to the venue tick rule.

    Prices carry at most 5 significant figures and at most
    ``MAX_DECIMALS - szDecimals`` decimals (6 perps / 8 spot); integers are
    always legal.  Violations raise ``InvalidOrderError`` client-side —
    never rely on the venue round trip.
    """
    raise NotImplementedError

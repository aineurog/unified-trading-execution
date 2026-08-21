"""Canonical type definitions — single source of truth shared by all adapters and v2 modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from unified_trading_execution.types.enums import AssetClass, OptionRight


@dataclass(frozen=True, slots=True)
class Instrument:
    """Structured instrument identifier — designed against the most demanding platform (IBKR).

    Frozen and hashable — usable as a dict key and cache lookup key.
    Equality and hashing consider all fields except ``platform_symbol``
    (venue-specific spelling, not identity).

    The shorthand str() form (BASE/QUOTE) is available only for crypto spot/perp
    pairs and forex pairs. All other instruments raise ValueError on str().

    ``platform_symbol`` is the optional venue-specific symbol string (e.g. MT5's
    ``"EURUSD.m"``).  When set, adapters use it verbatim instead of deriving a
    symbol from ``symbol``/``quote_currency``.
    """

    symbol: str
    asset_class: AssetClass
    quote_currency: str | None = None
    exchange: str | None = None
    currency: str | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_right: OptionRight | None = None
    multiplier: int | None = None
    # Venue-specific symbol string (e.g. MT5's "EURUSD.m").  Deliberately NOT
    # normalised to uppercase (venue symbols are case-sensitive) and NOT part
    # of equality/hash — spelling on one venue, not the instrument's identity.
    platform_symbol: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        # Normalize identifier fields to uppercase. Digits (e.g. symbol "4" in
        # "4USDT") are unaffected by .upper(), so this is safe for any input.
        # Only reassign when changed — avoids setattr on the already-uppercase path.
        if self.symbol != self.symbol.upper():
            object.__setattr__(self, "symbol", self.symbol.upper())
        if self.quote_currency is not None and self.quote_currency != self.quote_currency.upper():
            object.__setattr__(self, "quote_currency", self.quote_currency.upper())
        if self.currency is not None and self.currency != self.currency.upper():
            object.__setattr__(self, "currency", self.currency.upper())

        # Identifier fields must be non-empty and non-blank — a whitespace-only
        # symbol/quote/currency would otherwise round-trip through an adapter
        # and place orders against an invalid venue symbol.
        if not self.symbol.strip():
            raise ValueError(f"symbol must be non-empty, got {self.symbol!r}")
        if self.quote_currency is not None and not self.quote_currency.strip():
            raise ValueError(f"quote_currency must be non-empty, got {self.quote_currency!r}")
        if self.currency is not None and not self.currency.strip():
            raise ValueError(f"currency must be non-empty, got {self.currency!r}")

        # Pairs need a counter currency: SPOT and MARGIN_FX are always
        # BASE/QUOTE, and a FUTURES with expiry=None is a perpetual (also
        # BASE/QUOTE). Dated futures carry their settlement currency in
        # ``currency`` instead, so they are exempt.
        is_pair = self.asset_class in (AssetClass.SPOT, AssetClass.MARGIN_FX)
        is_perpetual = self.asset_class == AssetClass.FUTURES and self.expiry is None
        if (is_pair or is_perpetual) and not self.quote_currency:
            label = "perpetual FUTURES" if is_perpetual else self.asset_class.value
            raise ValueError(f"quote_currency is required for {label}")

        # expiry is required for OPTION only — FUTURES with expiry=None is a perpetual.
        if self.asset_class == AssetClass.OPTION:
            if self.expiry is None:
                raise ValueError("expiry is required for OPTION")

        if self.asset_class == AssetClass.OPTION:
            if self.strike is None:
                raise ValueError("strike is required for OPTION")
            if self.option_right is None:
                raise ValueError("option_right is required for OPTION")

        if self.asset_class in (AssetClass.FUTURES, AssetClass.OPTION):
            if self.multiplier is None:
                raise ValueError(f"multiplier is required for {self.asset_class}")

    def __str__(self) -> str:
        """Return BASE/QUOTE shorthand for forex and crypto spot/perp pairs only."""
        if self.asset_class in (AssetClass.SPOT, AssetClass.MARGIN_FX) and self.quote_currency:
            return f"{self.symbol}/{self.quote_currency}"
        if self.asset_class == AssetClass.FUTURES and self.expiry is None and self.quote_currency:
            # Perpetual futures (crypto) — shorthand available
            return f"{self.symbol}/{self.quote_currency}"
        raise ValueError(
            f"Instrument {self.symbol!r} with asset_class={self.asset_class} "
            f"cannot be represented as a shorthand string. Use explicit field access."
        )


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Trading rules per instrument — fetched once per platform and cached indefinitely."""

    tick_size: Decimal
    lot_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    price_precision: int
    qty_precision: int
    max_leverage: Decimal | None = None  # None for spot / platforms without leverage

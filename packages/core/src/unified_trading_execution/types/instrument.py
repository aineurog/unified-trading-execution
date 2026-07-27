"""Canonical type definitions — single source of truth shared by all adapters and v2 modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from unified_trading_execution.types.enums import AssetClass, OptionRight


@dataclass(frozen=True, slots=True)
class Instrument:
    """Structured instrument identifier — designed against the most demanding platform (IBKR).

    Frozen and hashable — usable as a dict key and cache lookup key.
    Equality and hashing consider all fields.

    The shorthand str() form (BASE/QUOTE) is available only for crypto spot/perp
    pairs and forex pairs. All other instruments raise ValueError on str().
    """

    symbol: str
    quote_currency: str | None
    asset_class: AssetClass
    exchange: str | None
    currency: str | None
    expiry: date | None
    strike: Decimal | None
    option_right: OptionRight | None
    multiplier: int | None
    broker_symbol_override: str | None

    def __post_init__(self) -> None:
        if not self.symbol or not self.symbol.isupper():
            raise ValueError(f"symbol must be non-empty and uppercase, got {self.symbol!r}")

        if self.asset_class in (AssetClass.FUTURES, AssetClass.OPTION):
            if self.expiry is None:
                raise ValueError(f"expiry is required for {self.asset_class}")

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

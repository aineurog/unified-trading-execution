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
    Equality and hashing consider all fields.

    The shorthand str() form (BASE/QUOTE) is available only for crypto spot/perp
    pairs and forex pairs. All other instruments raise ValueError on str().

    ``broker_symbol_override`` is NOT a constructor parameter — it is set exclusively
    by the MT5 adapter (v2) via the ``_with_broker_override`` factory.
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
    _broker_symbol_override: str | None = field(default=None, init=False, repr=False)

    @property
    def broker_symbol_override(self) -> str | None:
        """Adapter-only passthrough for broker-specific symbol translation (MT5 alias table).

        Never set by user code. Populated exclusively via ``_with_broker_override``.
        """
        return self._broker_symbol_override

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

        if not self.symbol:
            raise ValueError(f"symbol must be non-empty, got {self.symbol!r}")

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


def _with_broker_override(instrument: Instrument, override: str) -> Instrument:
    """Create a copy with ``broker_symbol_override`` set. For adapter use only.

    This is the only way to set ``broker_symbol_override``. It is not exposed
    on the public ``Instrument`` constructor. Imported by adapter packages;
    core never calls this function.
    """
    new = Instrument(
        symbol=instrument.symbol,
        quote_currency=instrument.quote_currency,
        asset_class=instrument.asset_class,
        exchange=instrument.exchange,
        currency=instrument.currency,
        expiry=instrument.expiry,
        strike=instrument.strike,
        option_right=instrument.option_right,
        multiplier=instrument.multiplier,
    )
    object.__setattr__(new, "_broker_symbol_override", override)
    return new


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

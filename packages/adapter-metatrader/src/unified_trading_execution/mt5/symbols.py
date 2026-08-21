"""Instrument ↔ MetaTrader 5 symbol translation.

MT5 brokers use non-standard symbol suffixes (``EURUSD.m``, ``EURUSDpro``,
``EURUSD+``).  The adapter translates between core's canonical types and
broker-specific MT5 symbol strings.

This module is **pure string logic** — it makes no terminal calls and never
guesses an asset class.

Translation contract:

**Outbound (canonical → broker):**

    1. The caller sets ``instrument.platform_symbol`` to the broker symbol
       (``"EURUSD.m"``).
    2. ``to_mt5_symbol(instrument)`` returns it verbatim.

``platform_symbol`` is **mandatory**: MT5 broker symbols cannot be derived
from ``symbol``/``quote_currency`` (suffixes are not standardized, and many
symbols are not currency pairs at all — ``US500``, ``AAPL``).  Raw
concatenation is deliberately NOT supported.

**Inbound (broker → canonical):**

    The adapter (not this module) reconstructs the canonical ``Instrument``
    from two authoritative sources:

    1. The state store — every order/position the engine has traded is
       persisted with its ``platform_symbol``, giving an exact
       ``platform_symbol → Instrument`` map.
    2. ``symbol_info()`` broker metadata — ``currency_base`` /
       ``currency_profit`` / ``path`` for symbols the engine has not traded
       (e.g. positions opened manually in the terminal).
"""

from __future__ import annotations

from unified_trading_execution.types.instrument import Instrument


def to_mt5_symbol(instrument: Instrument) -> str:
    """Return the MT5 broker symbol string for *instrument*.

    ``instrument.platform_symbol`` is returned verbatim.  It is mandatory —
    an MT5 broker symbol cannot be derived from ``symbol``/``quote_currency``,
    so a missing ``platform_symbol`` raises ``ValueError`` rather than
    guessing a (likely wrong) symbol.
    """
    if instrument.platform_symbol is None:
        raise ValueError(
            f"Instrument {instrument.symbol!r} has no platform_symbol — "
            "an MT5 broker symbol is required"
        )
    return instrument.platform_symbol

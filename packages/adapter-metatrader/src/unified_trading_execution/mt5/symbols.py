"""Instrument ↔ MetaTrader 5 symbol translation.

MT5 brokers use non-standard symbol suffixes (``EURUSD.m``, ``EURUSDpro``,
``EURUSD+``).  The adapter translates between core's canonical types and
broker-specific MT5 symbol strings.

This module is **pure string logic** — it makes no terminal calls and never
guesses an asset class.  The asset class is derived separately by the adapter
from ``mt5.symbol_info().path`` (the broker's market tree, e.g. ``"Forex\\EURUSD"``,
``"Metals\\XAUUSD"``, ``"Stocks\\AAPL"``), which is the only authoritative source.

Translation contract:

**Outbound (canonical → broker):**

    1. Adapter looks up ``str(instrument)`` (``"EUR/USD"``) in the alias table.
    2. Finds ``"EURUSD.m"`` → calls ``_with_broker_override(instrument, "EURUSD.m")``.
    3. ``to_mt5_symbol(instrument)`` returns ``"EURUSD.m"``.

**Inbound (broker → canonical):**

    1. Polling loop receives a position with symbol ``"EURUSD.m"``.
    2. ``from_mt5_symbol("EURUSD.m", reverse_alias_table)`` returns the
       ``(symbol, quote_currency)`` pair ``("EUR", "USD")``.
    3. The adapter derives ``asset_class`` from ``symbol_info().path`` and
       constructs the full ``Instrument``.

Aliasing is **mandatory**: a broker symbol that is not present in the alias
table is an error, not a silent best-effort parse.  Raw parsing of MT5 symbols
is deliberately NOT supported — suffixes are not standardized, and many symbols
are not currency pairs at all (``US500``, ``AAPL``).
"""

from __future__ import annotations

from unified_trading_execution.types.instrument import Instrument


def to_mt5_symbol(instrument: Instrument) -> str:
    """Convert a canonical ``Instrument`` to an MT5 broker symbol string.

    If ``instrument.broker_symbol_override`` is set (populated by the alias
    table via ``_with_broker_override``), it is returned directly.
    Otherwise, the base translation is applied: the symbol and quote currency
    are concatenated (e.g., ``"EUR"`` + ``"USD"`` → ``"EURUSD"``).
    """
    raise NotImplementedError


def from_mt5_symbol(
    mt5_symbol: str,
    reverse_alias_table: dict[str, str] | None = None,
) -> tuple[str, str | None]:
    """Convert an MT5 broker symbol string to a canonical ``(symbol, quote)`` pair.

    Looks up *mt5_symbol* in *reverse_alias_table* (mapping broker symbol →
    canonical ``"EUR/USD"`` shorthand) and splits the value on ``"/"`` into
    ``(symbol, quote_currency)``.

    Asset class is **not** derived here — it comes from the adapter via
    ``symbol_info().path``.  This function returns only the currency pair
    components; the adapter combines them with the asset class to build the
    full ``Instrument``.

    Raises ``ValueError`` if *mt5_symbol* is not in *reverse_alias_table*.
    Raw parsing of unlisted symbols is not supported — add the mapping to
    ``MT5Config.symbol_alias_table`` instead.
    """
    raise NotImplementedError


def build_reverse_alias_table(alias_table: dict[str, str]) -> dict[str, str]:
    """Build a reverse lookup from a forward alias table.

    Forward: ``{"EUR/USD": "EURUSD.m"}``
    Reverse: ``{"EURUSD.m": "EUR/USD"}`` .
    """
    raise NotImplementedError

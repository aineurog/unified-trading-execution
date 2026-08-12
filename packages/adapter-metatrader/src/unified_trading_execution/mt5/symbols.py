"""Instrument ↔ MetaTrader 5 symbol translation.

MT5 brokers use non-standard symbol suffixes (``EURUSD.m``, ``EURUSDpro``,
``EURUSD+``).  The adapter translates between core's canonical
``Instrument`` objects and broker-specific MT5 symbol strings.

The translation has two layers:

1. **Base translation** (always applied)
   - Convert ``Instrument`` to its raw MT5 symbol string.
   - Example: ``Instrument(symbol="EUR", quote_currency="USD")`` → ``"EURUSD"``

2. **Alias table overlay** (applied when ``broker_symbol_override`` is set)
   - Before base translation, ``_with_broker_override()`` is called to set
     the platform-specific literal string.
   - ``to_mt5_symbol()`` checks ``broker_symbol_override`` first; if set,
     returns it directly, bypassing the base translation.

Flow:

**Outbound (canonical → broker):**
    1. User creates ``Instrument(symbol="EUR", quote_currency="USD", ...)``
    2. Adapter looks up ``str(instrument)`` → ``"EUR/USD"`` in the alias table
    3. Finds ``"EURUSD.m"`` → calls ``_with_broker_override(instrument, "EURUSD.m")``
    4. ``to_mt5_symbol(instrument)`` returns ``"EURUSD.m"``

**Inbound (broker → canonical):**
    1. Polling loop receives a position with symbol ``"EURUSD.m"``
    2. ``from_mt5_symbol("EURUSD.m")`` looks up reverse alias table
    3. Finds ``"EUR/USD"`` → reconstructs canonical ``Instrument``
"""

from __future__ import annotations

from unified_trading_execution.types.instrument import Instrument, _with_broker_override


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
) -> Instrument:
    """Convert an MT5 broker symbol string back to a canonical ``Instrument``.

    If *reverse_alias_table* is provided and *mt5_symbol* is found in it,
    the canonical form is parsed from the value (e.g., ``"EUR/USD"`` →
    ``Instrument(symbol="EUR", quote_currency="USD")``).

    Otherwise, the raw MT5 symbol is parsed heuristically — the adapter
    attempts to split the symbol into base and quote components based on
    common broker conventions.

    Raises ``ValueError`` if the symbol cannot be parsed.
    """
    raise NotImplementedError


def build_reverse_alias_table(alias_table: dict[str, str]) -> dict[str, str]:
    """Build a reverse lookup from a forward alias table.

    Forward: ``{"EUR/USD": "EURUSD.m"}``
    Reverse: ``{"EURUSD.m": "EUR/USD"}`` .
    """
    raise NotImplementedError

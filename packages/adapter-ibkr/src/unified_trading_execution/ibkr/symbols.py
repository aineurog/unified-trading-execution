"""Instrument ↔ Interactive Brokers Contract translation.

IBKR uses structured Contract objects (defining symbol, secType, exchange,
currency, expiry, strike, right, and multiplier) rather than plain symbol strings.

This module provides bi-directional, pure translation helpers:

1. **Outbound (Canonical Instrument → IBKR Contract)**
   - Maps unified ``AssetClass`` enums to IBKR security types (``STK``, ``CASH``,
     ``OPT``, ``FUT``, ``CFD``, etc.).
   - Populates contract details using ``Instrument`` attributes, falling back to
     configuration defaults (``default_exchange``, ``default_currency``) when
     optional fields are ``None``.

2. **Inbound (IBKR Contract → Canonical Instrument)**
   - Maps IBKR security types back to unified ``AssetClass`` enums.
   - Extracts symbol, currency, exchange, expiry, strike, right, and multiplier
     to reconstruct a canonical ``Instrument``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ib_async import Contract

    from unified_trading_execution.ibkr.config import IBKRConfig

from unified_trading_execution.types.instrument import Instrument


def to_ibkr_contract(
    instrument: Instrument,
    config: IBKRConfig | None = None,
) -> Contract:
    """Convert a canonical ``Instrument`` to an ``ib_async.Contract``.

    Maps the unified ``AssetClass`` to IBKR ``secType``:
    - ``SPOT`` / ``MARGIN_FX`` → ``CASH`` (typically via ``Forex``)
    - ``STOCK`` → ``STK`` (typically via ``Stock``)
    - ``OPTION`` → ``OPT`` (typically via ``Option``)
    - ``FUTURES`` → ``FUT`` (typically via ``Future``)
    - ``CFD`` → ``CFD`` (typically via ``CFD``)

    Applies fallback exchange and currency from *config* if not explicitly set
    on the ``Instrument``.

    Raises ``ValueError`` if required derivative fields (e.g., expiry or strike)
    are missing for options or futures.
    """
    raise NotImplementedError


def from_ibkr_contract(contract: Contract) -> Instrument:
    """Convert an ``ib_async.Contract`` back to a canonical ``Instrument``.

    Parses contract fields (``secType``, ``symbol``, ``currency``, ``exchange``,
    ``lastTradeDateOrContractMonth``, ``strike``, ``right``, ``multiplier``)
    to reconstruct a fully populated ``Instrument``.

    Raises ``ValueError`` if ``secType`` is unmapped or missing required parameters.
    """
    raise NotImplementedError

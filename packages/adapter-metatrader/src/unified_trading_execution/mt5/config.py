"""MetaTrader 5 adapter configuration.

``MT5Config`` is a frozen dataclass — type-safe, hashable, and testable.
It is supplied by the user at construction time and never hardcoded by the adapter.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from unified_trading_execution.types.enums import AssetClass

DEFAULT_POLL_INTERVAL_SECONDS: float = 0.5
DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0


@dataclass(frozen=True, slots=True)
class MT5Config:
    """Immutable configuration for the MT5Adapter.

    Attributes:
        login: MT5 account number (integer).
        password: MT5 account password.
        server: Broker server name (e.g. ``"ICMarkets-Demo"``).
        path: Path to the terminal executable.  ``None`` means auto-detect
            (MT5 scans for an installed terminal).  Required when multiple
            terminal installations exist on the same machine.
        poll_interval_seconds: Seconds between poll cycles.  Default 0.5 s.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is
            trusted before being re-fetched.  Default 86400 (one day).
            ``None`` caches indefinitely, relying on invalidation only.
    """

    login: int
    password: str
    server: str
    path: str | None = None
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS
    # Broker-specific market-tree vocabulary.  Keys are ``symbol_info().path``
    # segments (matched case-insensitively), values the canonical AssetClass.
    # Extends and overrides the adapter's built-in thesaurus — the escape
    # hatch for a broker whose market folders are named differently (e.g. a
    # broker that groups metals under "PreciousMetals" instead of "Metals").
    asset_class_path_map: Mapping[str, AssetClass] | None = None

    def __post_init__(self) -> None:
        if self.poll_interval_seconds <= 0:
            raise ValueError(f"poll_interval_seconds must be > 0, got {self.poll_interval_seconds}")

        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}")

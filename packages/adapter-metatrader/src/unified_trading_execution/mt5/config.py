"""MetaTrader 5 adapter configuration.

``MT5Config`` is a frozen dataclass — type-safe, hashable, and testable.
It is supplied by the user at construction time and never hardcoded by the adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, field

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
        symbol_alias_table: Maps canonical ``"BASE/QUOTE"`` shorthand to
            broker-specific symbol strings (e.g. ``{"EUR/USD": "EURUSD.m"}``).
            Keys must contain a ``/``.  See ``symbols.py`` for usage.
        poll_interval_seconds: Seconds between poll cycles.  Default 0.5 s.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is
            trusted before being re-fetched.  Default 86400 (one day).
            ``None`` caches indefinitely, relying on invalidation only.
    """

    login: int
    password: str
    server: str
    path: str | None = None
    symbol_alias_table: dict[str, str] = field(default_factory=dict)
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS

    def __post_init__(self) -> None:
        # Validate alias table keys: must be canonical "BASE/QUOTE" form.
        for canonical, broker_symbol in self.symbol_alias_table.items():
            if "/" not in canonical:
                raise ValueError(
                    f"Symbol alias key must be canonical 'BASE/QUOTE' form, "
                    f"got {canonical!r}"
                )
            if not broker_symbol:
                raise ValueError(
                    f"Broker symbol for alias {canonical!r} must be non-empty"
                )

        if self.poll_interval_seconds <= 0:
            raise ValueError(
                f"poll_interval_seconds must be > 0, got {self.poll_interval_seconds}"
            )

        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(
                f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}"
            )

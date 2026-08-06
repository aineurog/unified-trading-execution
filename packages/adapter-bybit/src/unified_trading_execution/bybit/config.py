"""Bybit configuration — API credentials and environment switch.

The BybitAdapter constructor takes a BybitConfig instance rather than
loose strings — this keeps configuration type-safe and testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from unified_trading_execution.bybit.margin import LeverageConfig

DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0


@dataclass(frozen=True, slots=True)
class BybitConfig:
    """Immutable configuration for the BybitAdapter.

    Attributes:
        api_key: Bybit API key.
        api_secret: Bybit API secret.
        testnet: If True, connect to testnet.  If False, connect to mainnet.
        demo: If True, use the demo subdomain (api-demo / api-demo-testnet).
        platform_name: Human-readable platform identifier.
        account_id: Unique account label on this platform.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is trusted
            before being re-fetched.  Defaults to one day
            (``DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS``); ``None`` caches
            indefinitely, relying on invalidation only (Section 17.3).
        leverage: Leverage behavior configuration (Section 4.3).
    """

    api_key: str
    api_secret: str
    testnet: bool = True
    demo: bool = False
    platform_name: str = "bybit"
    account_id: str = "bybit-account"
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS
    leverage: LeverageConfig = field(default_factory=LeverageConfig)

    def __post_init__(self) -> None:
        """Validate configuration invariants at construction."""
        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}")

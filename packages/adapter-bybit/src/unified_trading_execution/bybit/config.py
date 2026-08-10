"""Bybit configuration — API credentials and environment switch.

The BybitAdapter constructor takes a BybitConfig instance rather than
loose strings — this keeps configuration type-safe and testable.
"""

from __future__ import annotations

from dataclasses import dataclass

from unified_trading_execution.bybit.enums import MarginMode

DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0


@dataclass(frozen=True, slots=True)
class BybitConfig:
    """Immutable configuration for the BybitAdapter.

    Attributes:
        api_key: Bybit API key.
        api_secret: Bybit API secret.
        testnet: If True, connect to testnet.  If False, connect to mainnet.
        demo: If True, use the demo subdomain (api-demo / api-demo-testnet).
        margin_mode: Account-wide margin mode applied on connect.  Accepts a
            ``MarginMode`` or its lowercase string value (``"cross"`` /
            ``"isolated"``).  A static default (cross) — set here at adapter
            construction, not at runtime.
        platform_name: Human-readable platform identifier.
        account_id: Unique account label on this platform.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is trusted
            before being re-fetched.  Defaults to one day
            (``DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS``); ``None`` caches
            indefinitely, relying on invalidation only (Section 17.3).
    """

    api_key: str
    api_secret: str
    testnet: bool = True
    demo: bool = False
    margin_mode: MarginMode | str = MarginMode.CROSS
    platform_name: str = "bybit"
    account_id: str = "bybit-account"
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS

    def __post_init__(self) -> None:
        """Validate configuration invariants at construction."""
        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}")
        if isinstance(self.margin_mode, str):
            try:
                object.__setattr__(self, "margin_mode", MarginMode(self.margin_mode))
            except ValueError:
                raise ValueError(
                    "margin_mode must be one of "
                    f"{[m.value for m in MarginMode]}, got {self.margin_mode!r}"
                ) from None

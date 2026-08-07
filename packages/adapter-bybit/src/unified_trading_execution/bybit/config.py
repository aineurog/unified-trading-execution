"""Bybit configuration — API credentials and environment switch.

The BybitAdapter constructor takes a BybitConfig instance rather than
loose strings — this keeps configuration type-safe and testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0


@dataclass(frozen=True, slots=True)
class LeverageConfig:
    """Behavioral policy for leverage management on the Bybit adapter.

    Controls how the adapter responds when leverage drift is detected,
    whether to reapply stored intent on connect, and safety guards.
    All fields carry safe defaults — a plain ``LeverageConfig()`` is
    a conservative starting point.

    This is not where leverage values (e.g. 10x for BTCUSDT) are stored.
    Leverage values are set via ``adapter.set_leverage(instrument, buy_leverage=N)``
    and persisted to the ``adapter_config`` table in the state store.
    """

    # What to do when reconciliation detects platform leverage differs from
    # stored intent: reapply it, just notify via event bus, or halt the instrument.
    on_drift: Literal["reapply", "notify", "halt"] = "reapply"

    # Reapply all stored leverage intent to the platform on connect.
    # When True (default), connect() reads every stored leverage value from
    # the DB and re-sends set_leverage calls — intent persists across restarts.
    auto_apply_on_connect: bool = True

    # Query the platform for current leverage before every order dispatch and
    # reject the order if it differs from stored intent.  When False (default),
    # trust that connect-time reapply and reconciliation keep leverage in sync
    # — no per-order network call on the hot path.
    strict_check: bool = False

    # Block leverage changes when the instrument has an open position.
    # Changing leverage with an open position recalculates margin immediately
    # and can cause liquidation — True is the safe default.
    block_on_open_position: bool = True


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
        leverage: Leverage behavioral policy (how the adapter manages leverage).
            Does not contain leverage values — those are set via
            ``adapter.set_leverage()``.
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

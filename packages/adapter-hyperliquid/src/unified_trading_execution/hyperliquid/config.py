"""Hyperliquid configuration — wallet identity, signing key, environment switch.

The HyperliquidAdapter constructor takes a HyperliquidConfig instance rather
than loose strings — this keeps configuration type-safe and testable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from unified_trading_execution.hyperliquid.enums import MarginMode

DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0

_WALLET_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_PRIVATE_KEY_RE = re.compile(r"^(0x)?[0-9a-fA-F]{64}$")


@dataclass(frozen=True, slots=True)
class HyperliquidConfig:
    """Immutable configuration for the HyperliquidAdapter.

    Attributes:
        wallet_address: 0x user address.  This IS the canonical account
            identity (no uid-resolve step like Bybit) and keys the
            state-store path.
        private_key: Signing key for the SDK ``Exchange`` transport, which
            requires it unconditionally (there is no keyless transport).
            API-wallet key recommended; main key also accepted.  An
            API-wallet key correctly differs from ``wallet_address`` — the
            two are never cross-checked.  64 hex chars, ``0x`` prefix
            optional.
        testnet: If True, use ``api.hyperliquid-testnet.xyz`` for REST+WS.
            If False, use ``api.hyperliquid.xyz`` (mainnet).
        default_margin_mode: Seed policy imposed per traded asset on connect
            (Hyperliquid keeps margin mode per asset on-venue — there is
            nothing account-wide to persist).  Accepts a ``MarginMode`` or
            its lowercase string value (``"cross"`` / ``"isolated"``).
        default_leverage: Seed policy imposed per traded asset on connect
            for assets carrying no explicit per-asset intent.  Must be >= 1.
        platform_name: Human-readable platform identifier.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is
            trusted before being re-fetched.  Defaults to one day; ``None``
            caches indefinitely, relying on invalidation only.
        request_timeout_seconds: Timeout for each blocking SDK call
            (executed via ``asyncio.to_thread`` — never awaited on the
            loop thread).
    """

    wallet_address: str
    private_key: str
    testnet: bool = False
    default_margin_mode: MarginMode | str = MarginMode.CROSS
    default_leverage: int = 1
    platform_name: str = "hyperliquid"
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS
    request_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        """Validate configuration invariants at construction."""
        if _WALLET_ADDRESS_RE.match(self.wallet_address) is None:
            raise ValueError(
                f"wallet_address must be a 0x address (40 hex chars), got {self.wallet_address!r}"
            )
        if _PRIVATE_KEY_RE.match(self.private_key) is None:
            raise ValueError("private_key must be 64 hex chars (0x prefix optional)")
        if self.default_leverage < 1:
            raise ValueError(f"default_leverage must be >= 1, got {self.default_leverage}")
        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}")
        if self.request_timeout_seconds <= 0:
            raise ValueError(
                f"request_timeout_seconds must be > 0, got {self.request_timeout_seconds}"
            )
        if isinstance(self.default_margin_mode, str):
            try:
                object.__setattr__(
                    self, "default_margin_mode", MarginMode(self.default_margin_mode)
                )
            except ValueError:
                raise ValueError(
                    "default_margin_mode must be one of "
                    f"{[m.value for m in MarginMode]}, got {self.default_margin_mode!r}"
                ) from None

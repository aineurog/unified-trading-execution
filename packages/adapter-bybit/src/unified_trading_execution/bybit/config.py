"""Bybit configuration — API credentials and environment switch.

The BybitAdapter constructor takes a BybitConfig instance rather than
loose strings — this keeps configuration type-safe and testable.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BybitConfig:
    """Immutable configuration for the BybitAdapter.

    Attributes:
        api_key: Bybit API key (testnet or live).
        api_secret: Bybit API secret (testnet or live).
        testnet: If True, connect to testnet; if False, connect to live.
        account_id: Human-readable account label (defaults to "bybit").
    """

    api_key: str
    api_secret: str
    testnet: bool = True

    # Default values for optional identification fields:
    platform_name: str = "bybit"
    account_id: str = "bybit-account"

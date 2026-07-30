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
        api_key: Bybit API key.
        api_secret: Bybit API secret.
        testnet: If True, connect to testnet.  If False, connect to mainnet.
        demo: If True, use the demo subdomain (api-demo / api-demo-testnet).
        platform_name: Human-readable platform identifier.
        account_id: Unique account label on this platform.
    """

    api_key: str
    api_secret: str
    testnet: bool = True
    demo: bool = False
    platform_name: str = "bybit"
    account_id: str = "bybit-account"

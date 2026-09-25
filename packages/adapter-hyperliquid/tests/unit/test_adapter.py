"""Unit tests for HyperliquidAdapter identity and implemented surface.

Asserts the concrete identity surface, the capability set, and the
disconnected flag — a future change that drops an abstract method or
alters capabilities must fail here, not at the first live use.
Implemented lifecycle (connect/disconnect) is covered in
``test_connection.py``.
"""

from __future__ import annotations

from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import OrderType

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"


async def test_identity_is_wallet_address(
    adapter: HyperliquidAdapter,
    hyperliquid_config: HyperliquidConfig,
) -> None:
    """The wallet address is the canonical account identity."""
    assert adapter.platform_name == "hyperliquid"
    assert adapter.account_id == _TEST_ADDRESS
    assert await adapter.resolve_account_id() == _TEST_ADDRESS
    assert adapter.account_id == hyperliquid_config.wallet_address


async def test_implemented_surface(adapter: HyperliquidAdapter) -> None:
    """Capability set is complete and the adapter starts disconnected."""
    assert adapter.supported_order_types() == frozenset(
        {
            OrderType.MARKET,
            OrderType.LIMIT,
            OrderType.STOP,
            OrderType.STOP_LIMIT,
        }
    )
    assert adapter.is_connected is False

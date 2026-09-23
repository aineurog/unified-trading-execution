"""Unit tests for HyperliquidAdapter identity and ABC conformance.

Smoke test only: the adapter is a scaffold, so every transport/market
method is intentionally ``NotImplementedError``.  This module asserts the
concrete identity surface (which is implemented) and that instantiating the
adapter satisfies the full ABC — a future change that drops an abstract
method must fail here, not at the first live use.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig

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


async def test_scaffold_methods_are_stubbed(adapter: HyperliquidAdapter) -> None:
    """Transport/market methods raise NotImplementedError until implemented."""
    with pytest.raises(NotImplementedError):
        adapter.supported_order_types()
    with pytest.raises(NotImplementedError):
        await adapter.connect()

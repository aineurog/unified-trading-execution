"""Unit tests for HyperliquidAdapter identity and ABC conformance.

Asserts the concrete identity surface (implemented) and that unimplemented
transport/market methods still raise ``NotImplementedError`` — a future
change that drops an abstract method must fail here, not at the first live
use.  Implemented lifecycle (connect/disconnect) is covered in
``test_connection.py``.
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
    """Unimplemented market methods raise NotImplementedError until implemented."""
    with pytest.raises(NotImplementedError):
        adapter.supported_order_types()
    assert adapter.is_connected is False

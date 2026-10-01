"""Integration: testnet connect/disconnect and account identity.

Gate: a signed session opens, the address is the identity, teardown is clean.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidConfig,
)


async def test_connect_establishes_session(
    connected_adapter: HyperliquidAdapter,
    hyperliquid_testnet_address: str,
) -> None:
    assert connected_adapter.is_connected
    assert connected_adapter.account_id == hyperliquid_testnet_address


async def test_disconnect_is_clean(
    hyperliquid_config: HyperliquidConfig,
    event_bus: EventBus,
) -> None:
    adapter = HyperliquidAdapter(hyperliquid_config, event_bus=event_bus)
    await adapter.connect()
    assert adapter.is_connected
    await adapter.disconnect()
    assert not adapter.is_connected


async def test_resolve_account_id_matches_address(
    connected_adapter: HyperliquidAdapter,
    hyperliquid_testnet_address: str,
) -> None:
    assert await connected_adapter.resolve_account_id() == hyperliquid_testnet_address


@pytest.mark.skip(reason="negative-credential probe would risk-lock the shared scratch key")
async def test_wrong_key_fails_loud() -> None:
    raise NotImplementedError("kept as documentation — do not enable on shared keys")

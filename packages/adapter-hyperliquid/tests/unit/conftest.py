"""Shared fixtures for Hyperliquid adapter unit tests.

No network is touched: the adapter constructor performs no I/O, so no
transport mocking is needed yet.  Later work adds SDK/WS mocks here.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh EventBus for each test — no cross-test subscriber leakage."""
    return EventBus()


@pytest.fixture
def hyperliquid_config() -> HyperliquidConfig:
    """Default testnet config with a dummy address — never hits the venue."""
    return HyperliquidConfig(
        wallet_address=_TEST_ADDRESS,
        private_key=_TEST_KEY,
        testnet=True,
    )


@pytest.fixture
def adapter(hyperliquid_config: HyperliquidConfig, event_bus: EventBus) -> HyperliquidAdapter:
    """A HyperliquidAdapter instance — not connected by default."""
    return HyperliquidAdapter(hyperliquid_config, event_bus=event_bus)

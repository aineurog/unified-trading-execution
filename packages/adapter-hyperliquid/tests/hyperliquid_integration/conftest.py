"""Shared fixtures for Hyperliquid integration tests (testnet only).

Integration tests run against testnet with a faucet-funded address and never
against mainnet.  Credentials come from the environment and every fixture
skips (never fails) when they are missing, so CI and local checkouts
without secrets stay green.  Venue-touching fixtures arrive as the adapter
gains connectivity; this file reserves the credential contract.
"""

from __future__ import annotations

import os

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import HyperliquidConfig


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping integration test")
    return value


@pytest.fixture(scope="session")
def hyperliquid_testnet_address() -> str:
    return _require_env("HYPERLIQUID_TESTNET_ADDRESS")


@pytest.fixture(scope="session")
def hyperliquid_testnet_private_key() -> str:
    return _require_env("HYPERLIQUID_TESTNET_PRIVATE_KEY")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def hyperliquid_config(
    hyperliquid_testnet_address: str,
    hyperliquid_testnet_private_key: str,
) -> HyperliquidConfig:
    return HyperliquidConfig(
        wallet_address=hyperliquid_testnet_address,
        private_key=hyperliquid_testnet_private_key,
        testnet=True,
    )

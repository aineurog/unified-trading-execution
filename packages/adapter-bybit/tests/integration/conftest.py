"""Shared fixtures for Bybit adapter integration tests.

Integration tests connect to the real Bybit testnet.  They are skipped
(not failed) when credentials are missing so that CI and local checkouts
without API keys stay green.
"""

from __future__ import annotations

import os

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.events import EventBus


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping integration test")
    return value


@pytest.fixture(scope="session")
def bybit_testnet_api_key() -> str:
    return _require_env("BYBIT_TESTNET_API_KEY")


@pytest.fixture(scope="session")
def bybit_testnet_api_secret() -> str:
    return _require_env("BYBIT_TESTNET_API_SECRET")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def bybit_config(
    bybit_testnet_api_key: str,
    bybit_testnet_api_secret: str,
) -> BybitConfig:
    return BybitConfig(
        api_key=bybit_testnet_api_key,
        api_secret=bybit_testnet_api_secret,
        testnet=True,
    )


@pytest.fixture
async def connected_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
):
    """A BybitAdapter connected to testnet — cleaned up after the test."""
    adapter = BybitAdapter(bybit_config, event_bus=event_bus)
    await adapter.connect()
    yield adapter
    try:
        await adapter.disconnect()
    except Exception:
        pass

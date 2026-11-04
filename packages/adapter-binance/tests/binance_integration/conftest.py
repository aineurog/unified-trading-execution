"""Shared fixtures for Binance testnet integration tests.

Integration tests are skipped unless dedicated testnet credentials are
configured and ``BINANCE_TESTNET=true`` is an explicit opt-in. The fixtures
always construct a Testnet config and never select Demo or production.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig
from unified_trading_execution.events import EventBus


def _load_env_files() -> None:
    """Load Binance variables from package/root env files without overriding exports."""
    directories = (
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[4],
    )
    seen: set[Path] = set()
    for directory in directories:
        env_file = directory / ".env"
        if env_file in seen or not env_file.is_file():
            continue
        seen.add(env_file)
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            name = name.strip()
            if not name.startswith("BINANCE_"):
                continue
            os.environ.setdefault(name, value.strip().strip('"').strip("'"))


_load_env_files()


def _require_testnet_opt_in() -> None:
    if os.getenv("BINANCE_TESTNET", "").strip().lower() != "true":
        pytest.skip("Set BINANCE_TESTNET=true to opt in to Binance testnet integration tests")
    if os.getenv("BINANCE_DEMO", "").strip().lower() == "true":
        pytest.skip("Binance integration fixtures require Testnet, not Demo Trading")


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping Binance testnet integration test")
    return value


@pytest.fixture(scope="session")
def binance_testnet_api_key() -> str:
    """Return the dedicated testnet API key or skip the integration test."""
    _require_testnet_opt_in()
    return _require_env("BINANCE_TESTNET_API_KEY")


@pytest.fixture(scope="session")
def binance_testnet_api_secret() -> str:
    """Return the dedicated testnet API secret or skip the integration test."""
    _require_testnet_opt_in()
    return _require_env("BINANCE_TESTNET_API_SECRET")


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh event bus for each integration test."""
    return EventBus()


@pytest.fixture
def binance_config(
    binance_testnet_api_key: str,
    binance_testnet_api_secret: str,
) -> BinanceConfig:
    """A config explicitly pinned to Testnet, never Demo or production."""
    return BinanceConfig(
        api_key=binance_testnet_api_key,
        api_secret=binance_testnet_api_secret,
        testnet=True,
        demo=False,
    )


@pytest.fixture
async def connected_adapter(
    binance_config: BinanceConfig,
    event_bus: EventBus,
) -> AsyncIterator[BinanceAdapter]:
    """Connect to Binance Testnet and always disconnect after the test."""
    adapter = BinanceAdapter(binance_config, event_bus=event_bus)
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()

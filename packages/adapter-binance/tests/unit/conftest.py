"""Shared fixtures for Binance adapter unit tests.

Unit tests use fake credentials and mocked SDK clients so they never contact
Binance. Keep exchange behavior configurable through the returned mocks.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import binance  # type: ignore[import-untyped]
import pytest
from binance import AsyncClient

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig
from unified_trading_execution.events import EventBus


@pytest.fixture
def binance_client_mock() -> AsyncMock:
    """An async SDK client mock with awaitable methods by default."""
    client = AsyncMock()
    client.close_connection = AsyncMock()
    return client


@pytest.fixture
def binance_socket_manager_mock() -> MagicMock:
    """A socket-manager mock that cannot open a real WebSocket."""
    return MagicMock()


@pytest.fixture(autouse=True)
def prevent_binance_network(
    monkeypatch: pytest.MonkeyPatch,
    binance_client_mock: AsyncMock,
    binance_socket_manager_mock: MagicMock,
) -> None:
    """Route SDK client and socket construction through local mocks."""
    monkeypatch.setattr(
        AsyncClient,
        "create",
        AsyncMock(return_value=binance_client_mock),
    )
    monkeypatch.setattr(
        binance,
        "BinanceSocketManager",
        MagicMock(return_value=binance_socket_manager_mock),
    )


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh event bus for each test."""
    return EventBus()


@pytest.fixture
def binance_config() -> BinanceConfig:
    """A testnet config with fake credentials and no live-account access."""
    return BinanceConfig(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=True,
    )


@pytest.fixture
def adapter(binance_config: BinanceConfig, event_bus: EventBus) -> BinanceAdapter:
    """An unconnected Binance adapter configured with fake testnet credentials."""
    return BinanceAdapter(binance_config, event_bus=event_bus)

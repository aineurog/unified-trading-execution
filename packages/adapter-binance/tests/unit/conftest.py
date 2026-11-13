"""Shared fixtures for Binance adapter unit tests.

Unit tests use fake credentials and mocked SDK clients so they never contact
Binance. Keep exchange behavior configurable through the returned mocks.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import binance  # type: ignore[import-untyped]
import pytest
from binance import AsyncClient

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig
from unified_trading_execution.events import EventBus


def _blocked_socket() -> MagicMock:
    socket = MagicMock()
    socket.__aenter__ = AsyncMock(return_value=socket)
    socket.__aexit__ = AsyncMock(return_value=False)
    socket.recv = AsyncMock(side_effect=asyncio.Event().wait)
    return socket


@pytest.fixture
def binance_client_mock() -> AsyncMock:
    """An async SDK client mock with awaitable methods by default."""
    client = AsyncMock()
    client.testnet = True
    client.demo = False
    client._create_api_uri = MagicMock(return_value="https://testnet.binance.vision/api/v3/ping")
    client._create_futures_api_uri = MagicMock(
        return_value="https://testnet.binancefuture.com/fapi/v1/ping"
    )
    client._create_futures_coin_api_url = MagicMock(
        return_value="https://testnet.binancefuture.com/dapi/v1/ping"
    )
    client.get_exchange_info.return_value = {"symbols": []}
    client.futures_exchange_info.return_value = {"symbols": []}
    client.futures_coin_exchange_info.return_value = {"symbols": []}
    client.close_connection = AsyncMock()

    # Mock _request_futures_api for USD-M v2 endpoints (positionRisk, etc.)
    # This routes to futures_position_information by default for backward compatibility
    async def mock_request_futures_api(
        method: str, path: str, signed: bool = False, version: int = 1, **kwargs
    ):
        if path == "positionRisk" and version == 2:
            # Route to futures_position_information mock for test compatibility
            return await client.futures_position_information(**kwargs.get("data", {}))
        # Default: return empty list for unknown endpoints
        return []

    client._request_futures_api = AsyncMock(side_effect=mock_request_futures_api)
    return client


@pytest.fixture
def binance_socket_manager_mock() -> MagicMock:
    """A socket-manager mock that cannot open a real WebSocket."""
    manager = MagicMock()
    manager.user_socket.return_value = _blocked_socket()
    manager.futures_user_socket.return_value = _blocked_socket()
    manager.coin_futures_socket.return_value = _blocked_socket()
    return manager


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

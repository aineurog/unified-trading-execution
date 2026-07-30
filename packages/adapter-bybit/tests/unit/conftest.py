"""Shared fixtures for Bybit adapter unit tests.

These fixtures mock Bybit's HTTP and WebSocket responses so unit tests
never hit the real network.  Every fixture must be injectable via pytest
markers — the dev should not need to know internal mocking details to
write a unit test against a specific adapter method.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.events import EventBus


@pytest.fixture(autouse=True)
def mock_pybit_http():
    """Mock pybit HTTP so adapter.__init__ never reaches the real network.

    Returns the mock session so individual tests can configure return values
    on ``mock_session.get_wallet_balance`` etc.
    """
    with patch("unified_trading_execution.bybit.adapter.HTTP") as mock_cls:
        mock_session = MagicMock()
        mock_cls.return_value = mock_session
        yield mock_session


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh EventBus for each test — no cross-test subscriber leakage."""
    return EventBus()


@pytest.fixture
def bybit_config() -> BybitConfig:
    """Default testnet config with fake credentials — never hits real Bybit."""
    return BybitConfig(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=True,
    )


@pytest.fixture
def adapter(bybit_config: BybitConfig, event_bus: EventBus) -> BybitAdapter:
    """A BybitAdapter instance wired with fake config — already importable.

    The adapter is NOT connected by default; individual tests call
    ``await adapter.connect()`` (or mock it) as needed.
    """
    return BybitAdapter(bybit_config, event_bus=event_bus)

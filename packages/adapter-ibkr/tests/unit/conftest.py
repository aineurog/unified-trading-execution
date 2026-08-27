"""Shared fixtures for IBKR adapter unit tests.

These fixtures mock the ``ib_async`` package so unit tests never call
into a real TWS or IB Gateway instance. Every fixture must be injectable
via pytest markers — the dev should not need to know internal mocking details to
write a unit test against a specific adapter method.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig


@pytest.fixture(autouse=True)
def mock_ib_async_module():
    """Mock ``ib_async.IB`` so no real TCP socket IPC occurs.

    Returns the mock IB instance so individual tests can configure return values
    on ``mock_ib.placeOrder``, ``mock_ib.reqContractDetailsAsync``, etc.
    """
    with patch("unified_trading_execution.ibkr.adapter.IB", create=True) as mock_ib_class:
        mock_ib = MagicMock()
        mock_ib.connectAsync = AsyncMock(return_value=None)
        mock_ib.disconnect = MagicMock()
        mock_ib.isConnected.return_value = True
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[])

        mock_ib_class.return_value = mock_ib
        yield mock_ib


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh EventBus for each test — no cross-test subscriber leakage."""
    return EventBus()


@pytest.fixture
def ibkr_config() -> IBKRConfig:
    """Default IBKR config with fake parameters — never hits a real gateway."""
    return IBKRConfig(
        host="127.0.0.1",
        port=4002,
        client_id=999,
        account="DU_TEST",
    )


@pytest.fixture
def adapter(ibkr_config: IBKRConfig, event_bus: EventBus) -> IBKRAdapter:
    """An IBKRAdapter instance wired with fake config — already importable.

    The adapter is NOT connected by default; individual tests call
    ``await adapter.connect()`` (or mock it) as needed.
    """
    return IBKRAdapter(ibkr_config, event_bus=event_bus)

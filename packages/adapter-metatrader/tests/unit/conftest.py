"""Shared fixtures for MT5 adapter unit tests.

These fixtures mock the ``MetaTrader5`` package so unit tests never call
into a real MT5 terminal.  Every fixture must be injectable via pytest
markers — the dev should not need to know internal mocking details to
write a unit test against a specific adapter method.

Important:
    The ``MetaTrader5`` module is NOT imported at module level (it is
    Windows-only).  Tests mock it out before the adapter's ``_get_mt5()``
    lazy import runs.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config


@pytest.fixture(autouse=True)
def mock_mt5_module():
    """Mock ``MetaTrader5`` so no real terminal IPC occurs.

    Returns the mock module so individual tests can configure return values
    on ``mock_mt5.initialize``, ``mock_mt5.order_send``, etc.
    """
    with patch("unified_trading_execution.mt5.adapter._get_mt5") as mock_get:
        mock_mt5 = MagicMock()
        mock_mt5.initialize.return_value = True
        mock_mt5.account_info.return_value = MagicMock(login=12345678)
        mock_mt5.last_error.return_value = (0, "")
        mock_mt5.symbols_get.return_value = []
        mock_get.return_value = mock_mt5
        yield mock_mt5


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh EventBus for each test — no cross-test subscriber leakage."""
    return EventBus()


@pytest.fixture
def mt5_config() -> MT5Config:
    """Default MT5 config with fake credentials — never hits a real terminal."""
    return MT5Config(
        login=12345678,
        password="test-password",
        server="TestBroker-Demo",
        symbol_alias_table={"EUR/USD": "EURUSD.m"},
    )


@pytest.fixture
def adapter(mt5_config: MT5Config, event_bus: EventBus) -> MT5Adapter:
    """An MT5Adapter instance wired with fake config — already importable.

    The adapter is NOT connected by default; individual tests call
    ``await adapter.connect()`` (or mock it) as needed.
    """
    return MT5Adapter(mt5_config, event_bus=event_bus)

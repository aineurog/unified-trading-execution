"""Shared fixtures for core unit tests."""

import pytest


@pytest.fixture
def event_bus():
    from unified_trading_execution.events import EventBus

    return EventBus()

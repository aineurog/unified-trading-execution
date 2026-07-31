"""Unit tests for BybitAdapter connection lifecycle (Section 17.10)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import ConnectionStateEvent


def _collect_events(event_bus) -> list[ConnectionStateEvent]:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    return events


async def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.005)


class TestConnect:
    async def test_publishes_connected_event(
        self,
        adapter,
        event_bus,
    ) -> None:
        events = _collect_events(event_bus)
        await adapter.connect()

        assert adapter.is_connected is True
        assert len(events) == 1
        event = events[0]
        assert event.connected is True
        assert event.adapter_name == "bybit"
        assert event.account_id == "bybit-account"
        assert event.correlation_id is None
        assert isinstance(event.event_id, str) and event.event_id
        assert event.timestamp.tzinfo is not None

        await adapter.disconnect()

    async def test_connect_is_idempotent(self, adapter, event_bus) -> None:
        events = _collect_events(event_bus)
        await adapter.connect()
        await adapter.connect()

        assert adapter.is_connected is True
        assert len(events) == 1

        await adapter.disconnect()

    async def test_connect_translates_connection_failure(
        self,
        adapter,
        event_bus,
        mock_bybit_websocket: MagicMock,
    ) -> None:
        mock_bybit_websocket.connect.side_effect = PlatformConnectionError("boom")
        events = _collect_events(event_bus)

        with pytest.raises(PlatformConnectionError):
            await adapter.connect()

        assert adapter.is_connected is False
        assert events == []

    async def test_reconnect_after_detected_drop_replaces_monitor(
        self,
        adapter,
        event_bus,
        mock_bybit_websocket: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "unified_trading_execution.bybit.adapter._CONNECTION_MONITOR_INTERVAL_SECONDS",
            0.01,
        )
        events = _collect_events(event_bus)
        await adapter.connect()

        mock_bybit_websocket.is_connected.return_value = False
        await _wait_until(lambda: len(events) >= 2)
        assert events[-1].connected is False

        await adapter.connect()
        assert adapter.is_connected is True
        assert len(events) == 3
        assert events[-1].connected is True

        mock_bybit_websocket.is_connected.return_value = False
        await _wait_until(lambda: len(events) >= 4)
        await asyncio.sleep(0.05)
        assert len(events) == 4

        await adapter.disconnect()


class TestDisconnect:
    async def test_publishes_disconnected_event(self, adapter, event_bus) -> None:
        await adapter.connect()
        events = _collect_events(event_bus)
        await adapter.disconnect()

        assert adapter.is_connected is False
        assert len(events) == 1
        assert events[0].connected is False

    async def test_disconnect_when_never_connected_is_noop(
        self,
        adapter,
        event_bus,
    ) -> None:
        events = _collect_events(event_bus)
        await adapter.disconnect()

        assert adapter.is_connected is False
        assert events == []

    async def test_disconnect_is_idempotent(self, adapter, event_bus) -> None:
        await adapter.connect()
        await adapter.disconnect()
        events = _collect_events(event_bus)
        await adapter.disconnect()

        assert events == []


class TestConnectionMonitor:
    async def test_publishes_state_change_on_drop_and_reconnect(
        self,
        adapter,
        event_bus,
        mock_bybit_websocket: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "unified_trading_execution.bybit.adapter._CONNECTION_MONITOR_INTERVAL_SECONDS",
            0.01,
        )
        events = _collect_events(event_bus)
        await adapter.connect()
        assert len(events) == 1

        mock_bybit_websocket.is_connected.return_value = False
        await _wait_until(lambda: len(events) >= 2)
        assert events[-1].connected is False

        mock_bybit_websocket.is_connected.return_value = True
        await _wait_until(lambda: len(events) >= 3)
        assert events[-1].connected is True

        await adapter.disconnect()

"""Unit tests for MT5 connection lifecycle.

Tests cases:
    - connect: acquires process-global guard, calls mt5.initialize()
    - connect: raises PlatformConnectionError if mt5.initialize() returns False
    - connect: raises PlatformConnectionError if already connected in this process
    - connect: raises PlatformConnectionError if account_info() returns None
    - connect: starts polling loop as background task
    - disconnect: cancels polling loop, calls mt5.shutdown(), releases guard
    - disconnect: publishes ConnectionStateEvent(connected=False)
    - disconnect: is idempotent (safe to call when already disconnected)
    - is_connected: reflects current state
    - Process-global guard: second adapter in same process is blocked
"""

from __future__ import annotations

import asyncio

import pytest

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import ConnectionStateEvent
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.mt5.adapter import _connected_lock


def _collect_events(event_bus) -> list[ConnectionStateEvent]:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    return events


async def _stub_poll_loop(adapter: MT5Adapter, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``_poll_loop`` with a never-completing no-op.

    Keeps the background task started by ``connect()`` alive until cancelled,
    so tests never observe the (unimplemented) real loop raising.
    """

    async def _never() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(adapter, "_poll_loop", _never)


async def _teardown(adapter: MT5Adapter) -> None:
    """Undo a successful connect — disconnect() is implemented in Step 10."""
    if adapter._poll_task is not None:
        adapter._poll_task.cancel()
        await asyncio.gather(adapter._poll_task, return_exceptions=True)
        adapter._poll_task = None
    if adapter._connected:
        _connected_lock.release()
        adapter._connected = False


class TestConnect:
    """MT5Adapter.connect() lifecycle."""

    async def test_successful_connection(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Acquires guard, initializes terminal, starts polling."""
        await _stub_poll_loop(adapter, monkeypatch)
        events = _collect_events(event_bus)

        await adapter.connect()

        mock_mt5_module.initialize.assert_called_once_with(
            adapter._config.path,
            adapter._config.login,
            adapter._config.password,
            adapter._config.server,
        )
        mock_mt5_module.account_info.assert_called_once_with()

        assert adapter.is_connected is True
        assert adapter.account_id == "12345678"
        assert adapter._reverse_alias == {"EURUSD.m": "EUR/USD"}
        assert adapter._poll_task is not None and not adapter._poll_task.done()

        assert len(events) == 1
        event = events[0]
        assert event.connected is True
        assert event.adapter_name == "metatrader"
        assert event.account_id == "12345678"
        assert event.correlation_id is None
        assert isinstance(event.event_id, str) and event.event_id
        assert event.timestamp.tzinfo is not None

        await _teardown(adapter)

    async def test_initialize_failure(
        self,
        adapter,
        mock_mt5_module,
    ) -> None:
        """mt5.initialize() returns False → PlatformConnectionError."""
        mock_mt5_module.initialize.return_value = False
        mock_mt5_module.last_error.return_value = (10011, "connection refused")

        with pytest.raises(PlatformConnectionError):
            await adapter.connect()

        assert adapter.is_connected is False
        assert adapter.account_id == str(adapter._config.login)

        # Guard was released — a fresh acquire succeeds.
        assert _connected_lock.acquire(blocking=False)
        _connected_lock.release()

    async def test_account_info_none(
        self,
        adapter,
        mock_mt5_module,
    ) -> None:
        """account_info() returns None → PlatformConnectionError."""
        mock_mt5_module.account_info.return_value = None
        mock_mt5_module.last_error.return_value = (32769, "not initialized")

        with pytest.raises(PlatformConnectionError):
            await adapter.connect()

        assert adapter.is_connected is False

        assert _connected_lock.acquire(blocking=False)
        _connected_lock.release()

    async def test_already_connected_in_process(
        self,
        adapter,
        event_bus,
        monkeypatch,
    ) -> None:
        """Second adapter in same process raises PlatformConnectionError."""
        await _stub_poll_loop(adapter, monkeypatch)
        await adapter.connect()

        other = MT5Adapter(MT5Config(login=999, password="other-password", server="Other-Demo"))
        with pytest.raises(PlatformConnectionError):
            await other.connect()

        assert other.is_connected is False
        assert adapter.is_connected is True

        await _teardown(adapter)

    async def test_starts_polling_loop(
        self,
        adapter,
        monkeypatch,
    ) -> None:
        """connect() creates an asyncio background task for _poll_loop."""
        await _stub_poll_loop(adapter, monkeypatch)

        await adapter.connect()

        assert adapter._poll_task is not None
        assert not adapter._poll_task.done()

        await _teardown(adapter)


class TestDisconnect:
    """MT5Adapter.disconnect() lifecycle."""

    def test_cancels_polling_and_shuts_down(self) -> None:
        """Polling loop cancelled, mt5.shutdown() called, guard released."""
        ...

    def test_publishes_disconnection_event(self) -> None:
        """disconnect() publishes ConnectionStateEvent(connected=False)."""
        ...

    def test_idempotent(self) -> None:
        """Calling disconnect twice is safe."""
        ...

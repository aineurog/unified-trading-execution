"""Unit tests for IBKRAdapter connection lifecycle — mock-only, no real Gateway."""

from __future__ import annotations

import asyncio

import pytest

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import ConnectionStateEvent
from unified_trading_execution.ibkr import IBKRAdapter


class TestIBKRConnectionLifecycle:
    """Test connection establishment, teardown, and event publication."""

    async def test_connect_success(
        self,
        adapter: IBKRAdapter,
        mock_ib_async_module: object,
        event_bus: object,
    ) -> None:
        """Successful connection initializes client, connects, and emits connected event."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        captured: list[ConnectionStateEvent] = []
        event_bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        await adapter.connect()

        # ib_async contract
        mock_ib = mock_ib_async_module  # type: ignore[assignment]
        mock_ib.connectAsync.assert_awaited_once_with(  # type: ignore[attr-defined]
            host="127.0.0.1",
            port=4002,
            clientId=999,
            timeout=10.0,
            readonly=False,
            account="DU_TEST",
        )
        assert adapter.is_connected is True
        assert adapter.account_id == "DU_TEST"
        assert len(captured) == 1
        assert captured[0].connected is True
        assert captured[0].adapter_name == "ibkr"

    async def test_disconnect_success(
        self,
        adapter: IBKRAdapter,
        mock_ib_async_module: object,
        event_bus: object,
    ) -> None:
        """Disconnect shuts down connection and emits disconnected event."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        captured: list[ConnectionStateEvent] = []
        event_bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        await adapter.connect()
        captured.clear()

        mock_ib = mock_ib_async_module  # type: ignore[assignment]
        mock_ib.disconnect.reset_mock()  # type: ignore[attr-defined]

        await adapter.disconnect()

        mock_ib.disconnect.assert_called_once()  # type: ignore[attr-defined]
        assert adapter.is_connected is False
        assert len(captured) == 1
        assert captured[0].connected is False

    async def test_duplicate_connect_refused(
        self,
        adapter: IBKRAdapter,
        mock_ib_async_module: object,
        event_bus: object,
    ) -> None:
        """Connecting an already connected adapter is a safe no-op (idempotent)."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        captured: list[ConnectionStateEvent] = []
        event_bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        await adapter.connect()
        mock_ib = mock_ib_async_module  # type: ignore[assignment]
        assert mock_ib.connectAsync.call_count == 1  # type: ignore[attr-defined]

        # Second call — should not create a second IB nor emit a second True
        await adapter.connect()

        assert mock_ib.connectAsync.call_count == 1  # type: ignore[attr-defined]
        assert len(captured) == 1  # still only the first connected=True

        # Overlapping concurrent connects are serialized by _connect_lock
        mock_ib.connectAsync.reset_mock()  # type: ignore[attr-defined]

        async def slow_connect(*_a: object, **_kw: object) -> None:
            await asyncio.sleep(0.05)

        mock_ib.connectAsync.side_effect = slow_connect  # type: ignore[attr-defined]
        # Need a fresh adapter for the overlap test (already connected above would short-circuit)
        from unified_trading_execution.ibkr import IBKRConfig

        fresh_bus = EventBus()
        fresh = IBKRAdapter(
            IBKRConfig(host="127.0.0.1", port=4002, client_id=999, account="DU_TEST"),
            event_bus=fresh_bus,
        )
        # fresh shares the same patched IB class — first call will be slow
        await asyncio.gather(fresh.connect(), fresh.connect())
        # Only one underlying connectAsync despite two concurrent callers
        assert mock_ib.connectAsync.call_count == 1  # type: ignore[attr-defined]

    async def test_connect_rejects_non_utc_timezone(
        self,
        adapter: IBKRAdapter,
        mock_ib_async_module: object,
        event_bus: object,
    ) -> None:
        """A known non-UTC TWS/Gateway timezone blocks connect with a clear error."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        mock_ib = mock_ib_async_module  # type: ignore[assignment]
        mock_ib.TimezoneTWS = "America/New_York"  # type: ignore[attr-defined]

        with pytest.raises(PlatformConnectionError, match="Time Zone"):
            await adapter.connect()
        assert adapter.is_connected is False
        assert adapter._ib is None

    async def test_connect_failure_raises_platform_error(
        self, adapter: IBKRAdapter, mock_ib_async_module: object, event_bus: object
    ) -> None:
        """Connection timeout or refusal raises PlatformConnectionError and cleans up."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        captured: list[ConnectionStateEvent] = []
        event_bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        mock_ib = mock_ib_async_module  # type: ignore[assignment]

        # Timeout path
        mock_ib.connectAsync.side_effect = TimeoutError("timed out")  # type: ignore[attr-defined]
        with pytest.raises(PlatformConnectionError, match="timed out"):
            await adapter.connect()
        assert adapter.is_connected is False
        assert adapter._ib is None
        assert len(captured) == 0  # no connected=True on failure

        # Generic refusal — also wrapped, and a subsequent connect can succeed
        mock_ib.connectAsync.side_effect = ConnectionRefusedError("refused")  # type: ignore[attr-defined]
        with pytest.raises(PlatformConnectionError, match="failed to connect"):
            await adapter.connect()
        assert adapter.is_connected is False

        # Recovery — next attempt succeeds
        mock_ib.connectAsync.side_effect = None  # type: ignore[attr-defined]
        mock_ib.connectAsync.return_value = None  # type: ignore[attr-defined]
        await adapter.connect()
        assert adapter.is_connected is True
        assert len(captured) == 1
        assert captured[0].connected is True

    async def test_disconnect_idempotence(
        self,
        adapter: IBKRAdapter,
        mock_ib_async_module: object,
        event_bus: object,
    ) -> None:
        """Calling disconnect multiple times does not raise and emits once."""
        from unified_trading_execution.events import EventBus

        assert isinstance(event_bus, EventBus)
        captured: list[ConnectionStateEvent] = []
        event_bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        await adapter.connect()
        captured.clear()
        mock_ib = mock_ib_async_module  # type: ignore[assignment]
        mock_ib.disconnect.reset_mock()  # type: ignore[attr-defined]

        await adapter.disconnect()
        await adapter.disconnect()  # second call — no-op
        await adapter.disconnect()  # third — still no-op

        # Only the first disconnect hits the underlying IB and publishes once
        assert mock_ib.disconnect.call_count == 1  # type: ignore[attr-defined]
        assert len(captured) == 1
        assert captured[0].connected is False
        assert adapter.is_connected is False

        # Disconnect without ever connecting is also a no-op (no event)
        from unified_trading_execution.ibkr import IBKRConfig

        fresh_bus = EventBus()
        fresh_captured: list[ConnectionStateEvent] = []
        fresh_bus.subscribe(ConnectionStateEvent, lambda e: fresh_captured.append(e))  # type: ignore[arg-type]
        fresh = IBKRAdapter(
            IBKRConfig(host="127.0.0.1", port=4002, client_id=999), event_bus=fresh_bus
        )
        await fresh.disconnect()
        assert len(fresh_captured) == 0

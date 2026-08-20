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
from types import SimpleNamespace

import pytest
from uuid_extensions import uuid7

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import ConnectionStateEvent
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.mt5.adapter import _connected_lock
from unified_trading_execution.mt5.comments import encode_client_order_id
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


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
            login=adapter._config.login,
            password=adapter._config.password,
            server=adapter._config.server,
        )
        mock_mt5_module.account_info.assert_called_once_with()

        assert adapter.is_connected is True
        assert adapter.account_id == "12345678"
        assert adapter._poll_task is not None and not adapter._poll_task.done()

        mock_mt5_module.symbol_select.assert_not_called()
        assert adapter._selected_symbols == set()

        assert len(events) == 1
        event = events[0]
        assert event.connected is True
        assert event.adapter_name == "metatrader"
        assert event.account_id == "12345678"
        assert event.correlation_id is None
        assert isinstance(event.event_id, str) and event.event_id
        assert event.timestamp.tzinfo is not None

        await adapter.disconnect()

    async def test_connect_passes_path_when_configured(
        self,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """path is forwarded as a keyword argument when MT5Config sets it."""
        config = MT5Config(
            login=12345678,
            password="test-password",
            server="TestBroker-Demo",
            path=r"C:\Program Files\MetaTrader 5\terminal64.exe",
        )
        adapter = MT5Adapter(config, event_bus=event_bus)
        await _stub_poll_loop(adapter, monkeypatch)

        await adapter.connect()

        mock_mt5_module.initialize.assert_called_once_with(
            path=config.path,
            login=config.login,
            password=config.password,
            server=config.server,
        )

        await adapter.disconnect()

    async def test_connect_recovers_order_mappings_from_comments(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Open orders and recent deals carrying U: comments repopulate the
        client_order_id ↔ ticket maps on connect."""
        await _stub_poll_loop(adapter, monkeypatch)
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        mock_mt5_module.symbols_get.return_value = ()
        mock_mt5_module.orders_get.return_value = (SimpleNamespace(ticket=5001, comment=comment),)
        mock_mt5_module.history_deals_get.return_value = (
            SimpleNamespace(ticket=6001, order=5001, comment=comment),
        )

        await adapter.connect()

        assert adapter._order_id_to_ticket == {cid: 5001}
        assert adapter._ticket_to_order_id == {5001: cid}
        await adapter.disconnect()

    async def test_connect_recovery_ignores_foreign_comments(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Foreign (manual/broker) comments and empty history leave the maps
        empty — recovery must never fail the connection."""
        await _stub_poll_loop(adapter, monkeypatch)
        mock_mt5_module.symbols_get.return_value = ()
        mock_mt5_module.orders_get.return_value = (
            SimpleNamespace(ticket=5001, comment="manual trade"),
        )
        mock_mt5_module.history_deals_get.return_value = ()

        await adapter.connect()

        assert adapter._order_id_to_ticket == {}
        assert adapter._ticket_to_order_id == {}
        await adapter.disconnect()


class _StubStore:
    """Minimal duck-typed StateStore: query_orders/query_positions with optional failure."""

    def __init__(
        self,
        records: tuple | list = (),
        *,
        positions: tuple | list = (),
        error: Exception | None = None,
    ) -> None:
        self._records = records
        self._positions = positions
        self._error = error

    async def query_orders(self, **kwargs):
        if self._error is not None:
            raise self._error
        return list(self._records)

    async def query_positions(self, **kwargs):
        if self._error is not None:
            raise self._error
        return list(self._positions)


class TestConnectStateStoreSeeding:
    """connect() seeds client_order_id ↔ ticket and platform_symbol → Instrument
    maps from the state store."""

    async def test_seeds_mappings_from_store(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Records in the store populate the maps on connect."""
        await _stub_poll_loop(adapter, monkeypatch)
        adapter.attach_state_store(
            _StubStore(
                (
                    SimpleNamespace(client_order_id="cid-1", platform_order_id="5001"),
                    SimpleNamespace(client_order_id="cid-2", platform_order_id="5002"),
                )
            )
        )

        await adapter.connect()

        assert adapter._order_id_to_ticket == {"cid-1": 5001, "cid-2": 5002}
        assert adapter._ticket_to_order_id == {5001: "cid-1", 5002: "cid-2"}
        await adapter.disconnect()

    async def test_seeds_symbol_mappings_from_store(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Records carrying an instrument seed platform_symbol → Instrument."""
        await _stub_poll_loop(adapter, monkeypatch)
        inst = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
            platform_symbol="EURUSD.m",
        )
        adapter.attach_state_store(
            _StubStore(
                (SimpleNamespace(instrument=inst, client_order_id=""),),
                positions=(SimpleNamespace(instrument=inst),),
            )
        )

        await adapter.connect()

        assert adapter._symbol_to_instrument == {"EURUSD.m": inst}
        await adapter.disconnect()

    async def test_seeding_tolerates_empty_client_id(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Records without a client_order_id are skipped, not fatal."""
        await _stub_poll_loop(adapter, monkeypatch)
        adapter.attach_state_store(
            _StubStore((SimpleNamespace(client_order_id="", platform_order_id="5001"),))
        )

        await adapter.connect()

        assert adapter._order_id_to_ticket == {}
        await adapter.disconnect()

    async def test_seeding_skips_non_numeric_platform_id(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """A non-numeric platform_order_id is skipped with a warning."""
        await _stub_poll_loop(adapter, monkeypatch)
        adapter.attach_state_store(
            _StubStore(
                (
                    SimpleNamespace(client_order_id="cid-1", platform_order_id="not-a-ticket"),
                    SimpleNamespace(client_order_id="cid-2", platform_order_id="5002"),
                )
            )
        )

        await adapter.connect()

        assert adapter._order_id_to_ticket == {"cid-2": 5002}
        await adapter.disconnect()

    async def test_seeding_failure_never_fails_connect(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failing store query is logged and skipped — connect survives."""
        await _stub_poll_loop(adapter, monkeypatch)
        adapter.attach_state_store(_StubStore((), error=RuntimeError("db locked")))

        await adapter.connect()

        assert adapter.is_connected is True
        assert "state-store query failed" in caplog.text
        await adapter.disconnect()

    async def test_store_mapping_beats_rewritten_comment(
        self,
        adapter,
        event_bus,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """A store entry is never overwritten by a (possibly rewritten)
        comment scan — the store is authoritative."""
        await _stub_poll_loop(adapter, monkeypatch)
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        adapter.attach_state_store(
            _StubStore((SimpleNamespace(client_order_id=cid, platform_order_id="5001"),))
        )
        # The open order's comment decodes to the same id but a DIFFERENT
        # ticket — a broker rewrite would have changed the stored comment's
        # payload.  The store's ticket must win.
        mock_mt5_module.symbols_get.return_value = ()
        mock_mt5_module.orders_get.return_value = (SimpleNamespace(ticket=6001, comment=comment),)
        mock_mt5_module.history_deals_get.return_value = ()

        await adapter.connect()

        assert adapter._order_id_to_ticket == {cid: 5001}
        assert adapter._ticket_to_order_id == {5001: cid}
        await adapter.disconnect()

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

        await adapter.disconnect()

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

        await adapter.disconnect()


class TestDisconnect:
    """MT5Adapter.disconnect() lifecycle."""

    async def test_cancels_polling_and_shuts_down(
        self,
        adapter,
        mock_mt5_module,
        monkeypatch,
    ) -> None:
        """Polling loop cancelled, mt5.shutdown() called, guard released."""
        await _stub_poll_loop(adapter, monkeypatch)
        await adapter.connect()
        assert adapter._poll_task is not None and not adapter._poll_task.done()

        await adapter.disconnect()

        mock_mt5_module.shutdown.assert_called_once_with()
        assert adapter._poll_task is None
        assert adapter.is_connected is False

        # Guard was released — a fresh acquire succeeds.
        assert _connected_lock.acquire(blocking=False)
        _connected_lock.release()

    async def test_publishes_disconnection_event(
        self,
        adapter,
        event_bus,
        monkeypatch,
    ) -> None:
        """disconnect() publishes ConnectionStateEvent(connected=False)."""
        await _stub_poll_loop(adapter, monkeypatch)
        await adapter.connect()
        events = _collect_events(event_bus)

        await adapter.disconnect()

        assert len(events) == 1
        event = events[0]
        assert event.connected is False
        assert event.adapter_name == "metatrader"
        assert event.correlation_id is None
        assert event.timestamp.tzinfo is not None

    async def test_idempotent(
        self,
        adapter,
        mock_mt5_module,
        event_bus,
        monkeypatch,
    ) -> None:
        """Calling disconnect twice is safe."""
        await _stub_poll_loop(adapter, monkeypatch)
        await adapter.connect()
        await adapter.disconnect()
        events = _collect_events(event_bus)

        await adapter.disconnect()

        assert adapter.is_connected is False
        assert events == []
        mock_mt5_module.shutdown.assert_called_once_with()

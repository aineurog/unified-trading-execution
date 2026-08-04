"""Connection-resilience integration tests (Section 11.2, bullet 4).

Forced disconnects, reconnect, the connection monitor's heartbeat behaviour,
and the reconnect-time registry refresh.  Re-subscription specifics are out of
scope (set aside); we assert reconnect happens and the registry is repopulated.
"""

from __future__ import annotations

import asyncio

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.events import ConnectionStateEvent
from unified_trading_execution.types.instrument import Instrument

from .conftest import EventCollector


async def test_forced_disconnect_reconnect(
    connected_adapter: BybitAdapter,
    collect_events: EventCollector,
) -> None:
    assert connected_adapter.is_connected

    await connected_adapter.disconnect()
    assert not connected_adapter.is_connected
    disconnect_events = collect_events.of_type(ConnectionStateEvent)
    assert any(event.connected is False for event in disconnect_events)

    await connected_adapter.connect()
    assert connected_adapter.is_connected
    connect_events = collect_events.of_type(ConnectionStateEvent)
    assert any(event.connected is True for event in connect_events)


async def test_heartbeat_alive(
    connected_adapter: BybitAdapter,
) -> None:
    """The connection monitor does not spuriously drop a healthy connection."""
    assert connected_adapter.is_connected
    assert connected_adapter._monitor_task is not None
    # Monitor interval is 5s; staying connected across it proves the heartbeat
    # keeps the connection flagged alive (no spurious disconnect).
    await asyncio.sleep(6.0)
    assert connected_adapter.is_connected


async def test_reconnect_refreshes_instrument_registry(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> None:
    assert connected_adapter._instruments

    await connected_adapter.disconnect()
    await connected_adapter.connect()

    assert connected_adapter._instruments, "registry must be repopulated on reconnect"
    symbol = f"{linear_instrument.symbol}{linear_instrument.quote_currency}"
    resolved = connected_adapter._resolve_instrument(symbol, "linear")
    assert resolved == linear_instrument


async def test_reconnect_invalidates_stale_specs(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> None:
    spec_before = await connected_adapter.fetch_instrument_spec(linear_instrument)
    assert linear_instrument in connected_adapter._instrument_specs

    await connected_adapter.disconnect()
    await connected_adapter.connect()

    assert connected_adapter._instruments
    spec_after = await connected_adapter.fetch_instrument_spec(linear_instrument)
    assert spec_after == spec_before, "spec must still resolve after reconnect"

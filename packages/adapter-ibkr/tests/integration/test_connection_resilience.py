"""Connection resilience — disconnect/reconnect and is_connected."""

# ruff: noqa: F401

from __future__ import annotations

import pytest

from unified_trading_execution.events import ConnectionStateEvent
from unified_trading_execution.ibkr import IBKRAdapter


async def test_disconnect_reconnect(connected_adapter: IBKRAdapter) -> None:
    assert connected_adapter.is_connected
    await connected_adapter.disconnect()
    assert not connected_adapter.is_connected
    await connected_adapter.connect()
    assert connected_adapter.is_connected
    from unified_trading_execution.types.enums import AssetClass
    from unified_trading_execution.types.instrument import Instrument

    inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
    spec = await connected_adapter.fetch_instrument_spec(inst)
    assert spec.tick_size > 0


async def test_disconnect_idempotent(connected_adapter: IBKRAdapter) -> None:
    await connected_adapter.disconnect()
    assert not connected_adapter.is_connected
    await connected_adapter.disconnect()
    assert not connected_adapter.is_connected
    await connected_adapter.connect()
    assert connected_adapter.is_connected


async def test_events_on_reconnect(ibkr_config, event_bus) -> None:  # type: ignore[no-untyped-def]
    bus = event_bus
    captured: list[ConnectionStateEvent] = []
    bus.subscribe(ConnectionStateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]
    adapter = IBKRAdapter(ibkr_config, event_bus=bus)
    await adapter.connect()
    assert any(e.connected for e in captured)
    captured.clear()
    await adapter.disconnect()
    assert any(not e.connected for e in captured)
    captured.clear()
    await adapter.connect()
    assert any(e.connected for e in captured)
    await adapter.disconnect()

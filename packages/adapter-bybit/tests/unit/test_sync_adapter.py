"""Unit tests for SyncBybitAdapter - blocking facade over BybitAdapter.

Requirements under test (sync-facade design, adapter package):
- The wrapper mirrors every *adapter-specific* async ``BybitAdapter`` method
  with a blocking counterpart, while order lifecycle stays on ``SyncEngine``.
- Each sync call dispatches the coroutine onto the engine's one persistent
  background loop (via ``SyncEngine.run``) - no ``asyncio.run()``, no coroutine
  in user code, and the running loop seen inside the adapter equals the loop
  SyncEngine uses (single-loop guarantee).
- ``SyncBybitAdapter`` rejects engines not backed by a ``BybitAdapter``.
- A shut-down engine surfaces ``EngineShutdownError`` from sync adapter calls.
"""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock

import pytest

import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter, SyncBybitAdapter
from unified_trading_execution.errors import EngineShutdownError
from unified_trading_execution.testing import MockAdapter

# Public async methods on BybitAdapter (no leading underscore).  Order lifecycle
# (place/modify/cancel/get-by-id) is intentionally NOT mirrored - that path lives
# on SyncEngine through the risk/state pipeline; SyncBybitAdapter covers the
# adapter-specific surface instead.
_ORDER_LIFECYCLE = {
    "place_order",
    "modify_order",
    "cancel_order",
    "get_order_by_client_id",
}


def _adapter_async_methods() -> set[str]:
    return {
        name
        for name in dir(BybitAdapter)
        if not name.startswith("_") and inspect.iscoroutinefunction(getattr(BybitAdapter, name))
    }


def test_every_adapter_async_method_is_mirrored() -> None:
    """SyncBybitAdapter mirrors every adapter-specific async method."""
    missing = {
        name
        for name in _adapter_async_methods() - _ORDER_LIFECYCLE
        if not hasattr(SyncBybitAdapter, name)
    }
    assert not missing, f"SyncBybitAdapter missing blocking methods: {missing}"

    # And the mirrored methods must actually be blocking, not coroutines.
    for name in _adapter_async_methods() - _ORDER_LIFECYCLE:
        fn = getattr(SyncBybitAdapter, name)
        assert not inspect.iscoroutinefunction(fn), f"{name} must be a sync def"

    # Order lifecycle must stay on SyncEngine - not duplicated on the wrapper.
    duplicated = _adapter_async_methods() & _ORDER_LIFECYCLE & set(dir(SyncBybitAdapter))
    assert not duplicated, f"Order lifecycle must not be duplicated: {duplicated}"


def test_rejects_non_bybit_adapter() -> None:
    engine = ute.SyncEngine(adapter=MockAdapter(event_bus=ute.EventBus()))
    with pytest.raises(TypeError):
        SyncBybitAdapter(engine)


def test_dispatches_onto_engine_loop(adapter: BybitAdapter) -> None:
    """Adapter coroutines run on SyncEngine's persistent loop - same object."""
    engine = ute.SyncEngine(adapter=adapter)
    sync = SyncBybitAdapter(engine)

    seen_loops: list[asyncio.AbstractEventLoop | None] = []

    async def fake_fetch_positions() -> dict:
        seen_loops.append(asyncio.get_running_loop())
        return {}

    adapter.fetch_positions = fake_fetch_positions  # type: ignore[method-assign]
    result = sync.fetch_positions()

    assert isinstance(result, dict)
    assert len(seen_loops) == 1
    assert seen_loops[0] is engine._loop  # exactly the engine's background loop


def test_returns_values_through_sync_surface(adapter: BybitAdapter) -> None:
    engine = ute.SyncEngine(adapter=adapter)
    sync = SyncBybitAdapter(engine)

    adapter.fetch_balances = AsyncMock(  # type: ignore[method-assign]
        return_value={"USDT": {"free": "1"}}
    )
    assert sync.fetch_balances() == {"USDT": {"free": "1"}}


def test_shutdown_raises_engine_shutdown_error(adapter: BybitAdapter) -> None:
    engine = ute.SyncEngine(adapter=adapter)
    sync = SyncBybitAdapter(engine)
    engine.shutdown()
    # _run raises EngineShutdownError before scheduling the coroutine, so the
    # unawaited coroutine is garbage-collected with a harmless RuntimeWarning.
    with pytest.warns(RuntimeWarning), pytest.raises(EngineShutdownError):
        sync.fetch_positions()

"""Unit tests for trailing-config wiring on the MT5 engines.

Tests cases:
    - MT5Engine forwards a supplied TrailingConfig to the core Engine
    - MT5Engine falls back to a default TrailingConfig when none is supplied
    - MT5Engine construction creates no trailing manager or worker
    - MT5Engine inherits the trailing property and it fails before connect
    - SyncMT5Engine forwards a supplied TrailingConfig to the async Engine
    - SyncMT5Engine exposes a SyncTrailingManager before connect
    - SyncMT5Engine construction starts neither the trailing worker nor the loop
    - SyncTrailingManager bridges calls through the sync engine
"""

from __future__ import annotations

import asyncio

import pytest

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.mt5 import MT5Config, MT5Engine, SyncMT5Engine
from unified_trading_execution.sync import SyncTrailingManager
from unified_trading_execution.trailing import TrailingConfig


class TestMT5EngineTrailingConfig:
    """The async engine must forward trailing tuning into the core Engine."""

    def test_forwards_supplied_config(self, mt5_config: MT5Config) -> None:
        config = TrailingConfig(poll_interval_seconds=3.0, fetch_failure_threshold=7)

        engine = MT5Engine(mt5_config, trailing_config=config)

        assert engine._trailing_config is config
        assert engine._trailing_config.poll_interval_seconds == 3.0
        assert engine._trailing_config.fetch_failure_threshold == 7

    def test_uses_default_config_when_omitted(self, mt5_config: MT5Config) -> None:
        engine = MT5Engine(mt5_config)

        assert isinstance(engine._trailing_config, TrailingConfig)
        assert engine._trailing_config == TrailingConfig()

    def test_construction_creates_no_manager_or_worker(self, mt5_config: MT5Config) -> None:
        engine = MT5Engine(mt5_config, trailing_config=TrailingConfig())

        assert engine._trailing_manager is None
        assert engine._trailing_worker is None

    def test_trailing_property_is_inherited_and_fails_before_connect(
        self, mt5_config: MT5Config
    ) -> None:
        engine = MT5Engine(mt5_config)

        assert isinstance(MT5Engine.trailing, property)
        with pytest.raises(PlatformConnectionError):
            _ = engine.trailing


class TestSyncMT5EngineTrailingConfig:
    """The blocking engine must forward trailing tuning and expose the facade."""

    def test_forwards_supplied_config(self, mt5_config: MT5Config) -> None:
        config = TrailingConfig(poll_interval_seconds=2.5, protection_failure_threshold=4)

        engine = SyncMT5Engine(mt5_config, trailing_config=config)

        assert engine._async_engine._trailing_config is config
        assert engine._async_engine._trailing_config.poll_interval_seconds == 2.5
        assert engine._async_engine._trailing_config.protection_failure_threshold == 4

    def test_uses_default_config_when_omitted(self, mt5_config: MT5Config) -> None:
        engine = SyncMT5Engine(mt5_config)

        assert engine._async_engine._trailing_config == TrailingConfig()

    def test_exposes_sync_trailing_manager_before_connect(self, mt5_config: MT5Config) -> None:
        engine = SyncMT5Engine(mt5_config)

        assert isinstance(engine.trailing, SyncTrailingManager)
        assert engine.trailing._engine is engine

    def test_construction_starts_no_worker_or_loop(self, mt5_config: MT5Config) -> None:
        engine = SyncMT5Engine(mt5_config, trailing_config=TrailingConfig())

        assert engine._loop is None
        assert engine._loop_thread is None
        assert engine._async_engine._trailing_manager is None
        assert engine._async_engine._trailing_worker is None


class _RecordingManager:
    """Minimal stand-in for the core manager recording bridged calls."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get(self, trailing_id: str) -> str:
        self.calls.append(trailing_id)
        return f"bridged:{trailing_id}"


class TestSyncTrailingManagerBridging:
    """Facade calls must run on the sync engine's persistent loop path."""

    def test_get_dispatches_through_sync_engine(
        self, mt5_config: MT5Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = SyncMT5Engine(mt5_config)
        manager = _RecordingManager()
        engine._async_engine._trailing_manager = manager  # type: ignore[assignment]
        dispatched: list[object] = []

        def run(coro: object) -> object:
            dispatched.append(coro)
            return asyncio.run(coro)  # type: ignore[arg-type]

        monkeypatch.setattr(engine, "_run", run)

        result = engine.trailing.get("ticket-42")

        assert result == "bridged:ticket-42"
        assert manager.calls == ["ticket-42"]
        assert len(dispatched) == 1

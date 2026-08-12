"""Unit tests for BybitEngine and SyncBybitEngine — single-object entry points."""

from __future__ import annotations

import inspect

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig, BybitEngine, SyncBybitEngine
from unified_trading_execution.engine import Engine
from unified_trading_execution.sync import SyncEngine


def _config() -> BybitConfig:
    return BybitConfig(testnet=True, api_key="k", api_secret="s")


class TestBybitEngine:
    def test_is_engine_subclass(self) -> None:
        assert issubclass(BybitEngine, Engine)

    def test_accepts_config(self) -> None:
        engine = BybitEngine(_config())
        assert isinstance(engine._adapter, BybitAdapter)

    def test_accepts_adapter(self) -> None:
        adapter = BybitAdapter(_config())
        engine = BybitEngine(adapter)
        assert engine._adapter is adapter

    def test_exposes_bybit_specific_methods(self) -> None:
        """Every public BybitAdapter-only method is on BybitEngine."""
        # get_order_by_client_id is the adapter method — Engine provides get_order()
        _skip = {"get_order_by_client_id"}
        engine_methods = set(dir(Engine)) - set(dir(object))
        adapter_only = {
            name
            for name in set(dir(BybitAdapter)) - engine_methods - _skip
            if not name.startswith("_") and inspect.iscoroutinefunction(getattr(BybitAdapter, name))
        }
        missing = {name for name in adapter_only if not hasattr(BybitEngine, name)}
        assert missing == set(), f"BybitEngine missing methods: {missing}"


class TestSyncBybitEngine:
    def test_is_sync_engine_subclass(self) -> None:
        assert issubclass(SyncBybitEngine, SyncEngine)

    def test_accepts_config(self) -> None:
        engine = SyncBybitEngine(_config())
        assert isinstance(engine.adapter, BybitAdapter)

    def test_exposes_bybit_specific_methods(self) -> None:
        """Every public BybitAdapter-only method is on SyncBybitEngine."""
        _skip = {"get_order_by_client_id"}
        sync_engine_methods = set(dir(SyncEngine)) - set(dir(object))
        adapter_only = {
            name
            for name in set(dir(BybitAdapter)) - sync_engine_methods - _skip
            if not name.startswith("_") and inspect.iscoroutinefunction(getattr(BybitAdapter, name))
        }
        missing = {name for name in adapter_only if not hasattr(SyncBybitEngine, name)}
        assert missing == set(), f"SyncBybitEngine missing methods: {missing}"

    def test_sync_methods_are_blocking(self) -> None:
        """Every BybitAdapter async method mirrored on SyncBybitEngine is a sync def."""
        for name in (
            "set_leverage",
            "get_leverage",
            "remove_leverage",
            "set_position_mode",
            "get_position_mode",
            "remove_position_mode",
            "set_position_mode_for_coin",
            "set_margin_mode",
            "get_margin_mode",
        ):
            fn = getattr(SyncBybitEngine, name)
            assert not inspect.iscoroutinefunction(fn), f"{name} must be a sync def"

"""Unit tests for HyperliquidEngine/SyncHyperliquidEngine (adapter mocked, no network)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unified_trading_execution.engine import Engine
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.engine import HyperliquidEngine
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.sync_engine import SyncHyperliquidEngine
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)


def _async_engine(adapter: Any = None) -> HyperliquidEngine:
    engine = HyperliquidEngine(_config())
    engine._adapter = adapter if adapter is not None else AsyncMock()
    return engine


def test_is_engine_subclass() -> None:
    assert issubclass(HyperliquidEngine, Engine)


def test_accepts_config_and_adapter() -> None:
    assert isinstance(HyperliquidEngine(_config())._adapter, HyperliquidAdapter)
    adapter = HyperliquidAdapter(_config())
    assert HyperliquidEngine(adapter)._adapter is adapter


async def test_connect_starts_streams_after_super() -> None:
    """connect() runs core setup first, then opens the push channel."""
    order: list[str] = []
    adapter = AsyncMock()
    adapter.start_streams = AsyncMock(side_effect=lambda: order.append("streams"))
    engine = _async_engine(adapter)
    with patch.object(Engine, "connect", new=AsyncMock(side_effect=lambda: order.append("core"))):
        await engine.connect()
    assert order == ["core", "streams"]


async def test_connect_streams_failure_propagates() -> None:
    """A dead push channel fails connect loudly — never half-connected silently."""
    adapter = AsyncMock()
    adapter.start_streams = AsyncMock(side_effect=RuntimeError("ws denied"))
    engine = _async_engine(adapter)
    with patch.object(Engine, "connect", new=AsyncMock()):
        try:
            await engine.connect()
        except RuntimeError as exc:
            assert "ws denied" in str(exc)
        else:
            raise AssertionError("expected start_streams failure to propagate")


async def test_set_leverage_forwards_all_knobs() -> None:
    adapter = AsyncMock()
    await _async_engine(adapter).set_leverage(
        _perp(),
        leverage=7,
        on_drift="halt",
        strict_check=False,
        block_on_open_position=False,
        auto_apply_on_connect=False,
    )
    adapter.set_leverage.assert_called_once_with(
        _perp(),
        leverage=7,
        on_drift="halt",
        strict_check=False,
        block_on_open_position=False,
        auto_apply_on_connect=False,
    )


async def test_reads_pass_values_through() -> None:
    adapter = AsyncMock()
    adapter.get_leverage.return_value = (10, False)
    adapter.get_margin_mode.return_value = MarginMode.ISOLATED
    engine = _async_engine(adapter)
    assert await engine.get_leverage(_perp()) == (10, False)
    assert await engine.get_margin_mode(_perp()) is MarginMode.ISOLATED


async def test_remove_and_reconcile_forward() -> None:
    adapter = AsyncMock()
    engine = _async_engine(adapter)
    await engine.remove_leverage(_perp())
    await engine.remove_margin_mode(_perp())
    await engine.reconcile_user_intent()
    adapter.remove_leverage.assert_called_once_with(_perp())
    adapter.remove_margin_mode.assert_called_once_with(_perp())
    adapter.reconcile_user_intent.assert_called_once_with()


async def test_top_up_forwards_amount() -> None:
    adapter = AsyncMock()
    await _async_engine(adapter).top_up_isolated_margin(_perp(), amount_usdc=Decimal("25"))
    adapter.top_up_isolated_margin.assert_called_once_with(_perp(), amount_usdc=Decimal("25"))


async def test_position_tpsl_forwards() -> None:
    adapter = AsyncMock()
    tp = TpSlAttachment(trigger_price=Decimal("60000"))
    engine = _async_engine(adapter)
    await engine.modify_position_tpsl(_perp(), "BTC:oneWay", take_profit=tp)
    adapter.modify_position_tpsl.assert_called_once_with(
        _perp(),
        "BTC:oneWay",
        take_profit=tp,
        stop_loss=None,
    )
    adapter.get_position_tpsl.return_value = (tp, None)
    assert await engine.get_position_tpsl(_perp(), "BTC:oneWay") == (tp, None)


async def test_snapshot_proxies_forward() -> None:
    adapter = AsyncMock()
    engine = _async_engine(adapter)
    since = datetime(2024, 1, 1, tzinfo=UTC)
    await engine.fetch_instrument_spec(_perp())
    await engine.fetch_ticker(_perp())
    await engine.get_rate_limits()
    await engine.fetch_positions()
    await engine.fetch_balances()
    await engine.fetch_open_orders()
    await engine.fetch_fills(since=since)
    adapter.fetch_instrument_spec.assert_called_once_with(_perp())
    adapter.fetch_ticker.assert_called_once_with(_perp())
    adapter.get_rate_limits.assert_called_once_with()
    adapter.fetch_positions.assert_called_once_with()
    adapter.fetch_balances.assert_called_once_with()
    adapter.fetch_open_orders.assert_called_once_with()
    adapter.fetch_fills.assert_called_once_with(since=since)


async def test_adapter_failure_propagates() -> None:
    adapter = AsyncMock()
    adapter.fetch_ticker.side_effect = RuntimeError("venue down")
    try:
        await _async_engine(adapter).fetch_ticker(_perp())
    except RuntimeError as exc:
        assert "venue down" in str(exc)
    else:
        raise AssertionError("expected adapter failure to propagate")


def _sync_engine(adapter: Any = None) -> SyncHyperliquidEngine:
    engine = SyncHyperliquidEngine(_config())
    if adapter is None:
        adapter = AsyncMock(spec=HyperliquidAdapter)
    engine._async_engine._adapter = adapter
    return engine


def test_is_sync_engine_subclass() -> None:
    assert issubclass(SyncHyperliquidEngine, SyncEngine)


def test_sync_accepts_config_and_adapter() -> None:
    assert isinstance(SyncHyperliquidEngine(_config()).adapter, HyperliquidAdapter)
    adapter = HyperliquidAdapter(_config())
    assert SyncHyperliquidEngine(adapter).adapter is adapter


def test_sync_hl_adapter_property_rejects_foreign() -> None:
    engine = SyncHyperliquidEngine(_config())
    engine._async_engine._adapter = MagicMock()
    with pytest.raises(AssertionError):
        _ = engine._hl_adapter


def test_sync_connect_starts_streams() -> None:
    """Sync path gets push streams too — core builds a generic engine, so override."""
    adapter = AsyncMock(spec=HyperliquidAdapter)
    adapter.start_streams = AsyncMock()
    engine = _sync_engine(adapter)
    with patch.object(SyncEngine, "connect") as super_connect:
        engine.connect()
    super_connect.assert_called_once_with()
    adapter.start_streams.assert_called_once_with()


def test_sync_delegation() -> None:
    adapter = AsyncMock(spec=HyperliquidAdapter)
    adapter.get_leverage.return_value = (10, True)
    engine = _sync_engine(adapter)
    assert engine.get_leverage(_perp()) == (10, True)
    adapter.get_leverage.assert_called_once_with(_perp())
    engine.set_leverage(
        _perp(),
        leverage=5,
        on_drift="notify",
        strict_check=False,
        block_on_open_position=False,
        auto_apply_on_connect=False,
    )
    adapter.set_leverage.assert_called_once_with(
        _perp(),
        leverage=5,
        on_drift="notify",
        strict_check=False,
        block_on_open_position=False,
        auto_apply_on_connect=False,
    )
    engine.remove_leverage(_perp())
    engine.top_up_isolated_margin(_perp(), amount_usdc=Decimal("5"))
    engine.set_margin_mode(_perp(), "isolated")
    assert engine.get_margin_mode(_perp()) is adapter.get_margin_mode.return_value
    engine.remove_margin_mode(_perp())
    engine.reconcile_user_intent()
    tp = TpSlAttachment(trigger_price=Decimal("1"))
    engine.modify_position_tpsl(_perp(), "BTC:oneWay", stop_loss=tp)
    adapter.modify_position_tpsl.assert_called_once_with(
        _perp(),
        "BTC:oneWay",
        take_profit=None,
        stop_loss=tp,
    )
    assert engine.get_position_tpsl(_perp(), "BTC:oneWay") is (
        adapter.get_position_tpsl.return_value
    )
    engine.fetch_ticker(_perp())
    engine.get_rate_limits()
    engine.fetch_positions()
    engine.fetch_balances()
    engine.fetch_open_orders()
    engine.fetch_fills()
    engine.fetch_instrument_spec(_perp())
    adapter.fetch_ticker.assert_called_once_with(_perp())
    adapter.fetch_fills.assert_called_once_with(since=None)


def test_sync_failure_propagates() -> None:
    adapter = AsyncMock(spec=HyperliquidAdapter)
    adapter.fetch_positions.side_effect = RuntimeError("venue down")
    try:
        _sync_engine(adapter).fetch_positions()
    except RuntimeError as exc:
        assert "venue down" in str(exc)
    else:
        raise AssertionError("expected adapter failure to propagate")

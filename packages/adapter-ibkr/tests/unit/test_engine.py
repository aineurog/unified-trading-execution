"""Unit tests for IBKREngine/SyncIBKREngine (adapter mocked, no network)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

from unified_trading_execution.engine import Engine
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.ibkr.engine import IBKREngine
from unified_trading_execution.ibkr.sync_engine import SyncIBKREngine
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.trailing import TrailingConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment


def _config() -> IBKRConfig:
    return IBKRConfig(
        host="127.0.0.1",
        port=7497,
        client_id=1,
        account="DU123456",
    )


def _instrument(**kwargs: object) -> Instrument:
    defaults = {
        "symbol": "AAPL",
        "quote_currency": "USD",
        "asset_class": AssetClass.STOCK,
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


def _async_engine(adapter: Any = None) -> IBKREngine:
    engine = IBKREngine(_config())
    engine._adapter = adapter if adapter is not None else AsyncMock()
    return engine


def test_is_engine_subclass() -> None:
    assert issubclass(IBKREngine, Engine)


def test_accepts_config_and_adapter() -> None:
    assert isinstance(IBKREngine(_config())._adapter, IBKRAdapter)
    adapter = IBKRAdapter(_config())
    assert IBKREngine(adapter)._adapter is adapter


def test_trailing_config_wiring() -> None:
    """trailing_config parameter is passed to base Engine."""
    config = TrailingConfig(poll_interval_seconds=5.0)
    engine = IBKREngine(_config(), trailing_config=config)
    assert engine._trailing_config.poll_interval_seconds == 5.0


async def test_clear_position_tpsl_forwards() -> None:
    adapter = AsyncMock()
    engine = _async_engine(adapter)
    await engine.clear_position_tpsl(_instrument(), "12345", take_profit=True, stop_loss=False)
    adapter.clear_position_tpsl.assert_called_once_with(
        _instrument(), "12345", take_profit=True, stop_loss=False
    )


async def test_fetch_candles_forwards() -> None:
    from unified_trading_execution.adapter import CandleTimeframe

    adapter = AsyncMock()
    engine = _async_engine(adapter)
    await engine.fetch_candles(_instrument(), CandleTimeframe.M1, limit=10)
    adapter.fetch_candles.assert_called_once_with(_instrument(), CandleTimeframe.M1, limit=10)


async def test_position_tpsl_forwards() -> None:
    adapter = AsyncMock()
    tp = TpSlAttachment(trigger_price=Decimal("200"))
    engine = _async_engine(adapter)
    await engine.modify_position_tpsl(_instrument(), "12345", take_profit=tp)
    adapter.modify_position_tpsl.assert_called_once_with(
        _instrument(), "12345", take_profit=tp, stop_loss=None
    )
    adapter.get_position_tpsl.return_value = (tp, None)
    assert await engine.get_position_tpsl(_instrument(), "12345") == (tp, None)


def _sync_engine(adapter: Any = None) -> SyncIBKREngine:
    if adapter is None:
        adapter = AsyncMock(spec=IBKRAdapter)
    # Construct with the adapter directly
    engine = SyncIBKREngine(adapter)
    return engine


def test_is_sync_engine_subclass() -> None:
    assert issubclass(SyncIBKREngine, SyncEngine)


def test_sync_accepts_config_and_adapter() -> None:
    assert isinstance(SyncIBKREngine(_config()).adapter, IBKRAdapter)
    adapter = IBKRAdapter(_config())
    assert SyncIBKREngine(adapter).adapter is adapter


def test_sync_clear_position_tpsl_forwards() -> None:
    adapter = AsyncMock(spec=IBKRAdapter)
    engine = _sync_engine(adapter)
    engine.clear_position_tpsl(_instrument(), "12345", take_profit=False, stop_loss=True)
    adapter.clear_position_tpsl.assert_called_once_with(
        _instrument(), "12345", take_profit=False, stop_loss=True
    )


def test_sync_fetch_candles_forwards() -> None:
    from unified_trading_execution.adapter import CandleTimeframe

    adapter = AsyncMock(spec=IBKRAdapter)
    engine = _sync_engine(adapter)
    engine.fetch_candles(_instrument(), CandleTimeframe.H1, limit=20)
    adapter.fetch_candles.assert_called_once_with(_instrument(), CandleTimeframe.H1, limit=20)


def test_sync_position_tpsl_forwards() -> None:
    adapter = AsyncMock(spec=IBKRAdapter)
    tp = TpSlAttachment(trigger_price=Decimal("200"))
    engine = _sync_engine(adapter)
    engine.modify_position_tpsl(_instrument(), "12345", take_profit=tp)
    adapter.modify_position_tpsl.assert_called_once_with(
        _instrument(), "12345", take_profit=tp, stop_loss=None
    )
    assert (
        engine.get_position_tpsl(_instrument(), "12345") is adapter.get_position_tpsl.return_value
    )

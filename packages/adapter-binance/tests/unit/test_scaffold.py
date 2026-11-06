"""Smoke tests for the Binance adapter scaffold.

The scaffold performs no network I/O and claims no exchange capability.
These tests pin that contract until the implementation branches land.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    SyncBinanceEngine,
)
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _instrument() -> Instrument:
    return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)


def test_capabilities_declare_no_support(adapter: BinanceAdapter) -> None:
    """The scaffold must not claim protection, trailing, or market data."""
    instrument = _instrument()

    protection = adapter.position_protection_capability(instrument)
    assert protection.supported is False
    assert protection.can_set_stop is False
    assert protection.can_set_take_profit is False
    assert protection.preserves_unmentioned_side is False
    assert protection.trigger_bases == frozenset()

    trailing = adapter.trailing_capability(instrument)
    assert trailing.supported is False
    assert trailing.offset_types == frozenset()
    assert trailing.position_scoped is False

    market_data = adapter.market_data_capability(instrument)
    assert market_data.ticker_bases == frozenset()
    assert market_data.candle_timeframes == frozenset()
    assert market_data.streaming_ticker is False


def test_config_redacts_credentials(binance_config: BinanceConfig) -> None:
    """Credential values must never appear in the config representation."""
    text = repr(binance_config)
    assert "test-api-key" not in text
    assert "test-api-secret" not in text


def test_testnet_and_demo_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        BinanceConfig(api_key="key", api_secret="secret", testnet=True, demo=True)


def test_instrument_spec_cache_ttl_must_be_positive_or_none() -> None:
    with pytest.raises(ValueError, match="instrument_spec_cache_ttl"):
        BinanceConfig(api_key="key", api_secret="secret", instrument_spec_cache_ttl=0)

    config = BinanceConfig(api_key="key", api_secret="secret", instrument_spec_cache_ttl=None)
    assert config.instrument_spec_cache_ttl is None


def test_account_id_does_not_expose_the_api_key(adapter: BinanceAdapter) -> None:
    """The derived label is stable and non-secret until the venue identity is read."""
    account_id = adapter.account_id
    assert account_id.startswith("binance-")
    assert "test-api-key" not in account_id


def test_sync_engine_constructs_without_connecting(binance_config: BinanceConfig) -> None:
    assert isinstance(SyncBinanceEngine(binance_config), SyncEngine)

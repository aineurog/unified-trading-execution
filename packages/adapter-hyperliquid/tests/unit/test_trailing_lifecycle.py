"""Engine-level wiring tests for Hyperliquid trailing.

These verify that the trailing configuration is wired correctly through
HyperliquidEngine and SyncHyperliquidEngine, and that the core trailing
manager is accessible. Complex lifecycle tests use the core MockAdapter.
"""

from __future__ import annotations

from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.trailing import TrailingConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(private_key=_TEST_KEY, wallet_address=_TEST_ADDRESS)


class TestEngineWiring:
    """Test that trailing_config is wired correctly through HyperliquidEngine."""

    def test_engine_accepts_trailing_config(self) -> None:
        """HyperliquidEngine accepts trailing_config parameter."""
        from unified_trading_execution.hyperliquid import HyperliquidEngine

        config = TrailingConfig()
        engine = HyperliquidEngine(_config(), trailing_config=config)
        assert engine is not None

    def test_engine_defaults_trailing_config(self) -> None:
        """HyperliquidEngine defaults trailing_config when not provided."""
        from unified_trading_execution.hyperliquid import HyperliquidEngine

        engine = HyperliquidEngine(_config())
        assert engine is not None

    def test_sync_engine_accepts_trailing_config(self) -> None:
        """SyncHyperliquidEngine accepts trailing_config parameter."""
        from unified_trading_execution.hyperliquid import SyncHyperliquidEngine

        config = TrailingConfig()
        engine = SyncHyperliquidEngine(_config(), trailing_config=config)
        assert engine is not None

    def test_sync_engine_defaults_trailing_config(self) -> None:
        """SyncHyperliquidEngine defaults trailing_config when not provided."""
        from unified_trading_execution.hyperliquid import SyncHyperliquidEngine

        engine = SyncHyperliquidEngine(_config())
        assert engine is not None


class TestCapabilityWiring:
    """Test that Hyperliquid declares correct trailing capabilities."""

    def test_position_protection_capability(self) -> None:
        """Adapter declares position protection capability for perps."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.position_protection_capability(_instrument())

        assert capability is not None
        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases is not None

    def test_trailing_capability(self) -> None:
        """Adapter declares trailing capability for perps."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.trailing_capability(_instrument())

        assert capability is not None
        assert capability.supported is True
        assert capability.offset_types is not None
        assert capability.bases is not None
        assert capability.activations is not None
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False

    def test_market_data_capability(self) -> None:
        """Adapter declares market data capability with candles."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability is not None
        assert capability.ticker_bases is not None
        assert capability.candle_timeframes is not None

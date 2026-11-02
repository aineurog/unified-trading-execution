"""Test IBKR capability declarations for trailing and position protection."""


from unified_trading_execution.adapter import TrailingBasis
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


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


class TestPositionProtectionCapability:
    """Test position-protection capability reporting."""

    def test_unspecified_sec_type_defaults_to_supported(self):
        """When sec_type is not specified, default to supported."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()  # No sec_type attribute
        capability = adapter.position_protection_capability(instrument)

        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases == frozenset()
        assert capability.min_reprice_interval_seconds == 2.0
        assert capability.reprice_gap is True


class TestTrailingCapability:
    """Test trailing capability reporting."""

    def test_declares_native_trailing_unsupported(self):
        """Native TRAIL is out of scope, so supported=False."""
        adapter = IBKRAdapter(_config())
        capability = adapter.trailing_capability(_instrument())

        assert capability.supported is False
        assert capability.offset_types == frozenset()
        assert capability.bases == frozenset()
        assert capability.activations == frozenset()
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is False
        assert capability.min_reprice_interval_seconds == 2.0


class TestMarketDataCapability:
    """Test market data capability reporting."""

    def test_declares_ticker_bases_and_pacing(self):
        """Ticker provides LAST, BID, ASK with 2s minimum interval."""
        adapter = IBKRAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.ticker_bases == frozenset(
            {TrailingBasis.LAST, TrailingBasis.BID, TrailingBasis.ASK}
        )
        assert capability.min_ticker_interval_seconds == 2.0
        assert capability.streaming_ticker is True

    def test_candle_timeframes_empty_until_implementation(self):
        """Candles not yet implemented, so timeframe set is empty."""
        adapter = IBKRAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        assert capability.candle_timeframes == frozenset()


class TestCapabilityNoNetworkCalls:
    """Capability properties must not make network calls."""

    def test_capabilities_no_network(self):
        """Reading capabilities should not require IB connection."""
        adapter = IBKRAdapter(_config())
        # Adapter is not connected
        assert adapter._ib is None

        # All capability methods should work without network
        _ = adapter.position_protection_capability(_instrument())
        _ = adapter.trailing_capability(_instrument())
        _ = adapter.market_data_capability(_instrument())

        # IB instance still None (no connection attempt)
        assert adapter._ib is None

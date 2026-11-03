"""Test IBKR capability declarations for trailing and position protection."""

import asyncio

import pytest

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
        instrument = _instrument()
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

    def test_candle_timeframes_now_implemented(self):
        """Candles now implemented with 7 core timeframes."""
        from unified_trading_execution.adapter import CandleTimeframe

        adapter = IBKRAdapter(_config())
        capability = adapter.market_data_capability(_instrument())

        expected = frozenset(
            {
                CandleTimeframe.M1,
                CandleTimeframe.M5,
                CandleTimeframe.M15,
                CandleTimeframe.M30,
                CandleTimeframe.H1,
                CandleTimeframe.H4,
                CandleTimeframe.D1,
            }
        )
        assert capability.candle_timeframes == expected


class TestCapabilityNoNetworkCalls:
    """Capability properties must not make network calls."""

    def test_capabilities_no_network(self):
        """Reading capabilities should not require IB connection."""
        adapter = IBKRAdapter(_config())
        assert adapter._ib is None

        _ = adapter.position_protection_capability(_instrument())
        _ = adapter.trailing_capability(_instrument())
        _ = adapter.market_data_capability(_instrument())

        assert adapter._ib is None


class TestClearPositionTpsl:
    """Test clear_position_tpsl flag validation."""

    def test_at_least_one_flag_required(self):
        """Both flags False raises ValueError."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            with pytest.raises(ValueError, match="at least one"):
                await adapter.clear_position_tpsl(
                    instrument, "12345", take_profit=False, stop_loss=False
                )

        asyncio.run(test())

    def test_clear_tp_only_flag(self):
        """Clearing only take profit is accepted."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            try:
                await adapter.clear_position_tpsl(
                    instrument, "12345", take_profit=True, stop_loss=False
                )
            except Exception as exc:
                if "at least one" in str(exc):
                    raise

        asyncio.run(test())

    def test_clear_sl_only_flag(self):
        """Clearing only stop loss is accepted."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            try:
                await adapter.clear_position_tpsl(
                    instrument, "12345", take_profit=False, stop_loss=True
                )
            except Exception as exc:
                if "at least one" in str(exc):
                    raise

        asyncio.run(test())

    def test_clear_both_flags(self):
        """Clearing both sides is accepted."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument()

        async def test():
            try:
                await adapter.clear_position_tpsl(
                    instrument, "12345", take_profit=True, stop_loss=True
                )
            except Exception as exc:
                if "at least one" in str(exc):
                    raise

        asyncio.run(test())

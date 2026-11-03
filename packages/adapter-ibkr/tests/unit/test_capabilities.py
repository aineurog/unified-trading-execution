"""Test IBKR capability declarations for trailing and position protection."""

import asyncio
from unittest.mock import AsyncMock

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

    def test_supported_asset_class_is_supported(self):
        """Supported asset classes carry positions and accept protection."""
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

    def test_unsupported_asset_class_is_unsupported(self):
        """Bonds and funds cannot be addressed as IBKR contracts here."""
        adapter = IBKRAdapter(_config())
        instrument = _instrument(asset_class=AssetClass.BOND)

        capability = adapter.position_protection_capability(instrument)

        assert capability.supported is False
        assert capability.can_set_stop is False
        assert capability.can_set_take_profit is False


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
        assert capability.streaming_ticker is False

    def test_candle_timeframes_seven_of_nine(self):
        """Candles cover seven core timeframes; H8 and H12 have no IBKR bar size."""
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
        assert CandleTimeframe.H8 not in capability.candle_timeframes
        assert CandleTimeframe.H12 not in capability.candle_timeframes

    def test_unsupported_asset_class_declares_no_data(self):
        """Bonds and funds cannot be addressed, so no ticker or candles."""
        adapter = IBKRAdapter(_config())
        capability = adapter.market_data_capability(_instrument(asset_class=AssetClass.BOND))

        assert capability.ticker_bases == frozenset()
        assert capability.candle_timeframes == frozenset()
        assert capability.min_ticker_interval_seconds == 0.0


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

    @pytest.mark.parametrize(
        ("take_profit", "stop_loss"),
        [(True, False), (False, True), (True, True)],
    )
    def test_named_sides_are_forwarded(self, take_profit: bool, stop_loss: bool):
        """Each accepted flag combination cancels exactly the named sides."""
        adapter = IBKRAdapter(_config())
        adapter._cancel_protection_by_side = AsyncMock()  # type: ignore[method-assign]
        instrument = _instrument()

        async def test():
            await adapter.clear_position_tpsl(
                instrument,
                "12345",
                take_profit=take_profit,
                stop_loss=stop_loss,
            )
            adapter._cancel_protection_by_side.assert_awaited_once_with(  # type: ignore[attr-defined]
                "12345", cancel_tp=take_profit, cancel_sl=stop_loss
            )

        asyncio.run(test())

"""Unit tests for Hyperliquid stop-safe tick derived from mark price."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x" + "00" * 20
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(private_key=_TEST_KEY, wallet_address=_TEST_ADDRESS)


def _instrument(**kwargs: object) -> Instrument:
    """Build an Instrument with given overrides."""
    defaults = {
        "symbol": "BTC",
        "quote_currency": "USDC",
        "asset_class": AssetClass.FUTURES,
        "currency": "USDC",
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


class TestStopSafeTick:
    async def test_derives_tick_from_mark_for_btc_range(self):
        """Stop-safe tick at 63,000 uses mark/1000."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        # Mock meta_and_asset_ctxs response
        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "63000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # mark/1000 = 63, but decimal tick is 0.1, so stop-safe tick should be 63
        assert spec.tick_size == Decimal("63")
        assert spec.min_stop_distance == Decimal("63")

    async def test_derives_tick_from_mark_at_100k_boundary(self):
        """Stop-safe tick at exact 100,000 boundary uses mark/1000."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "100000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # mark/1000 = 100
        assert spec.tick_size == Decimal("100")
        assert spec.min_stop_distance == Decimal("100")

    async def test_derives_tick_from_mark_at_1m_boundary(self):
        """Stop-safe tick at exact 1,000,000 boundary uses mark/1000."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "1000000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # mark/1000 = 1000
        assert spec.tick_size == Decimal("1000")
        assert spec.min_stop_distance == Decimal("1000")

    async def test_derives_tick_from_mark_for_eth_range(self):
        """Stop-safe tick at 3,000 uses mark/1000."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "ETH", "szDecimals": 2, "maxLeverage": 20}]
        ctxs = [{"markPx": "3000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument(symbol="ETH"))

        # mark/1000 = 3, decimal tick is 0.01, so stop-safe tick should be 3
        assert spec.tick_size == Decimal("3")
        assert spec.min_stop_distance == Decimal("3")

    async def test_falls_back_to_decimal_tick_for_low_price(self):
        """Low-priced coins use decimal tick when mark/1000 is smaller."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "DOGE", "szDecimals": 3, "maxLeverage": 20}]
        ctxs = [{"markPx": "0.1"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument(symbol="DOGE"))

        # mark/1000 = 0.0001, decimal tick is 0.001, so stop-safe tick should be 0.001
        assert spec.tick_size == Decimal("0.001")
        assert spec.min_stop_distance == Decimal("0.001")

    async def test_falls_back_to_decimal_tick_when_mark_missing(self):
        """Missing mark price falls back to decimal tick."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{}]  # No markPx
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # Should fall back to decimal tick (10^-(6-1) = 0.00001)
        assert spec.tick_size == Decimal("0.00001")
        assert spec.min_stop_distance == Decimal("0.00001")

    async def test_max_qty_uses_decimal_tick_not_stop_safe(self):
        """max_qty preserves order-tier semantics by using decimal tick."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "63000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # Stop-safe tick is 63, but max_qty should use decimal tick (0.00001)
        # Tier for 20x leverage is 50M, so max_qty = 50M / 0.00001 = 5e12
        assert spec.max_qty == Decimal("5000000000000")

    async def test_price_precision_from_stop_safe_tick(self):
        """price_precision is derived from stop-safe tick exponent."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "63000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        spec = await adapter.fetch_instrument_spec(_instrument())

        # Stop-safe tick is 63 (0 decimal places)
        assert spec.price_precision == 0

    async def test_spec_caches_stop_safe_tick(self):
        """Cached spec returns the same stop-safe tick without re-fetching."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        meta = [{"name": "BTC", "szDecimals": 1, "maxLeverage": 20}]
        ctxs = [{"markPx": "63000"}]
        adapter._run_exchange = AsyncMock(return_value=[{"universe": meta}, ctxs])

        instrument = _instrument()
        spec1 = await adapter.fetch_instrument_spec(instrument)
        spec2 = await adapter.fetch_instrument_spec(instrument)

        # Should only call _run_exchange once due to caching
        assert adapter._run_exchange.call_count == 1
        assert spec1.tick_size == spec2.tick_size

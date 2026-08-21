"""Unit tests for ``MT5Adapter.fetch_instrument_spec()``.

Tests run against the mocked ``MetaTrader5`` module — no real terminal IPC.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _instrument(
    symbol: str = "EUR", quote: str = "USD", platform_symbol: str | None = "EURUSD.m"
) -> Instrument:
    return Instrument(
        symbol=symbol,
        quote_currency=quote,
        asset_class=AssetClass.MARGIN_FX,
        platform_symbol=platform_symbol,
    )


def _spec_info(**overrides: object) -> MagicMock:
    """A ``symbol_info()`` result with realistic trading-rule fields."""
    base = {
        "digits": 5,
        "trade_mode": 4,  # SYMBOL_TRADE_MODE_FULL
        "trade_tick_size": 0.00001,
        "volume_min": 0.01,
        "volume_max": 100.0,
        "volume_step": 0.01,
        "path": "Forex\\EURUSD",
    }
    base.update(overrides)
    return MagicMock(**base)


class TestFetchInstrumentSpec:
    """fetch_instrument_spec — fetch, map, cache, error paths."""

    async def test_fetches_and_maps_spec(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """Symbol info fields are translated into a full InstrumentSpec."""
        mock_mt5_module.symbol_info.return_value = _spec_info()

        spec = await adapter.fetch_instrument_spec(_instrument())

        assert spec.tick_size == Decimal("0.00001")
        assert spec.lot_size == Decimal("0.01")
        assert spec.min_qty == Decimal("0.01")
        assert spec.max_qty == Decimal("100")
        assert spec.min_notional == Decimal("0")
        assert spec.price_precision == 5
        assert spec.qty_precision == 2
        assert spec.max_leverage is None

    async def test_selects_symbol_before_fetch(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """The symbol is selected in Market Watch before symbol_info() runs."""
        mock_mt5_module.symbol_info.return_value = _spec_info()

        await adapter.fetch_instrument_spec(_instrument())

        mock_mt5_module.symbol_select.assert_called_once_with("EURUSD.m", True)
        mock_mt5_module.symbol_info.assert_called_once_with("EURUSD.m")

    async def test_whole_number_volume_step_zero_qty_precision(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A whole-number volume step (stocks/indices) maps to 0 decimals.

        ``Decimal(str(1.0))`` is ``Decimal("1.0")`` whose stored exponent is
        -1; without normalization that would wrongly report qty_precision 1.
        """
        mock_mt5_module.symbol_info.return_value = _spec_info(
            volume_min=1.0,
            volume_max=1000.0,
            volume_step=1.0,
        )

        spec = await adapter.fetch_instrument_spec(_instrument())

        assert spec.qty_precision == 0
        assert spec.min_qty == Decimal("1")
        assert spec.lot_size == Decimal("1")

    async def test_caches_within_ttl(self, mock_mt5_module: MagicMock, adapter: MT5Adapter) -> None:
        """A fresh spec is returned from cache — symbol_info called once."""
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = _instrument()

        first = await adapter.fetch_instrument_spec(inst)
        second = await adapter.fetch_instrument_spec(inst)

        assert first is second
        mock_mt5_module.symbol_info.assert_called_once_with("EURUSD.m")

    async def test_ttl_expiry_refetches(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """An expired cache entry is re-fetched."""
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = _instrument()
        await adapter.fetch_instrument_spec(inst)

        spec, _ = adapter._spec_cache[inst]
        adapter._spec_cache[inst] = (spec, datetime.now(UTC) - timedelta(seconds=90000.0))

        await adapter.fetch_instrument_spec(inst)
        assert mock_mt5_module.symbol_info.call_count == 2

    async def test_ttl_none_caches_indefinitely(
        self, mock_mt5_module: MagicMock, event_bus: EventBus
    ) -> None:
        """``instrument_spec_cache_ttl=None`` disables TTL expiry."""
        config = MT5Config(
            login=12345678,
            password="test-password",
            server="TestBroker-Demo",
            instrument_spec_cache_ttl=None,
        )
        adapter = MT5Adapter(config, event_bus=event_bus)
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = _instrument()

        first = await adapter.fetch_instrument_spec(inst)
        second = await adapter.fetch_instrument_spec(inst)

        assert first is second
        mock_mt5_module.symbol_info.assert_called_once()

    async def test_none_symbol_info_raises_invalid_symbol(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """``symbol_info()`` returning None is an invalid symbol, not no-data."""
        mock_mt5_module.symbol_info.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "Unknown symbol")
        inst = _instrument()

        with pytest.raises(InvalidSymbolError, match="Unknown symbol"):
            await adapter.fetch_instrument_spec(inst)
        assert inst not in adapter._spec_cache

    async def test_disabled_trade_mode_raises_invalid_symbol(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A symbol whose trade mode is disabled is reported not tradable."""
        mock_mt5_module.symbol_info.return_value = _spec_info(trade_mode=0)

        with pytest.raises(InvalidSymbolError, match="not tradable"):
            await adapter.fetch_instrument_spec(_instrument())

    async def test_platform_symbol_used_verbatim(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """The instrument's ``platform_symbol`` is the broker symbol verbatim."""
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = _instrument(symbol="GBP", platform_symbol="GBPUSDpro")

        await adapter.fetch_instrument_spec(inst)

        mock_mt5_module.symbol_info.assert_called_once_with("GBPUSDpro")

    async def test_stock_with_platform_symbol_resolves_broker_symbol(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A non-pair instrument (stock) with a platform_symbol resolves via
        it — ``str(instrument)`` raises for STOCK and must not be fatal."""
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, platform_symbol="AAPL.US")

        await adapter.fetch_instrument_spec(inst)

        mock_mt5_module.symbol_info.assert_called_once_with("AAPL.US")

    async def test_missing_platform_symbol_raises_value_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """No platform_symbol means no usable MT5 symbol."""
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK)

        with pytest.raises(ValueError):
            await adapter.fetch_instrument_spec(inst)

    async def test_invalidate_spec_cache_forces_refetch(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """Invalidation pops the cache so the next fetch re-queries."""
        mock_mt5_module.symbol_info.return_value = _spec_info()
        inst = _instrument()
        await adapter.fetch_instrument_spec(inst)

        adapter._invalidate_spec_cache(inst)
        await adapter.fetch_instrument_spec(inst)

        assert mock_mt5_module.symbol_info.call_count == 2

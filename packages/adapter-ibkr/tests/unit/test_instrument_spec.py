"""Unit tests for IBKRAdapter.fetch_instrument_spec — mock-only."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from ib_async import ContractDetails

from unified_trading_execution.errors import InvalidSymbolError, PlatformConnectionError
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _details(**overrides: object) -> ContractDetails:
    base: dict[str, object] = {
        "minTick": 0.01,
        "sizeIncrement": 1.0,
        "minSize": 1.0,
    }
    base.update(overrides)
    return ContractDetails(**base)  # type: ignore[arg-type]


class TestFetchInstrumentSpec:
    async def test_success_and_caching(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = [
            _details(minTick=0.01, sizeIncrement=0.5, minSize=1)
        ]  # type: ignore[attr-defined]

        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        spec1 = await adapter.fetch_instrument_spec(inst)

        assert spec1.tick_size == Decimal("0.01")
        assert spec1.lot_size == Decimal("0.5")
        assert spec1.min_qty == Decimal("1")
        assert spec1.price_precision == 2
        assert spec1.qty_precision == 1

        # second call — cache hit, no new reqContractDetailsAsync
        mock_ib.reqContractDetailsAsync.reset_mock()  # type: ignore[attr-defined]
        spec2 = await adapter.fetch_instrument_spec(inst)
        assert spec2 is spec1
        mock_ib.reqContractDetailsAsync.assert_not_called()  # type: ignore[attr-defined]

    async def test_ttl_expiry_refetches(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        short = IBKRConfig(host="127.0.0.1", port=4002, client_id=1, instrument_spec_cache_ttl=0.01)
        # reuse mock IB from fixture
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = [_details(minTick=0.05)]  # type: ignore[attr-defined]
        await adapter.connect()
        # swap config ttl for this test via direct assignment (frozen dataclass → replace)
        object.__setattr__(adapter, "_config", short)

        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        await adapter.fetch_instrument_spec(inst)
        # artificially age the cache entry
        spec, fetched_at = adapter._spec_cache[inst]
        adapter._spec_cache[inst] = (spec, fetched_at - timedelta(seconds=10))

        mock_ib.reqContractDetailsAsync.reset_mock()  # type: ignore[attr-defined]
        mock_ib.reqContractDetailsAsync.return_value = [_details(minTick=0.05)]  # type: ignore[attr-defined]
        await adapter.fetch_instrument_spec(inst)
        mock_ib.reqContractDetailsAsync.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_ttl_none_caches_forever(self, mock_ib_async_module: MagicMock) -> None:
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = [_details(minTick=0.01)]  # type: ignore[attr-defined]
        cfg = IBKRConfig(host="127.0.0.1", port=4002, client_id=1, instrument_spec_cache_ttl=None)
        from unified_trading_execution.events import EventBus

        adapter = IBKRAdapter(cfg, event_bus=EventBus())
        await adapter.connect()
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        await adapter.fetch_instrument_spec(inst)
        # age entry far past
        spec, fetched_at = adapter._spec_cache[inst]
        adapter._spec_cache[inst] = (spec, fetched_at - timedelta(days=30))
        mock_ib.reqContractDetailsAsync.reset_mock()  # type: ignore[attr-defined]
        spec2 = await adapter.fetch_instrument_spec(inst)
        assert spec2 is spec
        mock_ib.reqContractDetailsAsync.assert_not_called()  # type: ignore[attr-defined]
        await adapter.disconnect()

    async def test_invalid_symbol_empty_list(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = []  # type: ignore[attr-defined]
        inst = Instrument(symbol="FAKE", asset_class=AssetClass.STOCK, currency="USD")
        with pytest.raises(InvalidSymbolError, match="no contract details"):
            await adapter.fetch_instrument_spec(inst)

    async def test_connection_error_wrapped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.side_effect = TimeoutError("timeout")  # type: ignore[attr-defined]
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        with pytest.raises(PlatformConnectionError, match="failed to fetch"):
            await adapter.fetch_instrument_spec(inst)
        # reset for other tests
        mock_ib.reqContractDetailsAsync.side_effect = None  # type: ignore[attr-defined]

    async def test_not_connected_raises(self, adapter: IBKRAdapter) -> None:
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        with pytest.raises(PlatformConnectionError, match="not connected"):
            await adapter.fetch_instrument_spec(inst)

    async def test_different_instruments_cached_separately(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module

        def _side_effect(c: object) -> list[ContractDetails]:
            # crude: return different tick per symbol
            from ib_async import Contract

            assert isinstance(c, Contract)
            if c.symbol == "AAPL":
                return [_details(minTick=0.01)]
            return [_details(minTick=0.5)]

        mock_ib.reqContractDetailsAsync.side_effect = _side_effect  # type: ignore[attr-defined]

        aapl = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        msft = Instrument(symbol="MSFT", asset_class=AssetClass.STOCK, currency="USD")
        spec_a = await adapter.fetch_instrument_spec(aapl)
        spec_b = await adapter.fetch_instrument_spec(msft)
        assert spec_a.tick_size != spec_b.tick_size
        assert len(adapter._spec_cache) == 2

    async def test_zero_fields_fallback(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = [
            _details(minTick=0, sizeIncrement=0, minSize=0)
        ]  # type: ignore[attr-defined]
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        # Ensure fresh fetch (clear cache)
        adapter._spec_cache.pop(inst, None)
        spec = await adapter.fetch_instrument_spec(inst)
        assert spec.tick_size == Decimal("0.01")
        assert spec.lot_size == Decimal("1")
        assert spec.min_qty == Decimal("1")

    async def test_invalidate_clears_cache(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync.return_value = [_details(minTick=0.01)]  # type: ignore[attr-defined]
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
        await adapter.fetch_instrument_spec(inst)
        assert inst in adapter._spec_cache
        adapter._invalidate_spec_cache(inst)
        assert inst not in adapter._spec_cache
        # next fetch re-queries
        mock_ib.reqContractDetailsAsync.reset_mock()  # type: ignore[attr-defined]
        await adapter.fetch_instrument_spec(inst)
        mock_ib.reqContractDetailsAsync.assert_awaited_once()  # type: ignore[attr-defined]

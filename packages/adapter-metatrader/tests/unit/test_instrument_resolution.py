"""Unit tests for MT5 instrument resolution (asset class + identity derivation).

Tests run against the mocked ``MetaTrader5`` module — no real terminal IPC.

Tests cases:
    - _asset_class_from_path: layered classifier (metal base → path thesaurus → calc_mode)
    - _asset_class_from_path: config escape hatch (asset_class_path_map)
    - _split_symbol_name: broker suffix stripping + quote-currency splitting
    - _build_instrument_from_symbol_info: decomposable vs non-decomposable
    - resolve_instrument: public async discovery + caching
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _symbol_info(
    name: str,
    *,
    base: str,
    profit: str,
    path: str,
    calc_mode: int,
) -> MagicMock:
    """A ``symbol_info()``-shaped mock with explicit identity fields."""
    info = MagicMock()
    info.name = name
    info.currency_base = base
    info.currency_profit = profit
    info.path = path
    info.trade_calc_mode = calc_mode
    return info


class TestAssetClassClassifier:
    """Layered asset-class derivation from symbol_info() metadata."""

    def test_metal_base_currency_wins_over_path(self, adapter: MT5Adapter) -> None:
        """A metal under a 'Commodities' folder is MARGIN_FX (not CFD)."""
        assert (
            adapter._asset_class_from_path("Commodities\\XAGUSD", currency_base="XAG", calc_mode=0)
            == AssetClass.MARGIN_FX
        )

    def test_path_thesaurus_scans_all_segments(self, adapter: MT5Adapter) -> None:
        """An account-group root ('PRO') cannot hide the meaningful segment."""
        assert adapter._asset_class_from_path("PRO\\Noble\\GOLD.pro") == AssetClass.MARGIN_FX
        assert adapter._asset_class_from_path("PRO\\Indices\\Major", calc_mode=4) == AssetClass.CFD
        assert (
            adapter._asset_class_from_path("Equities_CFD\\US\\AAPL_CFD.US", calc_mode=2)
            == AssetClass.STOCK
        )

    def test_calc_mode_fallback(self, adapter: MT5Adapter) -> None:
        """A path with no thesaurus segment falls back to trade_calc_mode."""
        assert adapter._asset_class_from_path("Misc\\SOLUSD", calc_mode=2) == AssetClass.CFD
        assert adapter._asset_class_from_path("Misc\\X", calc_mode=33) == AssetClass.FUTURES

    def test_unrecognized_raises(self, adapter: MT5Adapter) -> None:
        """No layer resolving → ValueError, never a silent default."""
        with pytest.raises(ValueError):
            adapter._asset_class_from_path("Strange\\EURUSD")

    def test_config_escape_hatch_overrides_thesaurus(self) -> None:
        """MT5Config.asset_class_path_map extends/overrides the built-in table."""
        config = MT5Config(
            login=12345678,
            password="x",
            server="s",
            asset_class_path_map={"PreciousMetals": AssetClass.MARGIN_FX},
        )
        adapter = MT5Adapter(config)
        assert adapter._asset_class_from_path("PreciousMetals\\XAU") == AssetClass.MARGIN_FX


class TestSplitSymbolName:
    """Broker symbol name → (symbol, quote_currency|None)."""

    def test_strips_broker_suffixes(self, adapter: MT5Adapter) -> None:
        assert adapter._split_symbol_name("AAPL_CFD.US") == ("AAPL", None)
        assert adapter._split_symbol_name("GOLD.pro") == ("GOLD", None)
        assert adapter._split_symbol_name("US500.pro") == ("US500", None)

    def test_splits_quote_currency(self, adapter: MT5Adapter) -> None:
        assert adapter._split_symbol_name("SOLUSD") == ("SOL", "USD")
        assert adapter._split_symbol_name("BTCUSDT") == ("BTC", "USDT")

    def test_no_quote_suffix_returns_none(self, adapter: MT5Adapter) -> None:
        assert adapter._split_symbol_name("Apple") == ("APPLE", None)
        assert adapter._split_symbol_name("SUGAR") == ("SUGAR", None)


class TestBuildInstrumentFromSymbolInfo:
    """Reconstruction of the canonical Instrument from a symbol_info() row."""

    def test_decomposable_forex(self, adapter: MT5Adapter) -> None:
        info = _symbol_info("EURUSD", base="EUR", profit="USD", path="Forex\\EURUSD", calc_mode=0)
        inst = adapter._build_instrument_from_symbol_info("EURUSD.m", info)
        assert inst.symbol == "EUR"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.MARGIN_FX
        assert inst.platform_symbol == "EURUSD.m"

    def test_decomposable_metal(self, adapter: MT5Adapter) -> None:
        info = _symbol_info(
            "XAGUSD", base="XAG", profit="USD", path="Commodities\\XAGUSD", calc_mode=0
        )
        inst = adapter._build_instrument_from_symbol_info("XAGUSD", info)
        assert inst.symbol == "XAG"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.MARGIN_FX

    def test_non_decomposable_crypto(self, adapter: MT5Adapter) -> None:
        info = _symbol_info("SOLUSD", base="USD", profit="USD", path="Crypto\\SOLUSD", calc_mode=2)
        inst = adapter._build_instrument_from_symbol_info("SOLUSD", info)
        assert inst.symbol == "SOL"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.SPOT

    def test_non_decomposable_metal(self, adapter: MT5Adapter) -> None:
        # Oanda's GOLD.pro: base == profit == USD, metal lives in the name only.
        info = _symbol_info(
            "GOLD.pro", base="USD", profit="USD", path="PRO\\Noble\\GOLD.pro", calc_mode=4
        )
        inst = adapter._build_instrument_from_symbol_info("GOLD.pro", info)
        assert inst.symbol == "GOLD"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.MARGIN_FX

    def test_non_decomposable_stock(self, adapter: MT5Adapter) -> None:
        info = _symbol_info(
            "AAPL_CFD.US", base="USD", profit="USD", path="Equities_CFD\\US", calc_mode=2
        )
        inst = adapter._build_instrument_from_symbol_info("AAPL_CFD.US", info)
        assert inst.symbol == "AAPL"
        assert inst.quote_currency is None
        assert inst.currency == "USD"
        assert inst.asset_class == AssetClass.STOCK

    def test_non_decomposable_index(self, adapter: MT5Adapter) -> None:
        info = _symbol_info(
            "US500.pro", base="USD", profit="USD", path="PRO\\Indices\\Major", calc_mode=4
        )
        inst = adapter._build_instrument_from_symbol_info("US500.pro", info)
        assert inst.symbol == "US500"
        assert inst.quote_currency is None
        assert inst.currency == "USD"
        assert inst.asset_class == AssetClass.CFD


class TestResolveInstrument:
    """Public async resolve_instrument discovery."""

    async def test_returns_canonical_instrument(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        mock_mt5_module.symbol_info.return_value = _symbol_info(
            "SOLUSD", base="USD", profit="USD", path="Crypto\\SOLUSD", calc_mode=2
        )

        inst = await adapter.resolve_instrument("SOLUSD")

        assert inst.symbol == "SOL"
        assert inst.quote_currency == "USD"
        assert inst.asset_class == AssetClass.SPOT

    async def test_caches_result(self, mock_mt5_module: MagicMock, adapter: MT5Adapter) -> None:
        mock_mt5_module.symbol_info.return_value = _symbol_info(
            "EURUSD", base="EUR", profit="USD", path="Forex\\EURUSD", calc_mode=0
        )

        await adapter.resolve_instrument("EURUSD.m")
        cached = adapter._symbol_to_instrument["EURUSD.m"]

        assert cached.symbol == "EUR"
        assert cached.quote_currency == "USD"

    async def test_none_symbol_info_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        mock_mt5_module.symbol_info.return_value = None
        mock_mt5_module.last_error.return_value = (4302, "symbol not selected")

        with pytest.raises(PlatformError):
            await adapter.resolve_instrument("UNKNOWN")

"""Tests for Binance product and symbol translation."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.symbols import (
    from_binance_symbol,
    product_for_instrument,
    resolve_binance_symbol,
    to_binance_symbol,
)
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _spot_instrument(*, platform_symbol: str | None = None) -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol=platform_symbol,
    )


def _usd_m_instrument(
    *,
    expiry: date | None = None,
    platform_symbol: str | None = None,
) -> Instrument:
    return Instrument(
        symbol="ETH",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        expiry=expiry,
        multiplier=1,
        platform_symbol=platform_symbol,
    )


def _coin_m_instrument(
    *,
    expiry: date | None = None,
    platform_symbol: str | None = None,
) -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        expiry=expiry,
        multiplier=1,
        platform_symbol=platform_symbol,
    )


SPOT_ENTRY: dict[str, object] = {
    "symbol": "BTCUSDT",
    "status": "TRADING",
    "baseAsset": "BTC",
    "quoteAsset": "USDT",
}
USD_M_ENTRY: dict[str, object] = {
    "symbol": "ETHUSDT",
    "status": "TRADING",
    "baseAsset": "ETH",
    "quoteAsset": "USDT",
    "marginAsset": "USDT",
    "contractType": "PERPETUAL",
}
COIN_M_ENTRY: dict[str, object] = {
    "symbol": "BTCUSD_PERP",
    "status": "TRADING",
    "baseAsset": "BTC",
    "quoteAsset": "USD",
    "marginAsset": "BTC",
    "contractType": "PERPETUAL",
    "contractSize": 100,
}


class TestProductResolution:
    @pytest.mark.parametrize(
        ("instrument", "expected"),
        [
            (_spot_instrument(), BinanceProduct.SPOT),
            (_usd_m_instrument(), BinanceProduct.USD_M),
            (_coin_m_instrument(), BinanceProduct.COIN_M),
        ],
        ids=("spot", "usd-m", "coin-m"),
    )
    def test_resolves_product_from_explicit_identity(
        self,
        instrument: Instrument,
        expected: BinanceProduct,
    ) -> None:
        assert product_for_instrument(instrument) == expected

    def test_rejects_unsupported_asset_class(self) -> None:
        instrument = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK)
        with pytest.raises(InvalidSymbolError, match="not supported"):
            product_for_instrument(instrument)

    def test_rejects_ambiguous_futures_settlement(self) -> None:
        instrument = Instrument(
            symbol="ETH",
            quote_currency="USDT",
            asset_class=AssetClass.FUTURES,
            currency="BTC",
            multiplier=1,
        )
        with pytest.raises(InvalidSymbolError, match="Cannot resolve"):
            product_for_instrument(instrument)


class TestSymbolConversion:
    @pytest.mark.parametrize(
        ("instrument", "product", "expected"),
        [
            (_spot_instrument(), BinanceProduct.SPOT, "BTCUSDT"),
            (_usd_m_instrument(), BinanceProduct.USD_M, "ETHUSDT"),
            (_coin_m_instrument(), BinanceProduct.COIN_M, "BTCUSD_PERP"),
        ],
        ids=("spot", "usd-m", "coin-m"),
    )
    def test_conventional_symbols(
        self,
        instrument: Instrument,
        product: BinanceProduct,
        expected: str,
    ) -> None:
        assert to_binance_symbol(instrument, product) == expected

    def test_uses_platform_symbol_verbatim(self) -> None:
        instrument = _spot_instrument(platform_symbol="BTCUSDT_SPECIAL")
        assert to_binance_symbol(instrument) == "BTCUSDT_SPECIAL"

    def test_dated_coin_m_symbol(self) -> None:
        instrument = _coin_m_instrument(expiry=date(2026, 12, 25))
        assert to_binance_symbol(instrument) == "BTCUSD_261225"

    def test_dated_usd_m_symbol(self) -> None:
        instrument = _usd_m_instrument(expiry=date(2026, 12, 25))
        assert to_binance_symbol(instrument) == "ETHUSDT_261225"

    def test_rejects_conflicting_product_argument(self) -> None:
        with pytest.raises(InvalidSymbolError, match="resolves to"):
            to_binance_symbol(_spot_instrument(), BinanceProduct.USD_M)


class TestExchangeInfoResolution:
    @pytest.mark.parametrize(
        ("instrument", "product", "entry", "symbol"),
        [
            (_spot_instrument(), BinanceProduct.SPOT, SPOT_ENTRY, "BTCUSDT"),
            (_usd_m_instrument(), BinanceProduct.USD_M, USD_M_ENTRY, "ETHUSDT"),
            (_coin_m_instrument(), BinanceProduct.COIN_M, COIN_M_ENTRY, "BTCUSD_PERP"),
        ],
        ids=("spot", "usd-m", "coin-m"),
    )
    def test_resolves_product_specific_metadata(
        self,
        instrument: Instrument,
        product: BinanceProduct,
        entry: dict[str, object],
        symbol: str,
    ) -> None:
        assert resolve_binance_symbol(instrument, product, [entry]) == (symbol, entry)

    def test_rejects_same_pair_ambiguity_without_platform_symbol(self) -> None:
        second_entry = {**SPOT_ENTRY, "symbol": "XBTCUSDT"}
        with pytest.raises(InvalidSymbolError, match="ambiguous"):
            resolve_binance_symbol(
                _spot_instrument(),
                BinanceProduct.SPOT,
                [SPOT_ENTRY, second_entry],
            )

    def test_platform_symbol_resolves_same_pair_ambiguity(self) -> None:
        second_entry = {**SPOT_ENTRY, "symbol": "XBTCUSDT"}
        instrument = _spot_instrument(platform_symbol="XBTCUSDT")
        symbol, entry = resolve_binance_symbol(
            instrument,
            BinanceProduct.SPOT,
            [SPOT_ENTRY, second_entry],
        )
        assert symbol == "XBTCUSDT"
        assert entry == second_entry

    def test_rejects_platform_symbol_with_conflicting_metadata(self) -> None:
        instrument = _spot_instrument(platform_symbol="ETHUSDT")
        with pytest.raises(InvalidSymbolError, match="No Binance"):
            resolve_binance_symbol(instrument, BinanceProduct.SPOT, [SPOT_ENTRY])

    def test_rejects_wrong_product(self) -> None:
        with pytest.raises(InvalidSymbolError, match="does not belong"):
            resolve_binance_symbol(
                _spot_instrument(),
                BinanceProduct.USD_M,
                [SPOT_ENTRY],
            )


class TestFromBinanceSymbol:
    @pytest.mark.parametrize(
        ("entry", "product", "symbol", "quote", "currency", "asset_class", "multiplier"),
        [
            (SPOT_ENTRY, BinanceProduct.SPOT, "BTC", "USDT", None, AssetClass.SPOT, None),
            (
                USD_M_ENTRY,
                BinanceProduct.USD_M,
                "ETH",
                "USDT",
                "USDT",
                AssetClass.FUTURES,
                1,
            ),
            (
                COIN_M_ENTRY,
                BinanceProduct.COIN_M,
                "BTC",
                "USD",
                "BTC",
                AssetClass.FUTURES,
                100,
            ),
        ],
        ids=("spot", "usd-m", "coin-m"),
    )
    def test_maps_metadata_to_instrument(
        self,
        entry: dict[str, object],
        product: BinanceProduct,
        symbol: str,
        quote: str,
        currency: str | None,
        asset_class: AssetClass,
        multiplier: int | None,
    ) -> None:
        instrument = from_binance_symbol(entry, product)
        assert instrument.symbol == symbol
        assert instrument.quote_currency == quote
        assert instrument.currency == currency
        assert instrument.asset_class == asset_class
        assert instrument.multiplier == multiplier
        assert instrument.platform_symbol == entry["symbol"]

    @pytest.mark.parametrize(
        "entry",
        [
            {k: v for k, v in COIN_M_ENTRY.items() if k != "contractSize"},
            {**COIN_M_ENTRY, "contractSize": 0},
            {**COIN_M_ENTRY, "contractSize": "100.5"},
        ],
        ids=("missing", "non-positive", "non-integral"),
    )
    def test_rejects_invalid_coin_m_contract_size(self, entry: dict[str, object]) -> None:
        with pytest.raises(InvalidSymbolError, match="contractSize"):
            from_binance_symbol(entry, BinanceProduct.COIN_M)

    def test_maps_delivery_contract_with_space_separated_status(self) -> None:
        delivery = {
            **COIN_M_ENTRY,
            "contractType": "CURRENT_QUARTER DELIVERING",
            "deliveryDate": int(datetime(2026, 12, 25, 8, tzinfo=UTC).timestamp() * 1000),
        }
        instrument = from_binance_symbol(delivery, BinanceProduct.COIN_M)
        assert instrument.expiry == date(2026, 12, 25)

    def test_rejects_unsupported_contract_type(self) -> None:
        dated = {**USD_M_ENTRY, "contractType": "UNKNOWN"}
        with pytest.raises(InvalidSymbolError, match="contract type"):
            from_binance_symbol(dated, BinanceProduct.USD_M)

    def test_maps_delivery_contract_expiry_from_exchange_metadata(self) -> None:
        delivery = {
            **USD_M_ENTRY,
            "symbol": "ETHUSDT_261225",
            "contractType": "CURRENT_QUARTER",
            "deliveryDate": int(datetime(2026, 12, 25, 8, tzinfo=UTC).timestamp() * 1000),
        }
        instrument = from_binance_symbol(delivery, BinanceProduct.USD_M)
        assert instrument.expiry == date(2026, 12, 25)
        assert instrument.platform_symbol == "ETHUSDT_261225"

    def test_maps_current_month_contract_expiry(self) -> None:
        delivery = {
            **USD_M_ENTRY,
            "symbol": "ETHUSDT_261225",
            "contractType": "CURRENT_MONTH",
            "deliveryDate": int(datetime(2026, 12, 25, 8, tzinfo=UTC).timestamp() * 1000),
        }
        instrument = from_binance_symbol(delivery, BinanceProduct.USD_M)
        assert instrument.expiry == date(2026, 12, 25)

    def test_rejects_delivery_contract_with_malformed_expiry(self) -> None:
        delivery = {
            **USD_M_ENTRY,
            "contractType": "CURRENT_QUARTER",
            "deliveryDate": "0",
        }
        with pytest.raises(InvalidSymbolError, match="deliveryDate"):
            from_binance_symbol(delivery, BinanceProduct.USD_M)

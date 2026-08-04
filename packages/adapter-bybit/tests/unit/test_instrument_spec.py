"""Unit tests for BybitAdapter.fetch_instrument_spec and supported_order_types."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pybit.exceptions import FailedRequestError, InvalidRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import AssetClass, OrderType
from unified_trading_execution.types.instrument import Instrument

_SPOT_RESPONSE = {
    "retCode": 0,
    "retMsg": "OK",
    "result": {
        "category": "spot",
        "list": [
            {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "status": "Trading",
                "lotSizeFilter": {
                    "basePrecision": "0.000001",
                    "quotePrecision": "0.01",
                    "minOrderQty": "0.0001",
                    "maxOrderQty": "956.96588822",
                    "minNotionalValue": "5",
                },
                "priceFilter": {
                    "tickSize": "0.1",
                    "minPrice": "0.1",
                    "maxPrice": "999999.99999999",
                },
            }
        ],
    },
}

_LINEAR_RESPONSE = {
    "retCode": 0,
    "retMsg": "OK",
    "result": {
        "category": "linear",
        "list": [
            {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "contractType": "LinearPerpetual",
                "status": "Trading",
                "lotSizeFilter": {
                    "qtyStep": "0.001",
                    "minOrderQty": "0.001",
                    "maxOrderQty": "1000",
                    "minNotionalValue": "5",
                    "maxMktOrderQty": "500",
                },
                "priceFilter": {
                    "tickSize": "0.10",
                    "minPrice": "0.10",
                    "maxPrice": "1999999.80",
                },
            }
        ],
    },
}

_INVERSE_RESPONSE = {
    "retCode": 0,
    "retMsg": "OK",
    "result": {
        "category": "inverse",
        "list": [
            {
                "symbol": "BTCUSD",
                "baseCoin": "BTC",
                "quoteCoin": "USD",
                "contractType": "InversePerpetual",
                "status": "Trading",
                "lotSizeFilter": {
                    "qtyStep": "1",
                    "minOrderQty": "1",
                    "maxOrderQty": "1000000",
                    "minNotionalValue": "10",
                    "maxMktOrderQty": "100000",
                },
                "priceFilter": {
                    "tickSize": "0.5",
                    "minPrice": "0.5",
                    "maxPrice": "999999.5",
                },
            }
        ],
    },
}


class TestFetchInstrumentSpec:
    async def test_spot_instrument(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (
            _SPOT_RESPONSE,
            None,
            {},
        )

        instrument = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        spec = await adapter.fetch_instrument_spec(instrument)

        assert spec.tick_size == Decimal("0.1")
        assert spec.lot_size == Decimal("0.000001")
        assert spec.min_qty == Decimal("0.0001")
        assert spec.max_qty == Decimal("956.96588822")
        assert spec.min_notional == Decimal("5")
        assert spec.price_precision == 1
        assert spec.qty_precision == 6

    async def test_linear_perpetual(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (
            _LINEAR_RESPONSE,
            None,
            {},
        )

        instrument = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency="USDT",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=1,
        )

        spec = await adapter.fetch_instrument_spec(instrument)

        assert spec.tick_size == Decimal("0.10")
        assert spec.lot_size == Decimal("0.001")
        assert spec.price_precision == 2
        assert spec.qty_precision == 3

    async def test_inverse_perpetual(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (
            _INVERSE_RESPONSE,
            None,
            {},
        )

        instrument = Instrument(
            symbol="BTC",
            quote_currency="USD",
            asset_class=AssetClass.FUTURES,
            exchange=None,
            currency="BTC",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=1,
        )

        spec = await adapter.fetch_instrument_spec(instrument)

        assert spec.tick_size == Decimal("0.5")
        assert spec.lot_size == Decimal("1")
        assert spec.min_qty == Decimal("1")
        assert spec.max_qty == Decimal("1000000")
        assert spec.min_notional == Decimal("10")
        assert spec.price_precision == 1
        assert spec.qty_precision == 0

    async def test_unsupported_asset_class(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        instrument = Instrument(
            symbol="AAPL",
            quote_currency=None,
            asset_class=AssetClass.STOCK,
            exchange=None,
            currency="USD",
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_instrument_spec(instrument)

        mock_pybit_http.get_instruments_info.assert_not_called()

    async def test_invalid_symbol_from_bybit(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.side_effect = InvalidRequestError(
            request="GET /v5/market/instruments-info",
            message="Invalid symbol",
            status_code=10029,
            time="12:00:00",
            resp_headers=None,
        )

        instrument = Instrument(
            symbol="INVALID",
            quote_currency="USD",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_instrument_spec(instrument)

    async def test_http_error(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.side_effect = FailedRequestError(
            request="GET /v5/market/instruments-info",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        instrument = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        from unified_trading_execution.errors import PlatformConnectionError

        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_instrument_spec(instrument)

    async def test_empty_list_response(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (
            {"retCode": 0, "result": {"list": []}},
            None,
            {},
        )

        instrument = Instrument(
            symbol="NONEXIST",
            quote_currency="USD",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        with pytest.raises(PlatformError):
            await adapter.fetch_instrument_spec(instrument)

    async def test_not_trading_status(
        self,
        adapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        response = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "PreLaunch",
                        "lotSizeFilter": {},
                        "priceFilter": {},
                    }
                ]
            },
        }
        mock_pybit_http.get_instruments_info.return_value = (response, None, {})

        instrument = Instrument(
            symbol="BTC",
            quote_currency="USDT",
            asset_class=AssetClass.SPOT,
            exchange=None,
            currency=None,
            expiry=None,
            strike=None,
            option_right=None,
            multiplier=None,
        )

        with pytest.raises(PlatformError):
            await adapter.fetch_instrument_spec(instrument)


def _spot_instrument(symbol: str = "BTC") -> Instrument:
    return Instrument(
        symbol=symbol,
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


class _SyncLoop:
    """A stand-in event loop that invokes ``call_soon_threadsafe`` inline."""

    def call_soon_threadsafe(self, callback: Any, *args: Any) -> None:
        callback(*args)


class TestInstrumentSpecCache:
    async def test_second_call_hits_cache(self, adapter, mock_pybit_http: MagicMock) -> None:
        mock_pybit_http.get_instruments_info.side_effect = [
            (_SPOT_RESPONSE, None, {}),
            FailedRequestError(
                request="GET /v5/market/instruments-info",
                message="should not be called",
                status_code=500,
                time="12:00:00",
                resp_headers=None,
            ),
        ]

        instrument = _spot_instrument()
        first = await adapter.fetch_instrument_spec(instrument)
        second = await adapter.fetch_instrument_spec(instrument)

        assert first is second
        assert mock_pybit_http.get_instruments_info.call_count == 1

    async def test_distinct_instruments_do_not_share_cache(
        self, adapter, mock_pybit_http: MagicMock
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (_SPOT_RESPONSE, None, {})

        a = await adapter.fetch_instrument_spec(_spot_instrument("BTC"))
        b = await adapter.fetch_instrument_spec(_spot_instrument("ETH"))

        assert a is not b
        assert mock_pybit_http.get_instruments_info.call_count == 2

    async def test_explicit_invalidation_refetches(
        self, adapter, mock_pybit_http: MagicMock
    ) -> None:
        mock_pybit_http.get_instruments_info.side_effect = [
            (_SPOT_RESPONSE, None, {}),
            (_SPOT_RESPONSE, None, {}),
        ]

        instrument = _spot_instrument()
        await adapter.fetch_instrument_spec(instrument)
        adapter._invalidate_instrument_spec(instrument)
        await adapter.fetch_instrument_spec(instrument)

        assert mock_pybit_http.get_instruments_info.call_count == 2

    async def test_invalidation_of_uncached_is_noop(self, adapter) -> None:
        adapter._invalidate_instrument_spec(_spot_instrument())
        assert _spot_instrument() not in adapter._instrument_specs

    def test_ttl_invalid_raises_at_construction(self) -> None:
        with pytest.raises(ValueError):
            BybitConfig(
                api_key="k",
                api_secret="s",
                testnet=True,
                instrument_spec_cache_ttl=0,
            )
        with pytest.raises(ValueError):
            BybitConfig(
                api_key="k",
                api_secret="s",
                testnet=True,
                instrument_spec_cache_ttl=-1.5,
            )

    def test_ttl_defaults_to_one_day(self, adapter) -> None:
        from unified_trading_execution.bybit.config import (
            DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS,
        )

        assert adapter._instrument_spec_cache_ttl == DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS
        assert DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS == 86400.0

    async def test_ttl_expired_entry_is_refetched(
        self, mock_pybit_http: MagicMock, event_bus: EventBus
    ) -> None:
        mock_pybit_http.get_instruments_info.side_effect = [
            (_SPOT_RESPONSE, None, {}),
            (_SPOT_RESPONSE, None, {}),
        ]
        adapter = BybitAdapter(
            BybitConfig(api_key="k", api_secret="s", testnet=True, instrument_spec_cache_ttl=10),
            event_bus=event_bus,
        )

        with patch("unified_trading_execution.bybit.adapter.time.monotonic") as clock:
            clock.return_value = 100.0
            await adapter.fetch_instrument_spec(_spot_instrument())

            clock.return_value = 100.0 + 10.0
            # Exactly at TTL: < ttl is False, so the entry is expired and refetched.
            spec = await adapter.fetch_instrument_spec(_spot_instrument())
            assert spec is not None

            clock.return_value = 100.0 + 10.0 + 0.0001
            await adapter.fetch_instrument_spec(_spot_instrument())

        assert mock_pybit_http.get_instruments_info.call_count == 2

    async def test_ws_rejected_order_invalidates_spec(self, adapter, event_bus: EventBus) -> None:
        instrument = _spot_instrument()
        adapter._instruments = {("spot", "BTCUSDT"): instrument}
        adapter._loop = cast(asyncio.AbstractEventLoop, _SyncLoop())
        adapter._instrument_specs[instrument] = (
            cast(Any, object()),
            1.0,
        )
        assert instrument in adapter._instrument_specs

        adapter._on_order_message(
            {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "category": "spot",
                        "orderId": "order-1",
                        "orderLinkId": "client-1",
                        "side": "Buy",
                        "orderType": "Limit",
                        "stopOrderType": "",
                        "orderStatus": "Rejected",
                        "price": "100000",
                        "qty": "0.5",
                        "timeInForce": "GTC",
                        "cumExecQty": "0",
                        "avgPrice": "",
                        "createdTime": "1700000000000",
                        "updatedTime": "1700000000000",
                    }
                ]
            }
        )

        assert instrument not in adapter._instrument_specs

    async def test_non_rejected_order_keeps_spec(self, adapter, event_bus: EventBus) -> None:
        instrument = _spot_instrument()
        adapter._instruments = {("spot", "BTCUSDT"): instrument}
        adapter._loop = cast(asyncio.AbstractEventLoop, _SyncLoop())
        adapter._instrument_specs[instrument] = (cast(Any, object()), 1.0)

        adapter._on_order_message(
            {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "category": "spot",
                        "orderId": "order-1",
                        "orderLinkId": "client-1",
                        "side": "Buy",
                        "orderType": "Limit",
                        "stopOrderType": "",
                        "orderStatus": "New",
                        "price": "100000",
                        "qty": "0.5",
                        "timeInForce": "GTC",
                        "cumExecQty": "0",
                        "avgPrice": "",
                        "createdTime": "1700000000000",
                        "updatedTime": "1700000000000",
                    }
                ]
            }
        )

        assert instrument in adapter._instrument_specs

    async def test_registry_status_transition_invalidates_spec(
        self, adapter, mock_pybit_http: MagicMock
    ) -> None:
        instrument = _spot_instrument()
        adapter._instruments[("spot", "BTCUSDT")] = instrument
        adapter._instrument_specs[instrument] = (cast(Any, object()), 1.0)

        halted = {
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "status": "Closed",
                        "lotSizeFilter": {},
                        "priceFilter": {},
                    }
                ],
            },
        }
        empty = {"retCode": 0, "result": {"list": []}}
        mock_pybit_http.get_instruments_info.side_effect = [
            (halted, None, {}),
            (empty, None, {}),
            (empty, None, {}),
        ]

        await adapter._refresh_instrument_registry()

        assert instrument not in adapter._instrument_specs

    async def test_registry_trading_keeps_spec(self, adapter, mock_pybit_http: MagicMock) -> None:
        instrument = _spot_instrument()
        adapter._instruments[("spot", "BTCUSDT")] = instrument
        adapter._instrument_specs[instrument] = (cast(Any, object()), 1.0)

        empty = {"retCode": 0, "result": {"list": []}}
        mock_pybit_http.get_instruments_info.side_effect = [
            (_SPOT_RESPONSE, None, {}),
            (empty, None, {}),
            (empty, None, {}),
        ]

        await adapter._refresh_instrument_registry()

        assert instrument in adapter._instrument_specs


class TestSupportedOrderTypes:
    def test_returns_core_set(self, adapter) -> None:
        types = adapter.supported_order_types()
        assert OrderType.MARKET in types
        assert OrderType.LIMIT in types
        assert OrderType.STOP in types
        assert OrderType.STOP_LIMIT in types
        assert len(types) == 4

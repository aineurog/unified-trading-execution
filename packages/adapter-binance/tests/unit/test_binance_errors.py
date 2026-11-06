"""Tests for Binance-native error translation."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from aiohttp import ClientConnectionError, ClientError
from binance.exceptions import (  # type: ignore[import-untyped]
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
)

from unified_trading_execution.binance import BinanceProduct
from unified_trading_execution.binance.errors import map_binance_error
from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InsufficientBalanceError,
    InvalidOrderError,
    InvalidSymbolError,
    MarketClosedError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
)


def _api_error(code: int, status: int, message: str) -> BinanceAPIException:
    return BinanceAPIException(
        SimpleNamespace(text=message),
        status,
        f'{{"code": {code}, "msg": "{message}"}}',
    )


class TestMapBinanceHttpErrors:
    @pytest.mark.parametrize("status", [418, 429])
    def test_rate_limit_status(self, status: int) -> None:
        error = map_binance_error(_api_error(-1003, status, "too many requests"))
        assert isinstance(error, RateLimitError)
        assert "too many requests" in str(error)

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_server_status_is_connection_error(self, status: int) -> None:
        error = map_binance_error(_api_error(-1000, status, "server unavailable"))
        assert isinstance(error, PlatformConnectionError)

    @pytest.mark.parametrize("status", [400, 401, 403])
    def test_unmapped_client_status_preserves_error_context(self, status: int) -> None:
        error = map_binance_error(_api_error(-9999, status, "request rejected"))
        assert isinstance(error, PlatformError)
        assert error.platform_error == {
            "exception": "BinanceAPIException",
            "message": "request rejected",
            "code": -9999,
            "http_status": status,
        }

    def test_authentication_status_overrides_transport_code(self) -> None:
        error = map_binance_error(_api_error(-1001, 401, "unauthorized"))
        assert isinstance(error, PlatformError)
        assert not isinstance(error, PlatformConnectionError)

    @pytest.mark.parametrize(
        ("message", "expected_type"),
        [
            ("Server is currently overloaded.", PlatformConnectionError),
            ("Request throttled by system-level protection.", RateLimitError),
        ],
    )
    def test_server_busy_code_uses_product_specific_message(
        self,
        message: str,
        expected_type: type[Exception],
    ) -> None:
        error = map_binance_error(_api_error(-1008, 503, message))
        assert isinstance(error, expected_type)


class TestMapBinanceCodes:
    @pytest.mark.parametrize(
        ("code", "expected_type"),
        [
            (-1001, PlatformConnectionError),
            (-1003, RateLimitError),
            (-1015, RateLimitError),
            (-1002, PlatformError),
            (-1013, InvalidOrderError),
            (-1102, InvalidOrderError),
            (-1111, InvalidOrderError),
            (-1121, InvalidSymbolError),
            (-2010, InvalidOrderError),
            (-1021, PlatformError),
            (-1022, PlatformError),
            (-1014, InvalidOrderError),
            (-1016, PlatformConnectionError),
            (-1034, RateLimitError),
            (-1122, InvalidSymbolError),
            (-2016, PlatformError),
            (-2018, InsufficientBalanceError),
            (-2020, InvalidOrderError),
            (-2024, InvalidOrderError),
            (-2025, InvalidOrderError),
            (-2027, InvalidOrderError),
            (-2028, InsufficientBalanceError),
            (-2039, InvalidOrderError),
            (-2043, InvalidOrderError),
            (-4115, InvalidOrderError),
            (-4116, DuplicateOrderIdError),
            (-4117, InvalidOrderError),
            (-4118, InvalidOrderError),
            (-4120, InvalidOrderError),
            (-4131, InvalidOrderError),
            (-4135, InvalidOrderError),
            (-4137, InvalidOrderError),
            (-4138, InvalidOrderError),
            (-4139, InvalidOrderError),
            (-4140, MarketClosedError),
            (-4141, MarketClosedError),
            (-4142, InvalidOrderError),
            (-4144, InvalidSymbolError),
            (-4164, InvalidOrderError),
            (-5021, InvalidOrderError),
            (-4000, InvalidOrderError),
            (-4001, InvalidOrderError),
            (-4004, InvalidOrderError),
            (-4014, InvalidOrderError),
            (-4015, InvalidOrderError),
            (-4022, InvalidOrderError),
            (-4023, InvalidOrderError),
            (-4028, InvalidOrderError),
            (-4031, InvalidOrderError),
            (-4032, InvalidOrderError),
            (-4045, InvalidOrderError),
            (-4050, InsufficientBalanceError),
            (-4051, InsufficientBalanceError),
            (-4060, InvalidOrderError),
            (-4061, InvalidOrderError),
            (-4062, InvalidOrderError),
            (-4105, InvalidOrderError),
            (-4106, InvalidOrderError),
            (-4107, InvalidOrderError),
            (-4110, InvalidOrderError),
            (-4112, InvalidOrderError),
            (-4113, InvalidOrderError),
            (-4114, InvalidOrderError),
            (-4111, InvalidOrderError),
            (-2013, OrderNotFoundError),
            (-2014, PlatformError),
            (-2015, PlatformError),
            (-2019, InsufficientBalanceError),
            (-2021, InvalidOrderError),
        ],
    )
    def test_known_code_maps_to_expected_core_error(
        self,
        code: int,
        expected_type: type[Exception],
    ) -> None:
        error = map_binance_error(_api_error(code, 400, "Binance rejection"))
        assert isinstance(error, expected_type)
        assert "Binance rejection" in str(error)

    def test_sdk_order_exception_is_mapped(self) -> None:
        error = map_binance_error(BinanceOrderException(-1013, "invalid quantity"))
        assert isinstance(error, InvalidOrderError)
        assert "invalid quantity" in str(error)

    @pytest.mark.parametrize(
        ("code", "message", "expected_type"),
        [
            (
                -2010,
                "Account has insufficient balance for requested action.",
                InsufficientBalanceError,
            ),
            (-2010, "Market is closed.", MarketClosedError),
            (-2011, "Unknown order sent.", OrderNotFoundError),
            (-2010, "Duplicate order sent.", DuplicateOrderIdError),
        ],
    )
    def test_matching_engine_rejection_message_refines_error_type(
        self,
        code: int,
        message: str,
        expected_type: type[Exception],
    ) -> None:
        error = map_binance_error(_api_error(code, 400, message))
        assert isinstance(error, expected_type)

    @pytest.mark.parametrize(
        ("product", "expected_type"),
        [
            (BinanceProduct.SPOT, PlatformError),
            (BinanceProduct.USD_M, InvalidOrderError),
            (BinanceProduct.COIN_M, InvalidOrderError),
            (None, PlatformError),
        ],
    )
    def test_product_specific_code_requires_product_context(
        self,
        product: BinanceProduct | None,
        expected_type: type[Exception],
    ) -> None:
        error = map_binance_error(
            _api_error(-2026, 400, "product-specific error"),
            product=product,
        )
        assert isinstance(error, expected_type)

    def test_unknown_code_has_context(self) -> None:
        error = map_binance_error(_api_error(-987654, 400, "unrecognized response"))
        assert isinstance(error, PlatformError)
        assert error.platform_error == {
            "exception": "BinanceAPIException",
            "message": "unrecognized response",
            "code": -987654,
            "http_status": 400,
        }

    def test_malformed_api_payload_does_not_use_default_code_as_a_mapping(self) -> None:
        error = map_binance_error(
            BinanceAPIException(
                SimpleNamespace(text="not-json"),
                400,
                "not-json",
            )
        )
        assert isinstance(error, PlatformError)
        assert isinstance(error.platform_error, dict)
        assert error.platform_error["code"] == 0
        assert "Invalid JSON" in str(error)


class TestMapBinanceTransportErrors:
    def test_timeout_maps_to_connection_error(self) -> None:
        error = map_binance_error(TimeoutError("secret request details"))
        assert isinstance(error, PlatformConnectionError)
        assert "secret request details" not in str(error)

    def test_aiohttp_connection_error_maps_to_connection_error(self) -> None:
        error = map_binance_error(ClientConnectionError("connection refused"))
        assert isinstance(error, PlatformConnectionError)

    def test_transport_error_does_not_expose_signed_url(self) -> None:
        error = map_binance_error(ClientError("https://example.test/api?signature=private"))
        assert isinstance(error, PlatformConnectionError)
        assert "private" not in str(error)

    def test_invalid_sdk_response_preserves_context(self) -> None:
        error = map_binance_error(BinanceRequestException("invalid JSON response"))
        assert isinstance(error, PlatformError)
        assert error.platform_error == {
            "exception": "BinanceRequestException",
            "message": "invalid JSON response",
        }

    def test_unrecognized_exception_is_not_silently_discarded(self) -> None:
        error = map_binance_error(RuntimeError("unexpected SDK failure"))
        assert isinstance(error, PlatformError)
        assert error.platform_error == {
            "exception": "RuntimeError",
        }

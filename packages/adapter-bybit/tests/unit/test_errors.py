from __future__ import annotations

import pytest

from unified_trading_execution.bybit.errors import map_bybit_error
from unified_trading_execution.errors import (
    InsufficientBalanceError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
)


class TestMapBybitHttpError:
    def test_http_429_maps_to_rate_limit(self) -> None:
        exc = map_bybit_error(http_status=429, ret_msg="too many requests")
        assert isinstance(exc, RateLimitError)
        assert "too many requests" in str(exc)

    def test_http_403_maps_to_platform_error(self) -> None:
        exc = map_bybit_error(http_status=403, ret_msg="forbidden")
        assert isinstance(exc, PlatformError)
        assert "forbidden" in str(exc)

    def test_http_500_maps_to_platform_connection(self) -> None:
        exc = map_bybit_error(http_status=500, ret_msg="internal error")
        assert isinstance(exc, PlatformConnectionError)
        assert "internal error" in str(exc)

    def test_http_502_maps_to_platform_connection(self) -> None:
        exc = map_bybit_error(http_status=502, ret_msg="bad gateway")
        assert isinstance(exc, PlatformConnectionError)

    def test_http_400_maps_to_platform_error(self) -> None:
        exc = map_bybit_error(http_status=400, ret_msg="bad request")
        assert isinstance(exc, PlatformError)
        assert "bad request" in str(exc)

    def test_http_401_maps_to_platform_error(self) -> None:
        exc = map_bybit_error(http_status=401, ret_msg="unauthorized")
        assert isinstance(exc, PlatformError)
        assert "unauthorized" in str(exc)


class TestMapBybitRetCode:
    @pytest.mark.parametrize(
        ("ret_code", "expected_type"),
        [
            # Rate limiting
            (10006, RateLimitError),
            (10018, RateLimitError),
            (10429, RateLimitError),
            (20003, RateLimitError),
            (30035, RateLimitError),
            (170005, RateLimitError),
            (170222, RateLimitError),
            # Invalid symbol / coin
            (10029, InvalidSymbolError),
            (110050, InvalidSymbolError),
            (170121, InvalidSymbolError),
            (170221, InvalidSymbolError),
            # Insufficient balance
            (110004, InsufficientBalanceError),
            (110006, InsufficientBalanceError),
            (110007, InsufficientBalanceError),
            (110012, InsufficientBalanceError),
            (110044, InsufficientBalanceError),
            (110045, InsufficientBalanceError),
            (110051, InsufficientBalanceError),
            (110052, InsufficientBalanceError),
            (110053, InsufficientBalanceError),
            (110131, InsufficientBalanceError),
            (30256, InsufficientBalanceError),
            (170033, InsufficientBalanceError),
            (170131, InsufficientBalanceError),
            # Order not found
            (110001, OrderNotFoundError),
            (170143, OrderNotFoundError),
            (170213, OrderNotFoundError),
            # Connection / retryable
            (10000, PlatformConnectionError),
            (10016, PlatformConnectionError),
            (10019, PlatformConnectionError),
            (110079, PlatformConnectionError),
            (110118, PlatformConnectionError),
            (170001, PlatformConnectionError),
            (170007, PlatformConnectionError),
            (170032, PlatformConnectionError),
            (170146, PlatformConnectionError),
            (170147, PlatformConnectionError),
            (170191, PlatformConnectionError),
            (170234, PlatformConnectionError),
            (170310, PlatformConnectionError),
            (3400214, PlatformConnectionError),
        ],
    )
    def test_known_ret_code(
        self, ret_code: int, expected_type: type
    ) -> None:
        exc = map_bybit_error(ret_code=ret_code, ret_msg="test message")
        assert isinstance(exc, expected_type)
        assert "test message" in str(exc)

    def test_unknown_ret_code_maps_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=99999, ret_msg="unknown code")
        assert isinstance(exc, PlatformError)
        assert "unknown code" in str(exc)

    def test_unknown_ret_code_never_carries_raw(self) -> None:
        exc = map_bybit_error(ret_code=99999, ret_msg="unmapped")
        assert isinstance(exc, PlatformError)
        assert exc.platform_error is None

    def test_10003_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=10003, ret_msg="API key is invalid")
        assert isinstance(exc, PlatformError)

    def test_10004_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=10004, ret_msg="Error sign")
        assert isinstance(exc, PlatformError)

    def test_10027_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=10027, ret_msg="Transactions are banned")
        assert isinstance(exc, PlatformError)

    def test_110030_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=110030, ret_msg="Duplicate orderId")
        assert isinstance(exc, PlatformError)

    def test_110066_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=110066, ret_msg="Trading is currently not allowed")
        assert isinstance(exc, PlatformError)

    def test_170116_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=170116, ret_msg="Invalid orderType")
        assert isinstance(exc, PlatformError)

    def test_110072_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=110072, ret_msg="OrderLinkedID is duplicate")
        assert isinstance(exc, PlatformError)

    def test_170141_falls_to_platform_error(self) -> None:
        exc = map_bybit_error(ret_code=170141, ret_msg="Duplicate clientOrderId")
        assert isinstance(exc, PlatformError)


class TestMapBybitEdgeCases:
    def test_no_args_maps_to_platform_error(self) -> None:
        exc = map_bybit_error()
        assert isinstance(exc, PlatformError)
        assert "unknown" in str(exc).lower()

    def test_http_status_takes_precedence_over_ret_code(self) -> None:
        exc = map_bybit_error(
            http_status=429,
            ret_code=110001,
            ret_msg="http rate limit",
        )
        assert isinstance(exc, RateLimitError)

    def test_http_5xx_takes_precedence_over_ret_code(self) -> None:
        exc = map_bybit_error(
            http_status=503,
            ret_code=110001,
            ret_msg="server unavailable",
        )
        assert isinstance(exc, PlatformConnectionError)

    def test_empty_ret_msg_uses_fallback(self) -> None:
        exc = map_bybit_error(ret_code=110004)
        assert isinstance(exc, InsufficientBalanceError)

    def test_default_ret_msg_for_unknown_code(self) -> None:
        exc = map_bybit_error(ret_code=54321)
        assert "54321" in str(exc)

    def test_unmapped_code_never_carries_raw(self) -> None:
        exc = map_bybit_error(ret_code=10001, ret_msg="param error")
        assert isinstance(exc, PlatformError)
        assert exc.platform_error is None

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from binance.exceptions import BinanceRequestException

from unified_trading_execution.binance.coin_m_algo import (
    COIN_M_ALGO_ORDER_PATH,
    COIN_M_ALL_ALGO_ORDERS_PATH,
    COIN_M_OPEN_ALGO_ORDERS_PATH,
)
from unified_trading_execution.errors import PlatformError


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", COIN_M_ALGO_ORDER_PATH),
        ("GET", COIN_M_ALGO_ORDER_PATH),
        ("DELETE", COIN_M_ALGO_ORDER_PATH),
        ("GET", COIN_M_OPEN_ALGO_ORDERS_PATH),
        ("GET", COIN_M_ALL_ALGO_ORDERS_PATH),
    ],
)
async def test_coin_m_algo_request_uses_sdk_signed_transport(
    adapter,
    binance_client_mock: AsyncMock,
    method: str,
    path: str,
) -> None:
    adapter._client = binance_client_mock
    adapter._connected = True
    params = {"symbol": "BTCUSD_PERP", "algoId": 123}
    binance_client_mock._request_futures_coin_api.return_value = {"algoId": 123}

    response = await adapter._run_coin_m_algo_request(method, path, params)

    assert response == {"algoId": 123}
    binance_client_mock._request_futures_coin_api.assert_awaited_once_with(
        method.lower(),
        path,
        signed=True,
        version=1,
        data=params,
    )


async def test_coin_m_algo_request_maps_sdk_errors(
    adapter,
    binance_client_mock: AsyncMock,
) -> None:
    adapter._client = binance_client_mock
    adapter._connected = True
    binance_client_mock._request_futures_coin_api.side_effect = BinanceRequestException(
        "invalid JSON response"
    )

    with pytest.raises(PlatformError) as error:
        await adapter._run_coin_m_algo_request(
            "GET",
            COIN_M_OPEN_ALGO_ORDERS_PATH,
            {"symbol": "BTCUSD_PERP"},
        )

    assert error.value.platform_error == {
        "exception": "BinanceRequestException",
        "message": "invalid JSON response",
    }


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("PUT", COIN_M_ALGO_ORDER_PATH),
        ("GET", "unlistedEndpoint"),
    ],
)
async def test_coin_m_algo_request_rejects_unallowlisted_routes(
    adapter,
    method: str,
    path: str,
) -> None:
    with pytest.raises(ValueError, match="Unsupported COIN-M algo"):
        await adapter._run_coin_m_algo_request(method, path, {})

"""Read-only network smoke coverage for Binance REST ping endpoints."""

from __future__ import annotations

import pytest
from aiohttp import ClientSession, ClientTimeout
from binance import AsyncClient  # type: ignore[import-untyped]

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig

_ENVIRONMENTS = (
    ("production", False, False),
    ("testnet", True, False),
    ("demo", False, True),
)


@pytest.mark.parametrize(
    ("environment", "testnet", "demo"),
    _ENVIRONMENTS,
    ids=("production", "testnet", "demo"),
)
async def test_all_rest_ping_urls_are_reachable(
    environment: str,
    testnet: bool,
    demo: bool,
    binance_rest_url_smoke_opt_in: None,
) -> None:
    """Reach each product's public ping endpoint in each explicitly selected environment."""
    config = BinanceConfig(
        api_key="read-only-url-smoke-test",
        api_secret="read-only-url-smoke-test",
        testnet=testnet,
        demo=demo,
    )
    client = AsyncClient(testnet=testnet, demo=demo)
    adapter = BinanceAdapter(config)
    try:
        adapter._validate_client_urls(client)
        endpoints = (
            ("Spot", client._create_api_uri("ping", signed=False)),
            ("USDⓈ-M Futures", client._create_futures_api_uri("ping")),
            ("COIN-M Futures", client._create_futures_coin_api_url("ping")),
        )

        timeout = ClientTimeout(total=15)
        async with ClientSession(timeout=timeout) as session:
            for product, url in endpoints:
                async with session.get(url) as response:
                    assert response.status == 200, (
                        f"Binance {environment} {product} ping returned "
                        f"HTTP {response.status} from {url}"
                    )
                    payload = await response.json()
                    assert payload == {}, (
                        f"Binance {environment} {product} ping returned "
                        f"unexpected response: {payload!r}"
                    )
    finally:
        await client.close_connection()

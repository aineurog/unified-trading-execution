"""Shared fixtures for Binance integration tests.

Integration tests require explicit environment opt-ins and dedicated
credentials for authenticated order operations. Order tests default to Demo;
set ``BINANCE_TESTNET=true`` to select Testnet instead.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path
from typing import cast

import pytest
from binance import AsyncClient  # type: ignore[import-untyped]

from unified_trading_execution.binance import BinanceAdapter, BinanceConfig, BinanceProduct
from unified_trading_execution.events import EventBus


def _load_env_files() -> None:
    """Load Binance variables from package/root env files without overriding exports."""
    directories = (
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[4],
    )
    seen: set[Path] = set()
    for directory in directories:
        env_file = directory / ".env"
        if env_file in seen or not env_file.is_file():
            continue
        seen.add(env_file)
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, value = line.partition("=")
            name = name.strip()
            if not name.startswith("BINANCE_"):
                continue
            os.environ.setdefault(name, value.strip().strip('"').strip("'"))


_load_env_files()


def _require_testnet_opt_in() -> None:
    if os.getenv("BINANCE_TESTNET", "").strip().lower() != "true":
        pytest.skip("Set BINANCE_TESTNET=true to opt in to Binance testnet integration tests")
    if os.getenv("BINANCE_DEMO", "").strip().lower() == "true":
        pytest.skip("Binance integration fixtures require Testnet, not Demo Trading")


def _require_demo_opt_in() -> None:
    if os.getenv("BINANCE_DEMO", "").strip().lower() != "true":
        pytest.skip("Set BINANCE_DEMO=true to opt in to Binance Demo integration tests")
    if os.getenv("BINANCE_TESTNET", "").strip().lower() == "true":
        pytest.skip("Choose only one Binance integration environment")


@pytest.fixture
def binance_order_config(request: pytest.FixtureRequest) -> BinanceConfig:
    """Select an explicitly opted-in Testnet or Demo order-test configuration."""
    product = request.param
    testnet = os.getenv("BINANCE_TESTNET", "").strip().lower() == "true"
    demo = os.getenv("BINANCE_DEMO", "").strip().lower() == "true"
    if testnet and demo:
        pytest.fail("Choose only one Binance order integration environment")

    if testnet:
        if os.getenv("BINANCE_TESTNET_ORDER_INTEGRATION", "").strip().lower() != "true":
            pytest.skip(
                "Set BINANCE_TESTNET_ORDER_INTEGRATION=true to opt in to "
                "Binance Testnet order tests"
            )
        names = {
            "spot": ("BINANCE_SPOT_TESTNET_API_KEY", "BINANCE_SPOT_TESTNET_API_SECRET"),
            "usd_m": ("BINANCE_USDM_TESTNET_API_KEY", "BINANCE_USDM_TESTNET_API_SECRET"),
            "coin_m": ("BINANCE_COINM_TESTNET_API_KEY", "BINANCE_COINM_TESTNET_API_SECRET"),
        }
        if product not in names:
            raise ValueError(f"Unknown Binance Testnet order product {product!r}")
        api_key_name, api_secret_name = names[product]
        api_key, api_secret = _require_env(api_key_name), _require_env(api_secret_name)
        return BinanceConfig(api_key=api_key, api_secret=api_secret, testnet=True, demo=False)

    if os.getenv("BINANCE_DEMO_ORDER_INTEGRATION", "true").strip().lower() != "true":
        pytest.skip("Set BINANCE_DEMO_ORDER_INTEGRATION=false to disable Binance Demo order tests")
    api_key = _require_env("BINANCE_DEMO_API_KEY")
    api_secret = _require_env("BINANCE_DEMO_API_SECRET")
    return BinanceConfig(api_key=api_key, api_secret=api_secret, testnet=False, demo=True)


@pytest.fixture
def binance_testnet_opt_in() -> None:
    """Require explicit Testnet selection without requiring API credentials."""
    _require_testnet_opt_in()


@pytest.fixture
def binance_demo_opt_in() -> None:
    """Require explicit Demo Trading selection without API credentials."""
    _require_demo_opt_in()


@pytest.fixture
def binance_account_stream_order_opt_in() -> None:
    """Require a separate opt-in before stream tests create sandbox orders."""
    if os.getenv("BINANCE_ACCOUNT_STREAM_ORDER_INTEGRATION", "true").strip().lower() != "true":
        pytest.skip(
            "Set BINANCE_ACCOUNT_STREAM_ORDER_INTEGRATION=true to opt in to "
            "Binance account-stream order tests"
        )


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping Binance integration test")
    return value


async def _require_clean_futures_symbol(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    symbol: str,
) -> None:
    client = adapter._require_client()
    open_orders_method = (
        "futures_get_open_orders"
        if product == BinanceProduct.USD_M
        else "futures_coin_get_open_orders"
    )
    open_orders = await adapter._run_order_request(
        product,
        partial(getattr(client, open_orders_method), symbol=symbol),
    )
    if not isinstance(open_orders, list):
        raise AssertionError(f"Unexpected open-orders response: {open_orders!r}")
    if open_orders:
        pytest.skip(f"Refusing to mutate {symbol}: sandbox has open Futures orders")

    if product == BinanceProduct.USD_M:
        algo_orders = await adapter._run_order_request(
            product,
            partial(client.futures_get_open_algo_orders, symbol=symbol),
        )
    else:
        algo_orders = await adapter._coin_m_algo_request(
            "get",
            "openAlgoOrders",
            {"symbol": symbol},
        )
    if not isinstance(algo_orders, list):
        raise AssertionError(f"Unexpected open algo-orders response: {algo_orders!r}")
    if algo_orders:
        pytest.skip(f"Refusing to mutate {symbol}: sandbox has open algo orders")
    positions = await adapter._futures_position_risk(product, symbol=symbol)
    for row in positions:
        try:
            position_amount = Decimal(str(row.get("positionAmt")))
        except (InvalidOperation, ValueError) as exc:
            raise AssertionError(f"Malformed position amount in Binance response: {row!r}") from exc
        if not position_amount.is_finite():
            raise AssertionError(f"Non-finite position amount in Binance response: {row!r}")
        if position_amount != 0:
            pytest.skip(f"Refusing to mutate {symbol}: sandbox has a non-flat Futures position")


@pytest.fixture
def require_clean_futures_symbol() -> Callable[
    [BinanceAdapter, BinanceProduct, str], Awaitable[None]
]:
    return _require_clean_futures_symbol


@pytest.fixture
def event_bus() -> EventBus:
    """A fresh event bus for each integration test."""
    return EventBus()


@pytest.fixture
def binance_config() -> BinanceConfig:
    """Select an explicitly enabled sandbox config, never production."""
    testnet = os.getenv("BINANCE_TESTNET", "").strip().lower() == "true"
    demo = os.getenv("BINANCE_DEMO", "").strip().lower() == "true"
    if testnet and demo:
        pytest.fail("Choose only one Binance integration environment")
    if testnet:
        _require_testnet_opt_in()
        return BinanceConfig(
            api_key=_require_env("BINANCE_TESTNET_API_KEY"),
            api_secret=_require_env("BINANCE_TESTNET_API_SECRET"),
            testnet=True,
            demo=False,
        )
    if demo:
        _require_demo_opt_in()
        return BinanceConfig(
            api_key=_require_env("BINANCE_DEMO_API_KEY"),
            api_secret=_require_env("BINANCE_DEMO_API_SECRET"),
            testnet=False,
            demo=True,
        )
    pytest.skip("Set BINANCE_TESTNET=true or BINANCE_DEMO=true to select a sandbox")


@pytest.fixture
async def connected_adapter(
    binance_config: BinanceConfig,
    event_bus: EventBus,
) -> AsyncIterator[BinanceAdapter]:
    """Connect to the selected Binance sandbox and always disconnect afterward."""
    adapter = BinanceAdapter(binance_config, event_bus=event_bus)
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()


@pytest.fixture
async def demo_metadata_adapter(
    event_bus: EventBus,
    binance_demo_opt_in: None,
) -> AsyncIterator[BinanceAdapter]:
    """Provide an adapter backed by a verified Demo Trading SDK client.

    This fixture initializes the pinned SDK directly so metadata-only
    integration tests can exercise real public Demo endpoints without
    credentials or trading.
    """
    config = BinanceConfig(
        api_key="read-only-metadata-test",
        api_secret="read-only-metadata-test",
        testnet=False,
        demo=True,
    )
    client = await AsyncClient.create(testnet=False, demo=True)
    try:
        if client.testnet or not client.demo:
            raise AssertionError("Binance SDK client is not configured for Demo Trading")
        expected_urls = (
            (
                "https://demo-api.binance.com/api/v3/exchangeInfo",
                client._create_api_uri("exchangeInfo", signed=False),
            ),
            (
                "https://demo-fapi.binance.com/fapi/v1/exchangeInfo",
                client._create_futures_api_uri("exchangeInfo"),
            ),
            (
                "https://demo-dapi.binance.com/dapi/v1/exchangeInfo",
                client._create_futures_coin_api_url("exchangeInfo"),
            ),
        )
        for expected_url, actual_url in expected_urls:
            if actual_url != expected_url:
                raise AssertionError(
                    f"Binance SDK resolved non-Demo exchange-info URL: {actual_url}"
                )
        adapter = BinanceAdapter(config, event_bus=event_bus)
        adapter._client = cast(AsyncClient, client)
        yield adapter
    finally:
        await client.close_connection()

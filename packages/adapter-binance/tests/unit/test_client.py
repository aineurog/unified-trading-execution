"""Tests for Binance REST client construction and lifecycle."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientError
from binance import AsyncClient, BinanceSocketManager  # type: ignore[import-untyped]

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    BinanceEngine,
    SyncBinanceEngine,
)
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import ConnectionStateEvent, EventBus

_REST_PING_URLS = {
    "production": (
        "https://api.binance.com/api/v3/ping",
        "https://fapi.binance.com/fapi/v1/ping",
        "https://dapi.binance.com/dapi/v1/ping",
    ),
    "testnet": (
        "https://testnet.binance.vision/api/v3/ping",
        "https://testnet.binancefuture.com/fapi/v1/ping",
        "https://testnet.binancefuture.com/dapi/v1/ping",
    ),
    "demo": (
        "https://demo-api.binance.com/api/v3/ping",
        "https://demo-fapi.binance.com/fapi/v1/ping",
        "https://demo-dapi.binance.com/dapi/v1/ping",
    ),
}


@pytest.mark.parametrize(
    ("testnet", "demo", "environment"),
    [
        (False, False, "production"),
        (True, False, "testnet"),
        (False, True, "demo"),
    ],
)
async def test_pinned_sdk_resolves_all_rest_products_for_each_environment(
    testnet: bool,
    demo: bool,
    environment: str,
) -> None:
    config = BinanceConfig(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=testnet,
        demo=demo,
    )
    client = AsyncClient(
        api_key=config.api_key,
        api_secret=config.api_secret,
        testnet=testnet,
        demo=demo,
    )
    try:
        urls = (
            client._create_api_uri("ping", signed=False),
            client._create_futures_api_uri("ping"),
            client._create_futures_coin_api_url("ping"),
        )

        assert urls == _REST_PING_URLS[environment]
        BinanceAdapter(config)._validate_client_urls(client)
    finally:
        await client.close_connection()


@pytest.mark.parametrize(
    ("testnet", "demo", "expected_url"),
    [
        (False, False, "wss://dstream.binance.com/"),
        (True, False, "wss://dstream.binancefuture.com/"),
        (False, True, "wss://dstream.binancefuture.com/"),
    ],
)
async def test_pinned_sdk_coin_m_socket_selects_configured_environment(
    testnet: bool,
    demo: bool,
    expected_url: str,
) -> None:
    client = AsyncClient(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=testnet,
        demo=demo,
    )
    try:
        socket = BinanceSocketManager(client).coin_futures_socket()
        assert socket._url == expected_url
    finally:
        await client.close_connection()


async def test_connect_and_disconnect_publish_state_and_use_configured_environment(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus: EventBus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    factory = AsyncMock(return_value=binance_client_mock)
    monkeypatch.setattr(AsyncClient, "create", factory)

    await adapter.connect()

    assert adapter.is_connected is True
    factory.assert_awaited_once_with(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=True,
        demo=False,
    )
    assert [event.connected for event in events] == [True]

    await adapter.disconnect()

    assert adapter.is_connected is False
    binance_client_mock.close_connection.assert_awaited_once()
    assert [event.connected for event in events] == [True, False]


async def test_concurrent_and_repeated_lifecycle_calls_are_idempotent(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus: EventBus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    factory = AsyncMock(return_value=binance_client_mock)
    monkeypatch.setattr(AsyncClient, "create", factory)

    await asyncio.gather(adapter.connect(), adapter.connect(), adapter.connect())
    await asyncio.gather(adapter.disconnect(), adapter.disconnect(), adapter.disconnect())

    factory.assert_awaited_once()
    binance_client_mock.close_connection.assert_awaited_once()
    assert [event.connected for event in events] == [True, False]
    assert adapter.is_connected is False


@pytest.mark.parametrize(
    ("builder_name", "wrong_url", "product"),
    [
        ("_create_api_uri", _REST_PING_URLS["production"][0], "Spot"),
        ("_create_futures_api_uri", _REST_PING_URLS["production"][1], "USDⓈ-M Futures"),
        ("_create_futures_coin_api_url", _REST_PING_URLS["production"][2], "COIN-M Futures"),
    ],
)
async def test_connect_rejects_live_product_route_and_closes_partial_client(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus: EventBus,
    builder_name: str,
    wrong_url: str,
    product: str,
) -> None:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    getattr(binance_client_mock, builder_name).return_value = wrong_url

    with pytest.raises(PlatformConnectionError, match=product):
        await adapter.connect()

    assert adapter.is_connected is False
    assert adapter._client is None
    binance_client_mock.close_connection.assert_awaited_once()
    assert events == []


async def test_factory_transport_failure_is_mapped_to_connection_error(
    adapter: BinanceAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = AsyncMock(side_effect=ClientError("connection refused"))
    monkeypatch.setattr(AsyncClient, "create", factory)

    with pytest.raises(PlatformConnectionError, match="transport failure"):
        await adapter.connect()

    assert adapter.is_connected is False
    assert adapter._client is None


async def test_cancelled_startup_closes_client_created_by_sdk_factory(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory_started = asyncio.Event()
    finish_factory = asyncio.Event()

    async def delayed_factory(**_: object) -> AsyncMock:
        factory_started.set()
        await finish_factory.wait()
        return binance_client_mock

    factory = AsyncMock(side_effect=delayed_factory)
    monkeypatch.setattr(AsyncClient, "create", factory)
    connect_task = asyncio.create_task(adapter.connect())
    await factory_started.wait()
    connect_task.cancel()
    finish_factory.set()

    with pytest.raises(asyncio.CancelledError):
        await connect_task

    binance_client_mock.close_connection.assert_awaited_once()
    assert adapter._client is None
    assert adapter.is_connected is False


async def test_failed_close_retains_client_for_retry(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
    event_bus: EventBus,
) -> None:
    events: list[ConnectionStateEvent] = []
    event_bus.subscribe(ConnectionStateEvent, events.append)
    await adapter.connect()
    binance_client_mock.close_connection.side_effect = ClientError("close failed")

    with pytest.raises(PlatformConnectionError, match="transport failure"):
        await adapter.disconnect()

    assert adapter.is_connected is False
    assert adapter._client is binance_client_mock
    assert [event.connected for event in events] == [True, False]

    binance_client_mock.close_connection.side_effect = None
    await adapter.disconnect()

    assert adapter.is_connected is False
    assert adapter._client is None
    assert [event.connected for event in events] == [True, False]


async def test_binance_engine_delegates_connect_to_core_lifecycle(
    binance_config: BinanceConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connected_engines: list[Engine] = []

    async def connect_core(engine: Engine) -> None:
        connected_engines.append(engine)

    monkeypatch.setattr(Engine, "connect", connect_core)
    engine = BinanceEngine(binance_config)

    await engine.connect()

    assert connected_engines == [engine]


def test_sync_engine_delegates_connect_to_core_lifecycle(
    binance_config: BinanceConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connected_engines: list[Engine] = []

    async def connect_core(engine: Engine) -> None:
        connected_engines.append(engine)

    async def ashutdown_core(engine: Engine) -> None:
        return None

    monkeypatch.setattr(Engine, "connect", connect_core)
    monkeypatch.setattr(Engine, "ashutdown", ashutdown_core)
    engine = SyncBinanceEngine(binance_config)

    try:
        engine.connect()
    finally:
        engine.shutdown()

    assert len(connected_engines) == 1
    assert isinstance(connected_engines[0], Engine)

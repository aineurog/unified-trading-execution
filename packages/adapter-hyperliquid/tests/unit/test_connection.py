"""Unit tests for connect/disconnect (transport mocked, no network)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from unified_trading_execution.errors import PlatformConnectionError, PlatformError
from unified_trading_execution.events import ConnectionStateEvent, EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)


def _exchange_mock(*, role: str = "agent", abstraction: str = "unifiedAccount") -> MagicMock:
    exchange = MagicMock()
    exchange.info.user_role.return_value = {"role": role}
    exchange.info.query_user_abstraction_state.return_value = abstraction
    return exchange


def _events(event_bus: EventBus) -> list[Any]:
    captured: list[Any] = []
    event_bus.subscribe(ConnectionStateEvent, captured.append)
    return captured


async def test_connect_publishes_and_flags() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ):
        await adapter.connect()
    assert adapter.is_connected is True
    assert len(captured) == 1
    assert captured[0].connected is True
    assert captured[0].account_id == _TEST_ADDRESS


async def test_connect_idempotent() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ) as factory:
        await adapter.connect()
        await adapter.connect()
    assert factory.call_count == 1
    assert len(captured) == 1


async def test_connect_rejects_missing_role() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            return_value=_exchange_mock(role="missing"),
        ),
        pytest.raises(PlatformError, match="approveAgent"),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_connect_rejects_non_unified_abstraction() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            return_value=_exchange_mock(abstraction="portfolioMargin"),
        ),
        pytest.raises(PlatformError, match="unified"),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_connect_maps_transport_failure() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with (
        patch(
            "unified_trading_execution.hyperliquid.adapter.Exchange",
            side_effect=requests.exceptions.ConnectionError("down"),
        ),
        pytest.raises(PlatformConnectionError),
    ):
        await adapter.connect()
    assert adapter.is_connected is False


async def test_disconnect_publishes_and_clears() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    with patch(
        "unified_trading_execution.hyperliquid.adapter.Exchange",
        return_value=_exchange_mock(),
    ):
        await adapter.connect()
        await adapter.disconnect()
    assert adapter.is_connected is False
    assert [e.connected for e in captured] == [True, False]


async def test_disconnect_when_never_connected_is_quiet() -> None:
    event_bus = EventBus()
    captured = _events(event_bus)
    adapter = HyperliquidAdapter(_config(), event_bus=event_bus)
    await adapter.disconnect()
    assert captured == []


async def test_exchange_required_when_disconnected() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    with pytest.raises(PlatformConnectionError):
        adapter._require_exchange()


def _stored_row(cid: str, oid: str | None = "77") -> Any:
    from datetime import UTC, datetime
    from decimal import Decimal

    from unified_trading_execution.types.enums import (
        AssetClass,
        OrderSide,
        OrderStatus,
        OrderType,
        TimeInForce,
    )
    from unified_trading_execution.types.instrument import Instrument
    from unified_trading_execution.types.order import OrderRecord

    return OrderRecord(
        instrument=Instrument(
            symbol="BTC", quote_currency="USDC", asset_class=AssetClass.FUTURES,
            currency="USDC", multiplier=1,
        ),
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.IOC,
        client_order_id=cid,
        price=None,
        stop_price=None,
        reduce_only=False,
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id=oid,
        status=OrderStatus.FILLED,
        filled_quantity=Decimal("0.001"),
        average_fill_price=None,
        correlation_id="corr-1",
        created_at=datetime(2026, 10, 5, tzinfo=UTC),
        updated_at=datetime(2026, 10, 5, tzinfo=UTC),
    )


def _store_with(rows: list[Any] | None) -> MagicMock:
    from unittest.mock import AsyncMock

    store = MagicMock()
    store.query_orders = AsyncMock(return_value=rows)
    return store


async def test_seed_rebuilds_identity_maps() -> None:
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    adapter.attach_state_store(_store_with([_stored_row("my-order-123")]))
    await adapter._seed_identity_maps()
    assert adapter._client_coins["my-order-123"] == ("BTC", False)
    assert adapter._oid_clients["77"] == ("my-order-123", None, None)
    child = client_order_id_to_cloid("my-order-123:take_profit")
    assert adapter._child_parents[child][0] == "my-order-123"


async def test_seed_skips_bad_rows() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    adapter.attach_state_store(_store_with([_stored_row(""), _stored_row("ok-1", None)]))
    await adapter._seed_identity_maps()
    assert "" not in adapter._client_coins
    assert adapter._client_coins["ok-1"] == ("BTC", False)
    assert adapter._oid_clients == {}


async def test_seed_without_store_or_on_failure_is_quiet() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    await adapter._seed_identity_maps()  # no store attached
    assert adapter._client_coins == {}
    from unittest.mock import AsyncMock

    store = MagicMock()
    store.query_orders = AsyncMock(side_effect=RuntimeError("db down"))
    adapter.attach_state_store(store)
    await adapter._seed_identity_maps()
    assert adapter._client_coins == {}


async def test_seed_never_overwrites_session() -> None:
    adapter = HyperliquidAdapter(_config(), event_bus=EventBus())
    adapter._client_coins["my-order-123"] = ("ETH", False)
    adapter.attach_state_store(_store_with([_stored_row("my-order-123")]))
    await adapter._seed_identity_maps()
    assert adapter._client_coins["my-order-123"] == ("ETH", False)

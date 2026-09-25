"""Unit tests for order operations (transport mocked, no network)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from hyperliquid.utils.types import Cloid

from unified_trading_execution.errors import (
    InvalidOrderError,
    OrderNotFoundError,
    PlatformError,
)
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderModification, UnifiedOrder

_CLOID = "0x" + "ab" * 16


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _order(**kwargs: Any) -> UnifiedOrder:
    base: dict[str, Any] = {
        "instrument": _perp(),
        "order_type": OrderType.LIMIT,
        "side": OrderSide.BUY,
        "quantity": Decimal("0.01"),
        "price": Decimal("50000"),
        "time_in_force": TimeInForce.GTC,
    }
    base.update(kwargs)
    return UnifiedOrder(**base)


def _connected(adapter: HyperliquidAdapter) -> MagicMock:
    exchange = MagicMock()
    exchange.info.name_to_asset.side_effect = lambda coin: {"BTC": 0}[coin]
    exchange.info.asset_to_sz_decimals = {0: 5}
    exchange.info.name_to_coin = {}
    exchange.info.meta.return_value = {"universe": [{"name": "BTC", "maxLeverage": 40}]}
    adapter._exchange = exchange
    adapter._connected = True
    return exchange


def _ok_statuses(statuses: list[Any]) -> dict[str, Any]:
    return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}


async def test_place_limit_posts_and_caches(
    adapter: HyperliquidAdapter,
) -> None:
    exchange = _connected(adapter)
    exchange.bulk_orders.return_value = _ok_statuses([{"resting": {"oid": 11}}])
    result = await adapter.place_order(_order(client_order_id=_CLOID))
    assert (result.status, result.platform_order_id) == (OrderStatus.OPEN, "11")
    assert adapter._client_coins[_CLOID] == ("BTC", False)
    (requests,), kwargs = exchange.bulk_orders.call_args
    assert kwargs["grouping"] == "na"
    assert requests[0]["coin"] == "BTC"
    assert isinstance(requests[0]["cloid"], Cloid)


async def test_place_unknown_coin_rejects(adapter: HyperliquidAdapter) -> None:
    from unified_trading_execution.errors import InvalidSymbolError

    _connected(adapter)
    doge = Instrument(
        symbol="DOGE",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )
    with pytest.raises(InvalidSymbolError):
        await adapter.place_order(_order(instrument=doge))


async def test_place_rejects_bad_size_without_posting(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    with pytest.raises(InvalidOrderError):
        await adapter.place_order(_order(quantity=Decimal("0.0100001")))
    exchange.bulk_orders.assert_not_called()


async def test_place_maps_venue_error(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.bulk_orders.return_value = {
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [{"error": "Invalid TP/SL price."}]}},
    }
    with pytest.raises(InvalidOrderError):
        await adapter.place_order(_order())


async def test_place_rejects_top_level_err(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.bulk_orders.return_value = {"status": "err", "response": "Multi-sig required"}
    with pytest.raises(PlatformError, match="Multi-sig"):
        await adapter.place_order(_order())


async def test_place_market_band_is_tick_rounded(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.l2_snapshot.return_value = {
        "levels": [[{"px": "100", "sz": "1", "n": 1}], [{"px": "101", "sz": "1", "n": 1}]]
    }
    exchange.info.meta.return_value = {"universe": [{"name": "BTC", "maxLeverage": 40}]}
    exchange.bulk_orders.return_value = _ok_statuses([{"resting": {"oid": 1}}])
    await adapter.place_order(_order(order_type=OrderType.MARKET, price=None))
    (requests,), _ = exchange.bulk_orders.call_args
    # 101 * 1.01 = 102.01 exceeds the 1-decimal tick (szDecimals 5) and rounds
    # up to 102.1 — still aggressive over the 101 touch.
    assert requests[0]["limit_px"] == 102.1


async def test_place_market_tier_breach_rejects(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.l2_snapshot.return_value = {
        "levels": [[{"px": "100", "sz": "1", "n": 1}], [{"px": "101", "sz": "1", "n": 1}]]
    }
    exchange.info.meta.return_value = {"universe": [{"name": "BTC", "maxLeverage": 1}]}
    with pytest.raises(InvalidOrderError, match="tier cap"):
        await adapter.place_order(
            _order(order_type=OrderType.MARKET, price=None, quantity=Decimal("10000"))
        )
    exchange.bulk_orders.assert_not_called()


def _open_entry(**kwargs: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "coin": "BTC",
        "side": "B",
        "limitPx": "50000.0",
        "sz": "0.005",
        "origSz": "0.01",
        "oid": 11,
        "timestamp": 1724361546645,
        "orderType": "Limit",
        "tif": "Gtc",
        "cloid": _CLOID,
    }
    base.update(kwargs)
    return base


def _status_response() -> dict[str, Any]:
    return {
        "status": "order",
        "order": {
            "order": _open_entry(),
            "status": "open",
            "statusTimestamp": 1724361546700,
        },
    }


async def test_modify_roundtrip(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    adapter._client_coins[_CLOID] = ("BTC", False)
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = [_open_entry()]
    exchange.modify_order.return_value = {"status": "ok", "response": {}}
    exchange.info.query_order_by_cloid.return_value = _status_response()
    result = await adapter.modify_order(
        OrderModification(client_order_id=_CLOID, price=Decimal("51000"))
    )
    assert result.status is OrderStatus.OPEN
    assert result.platform_order_id == "11"
    _, kwargs = exchange.modify_order.call_args
    assert isinstance(kwargs["oid"], Cloid)
    assert kwargs["limit_px"] == 51000.0


async def test_modify_unknown_raises(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    with pytest.raises(OrderNotFoundError):
        await adapter.modify_order(OrderModification(client_order_id="nope", price=Decimal("1")))
    exchange.modify_order.assert_not_called()


async def test_cancel_gone_reads_cancelled(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    adapter._client_coins[_CLOID] = ("BTC", False)
    exchange.cancel_by_cloid.return_value = {
        "status": "ok",
        "response": {"type": "cancelByCloid", "data": {"statuses": ["success"]}},
    }
    exchange.info.query_order_by_cloid.return_value = {"status": "unknownOid"}
    result = await adapter.cancel_order(_CLOID)
    assert result.status is OrderStatus.CANCELLED
    assert result.platform_order_id is None
    args, _ = exchange.cancel_by_cloid.call_args
    assert args[0] == "BTC"
    assert isinstance(args[1], Cloid)


async def test_cancel_missing_raises(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.cancel_by_cloid.return_value = {
        "status": "err",
        "response": "Order was never placed, already canceled, or filled. asset=0",
    }
    adapter._client_coins["ghost"] = ("BTC", False)
    with pytest.raises(OrderNotFoundError):
        await adapter.cancel_order("ghost")


async def test_cancel_unknown_id_raises_without_posting(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    with pytest.raises(OrderNotFoundError):
        await adapter.cancel_order("ghost")
    exchange.cancel_by_cloid.assert_not_called()


async def test_get_order_maps_status_response(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.query_order_by_cloid.return_value = _status_response()
    result = await adapter.get_order_by_client_id(_CLOID)
    assert result is not None
    assert (result.status, result.platform_order_id) == (OrderStatus.OPEN, "11")
    assert result.filled_quantity == Decimal("0.005")
    args, _ = exchange.info.query_order_by_cloid.call_args
    assert isinstance(args[1], Cloid)


async def test_get_order_unknown_returns_none(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.query_order_by_cloid.return_value = {"status": "unknownOid"}
    assert await adapter.get_order_by_client_id("ghost") is None


async def test_get_order_prefers_open_over_stale_status(adapter: HyperliquidAdapter) -> None:
    """Post-modify, orderStatus returns the cancelled original — open wins."""
    exchange = _connected(adapter)
    live = _open_entry()
    live["oid"] = 999
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = [live]
    exchange.info.query_order_by_cloid.return_value = {
        "status": "order",
        "order": {"order": _open_entry(), "status": "canceled", "statusTimestamp": 2},
    }
    result = await adapter.get_order_by_client_id(_CLOID)
    assert result is not None
    assert (result.status, result.platform_order_id) == (OrderStatus.OPEN, "999")


async def test_get_order_picks_latest_duplicate(adapter: HyperliquidAdapter) -> None:
    """Duplicate open echoes resolve to the newest entry, not first-match."""
    exchange = _connected(adapter)
    old = _open_entry()
    old["oid"] = 1
    old["timestamp"] = 100
    new = _open_entry()
    new["oid"] = 2
    new["timestamp"] = 200
    exchange.info.open_orders.return_value = [old]
    exchange.info.frontend_open_orders.return_value = [new]
    result = await adapter.get_order_by_client_id(_CLOID)
    assert result is not None
    assert result.platform_order_id == "2"


async def test_resolve_coin_scans_open_orders(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.open_orders.return_value = [_open_entry()]
    exchange.info.frontend_open_orders.return_value = []
    assert await adapter._resolve_coin(_CLOID) == ("BTC", False)
    assert adapter._client_coins[_CLOID] == ("BTC", False)

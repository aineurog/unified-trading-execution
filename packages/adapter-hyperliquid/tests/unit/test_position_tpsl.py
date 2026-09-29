"""Unit tests for position TP/SL attach/replace/read (transport mocked, no network)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.errors import InvalidOrderError, InvalidSymbolError, OrderNotFoundError
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.orders import (
    SL_CLOID_SUFFIX,
    TP_CLOID_SUFFIX,
    build_position_tpsl_action,
    position_tpsl_cloid,
)
from unified_trading_execution.types.enums import AssetClass, FillEntry, FillReason
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32
_POSITION_ID = "BTC:oneWay"

_META = {
    "universe": [{"name": "BTC", "szDecimals": 2, "maxLeverage": 40, "marginTableId": 1}],
    "marginTables": [],
}


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _spot() -> Instrument:
    return Instrument(symbol="HYPE", quote_currency="USDC", asset_class=AssetClass.SPOT)


def _store_mock() -> MagicMock:
    store = MagicMock()
    store.get_adapter_config = AsyncMock(return_value=None)
    store.set_adapter_config = AsyncMock()
    store.delete_adapter_config = AsyncMock()
    store.list_adapter_config = AsyncMock(return_value={})
    return store


def _leg(szi: str = "0.5") -> dict[str, Any]:
    return {
        "type": "oneWay",
        "position": {
            "coin": "BTC",
            "szi": szi,
            "entryPx": "50000",
            "positionValue": "25000",
            "unrealizedPnl": "0",
            "returnOnEquity": "0",
            "leverage": {"type": "cross", "value": 5},
            "maxLeverage": 40,
            "marginUsed": "5000",
            "cumFunding": {"allTime": "0", "sinceOpen": "0", "sinceChange": "0"},
        },
    }


def _trigger_entry(*, cloid: str, oid: int = 11, trigger_px: str = "60000") -> dict[str, Any]:
    return {
        "coin": "BTC",
        "side": "A",
        "limitPx": trigger_px,
        "sz": "0.5",
        "oid": oid,
        "timestamp": 1700000000000,
        "triggerCondition": f"Price above {trigger_px}",
        "isTrigger": True,
        "triggerPx": trigger_px,
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": True,
        "cloid": cloid,
    }


def _ok_statuses(statuses: list[Any]) -> dict[str, Any]:
    return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}


def _exchange_mock(
    *,
    legs: list[dict[str, Any]] | None = None,
    entries: list[dict[str, Any]] | None = None,
    bulk_statuses: list[Any] | None = None,
) -> MagicMock:
    exchange = MagicMock()
    exchange.info.meta.return_value = _META
    exchange.info.user_state.return_value = {
        "assetPositions": legs if legs is not None else [_leg()],
        "time": 1700000000000,
    }
    exchange.info.open_orders.return_value = entries or []
    exchange.info.frontend_open_orders.return_value = []
    exchange.bulk_orders.return_value = _ok_statuses(
        bulk_statuses if bulk_statuses is not None else ["waitingForTrigger", "waitingForTrigger"]
    )
    exchange.cancel.return_value = _ok_statuses([{"status": "ok"}])
    return exchange


def _adapter(exchange: MagicMock) -> HyperliquidAdapter:
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    adapter.attach_state_store(_store_mock())
    adapter._exchange = exchange
    adapter._connected = True
    return adapter


def _tp(price: str = "60000") -> TpSlAttachment:
    return TpSlAttachment(trigger_price=Decimal(price))


def _sl(price: str = "40000") -> TpSlAttachment:
    return TpSlAttachment(trigger_price=Decimal(price))


# ---- builder ----

def test_builder_grouping_and_cloids() -> None:
    requests, grouping = build_position_tpsl_action(
        coin="BTC",
        position_id=_POSITION_ID,
        close_buy=False,
        quantity=Decimal("0.5"),
        take_profit=_tp(),
        stop_loss=_sl(),
    )
    assert grouping == "positionTpsl"
    assert len(requests) == 2
    tp_request, sl_request = requests
    assert tp_request["reduce_only"] is True and sl_request["reduce_only"] is True
    assert tp_request["is_buy"] is False and sl_request["is_buy"] is False
    assert tp_request["sz"] == sl_request["sz"] == 0.5
    assert tp_request["order_type"] == {"trigger": {"isMarket": True, "triggerPx": 60000.0, "tpsl": "tp"}}
    assert tp_request["cloid"] == position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    assert sl_request["cloid"] == position_tpsl_cloid(_POSITION_ID, SL_CLOID_SUFFIX)


def test_builder_single_side_and_limit() -> None:
    limited = TpSlAttachment(trigger_price=Decimal("60000"), limit_price=Decimal("59000"))
    (request,), grouping = build_position_tpsl_action(
        coin="BTC",
        position_id=_POSITION_ID,
        close_buy=True,
        quantity=Decimal("1"),
        take_profit=limited,
        stop_loss=None,
    )
    assert grouping == "positionTpsl"
    assert request["is_buy"] is True
    assert request["order_type"] == {"trigger": {"isMarket": False, "triggerPx": 60000.0, "tpsl": "tp"}}


# ---- modify validation ----

async def test_modify_rejects_empty() -> None:
    adapter = _adapter(_exchange_mock())
    with pytest.raises(ValueError, match="at least one"):
        await adapter.modify_position_tpsl(_perp(), _POSITION_ID)


async def test_modify_rejects_spot() -> None:
    adapter = _adapter(_exchange_mock())
    with pytest.raises(InvalidSymbolError):
        await adapter.modify_position_tpsl(_spot(), "HYPE:oneWay", take_profit=_tp())


async def test_modify_no_leg() -> None:
    exchange = _exchange_mock(legs=[])
    adapter = _adapter(exchange)
    with pytest.raises(OrderNotFoundError, match=_POSITION_ID):
        await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp())
    exchange.bulk_orders.assert_not_called()


async def test_modify_wrong_position_id() -> None:
    adapter = _adapter(_exchange_mock())
    with pytest.raises(OrderNotFoundError):
        await adapter.modify_position_tpsl(_perp(), "ETH:oneWay", take_profit=_tp())


# ---- modify attach/replace ----

async def test_modify_attaches_both_without_cancels() -> None:
    exchange = _exchange_mock()
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp(), stop_loss=_sl())
    exchange.cancel.assert_not_called()
    (requests,), kwargs = exchange.bulk_orders.mock_calls[0][1], exchange.bulk_orders.mock_calls[0][2]
    assert kwargs.get("grouping") == "positionTpsl"
    assert len(requests) == 2


async def test_modify_merges_only_mentioned_side() -> None:
    """An existing TP leg stays working when only SL is mentioned: no cancel, one SL request."""
    tp_cloid = position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    exchange = _exchange_mock(entries=[_trigger_entry(cloid=tp_cloid, oid=11)])
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, stop_loss=_sl())
    exchange.cancel.assert_not_called()
    (requests,), _ = exchange.bulk_orders.mock_calls[0][1], exchange.bulk_orders.mock_calls[0][2]
    assert len(requests) == 1
    assert requests[0]["order_type"]["trigger"]["tpsl"] == "sl"


async def test_modify_replaces_mentioned_side_only() -> None:
    """An existing SL leg is cancelled and replaced when SL is mentioned; TP untouched."""
    sl_cloid = position_tpsl_cloid(_POSITION_ID, SL_CLOID_SUFFIX)
    tp_cloid = position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    exchange = _exchange_mock(entries=[
        _trigger_entry(cloid=tp_cloid, oid=11),
        _trigger_entry(cloid=sl_cloid, oid=12),
    ])
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, stop_loss=_sl())
    exchange.cancel.assert_called_once()
    (requests,), _ = exchange.bulk_orders.mock_calls[0][1], exchange.bulk_orders.mock_calls[0][2]
    assert len(requests) == 1
    assert requests[0]["order_type"]["trigger"]["tpsl"] == "sl"


async def test_modify_replaces_both_sides() -> None:
    entries = [
        _trigger_entry(cloid=position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX), oid=11),
        _trigger_entry(cloid=position_tpsl_cloid(_POSITION_ID, SL_CLOID_SUFFIX), oid=12),
    ]
    exchange = _exchange_mock(entries=entries)
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp(), stop_loss=_sl())
    assert exchange.cancel.call_count == 2
    (requests,), _ = exchange.bulk_orders.mock_calls[0][1], exchange.bulk_orders.mock_calls[0][2]
    assert len(requests) == 2


async def test_modify_tolerates_vanished_leg() -> None:
    tp_cloid = position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    exchange = _exchange_mock(entries=[_trigger_entry(cloid=tp_cloid, oid=11)])
    exchange.cancel.return_value = {
        "status": "err",
        "response": "Order was never placed, already canceled, or filled. asset=3",
    }
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp(), stop_loss=_sl())
    exchange.bulk_orders.assert_called_once()


async def test_modify_rejects_bad_trigger() -> None:
    exchange = _exchange_mock(bulk_statuses=[{"error": "Invalid TP/SL price. asset=3"}])
    adapter = _adapter(exchange)
    with pytest.raises(InvalidOrderError, match="Invalid TP/SL price"):
        await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp())


async def test_modify_indexes_acked_oids() -> None:
    exchange = _exchange_mock(
        bulk_statuses=[{"resting": {"oid": 101}}, {"resting": {"oid": 102}}]
    )
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp(), stop_loss=_sl())
    tp_id = position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    sl_id = position_tpsl_cloid(_POSITION_ID, SL_CLOID_SUFFIX)
    assert adapter._oid_clients["101"] == (tp_id, FillReason.TAKE_PROFIT, FillEntry.OUT)
    assert adapter._oid_clients["102"] == (sl_id, FillReason.STOP_LOSS, FillEntry.OUT)


async def test_short_leg_closes_with_buy() -> None:
    exchange = _exchange_mock(legs=[{**_leg(), "position": {**_leg()["position"], "szi": "-0.5"}}])
    adapter = _adapter(exchange)
    await adapter.modify_position_tpsl(_perp(), _POSITION_ID, take_profit=_tp())
    (requests,), _ = exchange.bulk_orders.mock_calls[0][1], exchange.bulk_orders.mock_calls[0][2]
    assert requests[0]["is_buy"] is True


# ---- get ----

def _status_response(*, status: str = "open", trigger_px: str = "60000",
                     limit_px: str = "60000", is_market: bool = True) -> dict[str, Any]:
    return {
        "status": "order" if status == "open" else status,
        "order": {
            "order": {
                "coin": "BTC",
                "limitPx": limit_px,
                "orderType": {"trigger": {"triggerPx": trigger_px, "isMarket": is_market, "tpsl": "tp"}},
            },
            "status": status,
        },
    }


async def test_get_no_leg_returns_none() -> None:
    adapter = _adapter(_exchange_mock(legs=[]))
    assert await adapter.get_position_tpsl(_perp(), _POSITION_ID) is None


async def test_get_spot_returns_none() -> None:
    adapter = _adapter(_exchange_mock())
    assert await adapter.get_position_tpsl(_spot(), "HYPE:oneWay") is None


async def test_get_reads_both_legs() -> None:
    exchange = _exchange_mock()
    exchange.info.query_order_by_cloid.side_effect = [
        _status_response(trigger_px="60000"),
        _status_response(trigger_px="40000"),
    ]
    adapter = _adapter(exchange)
    tp, sl = await adapter.get_position_tpsl(_perp(), _POSITION_ID) or (None, None)
    assert tp is not None and tp.trigger_price == Decimal("60000") and tp.limit_price is None
    assert sl is not None and sl.trigger_price == Decimal("40000") and sl.limit_price is None
    assert exchange.info.query_order_by_cloid.call_count == 2


async def test_get_limit_leg_carries_limit() -> None:
    exchange = _exchange_mock()
    exchange.info.query_order_by_cloid.side_effect = [
        _status_response(trigger_px="60000", limit_px="59000", is_market=False),
        {"status": "unknownOid"},
    ]
    adapter = _adapter(exchange)
    tp, sl = await adapter.get_position_tpsl(_perp(), _POSITION_ID) or (None, None)
    assert tp is not None and tp.limit_price == Decimal("59000")
    assert sl is None


async def test_get_parses_order_status_string_shape() -> None:
    """orderStatus renders orderType as a display string, not a nested dict (live shape)."""
    exchange = _exchange_mock()
    exchange.info.query_order_by_cloid.side_effect = [
        {
            "status": "order",
            "order": {
                "order": {
                    "coin": "BTC",
                    "limitPx": "126650.0",
                    "sz": "0.5",
                    "oid": 1,
                    "triggerPx": "126650.0",
                    "isTrigger": True,
                    "orderType": "Take Profit Market",
                    "reduceOnly": True,
                    "cloid": position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX),
                },
                "status": "open",
            },
        },
        {"status": "unknownOid"},
    ]
    adapter = _adapter(exchange)
    tp, sl = await adapter.get_position_tpsl(_perp(), _POSITION_ID) or (None, None)
    assert tp is not None and tp.trigger_price == Decimal("126650.0") and tp.limit_price is None
    assert sl is None


async def test_get_terminal_leg_reads_missing() -> None:
    exchange = _exchange_mock()
    exchange.info.query_order_by_cloid.side_effect = [
        _status_response(status="filled"),
        {"status": "unknownOid"},
    ]
    adapter = _adapter(exchange)
    assert await adapter.get_position_tpsl(_perp(), _POSITION_ID) == (None, None)


# ---- snapshot inclusion ----

async def test_fetch_open_orders_includes_position_legs_by_cloid() -> None:
    tp_cloid = position_tpsl_cloid(_POSITION_ID, TP_CLOID_SUFFIX)
    exchange = _exchange_mock(entries=[_trigger_entry(cloid=tp_cloid, oid=11)])
    adapter = _adapter(exchange)
    snapshot = await adapter.fetch_open_orders()
    assert tp_cloid in snapshot

"""Unit tests for WS/REST message translation (no network).

Entry shapes mirror the venue subscription/info references; the spot and
TP/SL rows mirror live testnet traffic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.hyperliquid.streams import (
    translate_balance,
    translate_fill,
    translate_order_entry,
    translate_position,
    translate_ticker,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    FillEntry,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument

_NOW = datetime(2026, 9, 1, tzinfo=UTC)


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def test_translate_fill_perp_open() -> None:
    fill = translate_fill(
        {
            "coin": "BTC",
            "px": "85800.0",
            "sz": "0.0004",
            "side": "B",
            "time": 1724361546645,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "0xabc",
            "oid": 1,
            "crossed": True,
            "fee": "0.01",
            "feeToken": "USDC",
            "tid": 7,
        },
        instrument=_perp(),
        client_order_id="cid-1",
    )
    assert fill.platform_fill_id == "0xabc:7"
    assert fill.fill_quantity == Decimal("0.0004")
    assert fill.fill_price == Decimal("85800.0")
    assert fill.fee_amount == Decimal("0.01")
    assert fill.entry is FillEntry.IN
    assert fill.correlation_id == "cid-1"


def test_translate_fill_rebate_and_close() -> None:
    fill = translate_fill(
        {
            "coin": "BTC",
            "px": "1",
            "sz": "1",
            "side": "A",
            "time": 1724361546645,
            "dir": "Close Short",
            "hash": "0xabc",
            "oid": 1,
            "fee": "-0.002",
            "feeToken": "USDC",
            "tid": 8,
        },
        instrument=_perp(),
        client_order_id="cid-2",
    )
    assert fill.fee_amount == Decimal("-0.002")  # rebate sign preserved
    assert fill.entry is FillEntry.OUT


def test_translate_fill_unknown_dir_has_no_entry() -> None:
    fill = translate_fill(
        {
            "coin": "BTC",
            "px": "1",
            "sz": "1",
            "side": "B",
            "time": 1,
            "dir": "SomethingNew",
            "hash": "0xabc",
            "oid": 1,
            "tid": 9,
        },
        instrument=_perp(),
        client_order_id="cid-3",
    )
    assert fill.entry is None
    assert fill.reason is None


def test_translate_fill_requires_ids() -> None:
    with pytest.raises(PlatformError):
        translate_fill({"coin": "BTC", "tid": 1}, instrument=_perp(), client_order_id="x")


def test_translate_position_long_and_short() -> None:
    long = translate_position(
        {
            "type": "oneWay",
            "position": {
                "coin": "BTC",
                "szi": "0.0004",
                "entryPx": "85800.0",
                "liquidationPx": "100.0",
                "marginUsed": "1.0",
                "unrealizedPnl": "0.5",
            },
        },
        instrument=_perp(),
        timestamp=_NOW,
    )
    assert long.quantity == Decimal("0.0004")
    assert long.average_entry_price == Decimal("85800.0")
    assert long.position_id == "BTC:oneWay"
    assert long.updated_at == _NOW
    short = translate_position(
        {"type": "oneWay", "position": {"coin": "BTC", "szi": "-2", "entryPx": "1"}},
        instrument=_perp(),
        timestamp=_NOW,
    )
    assert short.quantity == Decimal("-2")


def test_translate_position_rejects_non_one_way() -> None:
    with pytest.raises(PlatformError):
        translate_position(
            {"type": "hedged", "position": {"coin": "BTC", "szi": "1", "entryPx": "1"}},
            instrument=_perp(),
            timestamp=_NOW,
        )


def test_translate_balance() -> None:
    balance = translate_balance(
        {"coin": "USDC", "total": "100.5", "hold": "20.5", "entryNtl": "0.0"},
        currency="USDC",
        timestamp=_NOW,
    )
    assert (balance.free, balance.used, balance.total) == (
        Decimal("80.0"),
        Decimal("20.5"),
        Decimal("100.5"),
    )


def test_translate_order_entry_frontend_shape() -> None:
    order = translate_order_entry(
        {
            "coin": "BTC",
            "side": "B",
            "limitPx": "50000.0",
            "sz": "0.005",
            "origSz": "0.01",
            "oid": 123,
            "timestamp": 1724361546645,
            "statusTimestamp": 1724361546700,
            "orderType": "Limit",
            "tif": "Gtc",
            "triggerPx": "0.0",
            "isTrigger": False,
            "reduceOnly": False,
            "isPositionTpsl": False,
            "cloid": "0x" + "ab" * 16,
            "status": "open",
        },
        instrument=_perp(),
    )
    assert order.side is OrderSide.BUY
    assert order.order_type is OrderType.LIMIT
    assert order.time_in_force is TimeInForce.GTC
    assert order.quantity == Decimal("0.01")
    assert order.filled_quantity == Decimal("0.005")
    assert order.client_order_id == "0x" + "ab" * 16
    assert order.platform_order_id == "123"
    assert order.status is OrderStatus.OPEN


def test_translate_order_entry_thin_ws_shape_defaults() -> None:
    order = translate_order_entry(
        {
            "coin": "BTC",
            "side": "A",
            "limitPx": "1.5",
            "sz": "2",
            "oid": 9,
            "timestamp": 1724361546645,
        },
        instrument=_perp(),
    )
    assert order.side is OrderSide.SELL
    assert order.order_type is OrderType.LIMIT  # resting default
    assert order.time_in_force is TimeInForce.GTC  # omitted default
    assert order.status is OrderStatus.OPEN  # open-orders endpoint default
    assert order.client_order_id == ""


def test_translate_order_entry_trigger_display_types() -> None:
    stop = translate_order_entry(
        {
            "coin": "BTC",
            "side": "B",
            "limitPx": "0.0",
            "sz": "1",
            "oid": 1,
            "timestamp": 1,
            "orderType": "Stop Market",
        },
        instrument=_perp(),
    )
    assert stop.order_type is OrderType.STOP
    assert stop.stop_price is None
    take_limit = translate_order_entry(
        {
            "coin": "BTC",
            "side": "B",
            "limitPx": "2",
            "sz": "1",
            "oid": 1,
            "timestamp": 1,
            "orderType": "Take Profit Limit",
            "triggerPx": "1",
        },
        instrument=_perp(),
    )
    assert take_limit.order_type is OrderType.STOP_LIMIT
    assert take_limit.stop_price == Decimal("1")


def test_translate_order_entry_rejects() -> None:
    with pytest.raises(PlatformError):
        translate_order_entry({"coin": "BTC", "side": "X"}, instrument=_perp())
    with pytest.raises(PlatformError):
        translate_order_entry({"coin": "BTC", "side": "B"}, instrument=_perp())


def test_translate_ticker() -> None:
    ticker = translate_ticker("100.0", best_bid="99.0", best_ask="101.0", mark="100.5")
    assert (ticker.bid, ticker.ask, ticker.last, ticker.mark) == (
        Decimal("99.0"),
        Decimal("101.0"),
        Decimal("100.0"),
        Decimal("100.5"),
    )
    empty = translate_ticker(None, best_bid=None, best_ask=None)
    assert (empty.bid, empty.ask, empty.last, empty.mark) == (None, None, None, None)

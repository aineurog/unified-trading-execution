"""Unit: the integration harness's ``flatten_all`` closes each leg on its coin.

``restore_defaults`` and the ``flattened_book`` fixture both need a fully flat
book, and legs are per-coin — so teardown must place each market close against
the leg's OWN coin, never against one caller-supplied instrument (doing so
would size a close against the wrong market).  No credentials needed: a
duck-typed adapter records the orders.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import inspect
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.position import Position

_HELPERS_PATH = Path(__file__).resolve().parents[1] / "hyperliquid_integration" / "helpers.py"
_spec = importlib.util.spec_from_file_location("hlit_helpers", _HELPERS_PATH)
assert _spec is not None and _spec.loader is not None
helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(helpers)


def _leg(coin: str, quantity: Decimal) -> Position:
    return Position(
        instrument=helpers.make_perp(coin),
        quantity=quantity,
        average_entry_price=Decimal("100"),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        position_id=f"{coin}:oneWay",
    )


class _FakeAdapter:
    """Records teardown calls; legs and opens are injected per test."""

    def __init__(self, legs: list[Position], opens: dict[str, Any] | None = None) -> None:
        self._legs = legs
        self._opens = dict(opens or {})
        self.placed: list[Any] = []
        self.cancelled: list[str] = []

    async def fetch_open_orders(self) -> dict[str, Any]:
        return dict(self._opens)

    async def cancel_order(self, client_order_id: str) -> None:
        self.cancelled.append(client_order_id)
        self._opens.pop(client_order_id, None)

    async def fetch_positions(self) -> list[Position]:
        return list(self._legs)

    async def place_order(self, order: Any) -> None:
        self.placed.append(order)


def test_flatten_all_takes_no_instrument() -> None:
    assert list(inspect.signature(helpers.flatten_all).parameters) == ["adapter"]


async def test_closes_each_leg_on_its_own_coin() -> None:
    fake = _FakeAdapter([_leg("BTC", Decimal("0.01")), _leg("ETH", Decimal("-2"))])

    await helpers.flatten_all(fake)

    by_coin = {order.instrument.symbol: order for order in fake.placed}
    assert set(by_coin) == {"BTC", "ETH"}
    assert by_coin["BTC"].side is OrderSide.SELL
    assert by_coin["BTC"].quantity == Decimal("0.01")
    assert by_coin["ETH"].side is OrderSide.BUY
    assert by_coin["ETH"].quantity == Decimal("2")
    assert all(order.order_type is OrderType.MARKET for order in fake.placed)


async def test_cancels_every_open_before_closing() -> None:
    fake = _FakeAdapter([], {"o-1": object(), "o-2": object()})

    await helpers.flatten_all(fake)

    assert fake.cancelled == ["o-1", "o-2"]


async def test_skips_flat_and_unidentified_legs() -> None:
    flat = _leg("BTC", Decimal("0"))
    unidentified = dataclasses.replace(_leg("ETH", Decimal("1")), position_id=None)
    fake = _FakeAdapter([flat, unidentified])

    await helpers.flatten_all(fake)

    assert fake.placed == []

"""Order-type round-trip integration tests (implementation plan, Step 19).

For every guaranteed core order type we place, query, modify (where
meaningful), and cancel, asserting the unified ``OrderResult`` values
survive the round trip against a live MT5 demo account.

MT5 has no push notifications, so MARKET fills additionally prove the
polling loop detects the deal and publishes a ``FillEvent`` on the bus —
the adapter's core delivery contract.  Every order and position created
here is cleaned up after the test.

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
The instrument under test is EUR/USD; the broker symbol defaults to
``EURUSD`` and can be overridden via ``MT5_SYMBOL`` (e.g. ``EURUSD.m``).
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TypeVar

import pytest

from unified_trading_execution.events import Event, EventBus, FillEvent
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    OrderModification,
    OrderResult,
    TpSlAttachment,
)

from .helpers import (
    build_unified_order,
    cleanup_adapter,
    position_for_symbol,
    random_client_id,
    spec_price,
    spec_qty,
)

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_INSTRUMENT = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol=_BROKER_SYMBOL,
)
_ORDER_TYPES = (OrderType.MARKET, OrderType.LIMIT, OrderType.STOP, OrderType.STOP_LIMIT)
_TEvent = TypeVar("_TEvent", bound=Event)


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    """Shared config — the broker symbol is carried by ``_INSTRUMENT.platform_symbol``."""
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
    )


async def _live_quotes() -> tuple[Decimal, Decimal]:
    """Current (bid, ask) for the broker symbol — the terminal is connected."""
    import MetaTrader5 as mt5

    tick = await asyncio.to_thread(mt5.symbol_info_tick, _BROKER_SYMBOL)
    if tick is None:
        pytest.fail(f"no market quote for {_BROKER_SYMBOL}")
    return Decimal(str(tick.bid)), Decimal(str(tick.ask))


async def _assert_complete_result(result: OrderResult) -> None:
    assert result.platform_order_id is not None
    assert result.client_order_id
    assert isinstance(result.filled_quantity, Decimal)
    if result.average_fill_price is not None:
        assert isinstance(result.average_fill_price, Decimal)
    assert result.created_at.tzinfo is not None
    assert result.updated_at.tzinfo is not None


class _EventCollector:
    """Subscribe to event types on the bus and wait for matching events."""

    def __init__(self, event_bus: EventBus, *event_types: type[Event]) -> None:
        self._events: list[Event] = []
        for event_type in event_types:
            event_bus.subscribe(event_type, self._events.append)

    def of_type(self, event_type: type[_TEvent]) -> list[_TEvent]:
        return [event for event in self._events if isinstance(event, event_type)]

    async def wait_for(
        self,
        event_type: type[_TEvent],
        *,
        count: int = 1,
        timeout: float = 20.0,
    ) -> list[_TEvent]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            matching = self.of_type(event_type)
            if len(matching) >= count:
                return matching
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for {count}x {event_type.__name__}; "
                    f"captured {len(self._events)} events"
                )
            await asyncio.sleep(0.1)


@pytest.mark.parametrize(
    "order_type, side",
    [(order_type, side) for order_type in _ORDER_TYPES for side in OrderSide],
)
async def test_order_roundtrip(
    connected_adapter: MT5Adapter,
    order_type: OrderType,
    side: OrderSide,
) -> None:
    """place -> query -> modify -> cancel for every (order type, side) permutation."""
    bid, ask = await _live_quotes()
    qty = await spec_qty(connected_adapter, _INSTRUMENT)

    kwargs: dict = {"client_order_id": random_client_id("rt")}
    if order_type == OrderType.LIMIT:
        # Rest away from market so the order stays open for modify/cancel.
        reference = bid if side == OrderSide.SELL else ask
        kwargs["price"] = await spec_price(
            connected_adapter,
            _INSTRUMENT,
            reference * (Decimal("1.05") if side == OrderSide.SELL else Decimal("0.95")),
        )
    elif order_type == OrderType.STOP:
        reference = ask if side == OrderSide.BUY else bid
        kwargs["stop_price"] = await spec_price(
            connected_adapter,
            _INSTRUMENT,
            reference * (Decimal("1.005") if side == OrderSide.BUY else Decimal("0.995")),
        )
    elif order_type == OrderType.STOP_LIMIT:
        reference = ask if side == OrderSide.BUY else bid
        stop = await spec_price(
            connected_adapter,
            _INSTRUMENT,
            reference * (Decimal("1.005") if side == OrderSide.BUY else Decimal("0.995")),
        )
        kwargs["stop_price"] = stop
        kwargs["price"] = stop  # limit at the trigger

    order = build_unified_order(_INSTRUMENT, order_type, side, qty, **kwargs)

    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.client_order_id == order.client_order_id

        if order_type == OrderType.MARKET:
            assert result.status == OrderStatus.FILLED
            return

        queried = await connected_adapter.get_order_by_client_id(order.client_order_id)
        assert queried is not None
        assert queried.platform_order_id == result.platform_order_id
        await _assert_complete_result(queried)

        if order_type == OrderType.LIMIT:
            # Move the limit further from the market so it stays pending.
            new_price = await spec_price(
                connected_adapter,
                _INSTRUMENT,
                (order.price or Decimal("0"))
                * (Decimal("0.99") if side == OrderSide.BUY else Decimal("1.01")),
            )
            modification = OrderModification(
                client_order_id=order.client_order_id,
                price=new_price,
            )
        else:
            # Move the stop further from the market so it stays untriggered.
            new_stop = await spec_price(
                connected_adapter,
                _INSTRUMENT,
                (order.stop_price or Decimal("0"))
                * (Decimal("1.006") if side == OrderSide.BUY else Decimal("0.994")),
            )
            modification = OrderModification(
                client_order_id=order.client_order_id,
                stop_price=new_stop,
            )
        modified = await connected_adapter.modify_order(modification)
        await _assert_complete_result(modified)
        assert modified.client_order_id == order.client_order_id

        cancelled = await connected_adapter.cancel_order(order.client_order_id)
        await _assert_complete_result(cancelled)
        assert cancelled.status == OrderStatus.CANCELLED
    finally:
        await cleanup_adapter(connected_adapter)


async def test_gtd_order_accepted(connected_adapter: MT5Adapter) -> None:
    """A GTD limit order with a future, UTC-aware expire_at is placed and cancelled."""
    _, ask = await _live_quotes()
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    order = build_unified_order(
        _INSTRUMENT,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("gtd"),
        price=await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.95")),
        time_in_force=TimeInForce.GTD,
        expire_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.status == OrderStatus.OPEN

        cancelled = await connected_adapter.cancel_order(order.client_order_id)
        await _assert_complete_result(cancelled)
        assert cancelled.status == OrderStatus.CANCELLED
    finally:
        await cleanup_adapter(connected_adapter)


async def test_market_fill_published_as_fill_event(
    connected_adapter: MT5Adapter,
    event_bus: EventBus,
) -> None:
    """A MARKET fill is picked up by the polling loop and published on the bus."""
    collector = _EventCollector(event_bus, FillEvent)
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    order = build_unified_order(
        _INSTRUMENT,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("poll-fill"),
    )
    try:
        result = await connected_adapter.place_order(order)
        assert result.status == OrderStatus.FILLED

        fills = await collector.wait_for(FillEvent, count=1)
        match = [event for event in fills if event.fill.client_order_id == order.client_order_id]
        assert match, (
            f"no FillEvent for {order.client_order_id}; "
            f"saw {[e.fill.client_order_id for e in fills]}"
        )
        fill = match[0].fill
        assert fill.instrument.symbol == _INSTRUMENT.symbol
        assert fill.instrument.asset_class == _INSTRUMENT.asset_class
        assert fill.fill_quantity == qty
        assert fill.fill_price > 0
        assert fill.platform_fill_id
        assert fill.fill_timestamp.tzinfo is not None
    finally:
        await cleanup_adapter(connected_adapter)


async def test_instant_open_and_close(connected_adapter: MT5Adapter) -> None:
    """Market buy opens a position; immediate market sell flattens it."""
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    try:
        opened = await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.BUY,
                qty,
                client_order_id=random_client_id("open"),
            )
        )
        await _assert_complete_result(opened)
        assert opened.status == OrderStatus.FILLED

        closed = await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.SELL,
                qty,
                client_order_id=random_client_id("close"),
            )
        )
        await _assert_complete_result(closed)
        assert closed.status == OrderStatus.FILLED

        positions = await connected_adapter.fetch_positions()
        position = position_for_symbol(positions, _INSTRUMENT)
        assert position is None or position.quantity == 0
    finally:
        await cleanup_adapter(connected_adapter)


async def test_tp_sl_attachment_market(connected_adapter: MT5Adapter) -> None:
    """Native TP/SL levels are accepted on a MARKET order."""
    _, ask = await _live_quotes()
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    order = build_unified_order(
        _INSTRUMENT,
        OrderType.MARKET,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("tpsl"),
        take_profit=TpSlAttachment(
            trigger_price=await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("1.002"))
        ),
        stop_loss=TpSlAttachment(
            trigger_price=await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.998"))
        ),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.status == OrderStatus.FILLED
    finally:
        await cleanup_adapter(connected_adapter)


async def test_modify_tp_sl(connected_adapter: MT5Adapter) -> None:
    """modify_order updates attached TP/SL trigger prices on a pending order."""
    _, ask = await _live_quotes()
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    entry = await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("0.95"))
    order = build_unified_order(
        _INSTRUMENT,
        OrderType.LIMIT,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("modify-tpsl"),
        price=entry,
        take_profit=TpSlAttachment(
            trigger_price=await spec_price(connected_adapter, _INSTRUMENT, entry * Decimal("1.01"))
        ),
        stop_loss=TpSlAttachment(
            trigger_price=await spec_price(connected_adapter, _INSTRUMENT, entry * Decimal("0.99"))
        ),
    )
    try:
        result = await connected_adapter.place_order(order)
        await _assert_complete_result(result)
        assert result.status in (OrderStatus.OPEN, OrderStatus.PENDING)

        modification = OrderModification(
            client_order_id=order.client_order_id,
            take_profit=TpSlAttachment(
                trigger_price=await spec_price(
                    connected_adapter,
                    _INSTRUMENT,
                    entry * Decimal("1.02"),
                )
            ),
            stop_loss=TpSlAttachment(
                trigger_price=await spec_price(
                    connected_adapter,
                    _INSTRUMENT,
                    entry * Decimal("0.98"),
                )
            ),
        )
        modified = await connected_adapter.modify_order(modification)
        await _assert_complete_result(modified)
        assert modified.client_order_id == order.client_order_id
    finally:
        await cleanup_adapter(connected_adapter)


async def test_stop_trigger_direction(connected_adapter: MT5Adapter) -> None:
    """BUY stops rest above market, SELL stops rest below market — all OPEN."""
    bid, ask = await _live_quotes()
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    buy_stop = build_unified_order(
        _INSTRUMENT,
        OrderType.STOP,
        OrderSide.BUY,
        qty,
        client_order_id=random_client_id("stop-buy"),
        stop_price=await spec_price(connected_adapter, _INSTRUMENT, ask * Decimal("1.005")),
    )
    sell_stop = build_unified_order(
        _INSTRUMENT,
        OrderType.STOP,
        OrderSide.SELL,
        qty,
        client_order_id=random_client_id("stop-sell"),
        stop_price=await spec_price(connected_adapter, _INSTRUMENT, bid * Decimal("0.995")),
    )
    stop = await spec_price(connected_adapter, _INSTRUMENT, bid * Decimal("0.995"))
    sell_stop_limit = build_unified_order(
        _INSTRUMENT,
        OrderType.STOP_LIMIT,
        OrderSide.SELL,
        qty,
        client_order_id=random_client_id("stoplim-sell"),
        stop_price=stop,
        price=stop,
    )
    try:
        for order in (buy_stop, sell_stop, sell_stop_limit):
            result = await connected_adapter.place_order(order)
            await _assert_complete_result(result)
            assert result.status == OrderStatus.OPEN
    finally:
        await cleanup_adapter(connected_adapter)

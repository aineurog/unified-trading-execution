"""Unit tests for the MT5 polling loop.

Tests cases:
    - _poll_once: fetches orders, positions, balances, deals in single to_thread()
    - _poll_once: diff against last_orders detects new/updated/cancelled orders
    - _poll_once: diff against last_positions detects new/closed/modified positions
    - _poll_once: diff against last_balance detects balance changes
    - _poll_once: publishes FillEvent for new deals since last_deal_time
    - _poll_once: hedging legs are netted before PositionUpdateEvent
    - _poll_loop: runs until cancelled, respects poll_interval_seconds
    - _poll_loop: survives exceptions in a single cycle (logs and continues)
"""

from __future__ import annotations

import asyncio
from collections import namedtuple
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from unified_trading_execution.events import (
    BalanceUpdateEvent,
    Event,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.types.enums import AssetClass

# Fixed timestamps for deterministic deal/position diffs — deal times are
# second-granular and _last_deal_time is advanced to the newest deal seen.
_PAST = datetime(2024, 1, 1, tzinfo=UTC)
_DEAL_TIME = int(datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC).timestamp())

Mt5Order = namedtuple(
    "Mt5Order",
    ["ticket", "time_setup", "time_done", "state", "volume", "volume_current", "symbol"],
)
Mt5Position = namedtuple(
    "Mt5Position",
    ["ticket", "time", "time_update", "type", "symbol", "volume", "price_open"],
)
Mt5Deal = namedtuple(
    "Mt5Deal",
    [
        "ticket",
        "order",
        "time",
        "type",
        "entry",
        "symbol",
        "volume",
        "price",
        "commission",
        "fee",
        "position_id",
    ],
)


def _account(**overrides: object) -> MagicMock:
    """A MagicMock ``account_info()`` snapshot with sane defaults."""
    base = {
        "login": 12345678,
        "currency": "USD",
        "balance": Decimal("1000.00"),
        "equity": Decimal("1000.00"),
        "margin": Decimal("0"),
        "margin_free": Decimal("1000.00"),
    }
    base.update(overrides)
    return MagicMock(**base)


def _eurusd_symbol_info() -> MagicMock:
    return MagicMock(path="Forex\\EURUSD")


class TestPollOnce:
    """Single poll cycle behaviour."""

    async def test_fetches_all_state_in_one_to_thread(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """All MT5 calls happen inside a single asyncio.to_thread block."""
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = (
            Mt5Order(
                ticket=1001,
                time_setup=int(_PAST.timestamp()),
                time_done=0,
                state=1,
                volume=0.1,
                volume_current=0.1,
                symbol="EURUSD.m",
            ),
        )
        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.1,
                price_open=1.1000,
            ),
        )
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (
            Mt5Deal(
                ticket=3001,
                order=1001,
                time=_DEAL_TIME,
                type=0,
                entry=0,
                symbol="EURUSD.m",
                volume=0.1,
                price=1.1010,
                commission=0.0,
                fee=0.0,
                position_id=2001,
            ),
        )
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        adapter._last_deal_time = _PAST

        with patch("asyncio.to_thread", wraps=asyncio.to_thread) as mock_to_thread:
            await adapter._poll_once()

        mock_to_thread.assert_called_once()
        mock_mt5_module.orders_get.assert_called_once()
        mock_mt5_module.positions_get.assert_called_once()
        mock_mt5_module.account_info.assert_called_once()
        mock_mt5_module.history_deals_get.assert_called_once()

    async def test_detects_new_order(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """New order in orders_get → tracked internally, no order event published.

        Order lifecycle events (Placed/Modified/Cancelled) are emitted by the
        engine's dispatch, never by the polling loop — so a new order updates
        ``_last_orders`` and nothing else.
        """
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST

        captured: list[Event] = []
        event_bus.subscribe(Event, captured.append)

        await adapter._poll_once()  # baseline — no open orders
        captured.clear()

        mock_mt5_module.orders_get.return_value = (
            Mt5Order(
                ticket=1001,
                time_setup=int(_PAST.timestamp()),
                time_done=0,
                state=1,
                volume=0.1,
                volume_current=0.1,
                symbol="EURUSD.m",
            ),
        )
        await adapter._poll_once()

        assert 1001 in adapter._last_orders
        assert captured == []

    async def test_detects_filled_order(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Order disappeared from open orders + new fill → FillEvent."""
        adapter._build_reverse_alias()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST
        adapter._ticket_to_order_id = {1001: "client-1"}

        # Baseline: one open pending order.
        mock_mt5_module.orders_get.return_value = (
            Mt5Order(
                ticket=1001,
                time_setup=int(_PAST.timestamp()),
                time_done=0,
                state=1,
                volume=0.1,
                volume_current=0.1,
                symbol="EURUSD.m",
            ),
        )
        await adapter._poll_once()

        # Order is gone from open orders; a fill shows up in the deal history.
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.history_deals_get.return_value = (
            Mt5Deal(
                ticket=3001,
                order=1001,
                time=_DEAL_TIME,
                type=0,
                entry=0,
                symbol="EURUSD.m",
                volume=0.1,
                price=1.1010,
                commission=0.0,
                fee=0.0,
                position_id=2001,
            ),
        )
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()

        fills: list[FillEvent] = []
        event_bus.subscribe(FillEvent, fills.append)
        await adapter._poll_once()

        assert 1001 not in adapter._last_orders
        assert len(fills) == 1
        assert fills[0].correlation_id == "client-1"
        assert fills[0].fill.platform_fill_id == "3001"
        assert fills[0].fill.instrument.asset_class == AssetClass.MARGIN_FX
        assert fills[0].fill.fill_quantity == Decimal("0.1")
        assert fills[0].fill.position_id == "2001"

    async def test_detects_position_change(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Position quantity/price changed → PositionUpdateEvent."""
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()

        positions: list[PositionUpdateEvent] = []
        event_bus.subscribe(PositionUpdateEvent, positions.append)

        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.1,
                price_open=1.1000,
            ),
        )
        await adapter._poll_once()  # initial sighting
        positions.clear()

        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.2,
                price_open=1.1100,
            ),
        )
        await adapter._poll_once()  # modified

        assert len(positions) == 1
        assert positions[0].position.quantity == Decimal("0.2")
        assert positions[0].position.average_entry_price == Decimal("1.11")

    async def test_detects_balance_change(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Balance changed → BalanceUpdateEvent."""
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST

        balances: list[BalanceUpdateEvent] = []
        event_bus.subscribe(BalanceUpdateEvent, balances.append)

        mock_mt5_module.account_info.return_value = _account()
        await adapter._poll_once()  # initial sighting
        balances.clear()

        mock_mt5_module.account_info.return_value = _account(
            balance=Decimal("1050.00"),
            equity=Decimal("1050.00"),
            margin_free=Decimal("1050.00"),
        )
        await adapter._poll_once()  # changed

        assert len(balances) == 1
        assert balances[0].balance.total == Decimal("1050.00")
        assert balances[0].balance.free == Decimal("1050.00")
        assert balances[0].balance.used == Decimal("0")

    async def test_no_event_when_nothing_changed(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Identical state → no events published."""
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.1,
                price_open=1.1000,
            ),
        )

        captured: list[Event] = []
        event_bus.subscribe(Event, captured.append)

        await adapter._poll_once()  # establish baseline (events fire here)
        captured.clear()
        await adapter._poll_once()  # identical state → nothing

        assert captured == []

    async def test_hedging_legs_netted(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Two legs (buy + sell on same instrument) are netted to one Position."""
        adapter._build_reverse_alias()
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = _PAST
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.5,
                price_open=1.1000,
            ),
            Mt5Position(
                ticket=2002,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=1,
                symbol="EURUSD.m",
                volume=0.3,
                price_open=1.1050,
            ),
        )

        positions: list[PositionUpdateEvent] = []
        event_bus.subscribe(PositionUpdateEvent, positions.append)
        await adapter._poll_once()

        assert len(positions) == 1
        netted = positions[0].position
        assert netted.quantity == Decimal("0.2")
        expected_avg = (
            Decimal("0.5") * Decimal("1.1") + Decimal("0.3") * Decimal("1.105")
        ) / Decimal("0.8")
        assert netted.average_entry_price == expected_avg


class TestPollLoop:
    """Background polling loop lifecycle."""

    async def test_respects_poll_interval(self) -> None:
        """Cycles are spaced by poll_interval_seconds."""
        ...

    async def test_survives_cycle_exception(self) -> None:
        """Exception in one cycle doesn't kill the loop."""
        ...

    async def test_cancelled_cleanly(self) -> None:
        """Cancelling the task stops the loop without errors."""
        ...

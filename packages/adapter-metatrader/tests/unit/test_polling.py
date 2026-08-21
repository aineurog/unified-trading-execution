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
import logging
import time
from collections import namedtuple
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from uuid_extensions import uuid7

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    Event,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.mt5.adapter import (
    _DEAL_QUERY_BACKLOG_SECONDS,
    _DEAL_QUERY_FORWARD_SECONDS,
)
from unified_trading_execution.mt5.comments import encode_client_order_id
from unified_trading_execution.types.enums import AssetClass

# Fixed timestamps for deterministic deal/position diffs — deal times are
# second-granular and _last_deal_time (a raw server-as-epoch int) is advanced
# to the newest deal seen.
_PAST = datetime(2024, 1, 1, tzinfo=UTC)
_DEAL_TIME = int(datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC).timestamp())

Mt5Order = namedtuple(
    "Mt5Order",
    [
        "ticket",
        "time_setup",
        "time_done",
        "state",
        "volume_initial",
        "volume_current",
        "symbol",
        "comment",
    ],
    defaults=[""],
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
        "comment",
    ],
    defaults=[""],
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
    return MagicMock(path="Forex\\EURUSD", currency_base="EUR", currency_profit="USD")


class TestPollOnce:
    """Single poll cycle behaviour."""

    async def test_fetches_all_state_in_one_to_thread(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """All MT5 calls happen inside a single asyncio.to_thread block."""
        mock_mt5_module.orders_get.return_value = (
            Mt5Order(
                ticket=1001,
                time_setup=int(_PAST.timestamp()),
                time_done=0,
                state=1,
                volume_initial=0.1,
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
        adapter._last_deal_time = int(_PAST.timestamp())

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
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())

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
                volume_initial=0.1,
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
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())
        adapter._ticket_to_order_id = {1001: "client-1"}

        # Baseline: one open pending order.
        mock_mt5_module.orders_get.return_value = (
            Mt5Order(
                ticket=1001,
                time_setup=int(_PAST.timestamp()),
                time_done=0,
                state=1,
                volume_initial=0.1,
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

    def test_market_order_fill_attributed_by_deal_ticket(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A market order's fill (``deal.order == 0``) is attributed via the
        deal ticket — ``place_order`` records the deal ticket, not an order
        ticket, for market executions."""
        adapter._ticket_to_order_id = {3001: "client-1"}  # market order → deal ticket
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        deal = Mt5Deal(
            ticket=3001,
            order=0,  # market execution has no order ticket
            time=_DEAL_TIME,
            type=0,
            entry=0,
            symbol="EURUSD.m",
            volume=0.1,
            price=1.1010,
            commission=0.0,
            fee=0.0,
            position_id=2001,
        )

        instruments = adapter._resolve_poll_instruments(mock_mt5_module, (), (deal,))
        fill = adapter._build_fill(deal, instruments, _account())

        assert fill.client_order_id == "client-1"
        assert fill.correlation_id == "client-1"

    def test_fill_comment_attributed_before_ticket_maps(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A deal carrying the engine's U: tag attributes the fill by comment
        even when the ticket maps are empty (fresh process after a restart)."""
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        deal = Mt5Deal(
            ticket=3001,
            order=0,  # market execution has no order ticket
            time=_DEAL_TIME,
            type=0,
            entry=0,
            symbol="EURUSD.m",
            volume=0.1,
            price=1.1010,
            commission=0.0,
            fee=0.0,
            position_id=2001,
            comment=comment,
        )

        instruments = adapter._resolve_poll_instruments(mock_mt5_module, (), (deal,))
        fill = adapter._build_fill(deal, instruments, _account())

        assert fill.client_order_id == cid
        assert fill.correlation_id == cid

    async def test_detects_position_change(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Position quantity/price changed → PositionUpdateEvent."""
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())
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
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())

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

    async def test_balance_float_rounding_satisfies_invariant(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Float account values (production shape) never break free+used==total.

        MT5 reports ``margin``, ``equity``, and ``margin_free`` as three
        independent floats; ``margin_free = equity - margin`` can round such
        that ``Decimal(str())`` of all three no longer satisfies the core
        Balance invariant.  ``_process_balance`` must derive one field from
        the others so the invariant holds exactly.
        """
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())

        mock_mt5_module.account_info.return_value = MagicMock(
            currency="USD",
            margin=222.22,
            equity=9876.54,
            margin_free=9876.54 - 222.22,  # 9654.320000000002 as a float
        )

        balances: list[BalanceUpdateEvent] = []
        event_bus.subscribe(BalanceUpdateEvent, balances.append)
        await adapter._poll_once()

        assert len(balances) == 1
        b = balances[0].balance
        assert b.free + b.used == b.total  # invariant holds exactly

    async def test_no_event_when_nothing_changed(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Identical state → no events published."""
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())
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
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())
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

    async def test_same_second_deals_both_published(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """Two deals stamped in the same second are both reported.

        Time-only diffing would drop the second (its time is not strictly
        greater than the first); monotonic ticket dedup catches both.
        """
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
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
            Mt5Deal(
                ticket=3002,
                order=1001,
                time=_DEAL_TIME,
                type=0,
                entry=1,
                symbol="EURUSD.m",
                volume=0.2,
                price=1.1020,
                commission=0.0,
                fee=0.0,
                position_id=2001,
            ),
        )
        adapter._last_deal_time = int(_PAST.timestamp())
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()

        fills: list[FillEvent] = []
        event_bus.subscribe(FillEvent, fills.append)
        await adapter._poll_once()

        assert len(fills) == 2
        assert adapter._last_deal_ticket == 3002

    async def test_deals_not_re_published_on_refetch(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """The same deal re-fetched next cycle is not published twice."""
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
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
        adapter._last_deal_time = int(_PAST.timestamp())

        fills: list[FillEvent] = []
        event_bus.subscribe(FillEvent, fills.append)
        await adapter._poll_once()
        await adapter._poll_once()

        assert len(fills) == 1
        assert adapter._last_deal_ticket == 3001

    async def test_server_offset_deal_normalized_to_real_utc(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """A deal stored server-as-epoch (real time + server offset) is
        published with a real-UTC ``fill_timestamp`` and advances the dedup
        baseline ``_last_deal_time`` in the raw server-as-epoch basis."""
        offset = 10800  # e.g. a UTC+3 broker
        adapter._server_time_offset = offset
        server_stamp = _DEAL_TIME + offset  # how the terminal stores it
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (
            Mt5Deal(
                ticket=3001,
                order=1001,
                time=server_stamp,
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
        adapter._last_deal_time = int(_PAST.timestamp())

        fills: list[FillEvent] = []
        event_bus.subscribe(FillEvent, fills.append)
        await adapter._poll_once()

        assert len(fills) == 1
        assert fills[0].fill.fill_timestamp == datetime.fromtimestamp(_DEAL_TIME, tz=UTC)
        assert adapter._last_deal_time == _DEAL_TIME + offset

    async def test_history_window_shifted_by_server_offset(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """``history_deals_get`` is queried in the server-as-epoch basis: the
        window is shifted by the server offset and padded by the margins."""
        offset = 7200
        adapter._server_time_offset = offset
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        mock_mt5_module.symbol_info.return_value = _eurusd_symbol_info()
        adapter._last_deal_time = int(_PAST.timestamp()) + offset

        await adapter._poll_once()

        call_args = mock_mt5_module.history_deals_get.call_args.args
        assert call_args[0] == (adapter._last_deal_time - _DEAL_QUERY_BACKLOG_SECONDS)
        expected_to = int(time.time()) + offset + _DEAL_QUERY_FORWARD_SECONDS
        assert expected_to - 1 <= call_args[1] <= expected_to + 1

    async def test_none_snapshot_call_raises_immediately(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """An intermediate ``None`` is caught before later calls reset last_error.

        Previously a single end-of-snapshot check saw only the final call's
        status and an ``orders_get()`` failure could slip through as "no data".
        """
        mock_mt5_module.orders_get.return_value = None
        mock_mt5_module.positions_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        mock_mt5_module.last_error.return_value = (10011, "processing error")
        adapter._last_deal_time = int(_PAST.timestamp())

        with pytest.raises(PlatformError):
            await adapter._poll_once()

    async def test_positions_baseline_committed_before_publish(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, event_bus: EventBus
    ) -> None:
        """A publish failure mid-cycle leaves the baseline advanced — no re-report."""
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())
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

        with (
            patch.object(adapter, "_publish_position", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError),
        ):
            await adapter._poll_once()

        assert len(adapter._last_positions) == 1

    async def test_bad_symbol_isolated_not_whole_cycle(
        self,
        mock_mt5_module: MagicMock,
        adapter: MT5Adapter,
        event_bus: EventBus,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """One unresolvable symbol doesn't silence events for the rest."""
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = ()
        adapter._last_deal_time = int(_PAST.timestamp())

        def _symbol_info(symbol: str) -> MagicMock:
            if symbol == "CRYPTOX.m":
                return MagicMock(path="Other\\CryptoX")
            return MagicMock(path="Forex\\EURUSD", currency_base="EUR", currency_profit="USD")

        mock_mt5_module.symbol_info.side_effect = _symbol_info
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
            Mt5Position(
                ticket=2002,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="CRYPTOX.m",
                volume=0.5,
                price_open=60000.0,
            ),
        )

        positions: list[PositionUpdateEvent] = []
        event_bus.subscribe(PositionUpdateEvent, positions.append)
        with caplog.at_level(logging.WARNING, logger="unified_trading_execution.mt5.adapter"):
            await adapter._poll_once()

        assert len(positions) == 1  # only EURUSD.m survives
        assert positions[0].position.instrument.symbol == "EUR"
        assert "CRYPTOX.m" in adapter._failed_symbols
        assert "CRYPTOX.m" in caplog.text

    async def test_transient_selection_failure_retried_next_cycle(
        self,
        mock_mt5_module: MagicMock,
        adapter: MT5Adapter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A transient symbol_select failure is retried — not blacklisted.

        ``_resolve_poll_instruments`` caches permanent errors (unknown symbol /
        unrecognized path) in ``_failed_symbols`` but lets transient
        ``UteError`` subclasses retry next cycle, so one flaky IPC call doesn't
        skip the symbol for the rest of the session.
        """
        position = Mt5Position(
            ticket=2001,
            time=_DEAL_TIME,
            time_update=_DEAL_TIME,
            type=0,
            symbol="GOLD.m",
            volume=0.1,
            price_open=1.1000,
        )

        # First sighting: symbol_select() flakily fails with a transient code.
        mock_mt5_module.symbol_select.side_effect = [False, True]
        mock_mt5_module.last_error.side_effect = [(4302, "not selected"), (1, "")]
        mock_mt5_module.symbol_info.return_value = MagicMock(
            path="Metals\\XAUUSD", currency_base="XAU", currency_profit="USD"
        )

        with caplog.at_level(logging.WARNING, logger="unified_trading_execution.mt5.adapter"):
            resolved = adapter._resolve_poll_instruments(mock_mt5_module, (position,), ())

        assert resolved == {}
        assert "GOLD.m" not in adapter._failed_symbols
        assert "retry next cycle" in caplog.text

        # Next cycle the symbol resolves normally — not blacklisted.
        resolved = adapter._resolve_poll_instruments(mock_mt5_module, (position,), ())

        assert "GOLD.m" in resolved
        assert resolved["GOLD.m"].symbol == "XAU"


class TestAssetClassFromPath:
    """Asset-class derivation from symbol_info().path."""

    def test_first_segment_wins_over_substring(self, adapter: MT5Adapter) -> None:
        """'Stocks\\CryptoMining' is STOCK — substring 'CRYPTO' must not match."""
        assert adapter._asset_class_from_path("Stocks\\CryptoMining\\CRPT") == AssetClass.STOCK
        assert adapter._asset_class_from_path("Forex\\EURUSD") == AssetClass.MARGIN_FX
        assert adapter._asset_class_from_path("Metals\\XAUUSD") == AssetClass.MARGIN_FX

    def test_unrecognized_first_segment_raises(self, adapter: MT5Adapter) -> None:
        """Unknown first segment raises — never a silent default."""
        with pytest.raises(ValueError):
            adapter._asset_class_from_path("Strange\\EURUSD")


class TestPollLoop:
    """Background polling loop lifecycle."""

    async def test_respects_poll_interval(self, adapter: MT5Adapter) -> None:
        """Cycles are spaced by poll_interval_seconds."""
        adapter._connected = True
        intervals: list[float] = []

        async def fake_sleep(interval: float) -> None:
            intervals.append(interval)
            if len(intervals) >= 2:
                adapter._connected = False

        with (
            patch.object(adapter, "_poll_once", new=AsyncMock()),
            patch("asyncio.sleep", new=fake_sleep),
        ):
            await adapter._poll_loop()

        assert intervals == [adapter._config.poll_interval_seconds] * 2

    async def test_survives_cycle_exception(
        self, adapter: MT5Adapter, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Exception in one cycle doesn't kill the loop."""
        adapter._connected = True
        calls = 0

        async def flaky_poll() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")

        async def fake_sleep(_interval: float) -> None:
            if calls >= 2:
                adapter._connected = False

        with (
            caplog.at_level(logging.WARNING, logger="unified_trading_execution.mt5.adapter"),
            patch.object(adapter, "_poll_once", new=flaky_poll),
            patch("asyncio.sleep", new=fake_sleep),
        ):
            await adapter._poll_loop()

        assert calls == 2
        assert "Poll cycle failed" in caplog.text

    async def test_cancelled_cleanly(self, adapter: MT5Adapter) -> None:
        """Cancelling the task stops the loop without errors."""
        adapter._connected = True
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocking_sleep(_interval: float) -> None:
            started.set()
            await release.wait()

        with (
            patch.object(adapter, "_poll_once", new=AsyncMock()),
            patch("asyncio.sleep", new=blocking_sleep),
        ):
            task = asyncio.create_task(adapter._poll_loop())
            await asyncio.wait_for(started.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

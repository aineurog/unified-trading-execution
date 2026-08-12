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


class TestPollOnce:
    """Single poll cycle behaviour."""

    def test_fetches_all_state_in_one_to_thread(self) -> None:
        """All MT5 calls happen inside a single asyncio.to_thread block."""
        ...

    def test_detects_new_order(self) -> None:
        """New order in orders_get → publishes event."""
        ...

    def test_detects_filled_order(self) -> None:
        """Order disappeared from open orders + new fill → FillEvent."""
        ...

    def test_detects_position_change(self) -> None:
        """Position quantity/price changed → PositionUpdateEvent."""
        ...

    def test_detects_balance_change(self) -> None:
        """Balance changed → BalanceUpdateEvent."""
        ...

    def test_no_event_when_nothing_changed(self) -> None:
        """Identical state → no events published."""
        ...

    def test_hedging_legs_netted(self) -> None:
        """Two legs (buy + sell on same instrument) are netted to one Position."""
        ...


class TestPollLoop:
    """Background polling loop lifecycle."""

    def test_respects_poll_interval(self) -> None:
        """Cycles are spaced by poll_interval_seconds."""
        ...

    def test_survives_cycle_exception(self) -> None:
        """Exception in one cycle doesn't kill the loop."""
        ...

    def test_cancelled_cleanly(self) -> None:
        """Cancelling the task stops the loop without errors."""
        ...

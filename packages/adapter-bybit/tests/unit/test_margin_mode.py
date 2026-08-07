"""Unit tests for MarginMode (Phase 3, Step 4)."""

from __future__ import annotations

from unified_trading_execution.bybit.margin import MarginMode


class TestMarginMode:
    def test_cross_value(self) -> None:
        assert MarginMode.CROSS == "cross"
        assert MarginMode.CROSS.value == "cross"

    def test_isolated_value(self) -> None:
        assert MarginMode.ISOLATED == "isolated"
        assert MarginMode.ISOLATED.value == "isolated"

    def test_is_str_enum(self) -> None:
        assert str(MarginMode.CROSS) == "cross"
        assert str(MarginMode.ISOLATED) == "isolated"

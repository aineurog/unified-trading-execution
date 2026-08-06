"""Unit tests for MarginMode and LeverageConfig (Phase 3, Step 4)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from unified_trading_execution.bybit.margin import LeverageConfig, MarginMode


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


class TestLeverageConfig:
    def test_defaults(self) -> None:
        cfg = LeverageConfig()
        assert cfg.on_drift == "reapply"
        assert cfg.auto_apply_on_connect is True
        assert cfg.strict_check is False
        assert cfg.block_on_open_position is True

    def test_frozen(self) -> None:
        cfg = LeverageConfig()
        with pytest.raises(FrozenInstanceError):
            cfg.on_drift = "notify"  # type: ignore[misc]

    def test_slots(self) -> None:
        cfg = LeverageConfig()
        assert not hasattr(cfg, "__dict__")

    def test_custom_values(self) -> None:
        cfg = LeverageConfig(
            on_drift="halt",
            auto_apply_on_connect=False,
            strict_check=True,
            block_on_open_position=False,
        )
        assert cfg.on_drift == "halt"
        assert cfg.auto_apply_on_connect is False
        assert cfg.strict_check is True
        assert cfg.block_on_open_position is False

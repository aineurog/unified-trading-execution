"""Unit tests for MarginMode (Phase 3, Step 4)."""

from __future__ import annotations

import pytest

from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.enums import MarginMode


class TestMarginMode:
    def test_cross_value(self) -> None:
        assert MarginMode.CROSS.value == "cross"

    def test_isolated_value(self) -> None:
        assert MarginMode.ISOLATED.value == "isolated"

    def test_is_str_enum(self) -> None:
        assert str(MarginMode.CROSS) == "cross"
        assert str(MarginMode.ISOLATED) == "isolated"


class TestBybitConfigMarginMode:
    def test_default_is_cross(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s")
        assert config.margin_mode is MarginMode.CROSS

    def test_accepts_enum(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", margin_mode=MarginMode.ISOLATED)
        assert config.margin_mode is MarginMode.ISOLATED

    def test_accepts_string(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", margin_mode="isolated")
        assert config.margin_mode is MarginMode.ISOLATED

    def test_rejects_unknown_string(self) -> None:
        with pytest.raises(ValueError):
            BybitConfig(api_key="k", api_secret="s", margin_mode="portfolio")

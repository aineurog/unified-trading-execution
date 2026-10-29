"""Unit tests for MT5 static position-protection capabilities."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.adapter import PositionProtectionCapability
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.trailing.types import TrailingBasis
from unified_trading_execution.types.enums import AssetClass, OptionRight
from unified_trading_execution.types.instrument import Instrument

_DEFAULT_SYMBOL = "EURUSD.m"


def _instrument(
    *,
    asset_class: AssetClass = AssetClass.MARGIN_FX,
    platform_symbol: str | None = _DEFAULT_SYMBOL,
) -> Instrument:
    if asset_class is AssetClass.FUTURES:
        return Instrument(
            symbol="ES",
            asset_class=asset_class,
            quote_currency="USD",
            multiplier=50,
            platform_symbol=platform_symbol,
        )
    if asset_class is AssetClass.OPTION:
        return Instrument(
            symbol="AAPL",
            asset_class=asset_class,
            currency="USD",
            expiry=date(2026, 12, 18),
            strike=Decimal("200"),
            option_right=OptionRight.CALL,
            multiplier=100,
            platform_symbol=platform_symbol,
        )
    return Instrument(
        symbol="EUR",
        asset_class=asset_class,
        quote_currency="USD",
        platform_symbol=platform_symbol,
    )


class TestPositionProtectionCapability:
    def test_declares_full_static_protection(self, adapter: MT5Adapter) -> None:
        _assert_supported(adapter.position_protection_capability(_instrument()))

    @pytest.mark.parametrize("asset_class", list(AssetClass))
    def test_every_asset_class_uses_the_same_position_api(
        self, adapter: MT5Adapter, asset_class: AssetClass
    ) -> None:
        _assert_supported(
            adapter.position_protection_capability(_instrument(asset_class=asset_class))
        )

    def test_trigger_bases_are_exactly_bid_and_ask(self, adapter: MT5Adapter) -> None:
        capability = adapter.position_protection_capability(_instrument())

        assert capability.trigger_bases == frozenset({TrailingBasis.BID, TrailingBasis.ASK})

    def test_missing_platform_symbol_falls_back_to_unsupported(self, adapter: MT5Adapter) -> None:
        capability = adapter.position_protection_capability(_instrument(platform_symbol=None))

        _assert_unsupported(capability)

    def test_blank_platform_symbol_falls_back_to_unsupported(self, adapter: MT5Adapter) -> None:
        _assert_unsupported(adapter.position_protection_capability(_instrument(platform_symbol="")))

    def test_whitespace_platform_symbol_falls_back_to_unsupported(
        self, adapter: MT5Adapter
    ) -> None:
        _assert_unsupported(
            adapter.position_protection_capability(_instrument(platform_symbol="   "))
        )

    def test_construction_does_not_read_the_terminal(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        _assert_supported(adapter.position_protection_capability(_instrument()))

        mock_mt5_module.symbol_info.assert_not_called()
        mock_mt5_module.symbol_info_tick.assert_not_called()
        mock_mt5_module.positions_get.assert_not_called()
        mock_mt5_module.symbol_select.assert_not_called()
        mock_mt5_module.symbols_get.assert_not_called()
        mock_mt5_module.initialize.assert_not_called()
        mock_mt5_module.account_info.assert_not_called()

    def test_repeated_calls_return_equal_declarations(self, adapter: MT5Adapter) -> None:
        first = adapter.position_protection_capability(_instrument())
        second = adapter.position_protection_capability(_instrument())

        assert first == second

    def test_no_native_trailing_is_declared(self, adapter: MT5Adapter) -> None:
        capability = adapter.trailing_capability(_instrument())

        assert capability.supported is False
        assert capability.offset_types == frozenset()
        assert capability.bases == frozenset()
        assert capability.activations == frozenset()
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is False
        assert capability.min_reprice_interval_seconds == 0.0


def _assert_supported(capability: PositionProtectionCapability) -> None:
    assert capability.supported is True
    assert capability.can_set_stop is True
    assert capability.can_set_take_profit is True
    assert capability.can_clear_stop is True
    assert capability.can_clear_take_profit is True
    assert capability.preserves_unmentioned_side is True
    assert capability.trigger_bases == frozenset({TrailingBasis.BID, TrailingBasis.ASK})
    assert capability.min_reprice_interval_seconds == 1.0
    assert capability.reprice_gap is False


def _assert_unsupported(capability: PositionProtectionCapability) -> None:
    assert capability.supported is False
    assert capability.can_set_stop is False
    assert capability.can_set_take_profit is False
    assert capability.can_clear_stop is False
    assert capability.can_clear_take_profit is False
    assert capability.preserves_unmentioned_side is False
    assert capability.trigger_bases == frozenset()
    assert capability.min_reprice_interval_seconds == 0.0
    assert capability.reprice_gap is False

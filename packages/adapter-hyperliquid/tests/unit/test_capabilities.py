"""Unit tests for Hyperliquid position protection and trailing capabilities."""

from __future__ import annotations

from typing import Any

from unified_trading_execution.adapter import TrailingBasis
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _perp() -> dict[str, Any]:
    return {
        "symbol": "BTC",
        "quote_currency": "USDC",
        "asset_class": AssetClass.FUTURES,
        "currency": "USDC",
        "multiplier": 1,
    }


def _spot() -> dict[str, Any]:
    return {
        "symbol": "HYPE",
        "quote_currency": "USDC",
        "asset_class": AssetClass.SPOT,
    }


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(
        wallet_address=_TEST_ADDRESS,
        private_key=_TEST_KEY,
    )


def _instrument(**kwargs: Any) -> Instrument:
    """Build an Instrument with given overrides."""
    defaults: dict[str, Any] = _perp()
    defaults.update(kwargs)
    return Instrument(**defaults)


class TestPositionProtectionCapability:
    def test_declares_full_protection_for_perps(self):
        """Perp markets report full TP/SL support with MARK trigger basis."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.position_protection_capability(_instrument())

        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases == frozenset({TrailingBasis.MARK})
        assert capability.min_reprice_interval_seconds == 15.0
        assert capability.reprice_gap is False

    def test_spot_is_unsupported(self):
        """Spot markets have no position leg and report unsupported."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.position_protection_capability(
            _instrument(asset_class=AssetClass.SPOT)
        )

        assert capability.supported is False
        assert capability.can_set_stop is False
        assert capability.can_set_take_profit is False
        assert capability.can_clear_stop is False
        assert capability.can_clear_take_profit is False
        assert capability.preserves_unmentioned_side is False
        assert capability.trigger_bases == frozenset()
        assert capability.min_reprice_interval_seconds == 0.0
        assert capability.reprice_gap is False

    def test_capability_is_pure_no_venue_read(self):
        """The capability property performs no venue I/O."""
        adapter = HyperliquidAdapter(_config())
        _ = adapter.position_protection_capability(_instrument())

        # No exchange object created, no network calls
        assert adapter._exchange is None


class TestTrailingCapability:
    def test_perps_use_synthetic_trailing(self):
        """Perp markets do not advertise native trailing-stop support."""
        adapter = HyperliquidAdapter(_config())
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

    def test_spot_is_unsupported(self):
        """Spot markets have no position leg and report unsupported."""
        adapter = HyperliquidAdapter(_config())
        capability = adapter.trailing_capability(_instrument(asset_class=AssetClass.SPOT))

        assert capability.supported is False
        assert capability.offset_types == frozenset()
        assert capability.bases == frozenset()
        assert capability.activations == frozenset()
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is False
        assert capability.min_reprice_interval_seconds == 0.0

    def test_capability_is_pure_no_venue_read(self):
        """The capability property performs no venue I/O."""
        adapter = HyperliquidAdapter(_config())
        _ = adapter.trailing_capability(_instrument())

        # No exchange object created, no network calls
        assert adapter._exchange is None

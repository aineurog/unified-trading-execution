"""Adapter-level tests for the MT5 trailing execution surface.

MT5 has no native trailing primitive, so these tests pin the pieces the core
synthetic trail depends on: an unsupported native capability, exact per-ticket
TP/SL writes, unmentioned-side preservation, and the clear path.  The
engine-level lifecycle lives in ``test_trailing_lifecycle.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.errors import TrailingNotSupportedError
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.trailing import (
    TrailingMode,
    TrailingOffsetType,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.trailing.manager import resolve_routing
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment

from ._scripted_terminal import DEFAULT_SYMBOL, ScriptedLeg, ScriptedTerminal


def _instrument() -> Instrument:
    return Instrument(
        symbol="EUR",
        asset_class=AssetClass.MARGIN_FX,
        quote_currency="USD",
        platform_symbol=DEFAULT_SYMBOL,
    )


def _spec(**overrides: object) -> TrailingSpec:
    values: dict[str, object] = {
        "offset_type": TrailingOffsetType.PRICE,
        "offset": Decimal("0.0200"),
    }
    values.update(overrides)
    return TrailingSpec(**values)  # type: ignore[arg-type]


def _install(mock_mt5_module: MagicMock, *legs: ScriptedLeg) -> ScriptedTerminal:
    terminal = ScriptedTerminal(legs=list(legs))
    terminal.install(mock_mt5_module)
    return terminal


class TestTrailingCapabilities:
    def test_no_native_trailing_is_declared(self, adapter: MT5Adapter) -> None:
        capability = adapter.trailing_capability(_instrument())

        assert capability.supported is False
        assert capability.offset_types == frozenset()
        assert capability.bases == frozenset()
        assert capability.activations == frozenset()
        assert capability.position_scoped is False
        assert capability.min_reprice_interval_seconds == 0.0

    def test_auto_routes_every_request_to_synthetic(self, adapter: MT5Adapter) -> None:
        instrument = _instrument()
        request = TrailingRequest(instrument, _spec())

        mode, reason = resolve_routing(
            request,
            protection=adapter.position_protection_capability(instrument),
            native=adapter.trailing_capability(instrument),
            market_data=adapter.market_data_capability(instrument),
        )

        assert mode is TrailingMode.SYNTHETIC
        assert reason == "native_unsupported"

    def test_native_request_names_the_missing_capability(self, adapter: MT5Adapter) -> None:
        instrument = _instrument()
        request = TrailingRequest(instrument, _spec(), mode=TrailingMode.NATIVE)

        with pytest.raises(TrailingNotSupportedError, match="native trailing"):
            resolve_routing(
                request,
                protection=adapter.position_protection_capability(instrument),
                native=adapter.trailing_capability(instrument),
                market_data=adapter.market_data_capability(instrument),
            )


class TestNoNativeMethods:
    """MT5 must not fake a native trailing implementation."""

    @pytest.mark.parametrize(
        "method",
        [
            "place_trailing_stop",
            "cancel_trailing_stop",
            "get_trailing_stop",
            "get_position_trailing_stop",
        ],
    )
    def test_native_methods_are_the_inherited_defaults(self, method: str) -> None:
        assert getattr(MT5Adapter, method) is getattr(Adapter, method)

    async def test_place_trailing_stop_raises(self, adapter: MT5Adapter) -> None:
        with pytest.raises(NotImplementedError):
            await adapter.place_trailing_stop(MagicMock())

    async def test_cancel_trailing_stop_raises(self, adapter: MT5Adapter) -> None:
        with pytest.raises(NotImplementedError):
            await adapter.cancel_trailing_stop("native-1", _instrument())

    async def test_get_trailing_stop_raises(self, adapter: MT5Adapter) -> None:
        with pytest.raises(NotImplementedError):
            await adapter.get_trailing_stop("native-1", _instrument())

    async def test_get_position_trailing_stop_raises(self, adapter: MT5Adapter) -> None:
        with pytest.raises(NotImplementedError):
            await adapter.get_position_trailing_stop(_instrument(), "1001")


class TestExactTicketWrites:
    """Synthetic protection must target exactly one MT5 ticket."""

    async def test_stop_update_targets_only_the_named_ticket(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(
            mock_mt5_module,
            ScriptedLeg(
                ticket=1001,
                volume=0.1,
                price_open=1.1000,
                stop_loss=1.0000,
                take_profit=1.2000,
            ),
            ScriptedLeg(
                ticket=1002,
                volume=0.2,
                price_open=1.1050,
                stop_loss=1.0500,
                take_profit=1.3000,
            ),
        )

        await adapter.modify_position_tpsl(
            _instrument(),
            "1001",
            stop_loss=TpSlAttachment(trigger_price=Decimal("1.0800")),
        )

        assert len(terminal.sends) == 1
        request = terminal.sends[0]
        assert request["position"] == 1001
        assert request["symbol"] == DEFAULT_SYMBOL
        assert request["sl"] == 1.08
        assert request["tp"] == 1.20
        assert terminal.legs[1001].stop_loss == 1.08
        assert terminal.legs[1001].take_profit == 1.20
        assert terminal.legs[1002].stop_loss == 1.05
        assert terminal.legs[1002].take_profit == 1.30

    async def test_take_profit_update_preserves_the_existing_stop(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(
            mock_mt5_module,
            ScriptedLeg(ticket=1001, stop_loss=1.0800, take_profit=1.2000),
        )

        await adapter.modify_position_tpsl(
            _instrument(),
            "1001",
            take_profit=TpSlAttachment(trigger_price=Decimal("1.1500")),
        )

        assert terminal.legs[1001].take_profit == 1.15
        assert terminal.legs[1001].stop_loss == 1.08


class TestClearProtection:
    """A trailing cancel clears only the side it owns."""

    async def test_clear_stop_preserves_take_profit_and_other_legs(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(
            mock_mt5_module,
            ScriptedLeg(ticket=1001, stop_loss=1.0800, take_profit=1.2000),
            ScriptedLeg(ticket=1002, stop_loss=1.0500, take_profit=1.3000),
        )

        await adapter.clear_position_tpsl(_instrument(), "1001", stop_loss=True)

        assert terminal.legs[1001].stop_loss == 0.0
        assert terminal.legs[1001].take_profit == 1.20
        assert terminal.legs[1002].stop_loss == 1.05
        assert terminal.legs[1002].take_profit == 1.30
        assert terminal.sends[-1]["position"] == 1001

    async def test_clear_both_sides_sends_two_zeros(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(
            mock_mt5_module,
            ScriptedLeg(ticket=1001, stop_loss=1.0800, take_profit=1.2000),
        )

        await adapter.clear_position_tpsl(_instrument(), "1001", take_profit=True, stop_loss=True)

        assert terminal.legs[1001].stop_loss == 0.0
        assert terminal.legs[1001].take_profit == 0.0
        assert terminal.sends[-1]["sl"] == 0.0
        assert terminal.sends[-1]["tp"] == 0.0

    async def test_without_flags_raises_before_any_venue_call(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(mock_mt5_module, ScriptedLeg(ticket=1001))

        with pytest.raises(ValueError):
            await adapter.clear_position_tpsl(_instrument(), "1001")

        assert terminal.sends == []

    async def test_flat_position_returns_quietly(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        terminal = _install(mock_mt5_module)

        await adapter.clear_position_tpsl(_instrument(), "9999", stop_loss=True)

        assert terminal.sends == []

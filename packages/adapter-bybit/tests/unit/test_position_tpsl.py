"""Bybit position TP/SL writes preserve the side they do not mention."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError, InvalidRequestError

from unified_trading_execution.adapter import PositionProtectionCapability
from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import (
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.trailing.types import TrailingBasis
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment


def _futures_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        multiplier=1,
    )


def _inverse_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        multiplier=1,
    )


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
    )


def _ok() -> tuple[dict[str, object], None, dict[str, str]]:
    return {"retCode": 0, "retMsg": "OK", "result": {}}, None, {}


def _positions(
    *,
    size: str = "1",
    position_idx: int = 0,
) -> tuple[dict[str, object], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "positionIdx": position_idx,
                        "size": size,
                    }
                ]
            },
        },
        None,
        {},
    )


class TestModifyPositionTpSlPreservesUnmentionedSide:
    async def test_stop_loss_write_preserves_take_profit(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            stopLoss="90000",
            slOrderType="Market",
        )

    async def test_take_profit_write_preserves_stop_loss(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            take_profit=TpSlAttachment(trigger_price=Decimal("120000")),
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            takeProfit="120000",
            tpOrderType="Market",
        )

    async def test_both_sides_are_written(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:1",
            take_profit=TpSlAttachment(trigger_price=Decimal("120000")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=1,
            tpslMode="Full",
            takeProfit="120000",
            tpOrderType="Market",
            stopLoss="90000",
            slOrderType="Market",
        )

    async def test_partial_limit_stop_preserves_take_profit(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=TpSlAttachment(
                trigger_price=Decimal("90000"),
                limit_price=Decimal("89900"),
            ),
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Partial",
            stopLoss="90000",
            slOrderType="Limit",
            slLimitPrice="89900",
        )

    async def test_stop_loss_write_applies_the_trigger_basis(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
            trigger_basis=TrailingBasis.MARK,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            stopLoss="90000",
            slOrderType="Market",
            slTriggerBy="MarkPrice",
        )

    async def test_take_profit_write_applies_the_trigger_basis(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            take_profit=TpSlAttachment(trigger_price=Decimal("120000")),
            trigger_basis=TrailingBasis.LAST,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            takeProfit="120000",
            tpOrderType="Market",
            tpTriggerBy="LastPrice",
        )

    async def test_none_trigger_basis_leaves_the_venue_default(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
            trigger_basis=None,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            stopLoss="90000",
            slOrderType="Market",
        )

    async def test_unsupported_trigger_basis_is_left_at_the_venue_default(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
            trigger_basis=TrailingBasis.BID,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            stopLoss="90000",
            slOrderType="Market",
        )

    async def test_no_sides_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(ValueError, match="at least one"):
            await adapter.modify_position_tpsl(_futures_instrument(), "BTCUSDT:0")

        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_spot_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                _spot_instrument(),
                "0",
                stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
            )

        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_transport_error_is_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.side_effect = FailedRequestError(
            request="POST /v5/position/trading-stop",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.modify_position_tpsl(
                _futures_instrument(),
                "BTCUSDT:0",
                stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
            )


class TestClearPositionTpSl:
    async def test_clear_stop_loss_preserves_take_profit(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions()
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.clear_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=True,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            stopLoss="0",
        )

    async def test_clear_take_profit_preserves_stop_loss(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions()
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.clear_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            take_profit=True,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=0,
            tpslMode="Full",
            takeProfit="0",
        )

    async def test_clear_both_sides(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions(position_idx=1)
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.clear_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:1",
            take_profit=True,
            stop_loss=True,
        )

        mock_pybit_http.set_trading_stop.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            positionIdx=1,
            tpslMode="Full",
            takeProfit="0",
            stopLoss="0",
        )

    async def test_flat_position_returns_quietly(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions(size="0")

        await adapter.clear_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=True,
        )

        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_missing_position_returns_quietly(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = (
            {"retCode": 0, "result": {"list": []}},
            None,
            {},
        )

        await adapter.clear_position_tpsl(
            _futures_instrument(),
            "BTCUSDT:0",
            stop_loss=True,
        )

        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_spot_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.clear_position_tpsl(
                _spot_instrument(),
                "0",
                stop_loss=True,
            )

        mock_pybit_http.get_positions.assert_not_called()
        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_no_sides_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(ValueError, match="at least one"):
            await adapter.clear_position_tpsl(_futures_instrument(), "BTCUSDT:0")

        mock_pybit_http.get_positions.assert_not_called()

    async def test_api_error_is_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions()
        mock_pybit_http.set_trading_stop.side_effect = InvalidRequestError(
            request="POST /v5/position/trading-stop",
            message="unknown error",
            status_code=10001,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformError):
            await adapter.clear_position_tpsl(
                _futures_instrument(),
                "BTCUSDT:0",
                stop_loss=True,
            )

    async def test_transport_error_is_translated(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions()
        mock_pybit_http.set_trading_stop.side_effect = FailedRequestError(
            request="POST /v5/position/trading-stop",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.clear_position_tpsl(
                _futures_instrument(),
                "BTCUSDT:0",
                stop_loss=True,
            )


class TestPositionProtectionCapability:
    def test_linear_perpetual_declares_full_static_protection(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_supported(adapter.position_protection_capability(_futures_instrument()))

    def test_inverse_perpetual_declares_full_static_protection(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_supported(adapter.position_protection_capability(_inverse_instrument()))

    def test_spot_is_unsupported(self, adapter: BybitAdapter) -> None:
        self._assert_unsupported(adapter.position_protection_capability(_spot_instrument()))

    def test_unsupported_asset_class_is_unsupported(self, adapter: BybitAdapter) -> None:
        instrument = Instrument(
            symbol="EUR",
            quote_currency="USD",
            asset_class=AssetClass.MARGIN_FX,
        )

        self._assert_unsupported(adapter.position_protection_capability(instrument))

    async def test_inverse_protection_methods_use_the_inverse_category(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions()
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.modify_position_tpsl(
            _inverse_instrument(),
            "BTCUSD:0",
            stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
        )
        assert mock_pybit_http.set_trading_stop.call_args.kwargs["category"] == "inverse"
        assert mock_pybit_http.set_trading_stop.call_args.kwargs["symbol"] == "BTCUSD"

        result = await adapter.get_position_tpsl(_inverse_instrument(), "BTCUSD:0")
        assert result == (None, None)
        assert mock_pybit_http.get_positions.call_args.kwargs["category"] == "inverse"

        await adapter.clear_position_tpsl(_inverse_instrument(), "BTCUSD:0", stop_loss=True)
        assert mock_pybit_http.set_trading_stop.call_args.kwargs["category"] == "inverse"
        assert mock_pybit_http.set_trading_stop.call_args.kwargs["stopLoss"] == "0"

    async def test_declared_trigger_bases_are_the_ones_written(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        capability = adapter.position_protection_capability(_futures_instrument())
        expected = {
            TrailingBasis.LAST: "LastPrice",
            TrailingBasis.MARK: "MarkPrice",
        }

        for basis in capability.trigger_bases:
            mock_pybit_http.set_trading_stop.reset_mock()
            mock_pybit_http.set_trading_stop.return_value = _ok()
            await adapter.modify_position_tpsl(
                _futures_instrument(),
                "BTCUSDT:0",
                stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
                trigger_basis=basis,
            )
            assert (
                mock_pybit_http.set_trading_stop.call_args.kwargs["slTriggerBy"] == expected[basis]
            )

        for basis in (
            TrailingBasis.BID,
            TrailingBasis.ASK,
            TrailingBasis.MID,
        ):
            assert basis not in capability.trigger_bases
            mock_pybit_http.set_trading_stop.reset_mock()
            mock_pybit_http.set_trading_stop.return_value = _ok()
            await adapter.modify_position_tpsl(
                _futures_instrument(),
                "BTCUSDT:0",
                stop_loss=TpSlAttachment(trigger_price=Decimal("90000")),
                trigger_basis=basis,
            )
            assert "slTriggerBy" not in mock_pybit_http.set_trading_stop.call_args.kwargs

    @staticmethod
    def _assert_supported(capability: PositionProtectionCapability) -> None:
        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases == frozenset({TrailingBasis.LAST, TrailingBasis.MARK})
        assert capability.min_reprice_interval_seconds == 0.0
        assert capability.reprice_gap is False

    @staticmethod
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

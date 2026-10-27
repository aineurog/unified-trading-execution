"""Bybit position TP/SL writes preserve the side they do not mention."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import (
    PlatformConnectionError,
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


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
    )


def _ok() -> tuple[dict[str, object], None, dict[str, str]]:
    return {"retCode": 0, "retMsg": "OK", "result": {}}, None, {}


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

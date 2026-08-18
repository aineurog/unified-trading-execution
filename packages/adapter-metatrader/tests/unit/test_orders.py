"""Unit tests for MT5 order translation (orders.py).

Tests cases:
    - build_mt5_request: all 8 order type permutations (4 types x 2 sides)
    - MARKET BUY/SELL get ORDER_TYPE_BUY/SELL
    - LIMIT BUY/SELL get ORDER_TYPE_BUY_LIMIT/SELL_LIMIT
    - STOP/STOP_LIMIT type mapping
    - TP/SL are set as native price fields (sl, tp) on the request
    - TpSlAttachment.limit_price raises UnsupportedOrderTypeError
    - build_mt5_modify_request: quantity change raises UnsupportedOrderTypeError
    - build_mt5_cancel_request: TRADE_ACTION_REMOVE with correct ticket
    - build_mt5_sltp_request: TRADE_ACTION_SLTP with correct position_id
    - parse_mt5_result: maps retcode to OrderResult with status
    - parse_order_record: handles None / empty tuple
    - _select_filling: selects best available filling mode per symbol
    - GTD orders set expiration fields on the request
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.mt5.orders import (
    _select_filling,
    build_mt5_cancel_request,
    build_mt5_modify_request,
    build_mt5_request,
    build_mt5_sltp_request,
    parse_mt5_result,
    parse_order_record,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderModification, TpSlAttachment, UnifiedOrder


@pytest.fixture
def mt5_constants(mock_mt5_module) -> None:
    """Populate the mock MT5 module with the standard constant values."""
    mock_mt5_module.TRADE_ACTION_DEAL = 1
    mock_mt5_module.TRADE_ACTION_MODIFY = 2
    mock_mt5_module.TRADE_ACTION_REMOVE = 3
    mock_mt5_module.TRADE_ACTION_PENDING = 5
    mock_mt5_module.TRADE_ACTION_SLTP = 6
    mock_mt5_module.ORDER_TIME_SPECIFIED = 2
    mock_mt5_module.ORDER_TIME_DAY = 1
    mock_mt5_module.ORDER_TYPE_BUY = 0
    mock_mt5_module.ORDER_TYPE_SELL = 1
    mock_mt5_module.ORDER_TYPE_BUY_LIMIT = 2
    mock_mt5_module.ORDER_TYPE_SELL_LIMIT = 3
    mock_mt5_module.ORDER_TYPE_BUY_STOP = 4
    mock_mt5_module.ORDER_TYPE_SELL_STOP = 5
    mock_mt5_module.ORDER_TYPE_BUY_STOP_LIMIT = 6
    mock_mt5_module.ORDER_TYPE_SELL_STOP_LIMIT = 7
    mock_mt5_module.TRADE_RETCODE_PLACED = 10008
    mock_mt5_module.TRADE_RETCODE_DONE = 10009
    mock_mt5_module.TRADE_RETCODE_DONE_PARTIAL = 10010
    mock_mt5_module.TRADE_RETCODE_NO_CHANGES = 10025
    mock_mt5_module.TRADE_RETCODE_REJECT = 10006


def _instrument() -> Instrument:
    return Instrument(symbol="EUR/USD", asset_class=AssetClass.MARGIN_FX, quote_currency="USD")


def _order(
    order_type: OrderType,
    side: OrderSide,
    *,
    price: Decimal | None = None,
    stop_price: Decimal | None = None,
    time_in_force: TimeInForce = TimeInForce.GTC,
    take_profit: TpSlAttachment | None = None,
    stop_loss: TpSlAttachment | None = None,
    expire_at: datetime | None = None,
) -> UnifiedOrder:
    return UnifiedOrder(
        instrument=_instrument(),
        order_type=order_type,
        side=side,
        quantity=Decimal("0.1"),
        time_in_force=time_in_force,
        price=price,
        stop_price=stop_price,
        take_profit=take_profit,
        stop_loss=stop_loss,
        expire_at=expire_at,
    )


class TestBuildMT5Request:
    """UnifiedOrder → MqlTradeRequest translation."""

    def test_market_buy(self, mock_mt5_module, mt5_constants) -> None:
        """MARKET BUY → ORDER_TYPE_BUY."""
        request = build_mt5_request(
            _order(OrderType.MARKET, OrderSide.BUY), mt5_module=mock_mt5_module
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_BUY
        assert request["action"] == mock_mt5_module.TRADE_ACTION_DEAL
        assert "price" not in request

    def test_market_sell(self, mock_mt5_module, mt5_constants) -> None:
        """MARKET SELL → ORDER_TYPE_SELL."""
        request = build_mt5_request(
            _order(OrderType.MARKET, OrderSide.SELL), mt5_module=mock_mt5_module
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_SELL
        assert request["action"] == mock_mt5_module.TRADE_ACTION_DEAL

    def test_limit_buy(self, mock_mt5_module, mt5_constants) -> None:
        """LIMIT BUY → ORDER_TYPE_BUY_LIMIT."""
        request = build_mt5_request(
            _order(OrderType.LIMIT, OrderSide.BUY, price=Decimal("1.1000")),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_BUY_LIMIT
        assert request["action"] == mock_mt5_module.TRADE_ACTION_PENDING
        assert request["price"] == 1.1

    def test_limit_sell(self, mock_mt5_module, mt5_constants) -> None:
        """LIMIT SELL → ORDER_TYPE_SELL_LIMIT."""
        request = build_mt5_request(
            _order(OrderType.LIMIT, OrderSide.SELL, price=Decimal("1.1000")),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_SELL_LIMIT
        assert request["price"] == 1.1

    def test_stop_buy(self, mock_mt5_module, mt5_constants) -> None:
        """STOP BUY → ORDER_TYPE_BUY_STOP."""
        request = build_mt5_request(
            _order(OrderType.STOP, OrderSide.BUY, stop_price=Decimal("1.2000")),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_BUY_STOP
        assert request["price"] == 1.2

    def test_stop_sell(self, mock_mt5_module, mt5_constants) -> None:
        """STOP SELL → ORDER_TYPE_SELL_STOP."""
        request = build_mt5_request(
            _order(OrderType.STOP, OrderSide.SELL, stop_price=Decimal("1.0000")),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_SELL_STOP
        assert request["price"] == 1.0

    def test_stop_limit_buy(self, mock_mt5_module, mt5_constants) -> None:
        """STOP_LIMIT BUY → ORDER_TYPE_BUY_STOP_LIMIT."""
        request = build_mt5_request(
            _order(
                OrderType.STOP_LIMIT,
                OrderSide.BUY,
                price=Decimal("1.2500"),
                stop_price=Decimal("1.2000"),
            ),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_BUY_STOP_LIMIT
        assert request["stoplimit"] == 1.25
        assert request["price"] == 1.2

    def test_stop_limit_sell(self, mock_mt5_module, mt5_constants) -> None:
        """STOP_LIMIT SELL → ORDER_TYPE_SELL_STOP_LIMIT."""
        request = build_mt5_request(
            _order(
                OrderType.STOP_LIMIT,
                OrderSide.SELL,
                price=Decimal("0.9500"),
                stop_price=Decimal("1.0000"),
            ),
            mt5_module=mock_mt5_module,
        )
        assert request["type"] == mock_mt5_module.ORDER_TYPE_SELL_STOP_LIMIT
        assert request["stoplimit"] == 0.95
        assert request["price"] == 1.0

    def test_tp_sl_as_native_fields(self, mock_mt5_module, mt5_constants) -> None:
        """Take profit and stop loss are set as sl/tp fields on the request."""
        request = build_mt5_request(
            _order(
                OrderType.LIMIT,
                OrderSide.BUY,
                price=Decimal("1.1000"),
                take_profit=TpSlAttachment(Decimal("1.3000")),
                stop_loss=TpSlAttachment(Decimal("1.0000")),
            ),
            mt5_module=mock_mt5_module,
        )
        assert request["tp"] == 1.3
        assert request["sl"] == 1.0

    def test_tpsl_attachment_limit_price_unsupported(self, mock_mt5_module, mt5_constants) -> None:
        """TpSlAttachment with limit_price raises UnsupportedOrderTypeError."""
        with pytest.raises(UnsupportedOrderTypeError):
            build_mt5_request(
                _order(
                    OrderType.LIMIT,
                    OrderSide.BUY,
                    price=Decimal("1.1000"),
                    take_profit=TpSlAttachment(Decimal("1.3000"), Decimal("1.2990")),
                ),
                mt5_module=mock_mt5_module,
            )
        with pytest.raises(UnsupportedOrderTypeError):
            build_mt5_request(
                _order(
                    OrderType.LIMIT,
                    OrderSide.BUY,
                    price=Decimal("1.1000"),
                    stop_loss=TpSlAttachment(Decimal("1.0000"), Decimal("1.0010")),
                ),
                mt5_module=mock_mt5_module,
            )

    def test_gtd_sets_expiration_fields(self, mock_mt5_module, mt5_constants) -> None:
        """GTD orders include type_time and expiration in the request."""
        expire_at = datetime.now(UTC) + timedelta(days=1)
        request = build_mt5_request(
            _order(
                OrderType.LIMIT,
                OrderSide.BUY,
                price=Decimal("1.1000"),
                time_in_force=TimeInForce.GTD,
                expire_at=expire_at,
            ),
            mt5_module=mock_mt5_module,
        )
        assert request["type_time"] == mock_mt5_module.ORDER_TIME_SPECIFIED
        assert request["expiration"] == int(expire_at.timestamp())

    def test_day_sets_type_time(self, mock_mt5_module, mt5_constants) -> None:
        """DAY orders set type_time = ORDER_TIME_DAY (not GTC)."""
        request = build_mt5_request(
            _order(
                OrderType.LIMIT,
                OrderSide.BUY,
                price=Decimal("1.1000"),
                time_in_force=TimeInForce.DAY,
            ),
            mt5_module=mock_mt5_module,
        )
        assert request["type_time"] == mock_mt5_module.ORDER_TIME_DAY


class TestBuildMT5ModifyRequest:
    """OrderModification → TRADE_ACTION_MODIFY translation."""

    def test_price_change(self, mock_mt5_module, mt5_constants) -> None:
        """Modifying a LIMIT order's price sets the PRICE field."""
        request = build_mt5_modify_request(
            OrderModification(client_order_id="c1", price=Decimal("1.2000")),
            ticket=123,
            order_type=OrderType.LIMIT,
            mt5_module=mock_mt5_module,
        )
        assert request["action"] == mock_mt5_module.TRADE_ACTION_MODIFY
        assert request["order"] == 123
        assert request["price"] == 1.2

    def test_stop_price_change_for_stop(self, mock_mt5_module, mt5_constants) -> None:
        """Modifying a STOP order's trigger sets the PRICE field (MT5 stores the
        trigger in price for plain stops)."""
        request = build_mt5_modify_request(
            OrderModification(client_order_id="c1", stop_price=Decimal("1.1500")),
            ticket=123,
            order_type=OrderType.STOP,
            mt5_module=mock_mt5_module,
        )
        assert request["action"] == mock_mt5_module.TRADE_ACTION_MODIFY
        assert request["order"] == 123
        assert request["price"] == 1.15
        assert "stoplimit" not in request

    def test_stop_price_change_for_stop_limit(self, mock_mt5_module, mt5_constants) -> None:
        """Modifying a STOP_LIMIT order's trigger sets the PRICE field and its
        limit price sets STOPLIMIT."""
        request = build_mt5_modify_request(
            OrderModification(
                client_order_id="c1",
                price=Decimal("1.3000"),
                stop_price=Decimal("1.1500"),
            ),
            ticket=123,
            order_type=OrderType.STOP_LIMIT,
            mt5_module=mock_mt5_module,
        )
        assert request["action"] == mock_mt5_module.TRADE_ACTION_MODIFY
        assert request["order"] == 123
        assert request["price"] == 1.15
        assert request["stoplimit"] == 1.3

    def test_tp_sl_change(self, mock_mt5_module, mt5_constants) -> None:
        """Modifying take_profit and stop_loss sets the TP and SL fields — and
        re-sends the current order price (MT5 requires it on every modify)."""
        request = build_mt5_modify_request(
            OrderModification(
                client_order_id="c1",
                take_profit=TpSlAttachment(Decimal("1.3000")),
                stop_loss=TpSlAttachment(Decimal("1.0000")),
            ),
            ticket=123,
            order_type=OrderType.LIMIT,
            mt5_module=mock_mt5_module,
            current_price=Decimal("1.1457"),
        )
        assert request["price"] == 1.1457
        assert request["tp"] == 1.3
        assert request["sl"] == 1.0

    def test_tp_sl_only_for_stop_limit_keeps_both_prices(
        self, mock_mt5_module, mt5_constants
    ) -> None:
        """A TP/SL-only modify on a STOP_LIMIT keeps trigger (price) and limit
        (stoplimit) from the current order."""
        request = build_mt5_modify_request(
            OrderModification(
                client_order_id="c1",
                stop_loss=TpSlAttachment(Decimal("1.0000")),
            ),
            ticket=123,
            order_type=OrderType.STOP_LIMIT,
            mt5_module=mock_mt5_module,
            current_price=Decimal("1.3000"),
            current_stop_price=Decimal("1.1500"),
        )
        assert request["price"] == 1.15
        assert request["stoplimit"] == 1.3
        assert request["sl"] == 1.0

    def test_tp_sl_limit_price_unsupported(self, mock_mt5_module, mt5_constants) -> None:
        """TP/SL modification with limit_price raises UnsupportedOrderTypeError."""
        with pytest.raises(UnsupportedOrderTypeError):
            build_mt5_modify_request(
                OrderModification(
                    client_order_id="c1",
                    stop_loss=TpSlAttachment(Decimal("1.0000"), Decimal("1.0010")),
                ),
                ticket=123,
                order_type=OrderType.LIMIT,
                mt5_module=mock_mt5_module,
            )

    def test_quantity_change_unsupported(self, mock_mt5_module, mt5_constants) -> None:
        """Modifying quantity raises UnsupportedOrderTypeError."""
        with pytest.raises(UnsupportedOrderTypeError):
            build_mt5_modify_request(
                OrderModification(client_order_id="c1", quantity=Decimal("0.2")),
                ticket=123,
                order_type=OrderType.LIMIT,
                mt5_module=mock_mt5_module,
            )


class TestBuildMT5CancelRequest:
    """Cancel → TRADE_ACTION_REMOVE translation."""

    def test_cancel_request(self, mock_mt5_module, mt5_constants) -> None:
        """Cancel request has TRADE_ACTION_REMOVE and correct order ticket."""
        request = build_mt5_cancel_request(456, mt5_module=mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_REMOVE
        assert request["order"] == 456


class TestBuildMT5SltpRequest:
    """TP/SL → TRADE_ACTION_SLTP translation."""

    def test_sltp_request(self, mock_mt5_module, mt5_constants) -> None:
        """SLTP request has TRADE_ACTION_SLTP and correct position_id."""
        request = build_mt5_sltp_request(
            "789",
            take_profit=1.3,
            stop_loss=1.0,
            mt5_module=mock_mt5_module,
        )
        assert request["action"] == mock_mt5_module.TRADE_ACTION_SLTP
        assert request["position"] == 789
        assert request["tp"] == 1.3
        assert request["sl"] == 1.0

    def test_sltp_accepts_only_one_level(self, mock_mt5_module, mt5_constants) -> None:
        """SLTP request with only take_profit or only stop_loss."""
        request = build_mt5_sltp_request("789", take_profit=1.3, mt5_module=mock_mt5_module)
        assert request["tp"] == 1.3
        assert "sl" not in request

        request = build_mt5_sltp_request("789", stop_loss=1.0, mt5_module=mock_mt5_module)
        assert request["sl"] == 1.0
        assert "tp" not in request

    def test_sltp_requires_at_least_one_level(self, mock_mt5_module, mt5_constants) -> None:
        """SLTP request without any level raises ValueError."""
        with pytest.raises(ValueError):
            build_mt5_sltp_request("789", mt5_module=mock_mt5_module)


class TestParseMT5Result:
    """OrderSendResult → OrderResult parsing."""

    def _result(self, **fields) -> SimpleNamespace:
        defaults = {
            "retcode": 10008,
            "deal": 0,
            "order": 987,
            "volume": 0.0,
            "price": 0.0,
            "bid": 0.0,
            "ask": 0.0,
            "comment": "",
            "request": {},
        }
        defaults.update(fields)
        return SimpleNamespace(**defaults)

    def test_placed_result(self, mock_mt5_module, mt5_constants) -> None:
        """TRADE_RETCODE_PLACED → OrderResult with order ticket."""
        result = parse_mt5_result(
            self._result(retcode=mock_mt5_module.TRADE_RETCODE_PLACED, order=987),
            "c1",
            mt5_module=mock_mt5_module,
        )
        assert result.client_order_id == "c1"
        assert result.platform_order_id == "987"
        assert result.status == OrderStatus.OPEN
        assert result.filled_quantity == Decimal("0")

    def test_done_result(self, mock_mt5_module, mt5_constants) -> None:
        """TRADE_RETCODE_DONE → OrderResult with filled status and deal ticket."""
        result = parse_mt5_result(
            self._result(
                retcode=mock_mt5_module.TRADE_RETCODE_DONE,
                order=0,
                deal=321,
                volume=0.5,
                price=1.2345,
            ),
            "c1",
            mt5_module=mock_mt5_module,
        )
        assert result.status == OrderStatus.FILLED
        assert result.platform_order_id == "321"
        assert result.filled_quantity == Decimal("0.5")
        assert result.average_fill_price == Decimal("1.2345")

    def test_done_without_deal_is_placed_pending(self, mock_mt5_module, mt5_constants) -> None:
        """TRADE_RETCODE_DONE without a deal = placed pending order, not a fill.

        The wrapper reports DONE for every successful request; a pending
        order has deal == 0 and must map to OPEN (a real fill always has a
        deal ticket).
        """
        result = parse_mt5_result(
            self._result(
                retcode=mock_mt5_module.TRADE_RETCODE_DONE,
                order=987,
                deal=0,
                volume=0.5,
                price=1.2345,
            ),
            "c1",
            mt5_module=mock_mt5_module,
        )
        assert result.status == OrderStatus.OPEN
        assert result.platform_order_id == "987"
        assert result.filled_quantity == Decimal("0")
        assert result.average_fill_price is None

    def test_placed_with_deal_is_filled(self, mock_mt5_module, mt5_constants) -> None:
        """TRADE_RETCODE_PLACED with a deal = executed immediately, so FILLED."""
        result = parse_mt5_result(
            self._result(
                retcode=mock_mt5_module.TRADE_RETCODE_PLACED,
                deal=321,
                volume=0.5,
                price=1.2345,
            ),
            "c1",
            mt5_module=mock_mt5_module,
        )
        assert result.status == OrderStatus.FILLED
        assert result.filled_quantity == Decimal("0.5")

    def test_none_result_raises(self, mock_mt5_module) -> None:
        """None result raises via error mapping."""
        mock_mt5_module.last_error.return_value = (10013, "invalid request")
        with pytest.raises(InvalidSymbolError):
            parse_mt5_result(None, "c1", mt5_module=mock_mt5_module)

    def test_rejected_result_raises(self, mock_mt5_module, mt5_constants) -> None:
        """TRADE_RETCODE_REJECT raises via error mapping."""
        mock_mt5_module.last_error.return_value = (
            mock_mt5_module.TRADE_RETCODE_REJECT,
            "order rejected",
        )
        with pytest.raises(PlatformError):
            parse_mt5_result(
                self._result(retcode=mock_mt5_module.TRADE_RETCODE_REJECT),
                "c1",
                mt5_module=mock_mt5_module,
            )

    def test_stale_success_uses_retcode_and_comment(self, mock_mt5_module) -> None:
        """A stale RES_S_OK last_error must not mask the real retcode message.

        When the wrapper reports success (RES_S_OK=1) but the trade retcode
        is a failure, the raised error carries the retcode and its comment
        instead of the misleading "Success".
        """
        mock_mt5_module.last_error.return_value = (1, "Success")
        with pytest.raises(PlatformConnectionError, match="AutoTrading disabled"):
            parse_mt5_result(
                self._result(retcode=10027, comment="AutoTrading disabled by client"),
                "c1",
                mt5_module=mock_mt5_module,
            )


class TestParseOrderRecord:
    """MT5 order tuple → OrderResult parsing."""

    def _order_tuple(self, **fields) -> SimpleNamespace:
        defaults = {
            "ticket": 555,
            "state": 1,
            "volume_initial": 0.5,
            "volume_current": 0.5,
            "price_open": 1.2345,
            "time_setup": 1700000000,
            "time_done": 0,
        }
        defaults.update(fields)
        return SimpleNamespace(**defaults)

    def test_none_and_empty_return_none(self, mock_mt5_module) -> None:
        """None and empty tuple → None."""
        assert parse_order_record(None, "c1", mt5_module=mock_mt5_module) is None
        assert parse_order_record((), "c1", mt5_module=mock_mt5_module) is None

    def test_pending_order(self, mock_mt5_module) -> None:
        """ORDER_STATE_PLACED → OPEN with zero filled."""
        record = parse_order_record(self._order_tuple(), "c1", mt5_module=mock_mt5_module)
        assert record is not None
        assert record.client_order_id == "c1"
        assert record.platform_order_id == "555"
        assert record.status == OrderStatus.OPEN
        assert record.filled_quantity == Decimal("0")
        assert record.average_fill_price is None

    def test_filled_order(self, mock_mt5_module) -> None:
        """ORDER_STATE_FILLED → FILLED with full volume; no avg price from order record."""
        record = parse_order_record(
            self._order_tuple(state=4, volume_current=0.0, time_done=1700000100),
            "c1",
            mt5_module=mock_mt5_module,
        )
        assert record is not None
        assert record.status == OrderStatus.FILLED
        assert record.filled_quantity == Decimal("0.5")
        assert record.average_fill_price is None

    def test_unknown_state_raises(self, mock_mt5_module) -> None:
        """Unknown ORDER_STATE → PlatformError."""
        with pytest.raises(PlatformError):
            parse_order_record(self._order_tuple(state=99), "c1", mt5_module=mock_mt5_module)

    def test_server_time_offset_normalizes_timestamps(self, mock_mt5_module) -> None:
        """``server_time_offset`` shifts server-as-epoch stamps to real UTC."""
        offset = 10800
        record = parse_order_record(
            self._order_tuple(
                time_setup=1700000000 + offset,
                time_done=1700000100 + offset,
            ),
            "c1",
            mt5_module=mock_mt5_module,
            server_time_offset=offset,
        )
        assert record is not None
        assert record.created_at == datetime.fromtimestamp(1700000000, tz=UTC)
        assert record.updated_at == datetime.fromtimestamp(1700000100, tz=UTC)


class TestSelectFilling:
    """Filling mode selection per symbol info and TIF."""

    def test_selects_ideal_mode_when_available(self, mock_mt5_module) -> None:
        """Preferred filling mode matches TIF and is supported."""
        # ORDER_FILLING_FOK=0, ORDER_FILLING_IOC=1, ORDER_FILLING_RETURN=2
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b001), TimeInForce.FOK, mt5_module=mock_mt5_module
            )
            == 0
        )
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b010), TimeInForce.IOC, mt5_module=mock_mt5_module
            )
            == 1
        )
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b100), TimeInForce.GTC, mt5_module=mock_mt5_module
            )
            == 2
        )
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b100), TimeInForce.DAY, mt5_module=mock_mt5_module
            )
            == 2
        )
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b100), TimeInForce.GTD, mt5_module=mock_mt5_module
            )
            == 2
        )

    def test_falls_back_when_ideal_unsupported(self, mock_mt5_module) -> None:
        """Fallback chain when ideal filling mode is not in bitmask."""
        # FOK → [FOK, IOC, RETURN]: ideal unset, falls to IOC
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b010), TimeInForce.FOK, mt5_module=mock_mt5_module
            )
            == 1
        )
        # FOK → [FOK, IOC, RETURN]: falls all the way to RETURN
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b100), TimeInForce.FOK, mt5_module=mock_mt5_module
            )
            == 2
        )
        # IOC → [IOC, FOK, RETURN]: ideal unset, falls to FOK
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b001), TimeInForce.IOC, mt5_module=mock_mt5_module
            )
            == 0
        )
        # RETURN (GTC) → [RETURN, IOC, FOK]: ideal unset, falls to IOC
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b010), TimeInForce.GTC, mt5_module=mock_mt5_module
            )
            == 1
        )
        # RETURN (GTC) → [RETURN, IOC, FOK]: falls all the way to FOK
        assert (
            _select_filling(
                SimpleNamespace(filling_mode=0b001), TimeInForce.GTC, mt5_module=mock_mt5_module
            )
            == 0
        )
        # No compatible mode at all → InvalidSymbolError
        with pytest.raises(InvalidSymbolError):
            _select_filling(
                SimpleNamespace(filling_mode=0b000), TimeInForce.IOC, mt5_module=mock_mt5_module
            )

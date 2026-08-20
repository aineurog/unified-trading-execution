"""Unit tests for MT5 order operations (adapter.py order methods).

Tests run against the mocked ``MetaTrader5`` module — no real terminal IPC.

Tests cases:
    - place_order: MARKET sets price from ask/bid; LIMIT/STOP/STOP_LIMIT field mapping
    - place_order: filling mode selected per symbol; symbol set on request
    - place_order: ticket mapping recorded on placed orders
    - place_order: generated client_order_id when None
    - place_order: reject invalidates spec cache and raises mapped error
    - place_order: symbol_info / tick failures raise mapped errors
    - place_order: incompatible filling mode raises InvalidSymbolError
    - place_order: TP/SL limit_price raises UnsupportedOrderTypeError
    - modify_order: order type resolved live via orders_get()
    - modify_order: price → price, stop_price → price (STOP), stoplimit (STOP_LIMIT)
    - modify_order: unknown client id raises OrderNotFoundError
    - modify_order: orders_get failure / unknown type raise mapped errors
    - cancel_order: TRADE_ACTION_REMOVE with correct ticket
    - get_order_by_client_id: unknown id / inactive order → None
    - get_order_by_client_id: active order → OrderResult; real error raises
    - modify_position_tpsl: TRADE_ACTION_SLTP with position, tp, sl
    - modify_position_tpsl: limit_price raises UnsupportedOrderTypeError
"""

from __future__ import annotations

from collections import namedtuple
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.mt5.comments import decode_comment
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    OrderModification,
    TpSlAttachment,
    UnifiedOrder,
)

_PAST = datetime(2024, 1, 1, tzinfo=UTC)

Mt5Order = namedtuple(
    "Mt5Order",
    [
        "ticket",
        "time_setup",
        "time_done",
        "type",
        "state",
        "volume_initial",
        "volume_current",
        "price_open",
        "price_stoplimit",
        "sl",
        "tp",
        "symbol",
        "type_time",
        "time_expiration",
        "position_id",
        "comment",
    ],
    defaults=[""],
)


@pytest.fixture
def mt5_constants(mock_mt5_module) -> None:
    """Populate the mock MT5 module with the standard constant values."""
    mock_mt5_module.TRADE_ACTION_DEAL = 1
    mock_mt5_module.TRADE_ACTION_MODIFY = 2
    mock_mt5_module.TRADE_ACTION_REMOVE = 3
    mock_mt5_module.TRADE_ACTION_PENDING = 5
    mock_mt5_module.TRADE_ACTION_SLTP = 6
    mock_mt5_module.TRADE_RETCODE_PLACED = 10008
    mock_mt5_module.TRADE_RETCODE_DONE = 10009
    mock_mt5_module.TRADE_RETCODE_DONE_PARTIAL = 10010
    mock_mt5_module.TRADE_RETCODE_NO_CHANGES = 10025
    mock_mt5_module.TRADE_RETCODE_REJECT = 10006


def _instrument() -> Instrument:
    return Instrument(
        symbol="EUR",
        asset_class=AssetClass.MARGIN_FX,
        quote_currency="USD",
        platform_symbol="EURUSD.m",
    )


def _order(
    order_type: OrderType = OrderType.LIMIT,
    side: OrderSide = OrderSide.BUY,
    *,
    price: Decimal | None = Decimal("1.1000"),
    stop_price: Decimal | None = None,
    client_order_id: str | None = "client-abc",
    time_in_force: TimeInForce = TimeInForce.GTC,
    take_profit: TpSlAttachment | None = None,
    stop_loss: TpSlAttachment | None = None,
) -> UnifiedOrder:
    return UnifiedOrder(
        instrument=_instrument(),
        order_type=order_type,
        side=side,
        quantity=Decimal("0.1"),
        time_in_force=time_in_force,
        client_order_id=client_order_id,
        price=price,
        stop_price=stop_price,
        take_profit=take_profit,
        stop_loss=stop_loss,
    )


def _send_result(**fields: object) -> SimpleNamespace:
    """An ``OrderSendResult``-like object with realistic defaults."""
    defaults: dict[str, object] = {
        "retcode": 10008,
        "deal": 0,
        "order": 1001,
        "volume": 0.0,
        "price": 0.0,
        "bid": 0.0,
        "ask": 0.0,
        "comment": "",
        "request": {},
    }
    defaults.update(fields)
    return SimpleNamespace(**defaults)


def _send_success(mock_mt5_module: MagicMock, **fields: object) -> None:
    mock_mt5_module.order_send.return_value = _send_result(
        retcode=mock_mt5_module.TRADE_RETCODE_PLACED, **fields
    )


def _set_symbol_info(
    mock_mt5_module: MagicMock, *, filling_mode: int = 0b100, side_effect=None
) -> None:
    """Configure ``symbol_info()`` for EURUSD.m (RETURN filling supported)."""
    info = MagicMock(filling_mode=filling_mode)
    if side_effect is None:
        mock_mt5_module.symbol_info.return_value = info
    else:
        mock_mt5_module.symbol_info.side_effect = side_effect


def _set_tick(mock_mt5_module: MagicMock, *, bid: float = 1.0998, ask: float = 1.1002) -> None:
    mock_mt5_module.symbol_info_tick.return_value = MagicMock(bid=bid, ask=ask)


def _request(mock_mt5_module: MagicMock) -> dict:
    """The request dict passed to the last ``order_send()`` call."""
    return mock_mt5_module.order_send.call_args[0][0]


class TestPlaceOrder:
    """place_order — request construction and submission."""

    async def test_market_buy_uses_ask(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """MARKET BUY sets request price from the live ask."""
        _set_symbol_info(mock_mt5_module)
        _set_tick(mock_mt5_module, bid=1.0998, ask=1.1002)
        _send_success(mock_mt5_module)
        order = _order(OrderType.MARKET, OrderSide.BUY, price=None)

        result = await adapter.place_order(order)

        assert result.client_order_id == "client-abc"
        request = _request(mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_DEAL
        assert request["symbol"] == "EURUSD.m"
        assert request["price"] == 1.1002
        assert request["type_filling"] == 2  # ORDER_FILLING_RETURN

    async def test_place_packs_uuid_into_comment(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A uuid client_order_id is losslessly packed into the order comment."""
        _set_symbol_info(mock_mt5_module)
        _set_tick(mock_mt5_module)
        _send_success(mock_mt5_module)
        order = _order(OrderType.MARKET, OrderSide.BUY, price=None, client_order_id=None)

        result = await adapter.place_order(order)

        request = _request(mock_mt5_module)
        assert decode_comment(request["comment"]) == result.client_order_id

    async def test_place_non_uuid_id_omits_comment(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A non-encodable client_order_id leaves the request commentless."""
        _set_symbol_info(mock_mt5_module)
        _set_tick(mock_mt5_module)
        _send_success(mock_mt5_module)
        order = _order(OrderType.MARKET, OrderSide.BUY, price=None, client_order_id="custom-1")

        await adapter.place_order(order)

        request = _request(mock_mt5_module)
        assert "comment" not in request

    async def test_market_sell_uses_bid(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """MARKET SELL sets request price from the live bid."""
        _set_symbol_info(mock_mt5_module)
        _set_tick(mock_mt5_module, bid=1.0998, ask=1.1002)
        _send_success(mock_mt5_module)

        await adapter.place_order(_order(OrderType.MARKET, OrderSide.SELL, price=None))

        assert _request(mock_mt5_module)["price"] == 1.0998

    async def test_limit_sets_price_and_filling(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """LIMIT order → TRADE_ACTION_PENDING with price and filling mode."""
        _set_symbol_info(mock_mt5_module)
        _send_success(mock_mt5_module)
        order = _order(OrderType.LIMIT, OrderSide.BUY, price=Decimal("1.1000"))

        result = await adapter.place_order(order)

        assert result.status == OrderStatus.OPEN
        assert result.platform_order_id == "1001"
        request = _request(mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_PENDING
        assert request["price"] == 1.1
        assert request["type_filling"] == 2
        assert "stoplimit" not in request

    async def test_stop_and_stop_limit_field_mapping(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """STOP puts the trigger in price; STOP_LIMIT adds the limit in stoplimit."""
        _set_symbol_info(mock_mt5_module)

        _send_success(mock_mt5_module)
        await adapter.place_order(
            _order(OrderType.STOP, OrderSide.BUY, stop_price=Decimal("1.2000"))
        )
        request = _request(mock_mt5_module)
        assert request["price"] == 1.2
        assert "stoplimit" not in request

        _send_success(mock_mt5_module)
        await adapter.place_order(
            _order(
                OrderType.STOP_LIMIT,
                OrderSide.BUY,
                price=Decimal("1.2500"),
                stop_price=Decimal("1.2000"),
            )
        )
        request = _request(mock_mt5_module)
        assert request["price"] == 1.2
        assert request["stoplimit"] == 1.25

    async def test_records_ticket_mapping_on_placed(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A placed order records the client_order_id ↔ ticket mapping."""
        _set_symbol_info(mock_mt5_module)
        _send_success(mock_mt5_module, order=1001)

        await adapter.place_order(_order(client_order_id="client-abc"))

        assert adapter._order_id_to_ticket["client-abc"] == 1001
        assert adapter._ticket_to_order_id[1001] == "client-abc"

    async def test_generates_client_order_id_when_none(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A missing client_order_id is generated and the mapping is keyed by it."""
        _set_symbol_info(mock_mt5_module)
        _send_success(mock_mt5_module, order=1001)

        result = await adapter.place_order(_order(client_order_id=None))

        assert result.client_order_id
        assert result.client_order_id in adapter._order_id_to_ticket
        assert adapter._order_id_to_ticket[result.client_order_id] == 1001

    async def test_reject_invalidates_spec_cache_and_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A rejected order invalidates the cached spec and raises the mapped error."""
        _set_symbol_info(mock_mt5_module)
        adapter._spec_cache[_instrument()] = MagicMock()
        mock_mt5_module.order_send.return_value = _send_result(
            retcode=10013, comment="invalid request"
        )
        mock_mt5_module.last_error.return_value = (1, "")

        with pytest.raises(InvalidSymbolError):
            await adapter.place_order(_order())

        assert _instrument() not in adapter._spec_cache

    async def test_symbol_info_none_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """symbol_info() returning None maps to an error."""
        mock_mt5_module.symbol_info.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "unknown symbol")

        with pytest.raises(PlatformError):
            await adapter.place_order(_order())

    async def test_tick_none_raises(
        self,
        mock_mt5_module: MagicMock,
        adapter: MT5Adapter,
        mt5_constants,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No market quote for a MARKET order maps to an error."""
        monkeypatch.setattr("unified_trading_execution.mt5.adapter.time.sleep", lambda _: None)
        _set_symbol_info(mock_mt5_module)
        mock_mt5_module.symbol_info_tick.return_value = None
        mock_mt5_module.last_error.return_value = (10018, "market closed")

        with pytest.raises(InvalidSymbolError):
            await adapter.place_order(_order(OrderType.MARKET, OrderSide.BUY, price=None))

    async def test_selects_symbol_in_market_watch_before_submit(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """place_order selects the symbol before building/sending the request."""
        _set_symbol_info(mock_mt5_module)
        _send_success(mock_mt5_module)

        await adapter.place_order(_order())

        mock_mt5_module.symbol_select.assert_called_once_with("EURUSD.m", True)
        assert "EURUSD.m" in adapter._selected_symbols
        mock_mt5_module.order_send.assert_called_once()

    async def test_unknown_symbol_selection_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A symbol the broker does not provide raises InvalidSymbolError."""
        mock_mt5_module.symbol_select.return_value = False
        mock_mt5_module.last_error.return_value = (4301, "unknown symbol")

        with pytest.raises(InvalidSymbolError, match="unknown symbol"):
            await adapter.place_order(_order())

        mock_mt5_module.order_send.assert_not_called()
        assert "EURUSD.m" in adapter._failed_symbols

    async def test_no_compatible_filling_mode_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """Symbol without a compatible filling mode raises InvalidSymbolError."""
        _set_symbol_info(mock_mt5_module, filling_mode=0b000)

        with pytest.raises(InvalidSymbolError):
            await adapter.place_order(_order())

    async def test_tp_sl_limit_price_unsupported(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """TpSlAttachment with limit_price raises UnsupportedOrderTypeError."""
        _set_symbol_info(mock_mt5_module)

        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.place_order(
                _order(take_profit=TpSlAttachment(Decimal("1.3000"), Decimal("1.2990")))
            )
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.place_order(
                _order(stop_loss=TpSlAttachment(Decimal("1.0000"), Decimal("1.0010")))
            )


class TestModifyOrder:
    """modify_order — type lookup, request construction, failures."""

    async def _registered(self, adapter: MT5Adapter) -> None:
        adapter._order_id_to_ticket["client-abc"] = 1001
        adapter._ticket_to_order_id[1001] = "client-abc"

    def _order_tuple(self, **fields: object) -> Mt5Order:
        base: dict[str, object] = {
            "ticket": 1001,
            "time_setup": int(_PAST.timestamp()),
            "time_done": 0,
            "type": 2,  # ORDER_TYPE_BUY_LIMIT
            "state": 1,  # ORDER_STATE_PLACED
            "volume_initial": 0.1,
            "volume_current": 0.1,
            "price_open": 1.1000,
            "price_stoplimit": 0.0,
            "sl": 0.0,
            "tp": 0.0,
            "symbol": "EURUSD.m",
            "type_time": 0,
            "time_expiration": 0,
            "position_id": 0,
        }
        base.update(fields)
        return Mt5Order(**base)

    async def test_modifies_price_via_live_type_lookup(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A LIMIT order's price change sets the price field."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(),)
        _send_success(mock_mt5_module)

        result = await adapter.modify_order(
            OrderModification(client_order_id="client-abc", price=Decimal("1.2000"))
        )

        assert result.client_order_id == "client-abc"
        mock_mt5_module.orders_get.assert_called_once_with(ticket=1001)
        request = _request(mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_MODIFY
        assert request["order"] == 1001
        assert request["price"] == 1.2

    async def test_modifies_stop_trigger(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A STOP order's trigger change goes in the price field."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(type=4),)  # BUY_STOP
        _send_success(mock_mt5_module)

        await adapter.modify_order(
            OrderModification(client_order_id="client-abc", stop_price=Decimal("1.1500"))
        )

        request = _request(mock_mt5_module)
        assert request["price"] == 1.15
        assert "stoplimit" not in request

    async def test_modifies_stop_limit_fields(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A STOP_LIMIT order maps trigger→price and limit→stoplimit."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(type=6),)  # BUY_STOP_LIMIT
        _send_success(mock_mt5_module)

        await adapter.modify_order(
            OrderModification(
                client_order_id="client-abc",
                price=Decimal("1.3000"),
                stop_price=Decimal("1.1500"),
            )
        )

        request = _request(mock_mt5_module)
        assert request["price"] == 1.15
        assert request["stoplimit"] == 1.3

    async def test_tpsl_only_modify_keeps_current_price(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """A TP/SL-only modification re-sends the current order price — MT5
        rejects a modify request that omits the price field."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(),)  # BUY_LIMIT @1.1000
        _send_success(mock_mt5_module)

        await adapter.modify_order(
            OrderModification(
                client_order_id="client-abc",
                take_profit=TpSlAttachment(Decimal("1.2000")),
                stop_loss=TpSlAttachment(Decimal("1.0500")),
            )
        )

        request = _request(mock_mt5_module)
        assert request["price"] == 1.1
        assert request["tp"] == 1.2
        assert request["sl"] == 1.05

    async def test_unknown_client_id_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """modify_order with an unknown client_order_id raises OrderNotFoundError."""
        with pytest.raises(OrderNotFoundError):
            await adapter.modify_order(
                OrderModification(client_order_id="unknown", price=Decimal("1.2000"))
            )

    async def test_order_get_none_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """orders_get() returning None maps to an error."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = None
        mock_mt5_module.last_error.return_value = (10035, "invalid order")

        with pytest.raises(OrderNotFoundError):
            await adapter.modify_order(
                OrderModification(client_order_id="client-abc", price=Decimal("1.2000"))
            )

    async def test_unknown_type_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """An order with an unmapped MT5 type raises PlatformError."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(type=99),)

        with pytest.raises(PlatformError):
            await adapter.modify_order(
                OrderModification(client_order_id="client-abc", price=Decimal("1.2000"))
            )

    async def test_quantity_change_unsupported(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """modify_order with a quantity change raises UnsupportedOrderTypeError."""
        await self._registered(adapter)
        mock_mt5_module.orders_get.return_value = (self._order_tuple(),)

        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_order(
                OrderModification(client_order_id="client-abc", quantity=Decimal("0.2"))
            )


class TestCancelOrder:
    """cancel_order — ticket lookup and TRADE_ACTION_REMOVE."""

    async def test_cancels_by_ticket(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """cancel_order sends TRADE_ACTION_REMOVE with the resolved ticket."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        _send_success(mock_mt5_module)

        result = await adapter.cancel_order("client-abc")

        assert result.client_order_id == "client-abc"
        # A cancel produces no deal, so parse_mt5_result would report OPEN;
        # the adapter must report the order as CANCELLED.
        assert result.status == OrderStatus.CANCELLED
        request = _request(mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_REMOVE
        assert request["order"] == 1001

    async def test_unknown_client_id_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """cancel_order with an unknown client_order_id raises OrderNotFoundError."""
        with pytest.raises(OrderNotFoundError):
            await adapter.cancel_order("unknown")


class TestGetOrderByClientId:
    """get_order_by_client_id — mapping lookup, inactivity, errors."""

    def _order_tuple(self, **fields: object) -> Mt5Order:
        base: dict[str, object] = {
            "ticket": 1001,
            "time_setup": int(_PAST.timestamp()),
            "time_done": 0,
            "type": 2,
            "state": 1,  # ORDER_STATE_PLACED
            "volume_initial": 0.1,
            "volume_current": 0.1,
            "price_open": 1.1000,
            "price_stoplimit": 0.0,
            "sl": 0.0,
            "tp": 0.0,
            "symbol": "EURUSD.m",
            "type_time": 0,
            "time_expiration": 0,
            "position_id": 0,
        }
        base.update(fields)
        return Mt5Order(**base)

    async def test_unknown_client_id_returns_none(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """An id never placed by this engine returns None without calling MT5."""
        result = await adapter.get_order_by_client_id("unknown")

        assert result is None
        mock_mt5_module.orders_get.assert_not_called()

    async def test_inactive_order_returns_none(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """orders_get() returning an empty tuple with no error means gone."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.last_error.return_value = (0, "")

        assert await adapter.get_order_by_client_id("client-abc") is None

    async def test_inactive_order_res_ok_returns_none(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """orders_get() empty with RES_S_OK (no error) → None."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.last_error.return_value = (1, "")

        assert await adapter.get_order_by_client_id("client-abc") is None

    async def test_order_not_found_returns_none(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """orders_get() empty with 10035 (order not found) → None."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        mock_mt5_module.orders_get.return_value = ()
        mock_mt5_module.last_error.return_value = (10035, "invalid order")

        assert await adapter.get_order_by_client_id("client-abc") is None

    async def test_active_order_returns_result(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """An active order returns an OrderResult with OPEN status."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        mock_mt5_module.orders_get.return_value = (self._order_tuple(),)

        result = await adapter.get_order_by_client_id("client-abc")

        assert result is not None
        assert result.client_order_id == "client-abc"
        assert result.platform_order_id == "1001"
        assert result.status == OrderStatus.OPEN
        assert result.filled_quantity == Decimal("0")

    async def test_real_error_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """orders_get() None with a genuine error code raises the mapped error."""
        adapter._order_id_to_ticket["client-abc"] = 1001
        mock_mt5_module.orders_get.return_value = None
        mock_mt5_module.last_error.return_value = (-2, "invalid params")

        with pytest.raises(PlatformError):
            await adapter.get_order_by_client_id("client-abc")


class TestModifyPositionTpsl:
    """modify_position_tpsl — TRADE_ACTION_SLTP construction and guards."""

    async def test_modifies_both_levels(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """TP/SL levels are sent as TRADE_ACTION_SLTP on the position ticket."""
        _send_success(mock_mt5_module)

        await adapter.modify_position_tpsl(
            "789",
            take_profit=TpSlAttachment(Decimal("1.3000")),
            stop_loss=TpSlAttachment(Decimal("1.0000")),
        )

        request = _request(mock_mt5_module)
        assert request["action"] == mock_mt5_module.TRADE_ACTION_SLTP
        assert request["position"] == 789
        assert request["tp"] == 1.3
        assert request["sl"] == 1.0

    async def test_modifies_only_stop_loss(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """Only the provided level is sent."""
        _send_success(mock_mt5_module)

        await adapter.modify_position_tpsl("789", stop_loss=TpSlAttachment(Decimal("1.0000")))

        request = _request(mock_mt5_module)
        assert request["sl"] == 1.0
        assert "tp" not in request

    async def test_limit_price_unsupported(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """limit_price on a TP/SL attachment raises UnsupportedOrderTypeError."""
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                "789", take_profit=TpSlAttachment(Decimal("1.3000"), Decimal("1.2990"))
            )
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                "789", stop_loss=TpSlAttachment(Decimal("1.0000"), Decimal("1.0010"))
            )

    async def test_no_levels_raises(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter, mt5_constants
    ) -> None:
        """modify_position_tpsl with neither level raises ValueError."""
        with pytest.raises(ValueError):
            await adapter.modify_position_tpsl("789")

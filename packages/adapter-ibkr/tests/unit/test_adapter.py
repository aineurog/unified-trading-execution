"""Unit tests for IBKRAdapter order operations — mock-only, no live Gateway.

Covers each branch of adapter.py order ops:
  - place_order: single, bracket (TP/SL), readonly, not-connected, bubbling
  - modify_order: success, not-found, unsupported TP/SL, not-connected
  - cancel_order: success, not-found, not-connected
  - get_order_by_client_id: found, not-found, not-connected (raises)
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from ib_async import Contract, Order
from ib_async.order import OrderStatus as IBOrderStatus
from ib_async.order import Trade

from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import OrderModification, TpSlAttachment, UnifiedOrder

AAPL = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
EURUSD = Instrument(symbol="EUR", quote_currency="USD", asset_class=AssetClass.MARGIN_FX)
BTC_USD = Instrument(symbol="BTC", quote_currency="USD", asset_class=AssetClass.SPOT)


def _make_order(**overrides: object) -> UnifiedOrder:
    base: dict[str, object] = {
        "instrument": AAPL,
        "order_type": OrderType.LIMIT,
        "side": OrderSide.BUY,
        "quantity": Decimal("10"),
        "time_in_force": TimeInForce.GTC,
        "client_order_id": "01900000-0000-7000-8000-000000000001",
        "price": Decimal("101.5"),
    }
    base.update(overrides)
    return UnifiedOrder(**base)  # type: ignore[arg-type]


def _trade(
    order_ref: str = "cid-1",
    order_id: int = 42,
    perm_id: int = 999,
    status: str = "Submitted",
    filled: float = 0,
    total_qty: float = 10,
) -> Trade:
    order = Order(
        orderId=order_id,
        permId=perm_id,
        orderRef=order_ref,
        action="BUY",
        totalQuantity=total_qty,
        orderType="LMT",
        lmtPrice=Decimal("101.5"),
    )
    order_status = IBOrderStatus(
        orderId=order_id,
        status=status,
        filled=filled,
        remaining=total_qty - filled,
        avgFillPrice=0,
        permId=perm_id,
    )
    return Trade(contract=Contract(), order=order, orderStatus=order_status)


# ---------------------------------------------------------------------------
# place_order
# ---------------------------------------------------------------------------


class TestPlaceOrder:
    async def test_single_order_success(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        captured: list[tuple[Contract, Order]] = []

        def _place(c: Contract, o: Order) -> Trade:
            captured.append((c, o))
            o.orderId = 101
            o.permId = 555
            return _trade(order_ref=o.orderRef, order_id=101, perm_id=555, status="Submitted")

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        order = _make_order()
        result = await adapter.place_order(order)

        assert len(captured) == 1
        contract, ib_order = captured[0]
        assert contract.symbol == "AAPL"
        assert ib_order.orderRef == "01900000-0000-7000-8000-000000000001"
        assert ib_order.orderType == "LMT"
        assert result.client_order_id == "01900000-0000-7000-8000-000000000001"
        assert result.platform_order_id == "555"
        assert result.status is OrderStatus.OPEN

    async def test_bracket_order_links_parent_id(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        placed: list[Order] = []

        def _place(c: Contract, o: Order) -> Trade:
            o.orderId = 100 + len(placed) + 1
            o.permId = 900 + len(placed)
            placed.append(o)
            return _trade(order_ref=o.orderRef, order_id=o.orderId, perm_id=o.permId)

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        order = _make_order(
            take_profit=TpSlAttachment(trigger_price=Decimal("110")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("95")),
        )
        result = await adapter.place_order(order)

        # parent + 2 children
        assert len(placed) == 3
        parent, tp, sl = placed
        assert parent.transmit is False
        assert sl.transmit is True
        # children linked after parentId assignment
        assert tp.parentId == parent.orderId
        assert sl.parentId == parent.orderId
        assert tp.ocaGroup == sl.ocaGroup
        assert result.client_order_id == order.client_order_id

    async def test_place_bracket_single_leg(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        placed: list[Order] = []

        def _place(c: Contract, o: Order) -> Trade:
            o.orderId = 200 + len(placed)
            placed.append(o)
            return _trade(order_ref=o.orderRef, order_id=o.orderId, perm_id=1)

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        order = _make_order(take_profit=TpSlAttachment(trigger_price=Decimal("110")))
        await adapter.place_order(order)
        assert len(placed) == 2
        assert placed[1].orderType == "LMT"

    async def test_place_readonly_blocked(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        adapter._config = IBKRConfig(host="127.0.0.1", port=4002, client_id=1, readonly=True)  # type: ignore[attr-defined]
        with pytest.raises(PlatformError, match="readonly"):
            await adapter.place_order(_make_order())

    async def test_place_not_connected_raises(self, adapter: IBKRAdapter) -> None:
        # never called connect()
        with pytest.raises(PlatformConnectionError, match="not connected"):
            await adapter.place_order(_make_order())

    async def test_place_bubbles_invalid_symbol(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        # BOND is not supported by to_ibkr_contract → InvalidSymbolError
        from unified_trading_execution.types.enums import AssetClass

        bond = Instrument(symbol="BOND", asset_class=AssetClass.BOND)
        order = _make_order(instrument=bond)  # type: ignore[arg-type]
        with pytest.raises(InvalidSymbolError):
            await adapter.place_order(order)

    async def test_place_bubbles_unsupported(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        order = _make_order(reduce_only=True)
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.place_order(order)

    async def test_place_crypto_market_buy_rejected(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        order = UnifiedOrder(
            instrument=BTC_USD,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.GTC,
            client_order_id="cid-crypto",
        )
        with pytest.raises(UnsupportedOrderTypeError, match="cashQty"):
            await adapter.place_order(order)


# ---------------------------------------------------------------------------
# modify_order
# ---------------------------------------------------------------------------


class TestModifyOrder:
    async def test_modify_price_and_quantity(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module

        # seed an open trade
        trade = _trade(order_ref="cid-mod", order_id=10, perm_id=1, total_qty=10)
        mock_ib.trades.return_value = [trade]  # type: ignore[attr-defined]
        mock_ib.openTrades.return_value = [trade]  # type: ignore[attr-defined]

        def _place(c: Contract, o: Order) -> Trade:
            # echo back modified order
            return Trade(
                contract=c,
                order=o,
                orderStatus=IBOrderStatus(
                    orderId=o.orderId,
                    status="Submitted",
                    filled=0,
                    remaining=o.totalQuantity,
                    avgFillPrice=0,
                    permId=o.permId,
                ),
            )

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        mod = OrderModification(
            client_order_id="cid-mod", price=Decimal("105"), quantity=Decimal("5")
        )
        result = await adapter.modify_order(mod)

        assert result.client_order_id == "cid-mod"
        # order was mutated in place
        assert trade.order.lmtPrice == Decimal("105")
        assert trade.order.totalQuantity == 5.0

    async def test_modify_not_found(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.openTrades.return_value = []  # type: ignore[attr-defined]
        mock_ib.trades.return_value = []  # type: ignore[attr-defined]
        with pytest.raises(OrderNotFoundError):
            await adapter.modify_order(
                OrderModification(client_order_id="nope", price=Decimal("1"))
            )

    async def test_modify_terminal_trade_not_found(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        # Already-filled order: absent from openTrades() but still in trades().
        # _find_open_trade's fallback must reject it as terminal.
        filled = _trade(order_ref="cid-done", order_id=99, status="Filled", filled=10, total_qty=10)
        mock_ib.openTrades.return_value = []  # type: ignore[attr-defined]
        mock_ib.trades.return_value = [filled]  # type: ignore[attr-defined]
        with pytest.raises(OrderNotFoundError):
            await adapter.modify_order(
                OrderModification(client_order_id="cid-done", price=Decimal("1"))
            )

    async def test_modify_rejects_tpsl(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        trade = _trade(order_ref="cid-tp", order_id=11)
        mock_ib.openTrades.return_value = [trade]  # type: ignore[attr-defined]
        mock_ib.trades.return_value = [trade]  # type: ignore[attr-defined]
        with pytest.raises(UnsupportedOrderTypeError, match="TP/SL"):
            await adapter.modify_order(
                OrderModification(
                    client_order_id="cid-tp", take_profit=TpSlAttachment(trigger_price=Decimal("1"))
                )
            )

    async def test_modify_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.modify_order(OrderModification(client_order_id="x", price=Decimal("1")))


# ---------------------------------------------------------------------------
# cancel_order
# ---------------------------------------------------------------------------


class TestCancelOrder:
    async def test_cancel_success(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        trade = _trade(order_ref="cid-cancel", order_id=20, status="Submitted")
        mock_ib.openTrades.return_value = [trade]  # type: ignore[attr-defined]
        mock_ib.trades.return_value = [trade]  # type: ignore[attr-defined]
        # cancel returns a trade with Cancelled status
        cancelled = _trade(order_ref="cid-cancel", order_id=20, status="Cancelled")
        mock_ib.cancelOrder.return_value = cancelled  # type: ignore[attr-defined]

        result = await adapter.cancel_order("cid-cancel")

        mock_ib.cancelOrder.assert_called_once()  # type: ignore[attr-defined]
        assert result.status is OrderStatus.CANCELLED

    async def test_cancel_not_found(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.openTrades.return_value = []  # type: ignore[attr-defined]
        mock_ib.trades.return_value = []  # type: ignore[attr-defined]
        with pytest.raises(OrderNotFoundError):
            await adapter.cancel_order("missing")

    async def test_cancel_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.cancel_order("x")


# ---------------------------------------------------------------------------
# get_order_by_client_id
# ---------------------------------------------------------------------------


class TestGetOrderByClientId:
    async def test_get_found(self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        trade = _trade(order_ref="cid-get", order_id=30, perm_id=777, status="Submitted")
        mock_ib.trades.return_value = [trade]  # type: ignore[attr-defined]

        result = await adapter.get_order_by_client_id("cid-get")

        assert result is not None
        assert result.client_order_id == "cid-get"
        assert result.platform_order_id == "777"

    async def test_get_not_found_returns_none(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.trades.return_value = []  # type: ignore[attr-defined]
        assert await adapter.get_order_by_client_id("nope") is None

    async def test_get_without_ib_raises(self, adapter: IBKRAdapter) -> None:
        # never connected -> _ib is None; get_order_by_client_id must raise,
        # consistent with the other order operations.
        with pytest.raises(PlatformConnectionError):
            await adapter.get_order_by_client_id("any")

    async def test_get_finds_filled_trade(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        trade = _trade(
            order_ref="cid-filled", order_id=31, status="Filled", filled=10, total_qty=10
        )
        mock_ib.trades.return_value = [trade]  # type: ignore[attr-defined]
        result = await adapter.get_order_by_client_id("cid-filled")
        assert result is not None
        assert result.status is OrderStatus.FILLED


# ---------------------------------------------------------------------------
# modify_position_tpsl
# ---------------------------------------------------------------------------


class TestModifyPositionTpsl:
    def _pos(self, con_id: int = 111, qty: float = 10, avg_cost: float = 100) -> object:
        from ib_async import Contract
        from ib_async.objects import Position as IBPos

        c = Contract()
        c.conId = con_id
        c.symbol = "AAPL"
        c.secType = "STK"
        c.exchange = "NASDAQ"
        c.currency = "USD"
        return IBPos(account="DU_TEST", contract=c, position=qty, avgCost=avg_cost)

    def _fx_pos(self, con_id: int = 222, qty: float = 1000) -> object:
        from ib_async import Contract
        from ib_async.objects import Position as IBPos

        c = Contract()
        c.conId = con_id
        c.symbol = "EUR"
        c.secType = "CASH"
        c.exchange = "IDEALPRO"
        c.currency = "USD"
        return IBPos(account="DU_TEST", contract=c, position=qty, avgCost=1.08)

    async def test_tpsl_places_oca_pair_stock(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = [self._pos(con_id=111, qty=10)]  # type: ignore[attr-defined]
        captured: list[tuple[Contract, Order]] = []

        def _place(c: Contract, o: Order) -> Trade:
            captured.append((c, o))
            o.permId = 1
            o.orderId = 100 + len(captured)
            return _trade(order_ref=o.orderRef, order_id=o.orderId, perm_id=1)

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        await adapter.modify_position_tpsl(
            "111",
            take_profit=TpSlAttachment(Decimal("150")),
            stop_loss=TpSlAttachment(Decimal("90")),
        )

        assert len(captured) == 2
        (c1, tp), (c2, sl) = captured
        assert c1.exchange == "SMART"  # NASDAQ direct replaced
        assert c2.exchange == "SMART"
        assert tp.orderType == "LMT" and tp.action == "SELL"
        assert sl.orderType == "STP" and sl.action == "SELL"
        assert tp.ocaGroup == sl.ocaGroup == "pos-tpsl-111"
        assert tp.transmit is False and sl.transmit is True
        assert tp.orderRef.startswith("111-tp-") and sl.orderRef.startswith("111-sl-")

    async def test_fx_uses_idealpro(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = [self._fx_pos(con_id=222, qty=1000)]  # type: ignore[attr-defined]
        captured: list[tuple[Contract, Order]] = []

        def _place(c: Contract, o: Order) -> Trade:
            captured.append((c, o))
            return _trade(order_ref=o.orderRef, order_id=1, perm_id=1)

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]

        await adapter.modify_position_tpsl("222", take_profit=TpSlAttachment(Decimal("1.20")))

        assert captured[0][0].exchange == "IDEALPRO"
        assert captured[0][1].orderType == "LMT"

    async def test_only_tp_or_sl(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = [self._pos(con_id=111, qty=5)]  # type: ignore[attr-defined]
        mock_ib.placeOrder.side_effect = lambda c, o: _trade(
            order_ref=o.orderRef, order_id=1, perm_id=1
        )  # type: ignore[attr-defined]
        await adapter.modify_position_tpsl("111", take_profit=TpSlAttachment(Decimal("10")))
        assert mock_ib.placeOrder.call_count == 1  # type: ignore[attr-defined]
        mock_ib.placeOrder.reset_mock()  # type: ignore[attr-defined]
        await adapter.modify_position_tpsl(
            "111", stop_loss=TpSlAttachment(Decimal("9"), limit_price=Decimal("8.9"))
        )
        assert mock_ib.placeOrder.call_count == 1  # type: ignore[attr-defined]
        # STP LMT
        assert mock_ib.placeOrder.call_args[0][1].orderType == "STP LMT"  # type: ignore[attr-defined]

    async def test_short_position_uses_buy(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = [self._pos(con_id=111, qty=-10)]  # type: ignore[attr-defined]
        captured: list[Order] = []

        def _place(c: Contract, o: Order) -> Trade:
            captured.append(o)
            return _trade(order_ref=o.orderRef, order_id=1, perm_id=1)

        mock_ib.placeOrder.side_effect = _place  # type: ignore[attr-defined]
        await adapter.modify_position_tpsl("111", take_profit=TpSlAttachment(Decimal("90")))
        assert captured[0].action == "BUY"

    async def test_no_position_raises(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = []  # type: ignore[attr-defined]
        with pytest.raises(OrderNotFoundError):
            await adapter.modify_position_tpsl("999", take_profit=TpSlAttachment(Decimal("1")))

    async def test_flat_position_raises(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = [self._pos(con_id=111, qty=0)]  # type: ignore[attr-defined]
        with pytest.raises(OrderNotFoundError, match="flat"):
            await adapter.modify_position_tpsl("111", take_profit=TpSlAttachment(Decimal("1")))

    async def test_no_tp_sl_raises(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        with pytest.raises(ValueError, match="at least one"):
            await adapter.modify_position_tpsl("111")

    async def test_tp_limit_price_rejected(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        with pytest.raises(ValueError, match="limit_price"):
            await adapter.modify_position_tpsl(
                "111", take_profit=TpSlAttachment(Decimal("1"), limit_price=Decimal("2"))
            )

    async def test_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.modify_position_tpsl("111", take_profit=TpSlAttachment(Decimal("1")))


# ---------------------------------------------------------------------------
# Push EventBus — _on_* handlers
# ---------------------------------------------------------------------------


class TestPushEvents:
    def test_position_update_publishes(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        from unified_trading_execution.events import PositionUpdateEvent

        captured: list[PositionUpdateEvent] = []
        adapter._event_bus.subscribe(PositionUpdateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.conId = 123
        from ib_async.objects import Position as IBPos

        pos = IBPos(account="DU_TEST", contract=contract, position=5, avgCost=150)
        adapter._on_position_update(pos)

        assert len(captured) == 1
        assert captured[0].position.instrument.symbol == "AAPL"
        assert captured[0].position.quantity == Decimal("5")
        assert captured[0].position.position_id == "123"

    def test_position_update_skips_unmappable(self, adapter: IBKRAdapter) -> None:
        from unified_trading_execution.events import PositionUpdateEvent

        captured: list[PositionUpdateEvent] = []
        adapter._event_bus.subscribe(PositionUpdateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        bad = Contract()
        bad.symbol = ""
        bad.secType = "BOND"
        from ib_async.objects import Position as IBPos

        pos = IBPos(account="DU_TEST", contract=bad, position=1, avgCost=100)
        adapter._on_position_update(pos)
        assert len(captured) == 0

    def test_account_value_publishes(self, adapter: IBKRAdapter) -> None:
        from ib_async.objects import AccountValue

        from unified_trading_execution.events import BalanceUpdateEvent

        captured: list[BalanceUpdateEvent] = []
        adapter._event_bus.subscribe(BalanceUpdateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        av = AccountValue(
            account="DU_TEST", tag="TotalCashValue", value="10000", currency="USD", modelCode=""
        )
        adapter._on_account_value(av)

        assert len(captured) == 1
        assert captured[0].balance.currency == "USD"
        assert captured[0].balance.total == Decimal("10000")

    def test_account_value_skips_irrelevant_tag(self, adapter: IBKRAdapter) -> None:
        from ib_async.objects import AccountValue

        from unified_trading_execution.events import BalanceUpdateEvent

        captured: list[BalanceUpdateEvent] = []
        adapter._event_bus.subscribe(BalanceUpdateEvent, lambda e: captured.append(e))  # type: ignore[arg-type]
        av = AccountValue(
            account="DU_TEST", tag="UnrealizedPnL", value="100", currency="USD", modelCode=""
        )
        adapter._on_account_value(av)
        assert len(captured) == 0

    def test_exec_details_publishes_fill(self, adapter: IBKRAdapter) -> None:
        from datetime import UTC, datetime

        from ib_async.objects import CommissionReport, Execution, Fill

        from unified_trading_execution.events import FillEvent

        captured: list[FillEvent] = []
        adapter._event_bus.subscribe(FillEvent, lambda e: captured.append(e))  # type: ignore[arg-type]

        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.conId = 1
        trade = _trade(order_ref="cid-fill", order_id=1, perm_id=1)
        execution = Execution(
            execId="exec-1",
            time=datetime.now(UTC),
            acctNumber="DU_TEST",
            exchange="SMART",
            side="BOT",
            shares=10,
            price=100,
            permId=1,
            clientId=1,
            orderId=1,
            orderRef="cid-fill",
        )
        commission = CommissionReport(
            execId="exec-1",
            commission=1.0,
            currency="USD",
            realizedPNL=0,
            yield_=0,
            yieldRedemptionDate=0,
        )
        fill = Fill(
            contract=contract,
            execution=execution,
            commissionReport=commission,
            time=datetime.now(UTC),
        )

        adapter._on_exec_details(trade, fill, execution)

        assert len(captured) == 1
        assert captured[0].fill.client_order_id == "cid-fill"
        assert captured[0].fill.fill_quantity == Decimal("10")
        assert captured[0].fill.fill_price == Decimal("100")

    def test_exec_details_skips_zero_qty(self, adapter: IBKRAdapter) -> None:
        from datetime import UTC, datetime

        from ib_async.objects import CommissionReport, Execution, Fill

        from unified_trading_execution.events import FillEvent

        captured: list[FillEvent] = []
        adapter._event_bus.subscribe(FillEvent, lambda e: captured.append(e))  # type: ignore[arg-type]
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        trade = _trade(order_ref="cid-zero", order_id=1, perm_id=1)
        execution = Execution(
            execId="e1", time=datetime.now(UTC), orderRef="cid-zero", shares=0, price=100, permId=1
        )
        fill = Fill(
            contract=contract,
            execution=execution,
            commissionReport=CommissionReport(
                execId="e1",
                commission=0,
                currency="USD",
                realizedPNL=0,
                yield_=0,
                yieldRedemptionDate=0,
            ),
            time=datetime.now(UTC),
        )
        adapter._on_exec_details(trade, fill, execution)
        assert len(captured) == 0

    def test_exec_details_skips_no_order_ref(self, adapter: IBKRAdapter) -> None:
        from datetime import UTC, datetime

        from ib_async.objects import CommissionReport, Execution, Fill

        from unified_trading_execution.events import FillEvent

        captured: list[FillEvent] = []
        adapter._event_bus.subscribe(FillEvent, lambda e: captured.append(e))  # type: ignore[arg-type]
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        trade = Trade(
            contract=contract,
            order=Order(
                orderId=1, permId=0, orderRef="", action="BUY", totalQuantity=1, orderType="LMT"
            ),
            orderStatus=IBOrderStatus(
                orderId=1, status="Submitted", filled=0, remaining=1, avgFillPrice=0, permId=0
            ),
        )
        execution = Execution(
            execId="e1", time=datetime.now(UTC), orderRef="", permId=0, shares=1, price=100
        )
        fill = Fill(
            contract=contract,
            execution=execution,
            commissionReport=CommissionReport(
                execId="e1",
                commission=0,
                currency="USD",
                realizedPNL=0,
                yield_=0,
                yieldRedemptionDate=0,
            ),
            time=datetime.now(UTC),
        )
        adapter._on_exec_details(trade, fill, execution)
        assert len(captured) == 0


# ---------------------------------------------------------------------------
# TWS UTC enforcement
# ---------------------------------------------------------------------------


class TestTwsUtcEnforcement:
    async def test_connect_blocks_non_utc(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        mock_ib = mock_ib_async_module
        mock_ib.TimezoneTWS = "US/Eastern"
        with pytest.raises(PlatformConnectionError, match=r"Time Zone.*UTC"):
            await adapter.connect()

    async def test_require_ib_blocks_non_utc_mid_session(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        # must not block fetch; only next connect() should.
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.TimezoneTWS = "US/Eastern"
        positions = await adapter.fetch_positions()
        assert isinstance(positions, list)

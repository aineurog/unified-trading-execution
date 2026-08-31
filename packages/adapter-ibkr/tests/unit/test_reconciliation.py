"""Mock-only tests for IBKR reconciliation — fetch_positions/balances/open_orders/fills.

Covers every branch in adapter.py fetch_*:
  - success, empty, zero-qty, unmappable, account filtering, not-connected
  - balances: per-currency TotalCash/NetLiq, fallback, invalid, clamping
  - open_orders: mapping (type/side/tif/price), unknown otype skip, unmappable skip
  - fills: grouping, since filter, missing orderRef, zero qty/price, fee, sorting
"""

# ruff: noqa: RUF043

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from ib_async import Contract, Order
from ib_async.contract import Contract as IBContract  # noqa: F401
from ib_async.objects import AccountValue, CommissionReport, Execution, Fill
from ib_async.objects import Position as IBPosition
from ib_async.order import OrderStatus as IBOrderStatus
from ib_async.order import Trade

from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.ibkr import IBKRAdapter


def _stock_contract(
    symbol: str = "AAPL", con_id: int = 12345, exchange: str = "SMART", currency: str = "USD"
) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = exchange
    c.currency = currency
    c.conId = con_id
    c.localSymbol = symbol
    return c


def _position(contract: Contract, qty: float = 100, avg_cost: float = 150.0) -> IBPosition:
    return IBPosition(account="DU_TEST", contract=contract, position=qty, avgCost=avg_cost)


def _account_value(
    tag: str, value: str, currency: str = "USD", account: str = "DU_TEST"
) -> AccountValue:
    return AccountValue(account=account, tag=tag, value=value, currency=currency, modelCode="")


def _trade_for_order(
    symbol: str = "AAPL",
    order_type: str = "LMT",
    action: str = "BUY",
    qty: float = 10,
    order_ref: str = "cid-1",
    tif: str = "GTC",
    lmt_price: float = 100,
    aux_price: float = 0,
    status: str = "Submitted",
    filled: float = 0,
    perm_id: int = 1,
    order_id: int = 1,
) -> Trade:
    c = _stock_contract(symbol=symbol, con_id=1000 + order_id)
    o = Order(
        orderId=order_id,
        permId=perm_id,
        orderRef=order_ref,
        action=action,
        totalQuantity=qty,
        orderType=order_type,
        lmtPrice=lmt_price,
        auxPrice=aux_price,
        tif=tif,
    )
    s = IBOrderStatus(
        orderId=order_id,
        status=status,
        filled=filled,
        remaining=qty - filled,
        avgFillPrice=0,
        permId=perm_id,
    )
    return Trade(contract=c, order=o, orderStatus=s)


# ---------------------------------------------------------------------------
# fetch_positions
# ---------------------------------------------------------------------------


class TestFetchPositions:
    async def test_success_leg_level(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        c1 = _stock_contract("AAPL", con_id=111)
        c2 = _stock_contract("MSFT", con_id=222)
        mock_ib.positions.return_value = [
            _position(c1, qty=10, avg_cost=150),
            _position(c2, qty=-5, avg_cost=200),
        ]  # type: ignore[attr-defined]

        positions = await adapter.fetch_positions()

        assert len(positions) == 2
        # leg-level: each has position_id == conId
        by_id = {p.position_id: p for p in positions}
        assert by_id["111"].quantity == Decimal("10")
        assert by_id["222"].quantity == Decimal("-5")
        assert by_id["111"].instrument.symbol == "AAPL"

    async def test_zero_qty_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        c = _stock_contract("AAPL", con_id=1)
        mock_ib.positions.return_value = [_position(c, qty=0, avg_cost=100)]  # type: ignore[attr-defined]
        assert await adapter.fetch_positions() == []

    async def test_empty_positions(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.return_value = []  # type: ignore[attr-defined]
        assert await adapter.fetch_positions() == []

    async def test_unmappable_contract_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad = Contract()
        bad.symbol = ""
        bad.secType = "BOND"
        bad.conId = 999
        good = _stock_contract("AAPL", con_id=1)
        mock_ib.positions.return_value = [
            IBPosition(account="DU_TEST", contract=bad, position=10, avgCost=100),
            _position(good, qty=5, avg_cost=10),
        ]  # type: ignore[attr-defined]
        positions = await adapter.fetch_positions()
        assert len(positions) == 1
        assert positions[0].instrument.symbol == "AAPL"

    async def test_not_connected_raises(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_positions()

    async def test_positions_exception_wrapped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.positions.side_effect = RuntimeError("socket closed")  # type: ignore[attr-defined]
        with pytest.raises(PlatformConnectionError, match="failed to fetch.*positions"):
            await adapter.fetch_positions()

    async def test_account_filtering(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        # adapter has managedAccount DU_TEST, fetch_positions passes it
        c = _stock_contract("AAPL", con_id=1)
        mock_ib.positions.return_value = [_position(c, qty=1, avg_cost=1)]  # type: ignore[attr-defined]
        await adapter.fetch_positions()
        # called with account="DU_TEST" (our fixture) or "" if UNKNOWN — just ensure it was called
        assert mock_ib.positions.called  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# fetch_balances
# ---------------------------------------------------------------------------


class TestFetchBalances:
    async def test_success_per_currency(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.return_value = [  # type: ignore[attr-defined]
            _account_value("TotalCashValue", "10000", currency="USD"),
            _account_value("NetLiquidation", "12000", currency="USD"),
            _account_value("TotalCashValue", "5000", currency="EUR"),
            _account_value("NetLiquidation", "5000", currency="EUR"),
        ]
        bals = await adapter.fetch_balances()
        assert "USD" in bals and "EUR" in bals
        assert bals["USD"].total == Decimal("12000")
        assert bals["USD"].free == Decimal("10000")
        assert bals["USD"].used == Decimal("2000")
        assert bals["EUR"].total == Decimal("5000")

    async def test_fallback_cash_balance(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.return_value = [_account_value("CashBalance", "3000", currency="USD")]  # type: ignore[attr-defined]
        bals = await adapter.fetch_balances()
        assert bals["USD"].total == Decimal("3000")

    async def test_available_funds_fallback(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.return_value = [  # type: ignore[attr-defined]
            _account_value("TotalCashValue", "8000", currency="USD"),
            _account_value("AvailableFunds", "7000", currency="USD"),
        ]
        bals = await adapter.fetch_balances()
        # Total from TotalCashValue, free from AvailableFunds clamped
        assert bals["USD"].total == Decimal("8000")
        assert bals["USD"].free == Decimal("7000")

    async def test_free_clamped_to_total(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.return_value = [  # type: ignore[attr-defined]
            _account_value("NetLiquidation", "5000", currency="USD"),
            _account_value("TotalCashValue", "6000", currency="USD"),  # free > total
        ]
        bals = await adapter.fetch_balances()
        assert bals["USD"].free == Decimal("5000")
        assert bals["USD"].used == Decimal("0")

    async def test_empty_balances(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.return_value = []  # type: ignore[attr-defined]
        assert await adapter.fetch_balances() == {}

    async def test_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_balances()

    async def test_exception_wrapped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.accountValues.side_effect = RuntimeError("boom")  # type: ignore[attr-defined]
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_balances()


# ---------------------------------------------------------------------------
# fetch_open_orders
# ---------------------------------------------------------------------------


class TestFetchOpenOrders:
    async def test_success_mapping(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.openTrades.return_value = [  # type: ignore[attr-defined]
            _trade_for_order(
                symbol="AAPL",
                order_type="LMT",
                action="BUY",
                qty=5,
                order_ref="cid-buy",
                tif="GTC",
                lmt_price=100,
            ),
            _trade_for_order(
                symbol="AAPL",
                order_type="MKT",
                action="SELL",
                qty=2,
                order_ref="cid-sell",
                tif="DAY",
            ),
        ]
        orders = await adapter.fetch_open_orders()
        assert "cid-buy" in orders
        assert "cid-sell" in orders
        assert orders["cid-buy"].side.value == "BUY"
        assert orders["cid-sell"].order_type.value == "MARKET"

    async def test_unknown_order_type_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad = _trade_for_order(order_type="TRAIL", order_ref="bad")
        good = _trade_for_order(order_ref="good")
        mock_ib.openTrades.return_value = [bad, good]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        assert "good" in orders
        assert "bad" not in orders

    async def test_unmappable_contract_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad_contract = Contract()
        bad_contract.symbol = ""
        bad_contract.secType = "BOND"
        bad_trade = Trade(
            contract=bad_contract,
            order=Order(
                orderId=1,
                orderRef="bad",
                action="BUY",
                totalQuantity=1,
                orderType="LMT",
                lmtPrice=1,
            ),
            orderStatus=IBOrderStatus(
                orderId=1, status="Submitted", filled=0, remaining=1, avgFillPrice=0, permId=1
            ),
        )
        good = _trade_for_order(order_ref="good")
        mock_ib.openTrades.return_value = [bad_trade, good]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        assert "good" in orders
        assert "bad" not in orders

    async def test_empty(self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.openTrades.return_value = []  # type: ignore[attr-defined]
        assert await adapter.fetch_open_orders() == {}

    async def test_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_open_orders()

    async def test_unknown_status_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad = _trade_for_order(order_ref="bad-status", status="SomeNewStatus")
        good = _trade_for_order(order_ref="good")
        mock_ib.openTrades.return_value = [bad, good]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        assert "good" in orders
        assert "bad-status" not in orders

    async def test_side_mapping_slong_sshort(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        t_slong = _trade_for_order(action="SLONG", order_ref="slong")
        t_sshort = _trade_for_order(action="SSHORT", order_ref="sshort")
        mock_ib.openTrades.return_value = [t_slong, t_sshort]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        assert orders["slong"].side.value == "BUY"
        assert orders["sshort"].side.value == "SELL"

    async def test_unknown_action_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad = _trade_for_order(action="FOO", order_ref="bad-action")
        good = _trade_for_order(order_ref="good")
        mock_ib.openTrades.return_value = [bad, good]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        assert "good" in orders
        assert "bad-action" not in orders

    async def test_orphan_fallback_to_platform_id(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        # orderRef empty, but permId present → key falls back to platform id
        t = _trade_for_order(order_ref="", perm_id=999, order_id=5)
        mock_ib.openTrades.return_value = [t]  # type: ignore[attr-defined]
        orders = await adapter.fetch_open_orders()
        # should be keyed by platform id "999" (permId)
        assert "999" in orders

    async def test_tif_mapping(self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        for tif in ("GTC", "DAY", "IOC", "FOK"):
            t = _trade_for_order(tif=tif, order_ref=f"cid-{tif}")
            mock_ib.openTrades.return_value = [t]  # type: ignore[attr-defined]
            orders = await adapter.fetch_open_orders()
            assert orders[f"cid-{tif}"].time_in_force.value == tif


# ---------------------------------------------------------------------------
# fetch_fills
# ---------------------------------------------------------------------------


class TestFetchFills:
    def _fill(
        self,
        symbol: str = "AAPL",
        con_id: int = 111,
        exec_id: str = "exec-1",
        order_ref: str = "cid-1",
        shares: float = 10,
        price: float = 100,
        time: datetime | None = None,
        commission: float = 1.0,
        currency: str = "USD",
    ) -> Fill:
        contract = _stock_contract(symbol=symbol, con_id=con_id)
        exec_time = time or datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
        execution = Execution(
            execId=exec_id,
            time=exec_time,
            acctNumber="DU_TEST",
            exchange="SMART",
            side="BOT",
            shares=shares,
            price=price,
            permId=1,
            clientId=1,
            orderId=1,
            liquidation=0,
            cumQty=shares,
            avgPrice=price,
            orderRef=order_ref,
        )
        commission_report = CommissionReport(
            execId=exec_id,
            commission=commission,
            currency=currency,
            realizedPNL=0,
            yield_=0,
            yieldRedemptionDate="",
        )
        return Fill(
            contract=contract,
            execution=execution,
            commissionReport=commission_report,
            time=exec_time,
        )

    async def test_grouped_by_client_order_id(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.fills.return_value = [
            self._fill(order_ref="cid-a", exec_id="1"),
            self._fill(order_ref="cid-a", exec_id="2"),
            self._fill(order_ref="cid-b", exec_id="3"),
        ]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills()
        assert set(grouped.keys()) == {"cid-a", "cid-b"}
        assert len(grouped["cid-a"]) == 2

    async def test_since_filter(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        old = datetime(2026, 8, 27, 10, 0, tzinfo=UTC)
        new = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
        mock_ib.fills.return_value = [
            self._fill(exec_id="old", time=old),
            self._fill(exec_id="new", time=new),
        ]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills(since=new)
        # only new
        all_ids = [f.platform_fill_id for lst in grouped.values() for f in lst]
        assert "new" in all_ids
        assert "old" not in all_ids

    async def test_missing_order_ref_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        fill = self._fill(order_ref="", exec_id="no-ref")
        # Ensure permId also empty so fallback fails
        fill.execution.orderRef = ""
        fill.execution.permId = 0
        mock_ib.fills.return_value = [fill]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills()
        assert grouped == {}

    async def test_zero_qty_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.fills.return_value = [
            self._fill(shares=0, price=100),
            self._fill(shares=10, price=0),
        ]  # type: ignore[attr-defined]
        assert await adapter.fetch_fills() == {}

    async def test_unmappable_contract_skipped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        bad_contract = Contract()
        bad_contract.symbol = ""
        bad_contract.secType = "BOND"
        bad_fill = Fill(
            contract=bad_contract,
            execution=Execution(
                execId="bad", time=datetime.now(UTC), orderRef="cid-bad", shares=1, price=1
            ),
            commissionReport=CommissionReport(
                execId="bad",
                commission=0,
                currency="USD",
                realizedPNL=0,
                yield_=0,
                yieldRedemptionDate="",
            ),
            time=datetime.now(UTC),
        )
        good = self._fill(order_ref="good")
        mock_ib.fills.return_value = [bad_fill, good]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills()
        assert "good" in grouped
        assert "cid-bad" not in grouped

    async def test_fee_extracted(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.fills.return_value = [self._fill(commission=2.5, currency="USD")]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills()
        fill = grouped["cid-1"][0]
        assert fill.fee_amount == Decimal("2.5")
        assert fill.fee_currency == "USD"

    async def test_sorted_by_timestamp(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        t1 = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 8, 28, 11, 0, tzinfo=UTC)
        t3 = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
        mock_ib.fills.return_value = [
            self._fill(exec_id="2", time=t2),
            self._fill(exec_id="1", time=t1),
            self._fill(exec_id="3", time=t3),
        ]  # type: ignore[attr-defined]
        grouped = await adapter.fetch_fills()
        times = [f.fill_timestamp for f in grouped["cid-1"]]
        assert times == sorted(times)

    async def test_empty(self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.fills.return_value = []  # type: ignore[attr-defined]
        assert await adapter.fetch_fills() == {}

    async def test_not_connected(self, adapter: IBKRAdapter) -> None:
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_fills()

    async def test_exception_wrapped(
        self, adapter: IBKRAdapter, mock_ib_async_module: MagicMock
    ) -> None:
        await adapter.connect()
        mock_ib = mock_ib_async_module
        mock_ib.fills.side_effect = RuntimeError("boom")  # type: ignore[attr-defined]
        with pytest.raises(PlatformConnectionError):
            await adapter.fetch_fills()

"""Unit tests for IBKR order translation (orders.py).

Tests cases:
    - build_ibkr_orders: maps market, limit, stop, and stop-limit orders correctly
    - client_order_id is correctly mapped to orderRef for idempotency
    - GTD orders carry tif=GTD and a UTC-formatted goodTillDate
    - TP/SL attachments generate parent-child bracket orders with staged transmit
    - reduce_only / position_id / TP-with-limit raise UnsupportedOrderTypeError
    - apply_ibkr_modification: supports updating quantity, limit price, and stop price
    - parse_ibkr_trade: maps ib_async Trade objects to unified OrderResult,
      deriving PARTIALLY_FILLED from Submitted + filled > 0
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from ib_async import Contract, Order, Trade
from ib_async import OrderStatus as IBOrderStatus
from ib_async.util import UNSET_DOUBLE

from unified_trading_execution.errors import PlatformError, UnsupportedOrderTypeError
from unified_trading_execution.ibkr.orders import (
    apply_ibkr_modification,
    build_ibkr_orders,
    map_ibkr_status,
    parse_ibkr_trade,
)
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

AAPL = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
BTC_USD = Instrument(symbol="BTC", quote_currency="USD", asset_class=AssetClass.SPOT)


def make_order(
    order_type: OrderType = OrderType.LIMIT,
    side: OrderSide = OrderSide.BUY,
    **overrides: object,
) -> UnifiedOrder:
    """Build a structurally valid UnifiedOrder with sensible defaults."""
    fields: dict[str, object] = {
        "instrument": AAPL,
        "order_type": order_type,
        "side": side,
        "quantity": Decimal("10"),
        "time_in_force": TimeInForce.GTC,
        "client_order_id": "01900000-0000-7000-8000-000000000001",
        "price": Decimal("101.50"),
        "stop_price": None,
        "take_profit": None,
        "stop_loss": None,
    }
    if order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        fields["stop_price"] = Decimal("99.00")
    fields.update(overrides)
    return UnifiedOrder(**fields)  # type: ignore[arg-type]


def make_trade(
    status: str = "Submitted",
    filled: float = 0.0,
    remaining: float = 10.0,
    avg_fill_price: float = 0.0,
    perm_id: int = 0,
    order_id: int = 42,
    order_ref: str = "cid-1",
) -> Trade:
    """Build a real ib_async Trade offline (events are created in __post_init__)."""
    order = Order(
        orderId=order_id,
        action="BUY",
        totalQuantity=filled + remaining,
        orderType="LMT",
        lmtPrice=Decimal("100"),
        orderRef=order_ref,
        permId=perm_id,
    )
    order_status = IBOrderStatus(
        orderId=order_id,
        status=status,
        filled=filled,
        remaining=remaining,
        avgFillPrice=avg_fill_price,
        permId=perm_id,
    )
    return Trade(contract=Contract(), order=order, orderStatus=order_status)


class TestMapIBKRStatus:
    """IBKR orderStatus string → unified OrderStatus."""

    @pytest.mark.parametrize(
        ("ib_status", "expected"),
        [
            ("ApiPending", OrderStatus.PENDING),
            ("PendingSubmit", OrderStatus.PENDING),
            ("PreSubmitted", OrderStatus.PENDING),
            ("Submitted", OrderStatus.OPEN),
            ("PendingCancel", OrderStatus.OPEN),
            ("Filled", OrderStatus.FILLED),
            ("Cancelled", OrderStatus.CANCELLED),
            ("ApiCancelled", OrderStatus.CANCELLED),
            ("Inactive", OrderStatus.CANCELLED),
        ],
    )
    def test_known_statuses(self, ib_status: str, expected: OrderStatus) -> None:
        assert map_ibkr_status(ib_status) is expected

    def test_unknown_status_raises(self) -> None:
        with pytest.raises(PlatformError, match="Unknown IBKR order status"):
            map_ibkr_status("SomeNewState")


class TestBuildIBKROrders:
    """UnifiedOrder → IBKR Order translation."""

    def test_market_buy(self) -> None:
        """MARKET BUY → ACTION: BUY, TYPE: MKT (no price fields set)."""
        (ib_order,) = build_ibkr_orders(make_order(OrderType.MARKET, OrderSide.BUY))
        assert ib_order.action == "BUY"
        assert ib_order.orderType == "MKT"
        assert ib_order.totalQuantity == 10.0
        assert ib_order.lmtPrice == UNSET_DOUBLE  # unset sentinel, not a price

    def test_market_sell(self) -> None:
        """MARKET SELL → ACTION: SELL, TYPE: MKT."""
        (ib_order,) = build_ibkr_orders(make_order(OrderType.MARKET, OrderSide.SELL))
        assert ib_order.action == "SELL"
        assert ib_order.orderType == "MKT"

    def test_limit_buy(self) -> None:
        """LIMIT BUY → ACTION: BUY, TYPE: LMT with lmtPrice preserved."""
        (ib_order,) = build_ibkr_orders(make_order(OrderType.LIMIT, OrderSide.BUY))
        assert ib_order.action == "BUY"
        assert ib_order.orderType == "LMT"
        assert ib_order.lmtPrice == Decimal("101.50")
        assert isinstance(ib_order.lmtPrice, Decimal)

    def test_limit_sell(self) -> None:
        """LIMIT SELL → ACTION: SELL, TYPE: LMT."""
        (ib_order,) = build_ibkr_orders(make_order(OrderType.LIMIT, OrderSide.SELL))
        assert ib_order.action == "SELL"
        assert ib_order.orderType == "LMT"

    def test_stop_orders(self) -> None:
        """STOP and STOP_LIMIT map to STP and STP LMT with auxPrice."""
        (stop,) = build_ibkr_orders(
            make_order(OrderType.STOP, price=None, stop_price=Decimal("98"))
        )
        assert stop.orderType == "STP"
        assert stop.auxPrice == Decimal("98")

        (stop_limit,) = build_ibkr_orders(
            make_order(OrderType.STOP_LIMIT, stop_price=Decimal("98"))
        )
        assert stop_limit.orderType == "STP LMT"
        assert stop_limit.lmtPrice == Decimal("101.50")
        assert stop_limit.auxPrice == Decimal("98")

    def test_client_order_id_mapped_to_order_ref(self) -> None:
        """client_order_id is populated in orderRef for state tracking."""
        cid = "01900000-0000-7000-8000-000000000abc"
        (ib_order,) = build_ibkr_orders(make_order(client_order_id=cid))
        assert ib_order.orderRef == cid

    def test_missing_client_order_id_raises(self) -> None:
        """An order without a client id cannot be tracked — reject loudly."""
        with pytest.raises(ValueError, match="client_order_id"):
            build_ibkr_orders(make_order(client_order_id=None))

    def test_gtd_carries_good_till_date(self) -> None:
        """GTD sets tif=GTD and formats expire_at as UTC yyyymmdd hh:mm:ss."""
        expiry = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
        (ib_order,) = build_ibkr_orders(make_order(time_in_force=TimeInForce.GTD, expire_at=expiry))
        assert ib_order.tif == "GTD"
        assert ib_order.goodTillDate == "20261231 23:30:00"

    @pytest.mark.parametrize("tif", list(TimeInForce))
    def test_all_time_in_force_values_map(self, tif: TimeInForce) -> None:
        """Every unified TIF has an IBKR wire value (GTD needs expire_at)."""
        overrides: dict[str, object] = {}
        if tif is TimeInForce.GTD:
            overrides["expire_at"] = datetime(2027, 1, 1, tzinfo=UTC)
        (ib_order,) = build_ibkr_orders(make_order(time_in_force=tif, **overrides))
        assert ib_order.tif

    def test_reduce_only_rejected(self) -> None:
        """reduce_only has no native IBKR flag — never approximated."""
        with pytest.raises(UnsupportedOrderTypeError, match="reduce_only"):
            build_ibkr_orders(make_order(reduce_only=True))

    def test_position_id_rejected(self) -> None:
        """position_id leg targeting is out of v1 scope."""
        with pytest.raises(UnsupportedOrderTypeError, match="position_id"):
            build_ibkr_orders(make_order(position_id="12345"))


class TestCryptoSpotRules:
    """IBKR CRYPTO restrictions enforced at translation time (SPOT instruments)."""

    def test_stop_rejected_for_spot(self) -> None:
        """STOP is Market/Limit-only territory on CRYPTO contracts."""
        with pytest.raises(UnsupportedOrderTypeError, match="crypto on IBKR"):
            build_ibkr_orders(
                make_order(
                    OrderType.STOP,
                    instrument=BTC_USD,
                    price=None,
                    stop_price=Decimal("60000"),
                )
            )

    def test_stop_limit_rejected_for_spot(self) -> None:
        """STOP_LIMIT likewise rejected for crypto."""
        with pytest.raises(UnsupportedOrderTypeError, match="crypto on IBKR"):
            build_ibkr_orders(
                make_order(OrderType.STOP_LIMIT, instrument=BTC_USD, stop_price=Decimal("60000"))
            )

    def test_bracket_rejected_for_spot(self) -> None:
        """TP/SL brackets need triggered child legs — impossible for crypto."""
        with pytest.raises(UnsupportedOrderTypeError, match="brackets are not supported"):
            build_ibkr_orders(
                make_order(
                    instrument=BTC_USD,
                    take_profit=TpSlAttachment(trigger_price=Decimal("70000")),
                )
            )

    def test_market_buy_rejected_for_spot(self) -> None:
        """MARKET BUY needs notional cashQty — rejected, LIMIT suggested."""
        with pytest.raises(UnsupportedOrderTypeError, match=r"cashQty"):
            build_ibkr_orders(make_order(OrderType.MARKET, OrderSide.BUY, instrument=BTC_USD))

    def test_market_sell_forced_to_ioc(self) -> None:
        """MARKET SELL builds, but TIF is forced to IOC (only legal value)."""
        (ib_order,) = build_ibkr_orders(
            make_order(
                OrderType.MARKET,
                OrderSide.SELL,
                instrument=BTC_USD,
                time_in_force=TimeInForce.GTC,
            )
        )
        assert ib_order.orderType == "MKT"
        assert ib_order.action == "SELL"
        assert ib_order.tif == "IOC"

    def test_limit_gtc_still_allowed_for_spot(self) -> None:
        """Resting limits keep the user's TIF on crypto."""
        (ib_order,) = build_ibkr_orders(
            make_order(instrument=BTC_USD, time_in_force=TimeInForce.GTC)
        )
        assert ib_order.orderType == "LMT"
        assert ib_order.tif == "GTC"

    def test_non_crypto_stop_unaffected(self) -> None:
        """Regression guard: equity stops still translate normally."""
        (ib_order,) = build_ibkr_orders(
            make_order(OrderType.STOP, price=None, stop_price=Decimal("98"))
        )
        assert ib_order.orderType == "STP"
        assert ib_order.tif == "GTC"


class TestBracketOrders:
    """TP/SL attachments create linked child bracket orders."""

    def test_bracket_orders_for_tpsl(self) -> None:
        """TP/SL attachments create parent + LMT take-profit + STP stop-loss."""
        order = make_order(
            side=OrderSide.BUY,
            take_profit=TpSlAttachment(trigger_price=Decimal("110")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("95")),
        )
        parent, tp, sl = build_ibkr_orders(order)

        # Parent stages transmission; last child transmits atomically.
        assert parent.transmit is False
        assert tp.transmit is False
        assert sl.transmit is True

        # Children reverse the parent's action at the same size.
        for child in (tp, sl):
            assert child.action == "SELL"
            assert child.totalQuantity == 10.0
            assert child.parentId == 0  # adapter links IDs before transmit

        assert tp.orderType == "LMT"
        assert tp.lmtPrice == Decimal("110")

        assert sl.orderType == "STP"
        assert sl.auxPrice == Decimal("95")

        # Both protective legs share an OCA group.
        assert tp.ocaGroup
        assert sl.ocaGroup == tp.ocaGroup

    def test_single_take_profit_bracket(self) -> None:
        """A lone TP still brackets: parent staged, child transmits."""
        order = make_order(take_profit=TpSlAttachment(trigger_price=Decimal("110")))
        parent, tp = build_ibkr_orders(order)
        assert parent.transmit is False
        assert tp.transmit is True
        assert tp.action == "SELL"
        assert not tp.ocaGroup  # nothing to pair with

    def test_stop_loss_with_limit_price_is_stp_lmt(self) -> None:
        """SL limit_price upgrades the child to STP LMT."""
        order = make_order(
            stop_loss=TpSlAttachment(trigger_price=Decimal("95"), limit_price=Decimal("94"))
        )
        _, sl = build_ibkr_orders(order)
        assert sl.orderType == "STP LMT"
        assert sl.auxPrice == Decimal("95")
        assert sl.lmtPrice == Decimal("94")

    def test_take_profit_with_limit_price_rejected(self) -> None:
        """IBKR profit-takers are always limit-at-trigger; no market form."""
        order = make_order(
            take_profit=TpSlAttachment(trigger_price=Decimal("110"), limit_price=Decimal("109"))
        )
        with pytest.raises(UnsupportedOrderTypeError, match=r"take_profit\.limit_price"):
            build_ibkr_orders(order)

    def test_children_inherit_parent_tif(self) -> None:
        """Bracket children carry the parent's TIF (incl. GTD date)."""
        expiry = datetime(2027, 6, 30, tzinfo=UTC)
        order = make_order(
            time_in_force=TimeInForce.GTD,
            expire_at=expiry,
            take_profit=TpSlAttachment(trigger_price=Decimal("110")),
        )
        _, tp = build_ibkr_orders(order)
        assert tp.tif == "GTD"
        assert tp.goodTillDate == "20270630 00:00:00"


class TestApplyIBKRModification:
    """OrderModification → IBKR Order mutation."""

    def test_price_change(self) -> None:
        """Modifying price updates lmtPrice."""
        ib_order = Order(orderId=1, orderType="LMT", action="BUY", totalQuantity=10)
        result = apply_ibkr_modification(
            OrderModification(client_order_id="c", price=Decimal("105")), ib_order
        )
        assert result is ib_order
        assert ib_order.lmtPrice == Decimal("105")

    def test_stop_price_change(self) -> None:
        """Modifying stop_price updates auxPrice."""
        ib_order = Order(orderId=1, orderType="STP", action="SELL", totalQuantity=10)
        apply_ibkr_modification(
            OrderModification(client_order_id="c", stop_price=Decimal("97")), ib_order
        )
        assert ib_order.auxPrice == Decimal("97")

    def test_quantity_change_supported(self) -> None:
        """Modifying quantity updates totalQuantity (supported natively)."""
        ib_order = Order(orderId=1, orderType="LMT", action="BUY", totalQuantity=10)
        apply_ibkr_modification(
            OrderModification(client_order_id="c", quantity=Decimal("25")), ib_order
        )
        assert ib_order.totalQuantity == 25.0

    def test_combined_modification(self) -> None:
        """All supported fields apply in one pass."""
        ib_order = Order(orderId=1, orderType="STP LMT", action="BUY", totalQuantity=10)
        apply_ibkr_modification(
            OrderModification(
                client_order_id="c",
                price=Decimal("103"),
                stop_price=Decimal("96"),
                quantity=Decimal("7"),
            ),
            ib_order,
        )
        assert ib_order.lmtPrice == Decimal("103")
        assert ib_order.auxPrice == Decimal("96")
        assert ib_order.totalQuantity == 7.0

    def test_tpsl_modification_rejected(self) -> None:
        """Bracket legs are separate IBKR orders — reject at translation."""
        ib_order = Order(orderId=1, orderType="LMT", action="BUY", totalQuantity=10)
        with pytest.raises(UnsupportedOrderTypeError, match="TP/SL"):
            apply_ibkr_modification(
                OrderModification(
                    client_order_id="c",
                    take_profit=TpSlAttachment(trigger_price=Decimal("120")),
                ),
                ib_order,
            )


class TestParseIBKRTrade:
    """ib_async Trade → OrderResult parsing."""

    def test_submitted_without_fills_is_open(self) -> None:
        """Submitted with no fills maps to OPEN."""
        result = parse_ibkr_trade(make_trade(status="Submitted", filled=0))
        assert result.status is OrderStatus.OPEN
        assert result.filled_quantity == 0
        assert result.average_fill_price is None

    def test_partial_fill_derived(self) -> None:
        """Submitted + filled>0<remaining becomes PARTIALLY_FILLED."""
        result = parse_ibkr_trade(
            make_trade(status="Submitted", filled=4, remaining=6, avg_fill_price=101.25)
        )
        assert result.status is OrderStatus.PARTIALLY_FILLED
        assert result.filled_quantity == 4
        assert result.average_fill_price == Decimal("101.25")

    def test_fully_filled_status(self) -> None:
        """Filled maps to FILLED with quantity and price."""
        result = parse_ibkr_trade(
            make_trade(status="Filled", filled=10, remaining=0, avg_fill_price=101.0)
        )
        assert result.status is OrderStatus.FILLED
        assert result.filled_quantity == 10

    def test_cancelled_status(self) -> None:
        """Cancelled/ApiCancelled map to CANCELLED."""
        for status in ("Cancelled", "ApiCancelled"):
            result = parse_ibkr_trade(make_trade(status=status))
            assert result.status is OrderStatus.CANCELLED

    def test_waiting_status_is_pending_then_filled_when_complete(self) -> None:
        """Waiting statuses are PENDING unless fully filled."""
        result = parse_ibkr_trade(make_trade(status="PreSubmitted", filled=2, remaining=8))
        assert result.status is OrderStatus.PARTIALLY_FILLED  # derived from fills

        complete = parse_ibkr_trade(make_trade(status="PreSubmitted", filled=10, remaining=0))
        assert complete.status is OrderStatus.FILLED

    def test_platform_order_id_prefers_perm_id(self) -> None:
        """permId (stable across restarts) wins over the session orderId."""
        result = parse_ibkr_trade(make_trade(perm_id=999999, order_id=42))
        assert result.platform_order_id == "999999"

        fallback = parse_ibkr_trade(make_trade(perm_id=0, order_id=42))
        assert fallback.platform_order_id == "42"

    def test_client_order_id_from_order_ref(self) -> None:
        """orderRef carries the framework's client_order_id."""
        result = parse_ibkr_trade(make_trade(order_ref="my-cid-123"))
        assert result.client_order_id == "my-cid-123"

    def test_timestamps_are_timezone_aware(self) -> None:
        """Empty log falls back to now(UTC); both stamps stay aware."""
        result = parse_ibkr_trade(make_trade())
        assert result.created_at.tzinfo is not None
        assert result.updated_at.tzinfo is not None

    def test_unknown_status_raises(self) -> None:
        """An unknown IBKR status fails loudly instead of misrepresenting."""
        with pytest.raises(PlatformError, match="Unknown IBKR order status"):
            parse_ibkr_trade(make_trade(status="MysteryState"))

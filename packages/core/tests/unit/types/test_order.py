"""Unit tests for all order-related types — Sections 17.4–17.8 and FillRecord from 17.11.

Every construction invariant tested for valid and invalid cases.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from decimal import Decimal

import pytest

from unified_trading_execution.types.enums import (
    AssetClass,
    OptionRight,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)

# ---- Helpers ----

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=timezone.utc)


def make_btc():
    return Instrument(
        symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT,
        exchange=None, currency=None, expiry=None, strike=None,
        option_right=None, multiplier=None,
    )


# ============================================================
# TpSlAttachment (Section 17.5)
# ============================================================

class TestTpSlAttachment:
    def test_valid_market_tp(self):
        tp = TpSlAttachment(trigger_price=Decimal("60000"))
        assert tp.trigger_price == Decimal("60000")
        assert tp.limit_price is None

    def test_valid_limit_tp(self):
        tp = TpSlAttachment(trigger_price=Decimal("60000"), limit_price=Decimal("60100"))
        assert tp.limit_price == Decimal("60100")

    def test_trigger_price_must_be_positive(self):
        with pytest.raises(ValueError, match="trigger_price must be > 0"):
            TpSlAttachment(trigger_price=Decimal("0"))

    def test_trigger_price_must_not_be_negative(self):
        with pytest.raises(ValueError, match="trigger_price must be > 0"):
            TpSlAttachment(trigger_price=Decimal("-1"))

    def test_limit_price_must_be_positive_when_set(self):
        with pytest.raises(ValueError, match="limit_price must be > 0"):
            TpSlAttachment(trigger_price=Decimal("60000"), limit_price=Decimal("0"))

    def test_is_frozen(self):
        tp = TpSlAttachment(trigger_price=Decimal("60000"))
        with pytest.raises(Exception):
            tp.trigger_price = Decimal("50000")  # type: ignore[misc]


# ============================================================
# UnifiedOrder (Section 17.5)
# ============================================================

class TestUnifiedOrder:
    def test_market_order_valid(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.IOC,
        )
        assert o.price is None
        assert o.stop_price is None

    def test_limit_order_valid(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.LIMIT,
            side=OrderSide.SELL,
            quantity=Decimal("0.001"),
            price=Decimal("50000"),
            time_in_force=TimeInForce.GTC,
        )
        assert o.price == Decimal("50000")

    def test_stop_order_valid(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.STOP,
            side=OrderSide.SELL,
            quantity=Decimal("0.001"),
            stop_price=Decimal("49000"),
            time_in_force=TimeInForce.GTC,
        )
        assert o.stop_price == Decimal("49000")
        assert o.price is None

    def test_stop_limit_order_valid(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.STOP_LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            price=Decimal("51000"),
            stop_price=Decimal("50500"),
            time_in_force=TimeInForce.GTC,
        )
        assert o.price == Decimal("51000")
        assert o.stop_price == Decimal("50500")

    def test_with_client_order_id(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.IOC,
            client_order_id="my-custom-id-123",
        )
        assert o.client_order_id == "my-custom-id-123"

    def test_with_tp_sl(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            price=Decimal("50000"),
            time_in_force=TimeInForce.GTC,
            take_profit=TpSlAttachment(trigger_price=Decimal("55000")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("48000")),
        )
        assert o.take_profit is not None
        assert o.stop_loss is not None

    def test_reduce_only_defaults_false(self):
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.MARKET,
            side=OrderSide.SELL,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.IOC,
        )
        assert o.reduce_only is False

    # ---- Invalid cases: missing required fields ----

    def test_limit_requires_price(self):
        with pytest.raises(ValueError, match="price is required for LIMIT"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=TimeInForce.GTC,
            )

    def test_stop_limit_requires_price(self):
        with pytest.raises(ValueError, match="price is required for STOP_LIMIT"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.STOP_LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                stop_price=Decimal("50000"),
                time_in_force=TimeInForce.GTC,
            )

    def test_stop_requires_stop_price(self):
        with pytest.raises(ValueError, match="stop_price is required for STOP"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.STOP,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=TimeInForce.GTC,
            )

    def test_stop_limit_requires_stop_price(self):
        with pytest.raises(ValueError, match="stop_price is required for STOP_LIMIT"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.STOP_LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                price=Decimal("50000"),
                time_in_force=TimeInForce.GTC,
            )

    def test_quantity_must_be_positive(self):
        with pytest.raises(ValueError, match="quantity must be > 0"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=Decimal("0"),
                time_in_force=TimeInForce.IOC,
            )

    def test_quantity_negative_rejected(self):
        with pytest.raises(ValueError, match="quantity must be > 0"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=Decimal("-1"),
                time_in_force=TimeInForce.IOC,
            )

    def test_price_zero_rejected(self):
        with pytest.raises(ValueError, match="price must be > 0"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                price=Decimal("0"),
                time_in_force=TimeInForce.GTC,
            )

    def test_stop_price_negative_rejected(self):
        with pytest.raises(ValueError, match="stop_price must be > 0"):
            UnifiedOrder(
                instrument=make_btc(),
                order_type=OrderType.STOP,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                stop_price=Decimal("-1"),
                time_in_force=TimeInForce.GTC,
            )

    def test_mutable_after_construction(self):
        """UnifiedOrder is NOT frozen — Engine sets client_order_id after construction."""
        o = UnifiedOrder(
            instrument=make_btc(),
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.IOC,
        )
        o.client_order_id = "engine-set-id"
        assert o.client_order_id == "engine-set-id"


# ============================================================
# OrderModification (Section 17.6)
# ============================================================

class TestOrderModification:
    def test_valid_price_mod(self):
        m = OrderModification(client_order_id="abc", price=Decimal("51000"))
        assert m.price == Decimal("51000")

    def test_valid_quantity_mod(self):
        m = OrderModification(client_order_id="abc", quantity=Decimal("0.002"))
        assert m.quantity == Decimal("0.002")

    def test_at_least_one_field_required(self):
        with pytest.raises(ValueError, match="at least one field"):
            OrderModification(client_order_id="abc")

    def test_multiple_fields_allowed(self):
        m = OrderModification(
            client_order_id="abc",
            price=Decimal("51000"),
            quantity=Decimal("0.002"),
        )
        assert m.price is not None
        assert m.quantity is not None


# ============================================================
# OrderResult (Section 17.7)
# ============================================================

class TestOrderResult:
    def test_valid_result(self):
        r = OrderResult(
            client_order_id="abc",
            platform_order_id="plat-123",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=NOW,
            updated_at=NOW,
        )
        assert r.status == OrderStatus.OPEN

    def test_rejected_before_platform_id(self):
        r = OrderResult(
            client_order_id="abc",
            platform_order_id=None,
            status=OrderStatus.REJECTED,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            created_at=NOW,
            updated_at=NOW,
        )
        assert r.platform_order_id is None

    def test_filled_result(self):
        r = OrderResult(
            client_order_id="abc",
            platform_order_id="plat-123",
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.001"),
            average_fill_price=Decimal("50000"),
            created_at=NOW,
            updated_at=NOW,
        )
        assert r.filled_quantity == Decimal("0.001")
        assert r.average_fill_price == Decimal("50000")

    def test_naive_datetime_rejected(self):
        naive = datetime(2026, 7, 28, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            OrderResult(
                client_order_id="abc",
                platform_order_id="plat-123",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=naive,
                updated_at=NOW,
            )

    def test_naive_updated_at_rejected(self):
        naive = datetime(2026, 7, 28, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            OrderResult(
                client_order_id="abc",
                platform_order_id="plat-123",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=NOW,
                updated_at=naive,
            )

    def test_is_frozen(self):
        r = OrderResult(
            client_order_id="abc", platform_order_id="plat-123",
            status=OrderStatus.OPEN, filled_quantity=Decimal("0"),
            average_fill_price=None, created_at=NOW, updated_at=NOW,
        )
        with pytest.raises(Exception):
            r.status = OrderStatus.FILLED  # type: ignore[misc]


# ============================================================
# OrderRecord (Section 17.8)
# ============================================================

class TestOrderRecord:
    def test_valid_record(self):
        inst = make_btc()
        r = OrderRecord(
            instrument=inst,
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.GTC,
            client_order_id="abc",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="plat-123",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-xyz",
            created_at=NOW,
            updated_at=NOW,
        )
        assert r.correlation_id == "corr-xyz"
        assert r.instrument == inst

    def test_is_frozen(self):
        inst = make_btc()
        r = OrderRecord(
            instrument=inst,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.IOC,
            client_order_id="abc",
            price=None,
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id=None,
            status=OrderStatus.PENDING,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-xyz",
            created_at=NOW,
            updated_at=NOW,
        )
        with pytest.raises(Exception):
            r.status = OrderStatus.FILLED  # type: ignore[misc]


# ============================================================
# FillRecord (Section 17.11)
# ============================================================

class TestFillRecord:
    def test_valid_fill(self):
        inst = make_btc()
        f = FillRecord(
            client_order_id="abc",
            platform_fill_id="fill-1",
            instrument=inst,
            fill_quantity=Decimal("0.001"),
            fill_price=Decimal("50000"),
            fill_timestamp=NOW,
            fee_currency="USDT",
            fee_amount=Decimal("0.05"),
            correlation_id="corr-xyz",
        )
        assert f.fill_quantity == Decimal("0.001")
        assert f.fee_amount == Decimal("0.05")

    def test_fee_fields_optional(self):
        inst = make_btc()
        f = FillRecord(
            client_order_id="abc",
            platform_fill_id="fill-1",
            instrument=inst,
            fill_quantity=Decimal("0.001"),
            fill_price=Decimal("50000"),
            fill_timestamp=NOW,
            fee_currency=None,
            fee_amount=None,
            correlation_id="corr-xyz",
        )
        assert f.fee_currency is None

    def test_fill_quantity_must_be_positive(self):
        inst = make_btc()
        with pytest.raises(ValueError, match="fill_quantity must be > 0"):
            FillRecord(
                client_order_id="abc", platform_fill_id="fill-1",
                instrument=inst, fill_quantity=Decimal("0"),
                fill_price=Decimal("50000"), fill_timestamp=NOW,
                fee_currency=None, fee_amount=None, correlation_id="corr-xyz",
            )

    def test_fill_price_must_be_positive(self):
        inst = make_btc()
        with pytest.raises(ValueError, match="fill_price must be > 0"):
            FillRecord(
                client_order_id="abc", platform_fill_id="fill-1",
                instrument=inst, fill_quantity=Decimal("0.001"),
                fill_price=Decimal("0"), fill_timestamp=NOW,
                fee_currency=None, fee_amount=None, correlation_id="corr-xyz",
            )

    def test_naive_timestamp_rejected(self):
        inst = make_btc()
        naive = datetime(2026, 7, 28, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            FillRecord(
                client_order_id="abc", platform_fill_id="fill-1",
                instrument=inst, fill_quantity=Decimal("0.001"),
                fill_price=Decimal("50000"), fill_timestamp=naive,
                fee_currency=None, fee_amount=None, correlation_id="corr-xyz",
            )

    def test_is_frozen(self):
        inst = make_btc()
        f = FillRecord(
            client_order_id="abc", platform_fill_id="fill-1",
            instrument=inst, fill_quantity=Decimal("0.001"),
            fill_price=Decimal("50000"), fill_timestamp=NOW,
            fee_currency=None, fee_amount=None, correlation_id="corr-xyz",
        )
        with pytest.raises(Exception):
            f.fill_quantity = Decimal("0.002")  # type: ignore[misc]

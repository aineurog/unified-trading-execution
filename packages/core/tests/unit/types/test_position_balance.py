"""Unit tests for Position and Balance — Section 17.9."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Balance, Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_btc():
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


# ============================================================
# Position
# ============================================================


class TestPosition:
    def test_long_position(self):
        p = Position(
            instrument=make_btc(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
        )
        assert p.quantity == Decimal("0.5")

    def test_short_position(self):
        p = Position(
            instrument=make_btc(),
            quantity=Decimal("-0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
        )
        assert p.quantity < 0

    def test_naive_datetime_rejected(self):
        naive = datetime(2026, 7, 28, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            Position(
                instrument=make_btc(),
                quantity=Decimal("0.5"),
                average_entry_price=Decimal("50000"),
                updated_at=naive,
            )

    def test_is_frozen(self):
        p = Position(
            instrument=make_btc(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
        )
        with pytest.raises(Exception):
            p.quantity = Decimal("1.0")  # type: ignore[misc]


# ============================================================
# Balance
# ============================================================


class TestBalance:
    def test_valid_balance(self):
        b = Balance(
            currency="USDT",
            free=Decimal("9000"),
            used=Decimal("1000"),
            total=Decimal("10000"),
            updated_at=NOW,
        )
        assert b.free == Decimal("9000")
        assert b.used == Decimal("1000")

    def test_invariant_total_equals_free_plus_used(self):
        with pytest.raises(ValueError, match="Balance invariant violated"):
            Balance(
                currency="USDT",
                free=Decimal("9000"),
                used=Decimal("1000"),
                total=Decimal("9999"),  # mismatch
                updated_at=NOW,
            )

    def test_invariant_zero_balance(self):
        b = Balance(
            currency="USDT",
            free=Decimal("0"),
            used=Decimal("0"),
            total=Decimal("0"),
            updated_at=NOW,
        )
        assert b.free == Decimal("0")

    def test_naive_datetime_rejected(self):
        naive = datetime(2026, 7, 28, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            Balance(
                currency="USDT",
                free=Decimal("9000"),
                used=Decimal("1000"),
                total=Decimal("10000"),
                updated_at=naive,
            )

    def test_is_frozen(self):
        b = Balance(
            currency="USDT",
            free=Decimal("9000"),
            used=Decimal("1000"),
            total=Decimal("10000"),
            updated_at=NOW,
        )
        with pytest.raises(Exception):
            b.free = Decimal("8000")  # type: ignore[misc]

    def test_available_ratio_all_free(self):
        b = Balance(
            currency="USDT",
            free=Decimal("10000"),
            used=Decimal("0"),
            total=Decimal("10000"),
            updated_at=NOW,
        )
        assert b.available_ratio == Decimal("1.0")

    def test_available_ratio_half_used(self):
        b = Balance(
            currency="USDT",
            free=Decimal("5000"),
            used=Decimal("5000"),
            total=Decimal("10000"),
            updated_at=NOW,
        )
        assert b.available_ratio == Decimal("0.5")

    def test_available_ratio_zero_total(self):
        b = Balance(
            currency="USDT",
            free=Decimal("0"),
            used=Decimal("0"),
            total=Decimal("0"),
            updated_at=NOW,
        )
        assert b.available_ratio == Decimal("1.0")

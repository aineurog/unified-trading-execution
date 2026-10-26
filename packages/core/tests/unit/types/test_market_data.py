"""Unit tests for Ticker and Candle — market-data types."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from unified_trading_execution.types.market_data import Candle, Ticker

OPEN_TIME = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)
CLOSE_TIME = datetime(2026, 7, 28, 12, 1, 0, tzinfo=UTC)


def make_candle(**overrides: object) -> Candle:
    values: dict[str, object] = {
        "open_time": OPEN_TIME,
        "close_time": CLOSE_TIME,
        "open": Decimal("100"),
        "high": Decimal("110"),
        "low": Decimal("90"),
        "close": Decimal("105"),
    }
    values.update(overrides)
    return Candle(**values)  # type: ignore[arg-type]


def test_mid_is_bid_ask_average() -> None:
    ticker = Ticker(bid=Decimal("1.0998"), ask=Decimal("1.1002"))

    assert ticker.mid == Decimal("1.1000")


def test_mid_falls_back_to_last() -> None:
    ticker = Ticker(last=Decimal("100000.5"))

    assert ticker.mid == Decimal("100000.5")


def test_mid_falls_back_to_mark_when_no_last() -> None:
    ticker = Ticker(mark=Decimal("100001"))

    assert ticker.mid == Decimal("100001")


def test_mid_none_when_empty() -> None:
    assert Ticker().mid is None


def test_defaults_are_none() -> None:
    ticker = Ticker()

    assert ticker.bid is None
    assert ticker.ask is None
    assert ticker.last is None
    assert ticker.mark is None


# ============================================================
# Candle
# ============================================================


def test_candle_valid() -> None:
    candle = make_candle()

    assert candle.open == Decimal("100")
    assert candle.high == Decimal("110")
    assert candle.low == Decimal("90")
    assert candle.close == Decimal("105")


@pytest.mark.parametrize("field", ["open_time", "close_time"])
def test_candle_rejects_naive_times(field: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_candle(**{field: datetime(2026, 7, 28, 12, 0, 0)})


def test_candle_rejects_close_before_open() -> None:
    with pytest.raises(ValueError, match="close_time must be after open_time"):
        make_candle(close_time=OPEN_TIME)


def test_candle_rejects_high_below_body() -> None:
    with pytest.raises(ValueError, match="high must be"):
        make_candle(high=Decimal("104"))


def test_candle_rejects_low_above_body() -> None:
    with pytest.raises(ValueError, match="low must be"):
        make_candle(low=Decimal("101"))

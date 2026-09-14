"""Unit tests for Ticker — the single-instrument price snapshot."""

from __future__ import annotations

from decimal import Decimal

from unified_trading_execution.types.market_data import Ticker


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

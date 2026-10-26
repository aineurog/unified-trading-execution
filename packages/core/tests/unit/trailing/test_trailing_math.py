"""Unit tests for pure trailing-stop and ATR math."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

import pytest

from unified_trading_execution.errors import (
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.trailing.math import (
    candidate_stop,
    initial_stop_price,
    is_activation_reached,
    profit_percent_activation_price,
    quantize_price,
    quantize_stop,
    quantize_take_profit,
    ratchet_accepts,
    resolve_activation_price,
    respects_min_stop_distance,
    take_profit_price,
    true_range,
    update_best_basis,
    validate_level_direction,
    wilder_atr,
)
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingOffsetType,
    TrailingSpec,
)
from unified_trading_execution.types.enums import OrderSide
from unified_trading_execution.types.market_data import Candle

LONG = OrderSide.BUY
SHORT = OrderSide.SELL
BASE_TIME = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_candle(high: str, low: str, close: str, index: int = 0) -> Candle:
    open_time = BASE_TIME + timedelta(minutes=index)
    return Candle(
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=Decimal(close),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def atr_series() -> list[Candle]:
    return [
        make_candle("10", "8", "9", 0),
        make_candle("12", "9", "11", 1),
        make_candle("13", "10", "12", 2),
        make_candle("14", "8", "10", 3),
        make_candle("11", "7", "8", 4),
    ]


def make_spec(**overrides: Any) -> TrailingSpec:
    values: dict[str, Any] = {
        "offset_type": TrailingOffsetType.PRICE,
        "offset": Decimal("5"),
    }
    values.update(overrides)
    return TrailingSpec(**values)


def make_atr_spec() -> TrailingSpec:
    return make_spec(
        offset_type=TrailingOffsetType.ATR,
        offset=Decimal("2"),
        atr_period=14,
        atr_timeframe=CandleTimeframe.H1,
    )


def level(level_type: ProtectionLevelType, value: str) -> ProtectionLevel:
    return ProtectionLevel(type=level_type, value=Decimal(value))


# ============================================================
# Wilder ATR
# ============================================================


def test_true_range_uses_previous_close() -> None:
    candles = atr_series()
    assert true_range(candles[1], candles[0].close) == Decimal("3")
    assert true_range(candles[3], candles[2].close) == Decimal("6")


def test_atr_seed_is_mean_of_first_period_ranges() -> None:
    assert wilder_atr(atr_series()[:4], 3) == Decimal("4")


def test_atr_recursion_updates_on_later_candles() -> None:
    assert wilder_atr(atr_series(), 3) == Decimal("4")
    assert wilder_atr(atr_series()[:4], 2) == Decimal("4.5")


def test_atr_requires_period_plus_one_candles() -> None:
    with pytest.raises(TrailingNotSupportedError, match="needs 4 closed candles, got 3"):
        wilder_atr(atr_series()[:3], 3)


def test_atr_rejects_period_below_two() -> None:
    with pytest.raises(ValueError, match="period must be >= 2"):
        wilder_atr(atr_series(), 1)


# ============================================================
# Best basis and activation
# ============================================================


def test_best_basis_seeds_from_first_sample() -> None:
    assert update_best_basis(LONG, None, Decimal("100")) == Decimal("100")


def test_best_basis_ratchets_by_side() -> None:
    assert update_best_basis(LONG, Decimal("100"), Decimal("110")) == Decimal("110")
    assert update_best_basis(LONG, Decimal("100"), Decimal("90")) == Decimal("100")
    assert update_best_basis(SHORT, Decimal("100"), Decimal("90")) == Decimal("90")
    assert update_best_basis(SHORT, Decimal("100"), Decimal("110")) == Decimal("100")


def test_immediate_activation_has_no_price() -> None:
    spec = make_spec()
    assert resolve_activation_price(spec, LONG, Decimal("100")) is None
    assert is_activation_reached(LONG, Decimal("1"), None) is True


def test_price_activation_reached_by_side() -> None:
    spec = make_spec(
        activation=TrailingActivation.PRICE,
        activation_price=Decimal("105"),
    )
    price = resolve_activation_price(spec, LONG, Decimal("100"))
    assert price == Decimal("105")
    assert is_activation_reached(LONG, Decimal("105"), price) is True
    assert is_activation_reached(LONG, Decimal("104.99"), price) is False

    short_spec = make_spec(
        activation=TrailingActivation.PRICE,
        activation_price=Decimal("95"),
    )
    short_price = resolve_activation_price(short_spec, SHORT, Decimal("100"))
    assert is_activation_reached(SHORT, Decimal("95"), short_price) is True
    assert is_activation_reached(SHORT, Decimal("95.01"), short_price) is False


def test_profit_percent_activation_is_recomputed_from_entry() -> None:
    spec = make_spec(
        activation=TrailingActivation.PROFIT_PERCENT,
        activation_profit_percent=Decimal("2"),
    )
    assert resolve_activation_price(spec, LONG, Decimal("100")) == Decimal("102")
    assert resolve_activation_price(spec, LONG, Decimal("200")) == Decimal("204")
    assert resolve_activation_price(spec, SHORT, Decimal("100")) == Decimal("98")


def test_profit_percent_helper_mirrors_side() -> None:
    assert profit_percent_activation_price(Decimal("100"), LONG, Decimal("2.5")) == Decimal("102.5")
    assert profit_percent_activation_price(Decimal("100"), SHORT, Decimal("2.5")) == Decimal("97.5")


# ============================================================
# Candidate stop
# ============================================================


def test_price_candidate_by_side() -> None:
    spec = make_spec(offset=Decimal("5"))
    assert candidate_stop(side=LONG, best_basis=Decimal("110"), spec=spec) == Decimal("105")
    assert candidate_stop(side=SHORT, best_basis=Decimal("110"), spec=spec) == Decimal("115")


def test_percent_candidate_by_side() -> None:
    spec = make_spec(offset_type=TrailingOffsetType.PERCENT, offset=Decimal("2.5"))
    assert candidate_stop(side=LONG, best_basis=Decimal("200"), spec=spec) == Decimal("195.000")
    assert candidate_stop(side=SHORT, best_basis=Decimal("200"), spec=spec) == Decimal("205.000")


def test_atr_candidate_by_side() -> None:
    spec = make_atr_spec()
    assert candidate_stop(
        side=LONG, best_basis=Decimal("100"), spec=spec, atr=Decimal("3")
    ) == Decimal("94")
    assert candidate_stop(
        side=SHORT, best_basis=Decimal("100"), spec=spec, atr=Decimal("3")
    ) == Decimal("106")


def test_atr_candidate_requires_atr() -> None:
    with pytest.raises(ValueError, match="requires an ATR value"):
        candidate_stop(side=LONG, best_basis=Decimal("100"), spec=make_atr_spec())


def test_one_atr_serves_offset_and_levels() -> None:
    spec = make_spec(
        offset_type=TrailingOffsetType.ATR,
        offset=Decimal("2"),
        initial_stop=level(ProtectionLevelType.ATR, "1.5"),
        take_profit=level(ProtectionLevelType.ATR, "3"),
        atr_period=14,
        atr_timeframe=CandleTimeframe.H1,
    )
    atr = Decimal("4")
    assert spec.initial_stop is not None
    assert spec.take_profit is not None
    assert candidate_stop(side=LONG, best_basis=Decimal("100"), spec=spec, atr=atr) == Decimal("92")
    assert initial_stop_price(spec.initial_stop, LONG, Decimal("100"), atr=atr) == Decimal("94")
    assert take_profit_price(spec.take_profit, LONG, Decimal("100"), atr=atr) == Decimal("112")


# ============================================================
# Static protection levels
# ============================================================


def test_price_initial_stop_and_take_profit() -> None:
    stop = level(ProtectionLevelType.PRICE, "95")
    target = level(ProtectionLevelType.PRICE, "105")
    assert initial_stop_price(stop, LONG, Decimal("100")) == Decimal("95")
    assert initial_stop_price(stop, SHORT, Decimal("100")) == Decimal("95")
    assert take_profit_price(target, LONG, Decimal("100")) == Decimal("105")
    assert take_profit_price(target, SHORT, Decimal("100")) == Decimal("105")


def test_percent_levels_mirror_side() -> None:
    stop = level(ProtectionLevelType.PERCENT, "5")
    target = level(ProtectionLevelType.PERCENT, "5")
    assert initial_stop_price(stop, LONG, Decimal("100")) == Decimal("95")
    assert initial_stop_price(stop, SHORT, Decimal("100")) == Decimal("105")
    assert take_profit_price(target, LONG, Decimal("100")) == Decimal("105")
    assert take_profit_price(target, SHORT, Decimal("100")) == Decimal("95")


def test_atr_levels_mirror_side() -> None:
    stop = level(ProtectionLevelType.ATR, "1.5")
    target = level(ProtectionLevelType.ATR, "3")
    atr = Decimal("4")
    assert initial_stop_price(stop, LONG, Decimal("100"), atr=atr) == Decimal("94")
    assert initial_stop_price(stop, SHORT, Decimal("100"), atr=atr) == Decimal("106")
    assert take_profit_price(target, LONG, Decimal("100"), atr=atr) == Decimal("112")
    assert take_profit_price(target, SHORT, Decimal("100"), atr=atr) == Decimal("88")


def test_atr_levels_require_atr() -> None:
    stop = level(ProtectionLevelType.ATR, "1.5")
    with pytest.raises(ValueError, match="requires an ATR value"):
        initial_stop_price(stop, LONG, Decimal("100"))
    with pytest.raises(ValueError, match="requires an ATR value"):
        take_profit_price(stop, LONG, Decimal("100"))


def test_level_direction_rejects_wrong_side() -> None:
    validate_level_direction(
        side=LONG,
        role="initial_stop",
        level_price=Decimal("99"),
        reference_price=Decimal("100"),
    )
    validate_level_direction(
        side=LONG,
        role="take_profit",
        level_price=Decimal("101"),
        reference_price=Decimal("100"),
    )
    validate_level_direction(
        side=SHORT,
        role="initial_stop",
        level_price=Decimal("101"),
        reference_price=Decimal("100"),
    )
    validate_level_direction(
        side=SHORT,
        role="take_profit",
        level_price=Decimal("99"),
        reference_price=Decimal("100"),
    )

    with pytest.raises(TrailingStateError, match="wrong side of 100"):
        validate_level_direction(
            side=LONG,
            role="initial_stop",
            level_price=Decimal("101"),
            reference_price=Decimal("100"),
        )
    with pytest.raises(TrailingStateError, match="wrong side of 100"):
        validate_level_direction(
            side=SHORT,
            role="take_profit",
            level_price=Decimal("101"),
            reference_price=Decimal("100"),
        )


# ============================================================
# Rounding
# ============================================================


def test_quantize_price_uses_explicit_rounding() -> None:
    assert quantize_price(Decimal("100.3"), Decimal("0.5"), ROUND_DOWN) == Decimal("100.0")
    assert quantize_price(Decimal("100.3"), Decimal("0.5"), ROUND_UP) == Decimal("100.5")


def test_stop_rounds_away_from_market() -> None:
    assert quantize_stop(Decimal("100.3"), Decimal("0.5"), LONG) == Decimal("100.0")
    assert quantize_stop(Decimal("100.3"), Decimal("0.5"), SHORT) == Decimal("100.5")


def test_take_profit_rounds_in_profitable_direction() -> None:
    assert quantize_take_profit(Decimal("100.3"), Decimal("0.5"), LONG) == Decimal("100.5")
    assert quantize_take_profit(Decimal("100.3"), Decimal("0.5"), SHORT) == Decimal("100.0")


# ============================================================
# Minimum distance and ratchet
# ============================================================


def test_minimum_distance_at_first_placement_and_reprice() -> None:
    assert respects_min_stop_distance(
        side=LONG,
        candidate=Decimal("98"),
        basis=Decimal("100"),
        min_stop_distance=Decimal("2"),
    )
    assert not respects_min_stop_distance(
        side=LONG,
        candidate=Decimal("98.01"),
        basis=Decimal("100"),
        min_stop_distance=Decimal("2"),
    )
    assert respects_min_stop_distance(
        side=SHORT,
        candidate=Decimal("102"),
        basis=Decimal("100"),
        min_stop_distance=Decimal("2"),
    )
    assert not respects_min_stop_distance(
        side=SHORT,
        candidate=Decimal("101.99"),
        basis=Decimal("100"),
        min_stop_distance=Decimal("2"),
    )


def test_no_minimum_distance_always_passes() -> None:
    assert respects_min_stop_distance(
        side=LONG,
        candidate=Decimal("100"),
        basis=Decimal("100"),
        min_stop_distance=None,
    )


def test_ratchet_long_only_tightens() -> None:
    assert ratchet_accepts(side=LONG, current_stop=None, candidate=Decimal("90"))
    assert ratchet_accepts(side=LONG, current_stop=Decimal("100"), candidate=Decimal("101"))
    assert not ratchet_accepts(side=LONG, current_stop=Decimal("100"), candidate=Decimal("100"))
    assert not ratchet_accepts(side=LONG, current_stop=Decimal("100"), candidate=Decimal("99"))


def test_ratchet_short_only_tightens() -> None:
    assert ratchet_accepts(side=SHORT, current_stop=None, candidate=Decimal("110"))
    assert ratchet_accepts(side=SHORT, current_stop=Decimal("100"), candidate=Decimal("99"))
    assert not ratchet_accepts(side=SHORT, current_stop=Decimal("100"), candidate=Decimal("100"))
    assert not ratchet_accepts(side=SHORT, current_stop=Decimal("100"), candidate=Decimal("101"))


def test_ratchet_honours_step_threshold() -> None:
    assert not ratchet_accepts(
        side=LONG,
        current_stop=Decimal("100"),
        candidate=Decimal("101"),
        step=Decimal("2"),
    )
    assert ratchet_accepts(
        side=LONG,
        current_stop=Decimal("100"),
        candidate=Decimal("102"),
        step=Decimal("2"),
    )
    assert not ratchet_accepts(
        side=SHORT,
        current_stop=Decimal("100"),
        candidate=Decimal("99"),
        step=Decimal("2"),
    )
    assert ratchet_accepts(
        side=SHORT,
        current_stop=Decimal("100"),
        candidate=Decimal("98"),
        step=Decimal("2"),
    )

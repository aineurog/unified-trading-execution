"""Unit tests for trailing contract types."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.trailing.types import (
    ACTIVE_TRAILING_PHASES,
    TERMINAL_TRAILING_PHASES,
    CandleTimeframe,
    NativeTrailingRequest,
    NativeTrailingResult,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_instrument() -> Instrument:
    return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)


def make_spec(**overrides: Any) -> TrailingSpec:
    kwargs: dict[str, Any] = {
        "offset_type": TrailingOffsetType.PRICE,
        "offset": Decimal("10"),
    }
    kwargs.update(overrides)
    return TrailingSpec(**kwargs)


def make_state(**overrides: Any) -> TrailingState:
    kwargs: dict[str, Any] = {
        "trailing_id": "tr-1",
        "instrument": make_instrument(),
        "position_id": "leg-1",
        "mode": TrailingMode.SYNTHETIC,
        "phase": TrailingPhase.ARMED,
        "spec": make_spec(),
        "side": OrderSide.BUY,
        "entry_price": Decimal("100"),
        "quantity": Decimal("1"),
        "created_at": NOW,
        "updated_at": NOW,
    }
    kwargs.update(overrides)
    return TrailingState(**kwargs)


# ============================================================
# Enums
# ============================================================


def test_mode_values() -> None:
    assert TrailingMode.AUTO == "auto"
    assert TrailingMode.NATIVE == "native"
    assert TrailingMode.SYNTHETIC == "synthetic"


def test_phase_values_are_lowercase() -> None:
    assert {p.value for p in TrailingPhase} == {
        "arming",
        "armed",
        "triggered",
        "done",
        "cancelled",
        "degraded",
        "failed",
    }


def test_candle_timeframes_are_closed_set() -> None:
    assert {tf.value for tf in CandleTimeframe} == {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}


def test_basis_values() -> None:
    assert {b.value for b in TrailingBasis} == {"last", "mark", "mid", "bid", "ask"}


def test_active_and_terminal_phase_sets_are_disjoint() -> None:
    active = ACTIVE_TRAILING_PHASES
    terminal = TERMINAL_TRAILING_PHASES
    assert active == {
        TrailingPhase.ARMING,
        TrailingPhase.ARMED,
        TrailingPhase.TRIGGERED,
        TrailingPhase.DEGRADED,
    }
    assert terminal == {
        TrailingPhase.DONE,
        TrailingPhase.CANCELLED,
        TrailingPhase.FAILED,
    }
    assert active.isdisjoint(terminal)


# ============================================================
# ProtectionLevel
# ============================================================


def test_protection_level_accepts_positive() -> None:
    level = ProtectionLevel(type=ProtectionLevelType.PRICE, value=Decimal("1"))
    assert level.value == Decimal("1")


@pytest.mark.parametrize("value", [Decimal("0"), Decimal("-1")])
def test_protection_level_rejects_non_positive(value: Decimal) -> None:
    with pytest.raises(ValueError, match="must be > 0"):
        ProtectionLevel(type=ProtectionLevelType.PERCENT, value=value)


# ============================================================
# TrailingSpec — offset and step
# ============================================================


def test_spec_accepts_price_offset() -> None:
    spec = make_spec()
    assert spec.offset_type is TrailingOffsetType.PRICE


@pytest.mark.parametrize("offset", [Decimal("0"), Decimal("-0.1")])
def test_spec_rejects_non_positive_offset(offset: Decimal) -> None:
    with pytest.raises(ValueError, match="offset must be > 0"):
        make_spec(offset=offset)


@pytest.mark.parametrize("offset", [Decimal("100"), Decimal("101")])
def test_spec_rejects_percent_offset_out_of_range(offset: Decimal) -> None:
    with pytest.raises(ValueError, match="percent offset"):
        make_spec(offset_type=TrailingOffsetType.PERCENT, offset=offset)


def test_spec_accepts_percent_offset_in_range() -> None:
    spec = make_spec(offset_type=TrailingOffsetType.PERCENT, offset=Decimal("2.5"))
    assert spec.offset == Decimal("2.5")


def test_spec_accepts_zero_step() -> None:
    assert make_spec(step=Decimal("0")).step == Decimal("0")


def test_spec_rejects_negative_step() -> None:
    with pytest.raises(ValueError, match="step must be >= 0"):
        make_spec(step=Decimal("-1"))


# ============================================================
# TrailingSpec — activation
# ============================================================


def test_immediate_forbids_activation_price() -> None:
    with pytest.raises(ValueError, match="IMMEDIATE forbids"):
        make_spec(activation_price=Decimal("100"))


def test_immediate_forbids_profit_percent() -> None:
    with pytest.raises(ValueError, match="IMMEDIATE forbids"):
        make_spec(activation_profit_percent=Decimal("1"))


def test_price_activation_requires_price() -> None:
    with pytest.raises(ValueError, match="requires activation_price > 0"):
        make_spec(activation=TrailingActivation.PRICE)


def test_price_activation_rejects_non_positive_price() -> None:
    with pytest.raises(ValueError, match="requires activation_price > 0"):
        make_spec(activation=TrailingActivation.PRICE, activation_price=Decimal("0"))


def test_price_activation_forbids_profit_percent() -> None:
    with pytest.raises(ValueError, match="forbids activation_profit_percent"):
        make_spec(
            activation=TrailingActivation.PRICE,
            activation_price=Decimal("100"),
            activation_profit_percent=Decimal("1"),
        )


@pytest.mark.parametrize("pct", [None, Decimal("0"), Decimal("100")])
def test_profit_percent_activation_bounds(pct: Decimal | None) -> None:
    with pytest.raises(ValueError, match="0 < activation_profit_percent < 100"):
        make_spec(
            activation=TrailingActivation.PROFIT_PERCENT,
            activation_profit_percent=pct,
        )


def test_profit_percent_activation_forbids_price() -> None:
    with pytest.raises(ValueError, match="forbids activation_price"):
        make_spec(
            activation=TrailingActivation.PROFIT_PERCENT,
            activation_profit_percent=Decimal("1"),
            activation_price=Decimal("100"),
        )


# ============================================================
# TrailingSpec — ATR coupling
# ============================================================


def test_atr_offset_requires_period_and_timeframe() -> None:
    with pytest.raises(ValueError, match="both atr_period and atr_timeframe"):
        make_spec(offset_type=TrailingOffsetType.ATR, offset=Decimal("2"))


def test_atr_offset_requires_both_fields() -> None:
    with pytest.raises(ValueError, match="both atr_period and atr_timeframe"):
        make_spec(
            offset_type=TrailingOffsetType.ATR,
            offset=Decimal("2"),
            atr_period=14,
        )


def test_atr_period_must_be_at_least_two() -> None:
    with pytest.raises(ValueError, match="atr_period must be >= 2"):
        make_spec(
            offset_type=TrailingOffsetType.ATR,
            offset=Decimal("2"),
            atr_period=1,
            atr_timeframe=CandleTimeframe.H1,
        )


def test_atr_fields_without_an_atr_value_are_rejected() -> None:
    with pytest.raises(ValueError, match="no ATR value is requested"):
        make_spec(atr_period=14, atr_timeframe=CandleTimeframe.H1)


def test_atr_initial_stop_requires_atr_fields() -> None:
    level = ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("1.5"))
    with pytest.raises(ValueError, match="both atr_period and atr_timeframe"):
        make_spec(initial_stop=level)


def test_atr_take_profit_requires_atr_fields() -> None:
    level = ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("2"))
    with pytest.raises(ValueError, match="both atr_period and atr_timeframe"):
        make_spec(take_profit=level)


def test_atr_take_profit_requires_both_fields() -> None:
    level = ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("2"))
    with pytest.raises(ValueError, match="both atr_period and atr_timeframe"):
        make_spec(take_profit=level, atr_period=14)


def test_atr_take_profit_accepts_atr_fields() -> None:
    level = ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("2"))
    spec = make_spec(
        take_profit=level,
        atr_period=14,
        atr_timeframe=CandleTimeframe.M15,
    )
    assert spec.atr_period == 14


def test_atr_is_shared_by_offset_and_levels() -> None:
    spec = make_spec(
        offset_type=TrailingOffsetType.ATR,
        offset=Decimal("2"),
        initial_stop=ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("1")),
        take_profit=ProtectionLevel(type=ProtectionLevelType.ATR, value=Decimal("3")),
        atr_period=14,
        atr_timeframe=CandleTimeframe.H1,
    )
    assert spec.atr_period == 14


# ============================================================
# TrailingRequest
# ============================================================


def test_request_defaults() -> None:
    request = TrailingRequest(instrument=make_instrument(), spec=make_spec())
    assert request.position_id is None
    assert request.mode is TrailingMode.AUTO
    assert request.take_over_existing_protection is False


# ============================================================
# TrailingState
# ============================================================


def test_state_accepts_resolved_mode() -> None:
    state = make_state()
    assert state.mode is TrailingMode.SYNTHETIC
    assert state.quantity == Decimal("1")


def test_state_rejects_auto_mode() -> None:
    with pytest.raises(ValueError, match="never AUTO"):
        make_state(mode=TrailingMode.AUTO)


def test_state_rejects_negative_quantity() -> None:
    with pytest.raises(ValueError, match="absolute size"):
        make_state(quantity=Decimal("-1"))


@pytest.mark.parametrize("field", ["created_at", "updated_at"])
def test_state_rejects_naive_required_timestamps(field: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_state(**{field: datetime(2026, 7, 28, 12, 0, 0)})


@pytest.mark.parametrize("field", ["position_opened_at", "atr_as_of", "last_reprice_at"])
def test_state_rejects_naive_optional_timestamps(field: str) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_state(**{field: datetime(2026, 7, 28, 12, 0, 0)})


def test_state_defaults() -> None:
    state = make_state()
    assert state.position_opened_at is None
    assert state.stop_owned is False
    assert state.take_profit_owned is False
    assert state.reprice_count == 0
    assert state.failure_count == 0
    assert state.activated is False


# ============================================================
# Native trailing types
# ============================================================


def test_native_request_fields() -> None:
    request = NativeTrailingRequest(
        instrument=make_instrument(),
        position_id="leg-1",
        side=OrderSide.SELL,
        quantity=Decimal("1"),
        offset=Decimal("2"),
        offset_type=TrailingOffsetType.ATR,
        basis=TrailingBasis.LAST,
        activation_price=Decimal("105"),
        client_id="tr-1",
    )
    assert request.position_id == "leg-1"
    assert request.side is OrderSide.SELL
    assert request.offset_type is TrailingOffsetType.ATR
    assert request.basis is TrailingBasis.LAST


def test_native_result_fields() -> None:
    result = NativeTrailingResult(
        native_id="native-1",
        confirmed_offset=Decimal("2"),
        current_stop=None,
        activated=False,
        position_scoped=True,
    )
    assert result.native_id == "native-1"
    assert result.confirmed_offset == Decimal("2")
    assert result.activated is False
    assert result.position_scoped is True

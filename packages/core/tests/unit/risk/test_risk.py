"""Risk-check validator chain tests — Section 7.

Covers all 5 validators individually, fail-fast behaviour, chain ordering,
price-sanity graceful degradation, TP/SL routing, and configuration.
"""

from __future__ import annotations

import logging
from dataclasses import fields
from decimal import Decimal

import pytest

from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InvalidSymbolError,
    RateLimitError,
)
from unified_trading_execution.risk import (
    RiskConfig,
    run_risk_checks,
    validate_no_duplicate,
    validate_order_size,
    validate_price_sanity,
    validate_rate_limit,
    validate_symbol_validity,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder

# ── helpers ──────────────────────────────────────────────────────────


def _instrument(symbol: str = "BTCUSDT") -> Instrument:
    return Instrument(
        symbol=symbol,
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def _spec(**overrides: object) -> InstrumentSpec:
    defaults: dict[str, object] = dict(
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("100"),
        min_notional=Decimal("10"),
        price_precision=2,
        qty_precision=3,
    )
    defaults.update(overrides)
    return InstrumentSpec(**defaults)  # type: ignore[arg-type]


def _order(**kwargs: object) -> UnifiedOrder:
    defaults: dict[str, object] = dict(
        instrument=_instrument(),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.GTC,
        price=Decimal("50000"),
        client_order_id="test-order-001",
    )
    defaults.update(kwargs)
    return UnifiedOrder(**defaults)  # type: ignore[arg-type]


# ── validator 1 — symbol validity ────────────────────────────────────


class TestValidateSymbolValidity:
    def test_passes_when_spec_is_not_none(self):
        validate_symbol_validity(_order(), _spec())

    def test_raises_when_spec_is_none(self):
        with pytest.raises(InvalidSymbolError, match="not tradable"):
            validate_symbol_validity(_order(), None)

    def test_error_message_includes_symbol(self):
        order = _order(instrument=_instrument("ETHUSDT"))
        with pytest.raises(InvalidSymbolError, match="ETHUSDT"):
            validate_symbol_validity(order, None)


# ── validator 2 — order size / quantity bounds ───────────────────────


class TestValidateOrderSize:
    def test_passes_for_valid_quantity(self):
        validate_order_size(_order(quantity=Decimal("1")), _spec(), RiskConfig())

    def test_passes_at_min_qty(self):
        validate_order_size(_order(quantity=Decimal("0.001")), _spec(), RiskConfig())

    def test_passes_at_max_qty(self):
        validate_order_size(_order(quantity=Decimal("100")), _spec(), RiskConfig())

    def test_raises_below_min_qty(self):
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            validate_order_size(
                _order(quantity=Decimal("0.0001")),
                _spec(),
                RiskConfig(),
            )

    def test_raises_above_max_qty(self):
        with pytest.raises(InvalidSymbolError, match="exceeds platform maximum"):
            validate_order_size(
                _order(quantity=Decimal("101")),
                _spec(),
                RiskConfig(),
            )

    def test_raises_on_qty_precision_violation(self):
        # 0.0015 is above min_qty (0.001) but 0.0015 % 0.001 = 0.0005 ≠ 0
        with pytest.raises(InvalidSymbolError, match="violates qty_precision"):
            validate_order_size(
                _order(quantity=Decimal("0.0015")),
                _spec(),
                RiskConfig(),
            )

    def test_passes_when_qty_precision_exact(self):
        # qty_precision=3 → tick=0.001; 0.005 % 0.001 == 0
        validate_order_size(_order(quantity=Decimal("0.005")), _spec(), RiskConfig())

    # -- per-instrument caps

    def test_raises_when_exceeds_per_instrument_max_size(self):
        cfg = RiskConfig(per_instrument_max_size={_instrument(): Decimal("0.5")})
        with pytest.raises(InvalidSymbolError, match="exceeds configured max"):
            validate_order_size(_order(quantity=Decimal("1")), _spec(), cfg)

    def test_passes_when_under_per_instrument_max_size(self):
        cfg = RiskConfig(per_instrument_max_size={_instrument(): Decimal("100")})
        validate_order_size(_order(quantity=Decimal("50")), _spec(), cfg)

    # -- global cap

    def test_raises_when_exceeds_global_max_order_size(self):
        cfg = RiskConfig(max_order_size=Decimal("0.5"))
        with pytest.raises(InvalidSymbolError, match="exceeds global max"):
            validate_order_size(_order(quantity=Decimal("1")), _spec(), cfg)

    # -- notional checks

    def test_raises_when_notional_exceeds_per_instrument_cap(self):
        cfg = RiskConfig(
            per_instrument_max_notional={_instrument(): Decimal("10000")},
        )
        with pytest.raises(InvalidSymbolError, match="exceeds configured max"):
            validate_order_size(
                _order(quantity=Decimal("1"), price=Decimal("50000")),
                _spec(),
                cfg,
            )

    def test_raises_when_notional_exceeds_global_cap(self):
        cfg = RiskConfig(max_order_notional=Decimal("10000"))
        with pytest.raises(InvalidSymbolError, match="exceeds global max"):
            validate_order_size(
                _order(quantity=Decimal("1"), price=Decimal("50000")),
                _spec(),
                cfg,
            )

    def test_raises_when_notional_below_min_notional(self):
        spec = _spec(min_notional=Decimal("100"))
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            validate_order_size(
                _order(quantity=Decimal("0.001"), price=Decimal("50000")),
                spec,
                RiskConfig(),
            )

    def test_notional_checks_skipped_when_no_price(self):
        # Market order has no price — notional checks should not run.
        # Neither max nor min notional checks apply.
        spec = _spec(min_notional=Decimal("1000000"))
        validate_order_size(
            _order(order_type=OrderType.MARKET, price=None),
            spec,
            RiskConfig(max_order_notional=Decimal("1")),
        )


# ── validator 3 — price sanity ──────────────────────────────────────


class TestValidatePriceSanity:
    def test_passes_when_price_within_deviation(self):
        validate_price_sanity(
            _order(price=Decimal("51000")),
            Decimal("50000"),
            RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    def test_passes_exactly_at_threshold(self):
        # 5% deviation exactly at 52500 from 50000
        validate_price_sanity(
            _order(price=Decimal("52500")),
            Decimal("50000"),
            RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    def test_raises_when_price_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="deviates"):
            validate_price_sanity(
                _order(price=Decimal("60000")),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    # -- stop price

    def test_passes_when_stop_price_within_deviation(self):
        validate_price_sanity(
            _order(
                order_type=OrderType.STOP_LIMIT,
                price=Decimal("50000"),
                stop_price=Decimal("51000"),
            ),
            Decimal("50000"),
            RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    def test_raises_when_stop_price_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="Stop price.*deviates"):
            validate_price_sanity(
                _order(
                    order_type=OrderType.STOP_LIMIT,
                    price=Decimal("50000"),
                    stop_price=Decimal("60000"),
                ),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    # -- take-profit

    def test_raises_when_tp_trigger_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="TP trigger.*deviates"):
            validate_price_sanity(
                _order(
                    take_profit=TpSlAttachment(trigger_price=Decimal("100000")),
                ),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    def test_raises_when_tp_limit_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="TP limit.*deviates"):
            validate_price_sanity(
                _order(
                    take_profit=TpSlAttachment(
                        trigger_price=Decimal("51000"),
                        limit_price=Decimal("100000"),
                    ),
                ),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    def test_passes_when_tp_within_deviation(self):
        validate_price_sanity(
            _order(
                take_profit=TpSlAttachment(
                    trigger_price=Decimal("51000"),
                    limit_price=Decimal("52000"),
                ),
            ),
            Decimal("50000"),
            RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    # -- stop-loss

    def test_raises_when_sl_trigger_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="SL trigger.*deviates"):
            validate_price_sanity(
                _order(
                    stop_loss=TpSlAttachment(trigger_price=Decimal("100000")),
                ),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    def test_raises_when_sl_limit_deviates_too_far(self):
        with pytest.raises(InvalidSymbolError, match="SL limit.*deviates"):
            validate_price_sanity(
                _order(
                    stop_loss=TpSlAttachment(
                        trigger_price=Decimal("49000"),
                        limit_price=Decimal("100000"),
                    ),
                ),
                Decimal("50000"),
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )

    def test_passes_when_sl_within_deviation(self):
        validate_price_sanity(
            _order(
                stop_loss=TpSlAttachment(
                    trigger_price=Decimal("49000"),
                    limit_price=Decimal("48000"),
                ),
            ),
            Decimal("50000"),
            RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    # -- no prices to check

    def test_passes_for_market_order_with_no_prices(self):
        validate_price_sanity(
            _order(
                order_type=OrderType.MARKET,
                price=None,
                stop_price=None,
                take_profit=None,
                stop_loss=None,
            ),
            Decimal("50000"),
            RiskConfig(),
        )

    # -- graceful degradation (Section 7, step 3)

    def test_skips_when_no_reference_price_and_passes(self, caplog):
        """When reference_price is None, validator 3 logs a warning and passes."""
        with caplog.at_level(logging.WARNING):
            validate_price_sanity(
                _order(price=Decimal("999999")),  # would fail if checked
                None,
                RiskConfig(max_price_deviation_pct=Decimal("5")),
            )
        assert "no reference price available" in caplog.text

    def test_skips_with_symbol_in_warning(self, caplog):
        order = _order(instrument=_instrument("ETHUSDT"))
        with caplog.at_level(logging.WARNING):
            validate_price_sanity(order, None, RiskConfig())
        assert "ETHUSDT" in caplog.text


# ── validator 4 — duplicate check ────────────────────────────────────


class TestValidateNoDuplicate:
    def test_passes_when_not_in_known_set(self):
        validate_no_duplicate("new-id", frozenset({"existing-1", "existing-2"}))

    def test_raises_when_in_known_set(self):
        with pytest.raises(DuplicateOrderIdError, match="already in use"):
            validate_no_duplicate("dup-id", frozenset({"dup-id", "other"}))

    def test_passes_with_empty_known_set(self):
        validate_no_duplicate("any-id", frozenset())


# ── validator 5 — rate-limit throttling ──────────────────────────────


class TestValidateRateLimit:
    def test_passes_when_budget_positive(self):
        validate_rate_limit(5)

    def test_passes_when_budget_exactly_one(self):
        validate_rate_limit(1)

    def test_raises_when_budget_zero(self):
        with pytest.raises(RateLimitError, match="budget exhausted"):
            validate_rate_limit(0)

    def test_raises_when_budget_negative(self):
        with pytest.raises(RateLimitError):
            validate_rate_limit(-1)


# ── chain runner — ordering and fail-fast ────────────────────────────


class TestRunRiskChecksOrdering:
    """Prove validator order by constructing scenarios where earlier validators
    would fail and verifying the error matches the first failing validator.

    Each test also doubles as a fail-fast proof: when validator N fails,
    validators N+1 through 5 never run (otherwise we would see a different
    error or error message).
    """

    def _chain_args(self, **overrides: object) -> dict[str, object]:
        defaults: dict[str, object] = dict(
            order=_order(),
            instrument_spec=_spec(),
            reference_price=Decimal("50000"),
            known_order_ids=frozenset(),
            remaining_budget=10,
            config=RiskConfig(),
        )
        defaults.update(overrides)
        return defaults

    def test_all_five_pass_with_valid_inputs(self):
        run_risk_checks(**self._chain_args())  # type: ignore[arg-type]

    # -- validator 1 fails first ---

    def test_validator_1_fails_before_validator_2(self):
        """Symbol validity (v1) fails. Quantity (v2) would also fail if it ran."""
        with pytest.raises(InvalidSymbolError, match="not tradable"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    instrument_spec=None,
                    order=_order(quantity=Decimal("0.0001")),  # would fail v2
                )
            )

    def test_validator_1_fails_before_validator_3(self):
        """Symbol validity fails before price sanity could reject the order."""
        with pytest.raises(InvalidSymbolError, match="not tradable"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    instrument_spec=None,
                    order=_order(price=Decimal("999999")),  # would fail v3
                )
            )

    def test_validator_1_fails_before_validator_4(self):
        """Symbol validity fails before duplicate check could fire."""
        with pytest.raises(InvalidSymbolError, match="not tradable"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    instrument_spec=None,
                    known_order_ids=frozenset({"test-order-001"}),  # would fail v4
                )
            )

    def test_validator_1_fails_before_validator_5(self):
        """Symbol validity fails before rate-limit check could fire."""
        with pytest.raises(InvalidSymbolError, match="not tradable"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    instrument_spec=None,
                    remaining_budget=0,  # would fail v5
                )
            )

    # -- validator 2 fails first ---

    def test_validator_2_fails_before_validator_3(self):
        """Size check (v2) fails before price sanity (v3) could run."""
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    order=_order(
                        quantity=Decimal("0.0001"),
                        price=Decimal("999999"),  # would fail v3
                    ),
                )
            )

    def test_validator_2_fails_before_validator_4(self):
        """Size check fails before duplicate check could fire."""
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    order=_order(quantity=Decimal("0.0001")),
                    known_order_ids=frozenset({"test-order-001"}),
                )
            )

    def test_validator_2_fails_before_validator_5(self):
        """Size check fails before rate-limit check could fire."""
        with pytest.raises(InvalidSymbolError, match="below minimum"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    order=_order(quantity=Decimal("0.0001")),
                    remaining_budget=0,
                )
            )

    # -- validator 3 fails first ---

    def test_validator_3_fails_before_validator_4(self):
        """Price sanity (v3) fails before duplicate check (v4) could run."""
        with pytest.raises(InvalidSymbolError, match="deviates"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    order=_order(price=Decimal("999999")),
                    known_order_ids=frozenset({"test-order-001"}),  # would fail v4
                )
            )

    def test_validator_3_fails_before_validator_5(self):
        """Price sanity fails before rate-limit check could fire."""
        with pytest.raises(InvalidSymbolError, match="deviates"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    order=_order(price=Decimal("999999")),
                    remaining_budget=0,  # would fail v5
                )
            )

    # -- validator 4 fails first ---

    def test_validator_4_fails_before_validator_5(self):
        """Duplicate check (v4) fails before rate-limit (v5) could run."""
        with pytest.raises(DuplicateOrderIdError, match="already in use"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    known_order_ids=frozenset({"test-order-001"}),
                    remaining_budget=0,  # would fail v5
                )
            )

    # -- validator 5 fails (last one) ---

    def test_validator_5_fails_when_everything_else_passes(self):
        with pytest.raises(RateLimitError, match="budget exhausted"):
            run_risk_checks(
                **self._chain_args(  # type: ignore[arg-type]
                    remaining_budget=0,
                )
            )

    # -- parametrized: exact order via first-failing-validator proof ---

    @pytest.mark.parametrize(
        "scenario_desc,overrides,expected_error,expected_match",
        [
            (
                "v1: symbol invalidity",
                dict(instrument_spec=None),
                InvalidSymbolError,
                "not tradable",
            ),
            (
                "v2: quantity below minimum",
                dict(order=_order(quantity=Decimal("0.0001"))),
                InvalidSymbolError,
                "below minimum",
            ),
            (
                "v3: price deviation too high",
                dict(order=_order(price=Decimal("999999"))),
                InvalidSymbolError,
                "deviates",
            ),
            (
                "v4: duplicate ID",
                dict(known_order_ids=frozenset({"test-order-001"})),
                DuplicateOrderIdError,
                "already in use",
            ),
            (
                "v5: rate-limit exhausted",
                dict(remaining_budget=0),
                RateLimitError,
                "budget exhausted",
            ),
        ],
        ids=lambda v: v if isinstance(v, str) else "",
    )
    def test_chain_fails_at_correct_validator(
        self,
        scenario_desc,
        overrides,
        expected_error,
        expected_match,
    ):
        """Each scenario only fails at the stated validator — proving order."""
        args = self._chain_args(**overrides)
        with pytest.raises(expected_error, match=expected_match):
            run_risk_checks(**args)  # type: ignore[arg-type]


# ── chain runner — client_order_id required ─────────────────────────


class TestRunRiskChecksRequiresClientOrderId:
    def test_raises_when_client_order_id_is_none(self):
        with pytest.raises(InvalidSymbolError, match="client_order_id must be set"):
            run_risk_checks(
                _order(client_order_id=None),
                instrument_spec=_spec(),
                reference_price=Decimal("50000"),
                known_order_ids=frozenset(),
                remaining_budget=10,
            )


# ── TP/SL orders go through the same chain ──────────────────────────


class TestTpSlOrdersThroughSameChain:
    """TP/SL orders are NOT special-cased — they go through all validators
    including the stop-price sanity check (Section 7 hard requirement)."""

    def test_tpsl_order_passes_all_validators(self):
        order = _order(
            order_type=OrderType.STOP_LIMIT,
            price=Decimal("50000"),
            stop_price=Decimal("51000"),
            take_profit=TpSlAttachment(
                trigger_price=Decimal("52000"),
                limit_price=Decimal("52500"),
            ),
            stop_loss=TpSlAttachment(
                trigger_price=Decimal("49000"),
                limit_price=Decimal("48500"),
            ),
        )
        run_risk_checks(
            order,
            instrument_spec=_spec(),
            reference_price=Decimal("50000"),
            known_order_ids=frozenset(),
            remaining_budget=10,
            config=RiskConfig(max_price_deviation_pct=Decimal("5")),
        )

    def test_tpsl_order_rejected_on_stop_price_deviation(self):
        """The stop_price on a TP/SL order is validated — not bypassed."""
        order = _order(
            order_type=OrderType.STOP,
            stop_price=Decimal("999999"),
            take_profit=TpSlAttachment(trigger_price=Decimal("60000")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("40000")),
        )
        with pytest.raises(InvalidSymbolError, match="Stop price.*deviates"):
            run_risk_checks(
                order,
                instrument_spec=_spec(),
                reference_price=Decimal("50000"),
                known_order_ids=frozenset(),
                remaining_budget=10,
            )

    def test_tpsl_order_rejected_on_tp_trigger_deviation(self):
        order = _order(
            order_type=OrderType.MARKET,
            price=None,
            take_profit=TpSlAttachment(trigger_price=Decimal("999999")),
        )
        with pytest.raises(InvalidSymbolError, match="TP trigger.*deviates"):
            run_risk_checks(
                order,
                instrument_spec=_spec(),
                reference_price=Decimal("50000"),
                known_order_ids=frozenset(),
                remaining_budget=10,
            )

    def test_tpsl_order_rejected_on_sl_trigger_deviation(self):
        order = _order(
            order_type=OrderType.MARKET,
            price=None,
            stop_loss=TpSlAttachment(trigger_price=Decimal("999999")),
        )
        with pytest.raises(InvalidSymbolError, match="SL trigger.*deviates"):
            run_risk_checks(
                order,
                instrument_spec=_spec(),
                reference_price=Decimal("50000"),
                known_order_ids=frozenset(),
                remaining_budget=10,
            )

    def test_tpsl_order_without_reference_price_passes(self, caplog):
        """Even with TP/SL, when reference price is unavailable it skips gracefully."""
        order = _order(
            order_type=OrderType.STOP_LIMIT,
            price=Decimal("50000"),
            stop_price=Decimal("999999"),  # would fail if checked
            take_profit=TpSlAttachment(trigger_price=Decimal("999999")),
            stop_loss=TpSlAttachment(trigger_price=Decimal("1")),
        )
        with caplog.at_level(logging.WARNING):
            run_risk_checks(
                order,
                instrument_spec=_spec(),
                reference_price=None,
                known_order_ids=frozenset(),
                remaining_budget=10,
            )
        assert "no reference price available" in caplog.text


# ── risk config — thresholds configurable, no toggles ───────────────


class TestRiskConfig:
    """Thresholds are configurable, but validators cannot be individually disabled
    (Section 7 — deliberate safety stance for real capital)."""

    def test_default_max_order_size_is_inf(self):
        assert RiskConfig().max_order_size == Decimal("Inf")

    def test_default_max_order_notional_is_inf(self):
        assert RiskConfig().max_order_notional == Decimal("Inf")

    def test_default_max_price_deviation_is_5_pct(self):
        assert RiskConfig().max_price_deviation_pct == Decimal("5.0")

    def test_default_rate_limit_budget_override_is_none(self):
        assert RiskConfig().rate_limit_budget_override is None

    def test_thresholds_are_customisable(self):
        cfg = RiskConfig(
            max_order_size=Decimal("10"),
            max_order_notional=Decimal("100000"),
            max_price_deviation_pct=Decimal("2.5"),
            rate_limit_budget_override=50,
            per_instrument_max_size={_instrument(): Decimal("5")},
            per_instrument_max_notional={_instrument(): Decimal("50000")},
        )
        assert cfg.max_order_size == Decimal("10")
        assert cfg.max_order_notional == Decimal("100000")
        assert cfg.max_price_deviation_pct == Decimal("2.5")
        assert cfg.rate_limit_budget_override == 50
        assert cfg.per_instrument_max_size[_instrument()] == Decimal("5")

    def test_config_is_frozen(self):
        cfg = RiskConfig()
        with pytest.raises(Exception):  # FrozenInstanceError or similar
            cfg.max_order_size = Decimal("100")  # type: ignore[misc]

    def test_no_validator_disable_toggle_exists(self):
        """RiskConfig must expose no boolean flags to disable individual validators."""
        boolean_fields = [f for f in fields(RiskConfig) if f.type is bool]
        assert boolean_fields == [], (
            f"RiskConfig has boolean fields that could act as disable toggles: {boolean_fields}"
        )

    def test_config_has_only_the_six_documented_fields(self):
        field_names = {f.name for f in fields(RiskConfig)}
        assert field_names == {
            "max_order_size",
            "max_order_notional",
            "per_instrument_max_size",
            "per_instrument_max_notional",
            "max_price_deviation_pct",
            "rate_limit_budget_override",
        }


# ── ReferencePriceFn Protocol ────────────────────────────────────────


class TestReferencePriceFnProtocol:
    def test_callable_returning_decimal_works(self):
        """A simple callable matching the protocol should work as a reference price fn."""
        from unified_trading_execution.risk import ReferencePriceFn

        def my_price_fn(instrument: Instrument) -> Decimal | None:
            return Decimal("42000")

        # Protocol is structural — any matching callable works.
        fn: ReferencePriceFn = my_price_fn
        result = fn(_instrument("BTCUSDT"))
        assert result == Decimal("42000")

    def test_callable_returning_none_works(self):
        def no_price(instrument: Instrument) -> Decimal | None:
            return None

        from unified_trading_execution.risk import ReferencePriceFn

        fn: ReferencePriceFn = no_price
        assert fn(_instrument()) is None

    def test_lambda_also_satisfies_protocol(self):
        from unified_trading_execution.risk import ReferencePriceFn

        fn: ReferencePriceFn = lambda inst: Decimal("100")
        assert fn(_instrument()) == Decimal("100")

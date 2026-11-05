from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.binance import BinanceProduct
from unified_trading_execution.binance.rate_limits import BinanceRateLimitTracker
from unified_trading_execution.errors import PlatformError

_RATE_LIMITS: list[dict[str, object]] = [
    {
        "rateLimitType": "REQUEST_WEIGHT",
        "interval": "MINUTE",
        "intervalNum": 1,
        "limit": 1200,
    },
    {
        "rateLimitType": "ORDERS",
        "interval": "SECOND",
        "intervalNum": 10,
        "limit": 10,
    },
    {
        "rateLimitType": "ORDERS",
        "interval": "MINUTE",
        "intervalNum": 1,
        "limit": 50,
    },
    {
        "rateLimitType": "RAW_REQUESTS",
        "interval": "MINUTE",
        "intervalNum": 5,
        "limit": 5000,
    },
]


class TestBinanceRateLimitTracker:
    def test_unknown_state_fails_closed(self) -> None:
        now = datetime.now(UTC)
        limits = BinanceRateLimitTracker().snapshot(now=now)

        assert limits == RateLimits(
            requests_per_interval=1,
            interval_seconds=60.0,
            remaining=0,
            reset_at=now,
        )

    def test_combines_weight_orders_and_local_raw_request_budgets(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={
                "x-mbx-used-weight-1m": "1195",
                "X-MBX-ORDER-COUNT-10S": "8",
                "X-MBX-ORDER-COUNT-1M": "12",
            },
            rate_limits=_RATE_LIMITS,
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.requests_per_interval == 10
        assert limits.interval_seconds == 10.0
        assert limits.remaining == 2
        assert limits.reset_at == now + timedelta(seconds=10)

    def test_header_names_are_case_insensitive_and_accept_all_intervals(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        limits = [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "HOUR",
                "intervalNum": 2,
                "limit": 9000,
            }
        ]
        tracker.update(
            BinanceProduct.USD_M,
            headers={"X-MBX-USED-WEIGHT-2H": "9000"},
            rate_limits=limits,
            now=now,
        )

        observed = tracker.snapshot(now=now)

        assert observed.remaining == 0
        assert observed.interval_seconds == 7200.0
        assert observed.reset_at == now + timedelta(seconds=7200)

    def test_malformed_observed_counter_fails_closed(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "not-a-number"},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(minutes=1)

    @pytest.mark.parametrize("used", ["-1", "1.5", "", "  ", "NaN"])
    def test_negative_or_noninteger_counters_fail_closed(self, used: str) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": used},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        assert tracker.snapshot(now=now).remaining == 0

    def test_unmatched_header_interval_fails_closed(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-2M": "1"},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(minutes=2)

    def test_header_without_rate_limit_configuration_fails_closed(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "1"},
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(minutes=1)

    def test_unrecognized_header_interval_fails_closed(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1W": "1"},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(minutes=1)

    def test_missing_order_header_bootstraps_one_action(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "1"},
            rate_limits=_RATE_LIMITS,
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 1
        assert limits.requests_per_interval == 1
        assert limits.interval_seconds == 60.0
        assert limits.reset_at == now + timedelta(minutes=1)

    def test_partial_response_does_not_erase_prior_order_observation(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        definitions = [
            definition
            for definition in _RATE_LIMITS
            if definition["rateLimitType"] != "ORDERS" or definition["intervalNum"] == 10
        ]
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.COIN_M,
            headers={
                "X-MBX-USED-WEIGHT-1M": "10",
                "X-MBX-ORDER-COUNT-10S": "7",
            },
            rate_limits=definitions,
            now=now,
        )
        tracker.update(
            BinanceProduct.COIN_M,
            headers={"X-MBX-USED-WEIGHT-1M": "20"},
            now=now + timedelta(seconds=1),
        )

        limits = tracker.snapshot(now=now + timedelta(seconds=1))

        assert limits.remaining == 3
        assert limits.interval_seconds == 10.0

    def test_product_budgets_are_combined_conservatively(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "100"},
            rate_limits=[
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1000,
                }
            ],
            now=now,
        )
        tracker.update(
            BinanceProduct.USD_M,
            headers={"X-MBX-USED-WEIGHT-10S": "8"},
            rate_limits=[
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "SECOND",
                    "intervalNum": 10,
                    "limit": 10,
                }
            ],
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 2
        assert limits.interval_seconds == 10.0
        assert limits.reset_at == now + timedelta(seconds=10)

    def test_elapsed_window_refills_from_exchange_limit_configuration(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "1200"},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        limits = tracker.snapshot(now=now + timedelta(minutes=1))

        assert limits.remaining == 1200
        assert limits.reset_at == now + timedelta(minutes=2)

    def test_raw_request_window_is_counted_locally(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        raw_limit = [_RATE_LIMITS[3]]
        tracker.update(
            BinanceProduct.SPOT,
            headers={},
            rate_limits=raw_limit,
            now=now,
        )
        tracker.update(
            BinanceProduct.SPOT,
            headers={},
            now=now + timedelta(seconds=1),
        )

        limits = tracker.snapshot(now=now + timedelta(seconds=1))

        assert limits.remaining == 4998
        assert limits.interval_seconds == 300.0

    @pytest.mark.parametrize(
        ("status_code", "retry_after", "expected_seconds"),
        [
            (429, "2.5", 2.5),
            (418, "120", 120.0),
            (418, None, 259200.0),
        ],
    )
    def test_retry_after_and_ip_ban_cooldown(
        self,
        status_code: int,
        retry_after: str | None,
        expected_seconds: float,
    ) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        headers = {"Retry-After": retry_after} if retry_after is not None else {}
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.USD_M,
            headers=headers,
            rate_limits=_RATE_LIMITS,
            status_code=status_code,
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(seconds=expected_seconds)

    def test_429_without_retry_after_uses_longest_configured_window(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={},
            rate_limits=_RATE_LIMITS,
            status_code=429,
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(minutes=5)

    @pytest.mark.parametrize("retry_after", ["", "-1", "NaN", "tomorrow"])
    def test_invalid_retry_after_fails_closed_for_418(self, retry_after: str) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"Retry-After": retry_after},
            status_code=418,
            now=now,
        )

        limits = tracker.snapshot(now=now)

        assert limits.remaining == 0
        assert limits.reset_at == now + timedelta(days=3)

    def test_unknown_rate_limit_types_are_ignored(self) -> None:
        now = datetime.now(UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={},
            rate_limits=[
                {
                    "rateLimitType": "CONNECTIONS",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 10,
                }
            ],
            now=now,
        )

        assert tracker.snapshot(now=now).remaining == 0

    def test_malformed_supported_limit_definition_is_reported(self) -> None:
        tracker = BinanceRateLimitTracker()

        with pytest.raises(PlatformError, match="invalid REQUEST_WEIGHT rate-limit interval"):
            tracker.update(
                BinanceProduct.SPOT,
                headers={},
                rate_limits=[
                    {
                        "rateLimitType": "REQUEST_WEIGHT",
                        "interval": "FORTNIGHT",
                        "intervalNum": 1,
                        "limit": 100,
                    }
                ],
            )

    def test_negative_retry_after_is_clamped_to_a_safe_cooldown(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"Retry-After": "999999999"},
            status_code=429,
            now=now,
        )

        assert tracker.snapshot(now=now).reset_at == now + timedelta(days=3)

    def test_observed_exhaustion_resets_after_interval(self) -> None:
        now = datetime(2026, 11, 5, 10, tzinfo=UTC)
        tracker = BinanceRateLimitTracker()
        tracker.update(
            BinanceProduct.SPOT,
            headers={"X-MBX-USED-WEIGHT-1M": "1200"},
            rate_limits=[_RATE_LIMITS[0]],
            now=now,
        )

        limits = tracker.snapshot(now=now + timedelta(minutes=1, seconds=1))

        assert limits.remaining == 1200
        assert limits.reset_at == now + timedelta(minutes=2, seconds=1)

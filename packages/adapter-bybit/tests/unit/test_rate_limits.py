from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from unified_trading_execution.bybit.adapter import (
    _MAX_GLOBAL_RESERVE,
    _has_rate_limit_headers,
    _parse_rate_limits,
    _safe_header_int,
)


class TestSafeHeaderInt:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("120", 120),
            ("0", 0),
            ("-5", -5),
        ],
    )
    def test_valid_integers(self, raw: str, expected: int) -> None:
        assert _safe_header_int({"k": raw}, "k", 999) == expected

    @pytest.mark.parametrize(
        ("raw", "label"),
        [
            ("", "empty-string"),
            ("abc", "non-numeric"),
            ("12.5", "float-like"),
            ("0x1", "hex-prefixed"),
            ("--5", "double-negative"),
        ],
    )
    def test_malformed_returns_default(self, raw: str, label: str) -> None:
        assert _safe_header_int({"k": raw}, "k", 99) == 99

    def test_key_absent_returns_default(self) -> None:
        assert _safe_header_int({}, "missing", 42) == 42

    def test_key_is_case_insensitive(self) -> None:
        assert _safe_header_int({"x-bapi-limit": "10"}, "X-Bapi-Limit", 99) == 10


class TestHasRateLimitHeaders:
    def test_authenticated_response(self) -> None:
        headers = {
            "X-Bapi-Limit": "10",
            "X-Bapi-Limit-Status": "9",
            "X-Bapi-Limit-Reset-Timestamp": "2000000000000",
        }
        assert _has_rate_limit_headers(headers) is True

    def test_public_response_without_limit_headers(self) -> None:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Ret_code": "0",
            "Traceid": "abc",
        }
        assert _has_rate_limit_headers(headers) is False

    def test_empty_headers(self) -> None:
        assert _has_rate_limit_headers({}) is False

    def test_lowercase_headers(self) -> None:
        assert _has_rate_limit_headers({"x-bapi-limit-status": "9"}) is True


class TestParseRateLimits:
    def test_full_headers(self) -> None:
        headers = {
            "X-Bapi-Limit": "10",
            "X-Bapi-Limit-Status": "9",
            "X-Bapi-Limit-Reset-Timestamp": "2000000000000",
        }
        rl = _parse_rate_limits(headers)
        assert rl.requests_per_interval == 10
        assert rl.remaining == 9
        assert rl.interval_seconds == 1.0
        assert rl.reset_at == datetime.fromtimestamp(2000000000, tz=UTC)

    def test_missing_headers_use_conservative_defaults(self) -> None:
        now = datetime.now(UTC)
        rl = _parse_rate_limits({})
        assert rl.requests_per_interval == _MAX_GLOBAL_RESERVE
        assert rl.remaining == _MAX_GLOBAL_RESERVE
        assert rl.reset_at > now

    # ---- Clamping: non-positive limit / negative remaining ----

    @pytest.mark.parametrize(
        ("limit_raw", "expected_limit"),
        [
            ("0", 1),
            ("-1", 1),
            ("-999", 1),
        ],
    )
    def test_limit_clamped_to_minimum_1(self, limit_raw: str, expected_limit: int) -> None:
        rl = _parse_rate_limits({"X-Bapi-Limit": limit_raw})
        assert rl.requests_per_interval == expected_limit

    def test_negative_remaining_clamped_to_zero(self) -> None:
        rl = _parse_rate_limits({"X-Bapi-Limit-Status": "-5"})
        assert rl.remaining == 0

    def test_remaining_capped_to_lowest_write_bucket(self) -> None:
        # A 50/s read endpoint must not authorize a 50-write burst.
        rl = _parse_rate_limits(
            {
                "X-Bapi-Limit": "50",
                "X-Bapi-Limit-Status": "49",
                "X-Bapi-Limit-Reset-Timestamp": "2000000000000",
            }
        )
        assert rl.requests_per_interval == 50
        assert rl.remaining == _MAX_GLOBAL_RESERVE

    # ---- Malformed headers do not crash ----

    @pytest.mark.parametrize(
        ("headers", "label"),
        [
            ({"X-Bapi-Limit": ""}, "empty-limit"),
            ({"X-Bapi-Limit-Status": ""}, "empty-status"),
            ({"X-Bapi-Limit-Reset-Timestamp": ""}, "empty-reset-ts"),
            ({"X-Bapi-Limit": "abc"}, "non-numeric-limit"),
            ({"X-Bapi-Limit-Status": "abc"}, "non-numeric-status"),
            ({"X-Bapi-Limit-Reset-Timestamp": "abc"}, "non-numeric-reset-ts"),
            ({"X-Bapi-Limit": "", "X-Bapi-Limit-Status": "abc"}, "multiple-malformed"),
        ],
    )
    def test_malformed_headers_use_defaults_and_dont_crash(
        self, headers: dict[str, str], label: str
    ) -> None:
        rl = _parse_rate_limits(headers)
        assert rl.requests_per_interval >= 1
        assert rl.remaining >= 0

    # ---- Reset timestamp edge cases ----

    def test_reset_in_the_past_is_replaced_with_a_future_window(self) -> None:
        # Bybit reports approximately "now" unless the limit was exceeded.
        # Core refuses every reservation when reset_at is already past.
        now = datetime.now(UTC)
        past_ms = int((now - timedelta(seconds=1)).timestamp() * 1000)
        rl = _parse_rate_limits({"X-Bapi-Limit-Reset-Timestamp": str(past_ms)}, now=now)
        assert rl.reset_at > now
        assert rl.reset_at <= now + timedelta(seconds=2)

    def test_reset_timestamp_zero_falls_through_to_a_future_window(self) -> None:
        now = datetime.now(UTC)
        rl = _parse_rate_limits({"X-Bapi-Limit-Reset-Timestamp": "0"}, now=now)
        assert rl.reset_at > now
        assert rl.reset_at <= now + timedelta(seconds=2)


class TestGetRateLimits:
    async def test_returns_usable_window_before_any_call(self, adapter) -> None:
        now = datetime.now(UTC)
        rl = await adapter.get_rate_limits()
        assert rl.requests_per_interval >= 1
        assert rl.remaining == _MAX_GLOBAL_RESERVE
        assert rl.reset_at > now

    async def test_returns_updated_state(self, adapter) -> None:
        adapter._update_rate_limits(
            {
                "X-Bapi-Limit": "10",
                "X-Bapi-Limit-Status": "7",
                "X-Bapi-Limit-Reset-Timestamp": "2100000000000",
            }
        )
        rl = await adapter.get_rate_limits()
        assert rl.remaining == 7
        assert rl.reset_at == datetime.fromtimestamp(2100000000, tz=UTC)

    async def test_multiple_updates(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Limit-Status": "7"})
        assert (await adapter.get_rate_limits()).remaining == 7
        adapter._update_rate_limits({"X-Bapi-Limit-Status": "3"})
        assert (await adapter.get_rate_limits()).remaining == 3

    async def test_public_response_does_not_clobber_the_last_observation(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Limit-Status": "3"})
        adapter._update_rate_limits({})
        assert (await adapter.get_rate_limits()).remaining == 3

    async def test_update_with_garbage_does_not_crash(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Limit-Status": "abc"})
        rl = await adapter.get_rate_limits()
        assert rl.remaining == _MAX_GLOBAL_RESERVE

    async def test_update_with_negative_clamps_to_zero(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Limit-Status": "-10"})
        assert (await adapter.get_rate_limits()).remaining == 0

    async def test_elapsed_window_refills_the_bucket(self, adapter) -> None:
        now = datetime.now(UTC)
        adapter._last_rate_limits = type(adapter._last_rate_limits)(
            requests_per_interval=10,
            interval_seconds=1.0,
            remaining=0,
            reset_at=now - timedelta(seconds=1),
        )
        rl = await adapter.get_rate_limits()
        assert rl.remaining == _MAX_GLOBAL_RESERVE
        assert rl.reset_at > now

    async def test_elapsed_high_limit_window_refills_to_the_cap(self, adapter) -> None:
        now = datetime.now(UTC)
        adapter._last_rate_limits = type(adapter._last_rate_limits)(
            requests_per_interval=50,
            interval_seconds=1.0,
            remaining=0,
            reset_at=now - timedelta(seconds=1),
        )
        rl = await adapter.get_rate_limits()
        assert rl.remaining == _MAX_GLOBAL_RESERVE
        assert rl.reset_at > now

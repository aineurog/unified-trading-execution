from __future__ import annotations

from datetime import UTC, datetime

import pytest

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.bybit.adapter import _parse_rate_limits, _safe_header_int


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


class TestParseRateLimits:
    def test_full_headers(self) -> None:
        headers = {
            "X-Bapi-Limit": "120",
            "X-Bapi-Remaining": "85",
            "X-Bapi-Reset-Timestamp": "2000000000000",
        }
        rl = _parse_rate_limits(headers)
        assert rl.requests_per_interval == 120
        assert rl.remaining == 85
        assert rl.interval_seconds == 60
        assert rl.reset_at == datetime.fromtimestamp(2000000000, tz=UTC)

    def test_missing_headers_defaults(self) -> None:
        rl = _parse_rate_limits({})
        assert rl.requests_per_interval == 120
        assert rl.remaining == 120

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
        rl = _parse_rate_limits({"X-Bapi-Remaining": "-5"})
        assert rl.remaining == 0

    # ---- Malformed headers do not crash ----

    @pytest.mark.parametrize(
        ("headers", "label"),
        [
            ({"X-Bapi-Limit": ""}, "empty-limit"),
            ({"X-Bapi-Remaining": ""}, "empty-remaining"),
            ({"X-Bapi-Reset-Timestamp": ""}, "empty-reset-ts"),
            ({"X-Bapi-Limit": "abc"}, "non-numeric-limit"),
            ({"X-Bapi-Remaining": "abc"}, "non-numeric-remaining"),
            ({"X-Bapi-Reset-Timestamp": "abc"}, "non-numeric-reset-ts"),
            ({"X-Bapi-Limit": "", "X-Bapi-Remaining": "abc"}, "multiple-malformed"),
        ],
    )
    def test_malformed_headers_use_defaults_and_dont_crash(
        self, headers: dict[str, str], label: str
    ) -> None:
        rl = _parse_rate_limits(headers)
        assert rl.requests_per_interval >= 1
        assert rl.remaining >= 0

    # ---- Reset timestamp edge cases ----

    def test_reset_timestamp_zero_falls_through_to_now(self) -> None:
        rl = _parse_rate_limits({"X-Bapi-Reset-Timestamp": "0"})
        now = datetime.now(UTC)
        delta = abs((rl.reset_at - now).total_seconds())
        assert delta < 5, f"reset_at should be ~now, got {rl.reset_at}"

    def test_reset_timestamp_explicit_zero_falls_through(self) -> None:
        # "0" as a string — same behavior
        rl = _parse_rate_limits({"X-Bapi-Reset-Timestamp": "0"})
        now = datetime.now(UTC)
        assert abs((rl.reset_at - now).total_seconds()) < 5

    def test_high_remaining_stored_as_is(self) -> None:
        rl = _parse_rate_limits({"X-Bapi-Remaining": "99999"})
        assert rl.remaining == 99999


class TestGetRateLimits:
    async def test_returns_default_before_any_call(self, adapter) -> None:
        rl = await adapter.get_rate_limits()
        assert rl.requests_per_interval >= 1
        assert rl.remaining >= 0

    async def test_returns_updated_state(self, adapter) -> None:
        adapter._update_rate_limits(
            {
                "X-Bapi-Limit": "120",
                "X-Bapi-Remaining": "72",
                "X-Bapi-Reset-Timestamp": "2100000000000",
            }
        )
        rl = await adapter.get_rate_limits()
        assert rl.remaining == 72
        assert rl.reset_at == datetime.fromtimestamp(2100000000, tz=UTC)

    async def test_multiple_updates(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Remaining": "100"})
        assert (await adapter.get_rate_limits()).remaining == 100
        adapter._update_rate_limits({"X-Bapi-Remaining": "50"})
        assert (await adapter.get_rate_limits()).remaining == 50

    async def test_update_with_garbage_does_not_crash(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Remaining": "abc"})
        rl = await adapter.get_rate_limits()
        # Falls back to default
        assert rl.remaining == 120

    async def test_update_with_negative_clamps_to_zero(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Remaining": "-10"})
        assert (await adapter.get_rate_limits()).remaining == 0

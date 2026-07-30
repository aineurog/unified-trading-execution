from __future__ import annotations

from datetime import datetime, timezone

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.bybit.adapter import _parse_rate_limits


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
        assert rl.reset_at == datetime.fromtimestamp(2000000000, tz=timezone.utc)

    def test_missing_headers_defaults(self) -> None:
        rl = _parse_rate_limits({})
        assert rl.requests_per_interval == 120
        assert rl.remaining == 120


class TestGetRateLimits:
    async def test_returns_default_before_any_call(self, adapter) -> None:
        rl = await adapter.get_rate_limits()
        assert rl.requests_per_interval == 120
        assert rl.remaining == 120

    async def test_returns_updated_state(self, adapter) -> None:
        adapter._update_rate_limits({
            "X-Bapi-Limit": "120",
            "X-Bapi-Remaining": "72",
            "X-Bapi-Reset-Timestamp": "2100000000000",
        })
        rl = await adapter.get_rate_limits()
        assert rl.remaining == 72
        assert rl.reset_at == datetime.fromtimestamp(2100000000, tz=timezone.utc)

    async def test_multiple_updates(self, adapter) -> None:
        adapter._update_rate_limits({"X-Bapi-Remaining": "100"})
        assert (await adapter.get_rate_limits()).remaining == 100
        adapter._update_rate_limits({"X-Bapi-Remaining": "50"})
        assert (await adapter.get_rate_limits()).remaining == 50

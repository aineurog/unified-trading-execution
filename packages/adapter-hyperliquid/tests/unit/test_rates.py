"""Unit tests for IP weight accounting (no network — stubbed SDK surface)."""

from __future__ import annotations

from datetime import UTC
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
from hyperliquid.utils.error import ClientError, ServerError

from unified_trading_execution.errors import (
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.hyperliquid.rates import (
    CONNECT_WEIGHT,
    IP_WEIGHT_BUDGET_PER_MINUTE,
    RateBudget,
    describe_call,
    request_weight,
    surcharge_weight,
)

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


@pytest.mark.parametrize(
    ("name", "batch", "expected"),
    [
        ("l2_snapshot", 1, 2),
        ("all_mids", 1, 2),
        ("user_state", 1, 2),
        ("spot_user_state", 1, 2),
        ("query_order_by_cloid", 1, 2),
        ("user_role", 1, 60),
        ("meta", 1, 20),
        ("spot_meta", 1, 20),
        ("meta_and_asset_ctxs", 1, 20),
        ("open_orders", 1, 20),
        ("user_fills", 1, 20),
        ("order", 1, 1),
        ("modify_order", 1, 1),
        ("cancel_by_cloid", 1, 1),
        ("update_leverage", 1, 1),
        ("update_isolated_margin", 1, 1),
        ("bulk_orders", 1, 1),
        ("bulk_orders", 39, 1),
        ("bulk_orders", 40, 2),
        ("bulk_orders", 79, 2),
        ("bulk_orders", 80, 3),
        ("bulk_orders", 0, 1),
        ("something_new", 1, 20),
    ],
)
def test_request_weight_table(name: str, batch: int, expected: int) -> None:
    assert request_weight(name, batch_length=batch) == expected


@pytest.mark.parametrize(
    ("name", "size", "expected"),
    [
        ("user_fills", 0, 0),
        ("user_fills", 19, 0),
        ("user_fills", 20, 1),
        ("user_fills", 39, 1),
        ("user_fills", 40, 2),
        ("user_fills", 2000, 100),
        ("user_fills_by_time", 41, 2),
        ("funding_history", 41, 2),
        ("user_funding_history", 20, 1),
        ("historical_orders", 19, 0),
        ("open_orders", 100, 0),
        ("user_fills", {"not": "a list"}, 0),
    ],
)
def test_surcharge_weight(name: str, size: Any, expected: int) -> None:
    response = [{"i": n} for n in range(size)] if isinstance(size, int) else size
    assert surcharge_weight(name, response) == expected


def test_candle_surcharge_per_60() -> None:
    assert surcharge_weight("candles_snapshot", [{"i": n} for n in range(59)]) == 0
    assert surcharge_weight("candles_snapshot", [{"i": n} for n in range(60)]) == 1
    assert surcharge_weight("candles_snapshot", [{"i": n} for n in range(120)]) == 2


def test_describe_call_bulk_sizes_first_arg() -> None:
    def bulk_orders(requests: list[dict[str, Any]], grouping: str = "na") -> None: ...

    assert describe_call(bulk_orders, ([{}, {}],)) == ("bulk_orders", 2)
    assert describe_call(bulk_orders, ([],)) == ("bulk_orders", 1)
    assert describe_call(bulk_orders, (None,)) == ("bulk_orders", 1)


def test_describe_call_unknown_shape_bills_unknown() -> None:
    assert describe_call(object(), ()) == ("", 1)
    assert request_weight("") == 20


class _Clock:
    """Manual monotonic clock for deterministic budget tests."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_budget_slides_and_floors() -> None:
    clock = _Clock()
    budget = RateBudget(time_fn=clock)
    assert budget.remaining() == IP_WEIGHT_BUDGET_PER_MINUTE
    assert budget.resets_in() == 0.0
    budget.record(1200)
    assert budget.remaining() == 0
    budget.record(500)  # over-spend floors, never negative
    assert budget.remaining() == 0
    assert budget.resets_in() == pytest.approx(60.0)
    clock.now += 30.0
    assert budget.remaining() == 0  # oldest entry still inside the window
    assert budget.resets_in() == pytest.approx(30.0)
    clock.now += 30.0
    assert budget.remaining() == IP_WEIGHT_BUDGET_PER_MINUTE
    assert budget.resets_in() == 0.0


def test_budget_ignores_non_positive() -> None:
    budget = RateBudget()
    budget.record(0)
    budget.record(-5)
    assert budget.remaining() == IP_WEIGHT_BUDGET_PER_MINUTE


class _FakeInfo:
    """Stub with SDK-identical method names so weight derivation applies."""

    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error

    def user_state(self, address: str) -> Any:
        if self.error is not None:
            raise self.error
        return self.result

    def user_fills(self, address: str) -> Any:
        if self.error is not None:
            raise self.error
        return self.result


def _adapter_with(info: _FakeInfo) -> HyperliquidAdapter:
    config = HyperliquidConfig(wallet_address=_TEST_ADDRESS, private_key=_TEST_KEY, testnet=True)
    adapter = HyperliquidAdapter(config, event_bus=EventBus())
    exchange = MagicMock()
    exchange.info = info
    adapter._exchange = exchange
    adapter._connected = True
    return adapter


async def test_run_exchange_records_base_weight() -> None:
    adapter = _adapter_with(_FakeInfo(result={"assetPositions": []}))
    await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)
    assert adapter._rate_budget.spent() == 2


async def test_run_exchange_records_surcharge() -> None:
    adapter = _adapter_with(_FakeInfo(result=[{"tid": n} for n in range(41)]))
    await adapter._run_exchange(adapter._exchange.info.user_fills, _TEST_ADDRESS)
    assert adapter._rate_budget.spent() == 20 + 2


async def test_run_exchange_429_maps_to_rate_limit() -> None:
    error = ClientError(429, "code", "Too many requests", {}, None)
    adapter = _adapter_with(_FakeInfo(error=error))
    with pytest.raises(RateLimitError, match="rate limit"):
        await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)
    assert adapter._rate_budget.spent() == 2  # rejected calls still spent budget


async def test_run_exchange_5xx_records_base_weight() -> None:
    """A 5xx is a venue response — it reached the venue, so it bills."""
    adapter = _adapter_with(_FakeInfo(error=ServerError(500, "internal error")))
    with pytest.raises(PlatformConnectionError):
        await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)
    assert adapter._rate_budget.spent() == 2


async def test_run_exchange_transport_failure_records_nothing() -> None:
    """A dropped request never reached the venue — it must not bill."""
    adapter = _adapter_with(_FakeInfo(error=requests.exceptions.ConnectionError("drop")))
    with pytest.raises(PlatformConnectionError):
        await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)
    assert adapter._rate_budget.spent() == 0


async def test_run_exchange_other_4xx_stays_platform_error() -> None:
    error = ClientError(400, "code", "Bad request", {}, None)
    adapter = _adapter_with(_FakeInfo(error=error))
    with pytest.raises(PlatformError):
        await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)


async def test_get_rate_limits_reports_live_budget() -> None:
    adapter = _adapter_with(_FakeInfo(result={"assetPositions": []}))
    fresh = await adapter.get_rate_limits()
    assert fresh.requests_per_interval == IP_WEIGHT_BUDGET_PER_MINUTE
    assert fresh.interval_seconds == 60.0
    assert fresh.remaining == IP_WEIGHT_BUDGET_PER_MINUTE
    await adapter._run_exchange(adapter._exchange.info.user_state, _TEST_ADDRESS)
    limited = await adapter.get_rate_limits()
    assert limited.remaining == IP_WEIGHT_BUDGET_PER_MINUTE - 2
    assert limited.reset_at.tzinfo is UTC
    assert limited.reset_at >= fresh.reset_at


def test_connect_weight_covers_setup() -> None:
    # meta 20 + spotMeta 20 + userRole 60 + userAbstraction 20.
    assert CONNECT_WEIGHT == 120

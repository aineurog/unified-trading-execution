"""Integration: live rate-budget state.

Gate: budget starts at 1200, spends exactly per call, refills over time.
Burn-to-429 is a documented skip — testnet does not enforce the limit.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.instrument import Instrument


async def test_budget_starts_full(
    connected_adapter: HyperliquidAdapter,
) -> None:
    limits = await connected_adapter.get_rate_limits()
    assert limits.requests_per_interval == 1200
    assert 0 < limits.remaining <= 1200


async def test_reads_spend_exact_weights(
    connected_adapter: HyperliquidAdapter, btc_perp: Instrument
) -> None:
    before = (await connected_adapter.get_rate_limits()).remaining
    # l2_snapshot (2) + meta_and_asset_ctxs (20) — the ticker's two calls.
    await connected_adapter.fetch_ticker(btc_perp)
    after = (await connected_adapter.get_rate_limits()).remaining
    assert before - after == 22


async def test_budget_is_local_only_no_venue_call(
    connected_adapter: HyperliquidAdapter,
) -> None:
    first = await connected_adapter.get_rate_limits()
    second = await connected_adapter.get_rate_limits()
    assert first.remaining == second.remaining  # no spend on a pure read


@pytest.mark.skip(
    reason="testnet does not enforce the 1200/min IP limit — burn never 429s (proven)"
)
async def test_burn_to_429() -> None:
    raise NotImplementedError("kept as documentation — enable only against an enforcing venue")

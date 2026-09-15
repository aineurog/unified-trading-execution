"""Ticker integration tests — live quote vs raw venue quote, mark semantics.

Verifies ``fetch_ticker`` returns a populated ``Ticker`` for a live trading
symbol in every category, that the unified fields match the raw
``get_tickers`` entry, and that mark is only populated for derivatives.
A syntactically-invalid instrument raises ``InvalidSymbolError`` before any
network call.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Ticker


async def _live_entry(
    adapter: BybitAdapter,
    instrument: Instrument,
) -> dict[str, Any]:
    """Fetch the raw Bybit tickers entry for ``instrument``."""
    category = adapter._instrument_to_category(instrument)
    symbol = f"{instrument.symbol}{instrument.quote_currency}"
    data, _ = await adapter._run_request(
        adapter._session.get_tickers,
        category=category,
        symbol=symbol,
        read=True,
    )
    entries: list[dict[str, Any]] = (data.get("result") or {}).get("list") or []
    assert entries, f"No tickers entry for {symbol}"
    entry = entries[0]
    assert isinstance(entry, dict)
    return entry


def _assert_close(actual: Decimal, expected: Decimal, field: str) -> None:
    """Assert two live quotes agree within a small relative tolerance.

    The unified ticker and the raw entry come from two sequential REST
    calls, so the market may move a tick between them — exact equality
    would flake on any live book. 0.1% relative tolerance absorbs that
    while still catching any mapping error (wrong field, scale bug).
    """
    assert actual > 0 and expected > 0, f"{field}: non-positive quote"
    drift = abs(actual - expected) / expected
    assert drift <= Decimal("0.001"), f"{field} drifted {drift}: {actual} vs {expected}"


def _assert_matches_live(ticker: Ticker, entry: dict[str, Any], category: str) -> None:
    """Assert the unified ticker equals the raw venue quote."""
    assert ticker.bid is not None and ticker.ask is not None
    assert ticker.last is not None
    _assert_close(ticker.bid, Decimal(str(entry["bid1Price"])), "bid")
    _assert_close(ticker.ask, Decimal(str(entry["ask1Price"])), "ask")
    _assert_close(ticker.last, Decimal(str(entry["lastPrice"])), "last")
    assert ticker.ask >= ticker.bid
    if category == "spot":
        assert ticker.mark is None, "spot has no mark price"
    else:
        assert ticker.mark is not None and ticker.mark > 0
        _assert_close(ticker.mark, Decimal(str(entry["markPrice"])), "mark")


async def test_ticker_matches_live_quote(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    category: str,
) -> None:
    """Every category returns a live ticker matching the raw entry."""
    ticker = await connected_adapter.fetch_ticker(traded_instrument)
    assert ticker is not None, "live trading symbol must have a quote"
    entry = await _live_entry(connected_adapter, traded_instrument)
    _assert_matches_live(ticker, entry, category)


async def test_ticker_spot_mark_always_none(
    connected_adapter: BybitAdapter,
    spot_instrument: Instrument,
) -> None:
    """Spot tickers carry bid/ask/last but never a mark."""
    ticker = await connected_adapter.fetch_ticker(spot_instrument)
    assert ticker is not None
    assert ticker.last is not None and ticker.last > 0
    assert ticker.mark is None


async def test_ticker_invalid_instrument_raises(
    connected_adapter: BybitAdapter,
) -> None:
    """An asset class Bybit does not support fails before any network call."""
    bad = Instrument(symbol="X", asset_class=AssetClass.BOND, currency="USD")
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_ticker(bad)

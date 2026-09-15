"""Ticker integration tests.

Fetches live bid/ask/last snapshots from a real MT5 demo account and
verifies the unified ``Ticker`` shape. A symbol the broker does not list
raises ``InvalidSymbolError``.

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
Broker symbols default to ``EURUSD`` / ``XAUUSD`` and can be overridden via
``MT5_SYMBOL`` / ``MT5_SYMBOL_XAU``.

Weekend note: FX/metals have no live quote while the market is closed, in
   which case ``fetch_ticker`` returns ``None`` by contract - those tests skip
   rather than fail so Monday-Friday CI stays meaningful either way.
"""

from __future__ import annotations

import os

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_BROKER_SYMBOL_XAU = os.getenv("MT5_SYMBOL_XAU", "XAUUSD").strip()
_EURUSD = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol=_BROKER_SYMBOL,
)
_XAUUSD = Instrument(
    symbol="XAU",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol=_BROKER_SYMBOL_XAU,
)


def _assert_live_quote(ticker, broker_symbol: str) -> None:
    if ticker is None:
        pytest.skip(f"no live quote for {broker_symbol} — market closed")
    assert ticker.bid is not None and ticker.bid > 0
    assert ticker.ask is not None and ticker.ask > 0
    assert ticker.ask >= ticker.bid
    assert ticker.mark is None, "MT5 has no mark price"


async def test_ticker_populated(connected_adapter: MT5Adapter) -> None:
    """EUR/USD snapshot carries a live bid/ask spread and no mark."""
    ticker = await connected_adapter.fetch_ticker(_EURUSD)
    _assert_live_quote(ticker, _BROKER_SYMBOL)


async def test_ticker_second_symbol_populated(connected_adapter: MT5Adapter) -> None:
    """XAU/USD snapshot is populated independently of EUR/USD."""
    ticker = await connected_adapter.fetch_ticker(_XAUUSD)
    _assert_live_quote(ticker, _BROKER_SYMBOL_XAU)


async def test_ticker_unknown_symbol_raises(connected_adapter: MT5Adapter) -> None:
    """A symbol the broker does not list raises InvalidSymbolError."""
    unknown = Instrument(
        symbol="ZZZ",
        quote_currency="ZZZ",
        asset_class=AssetClass.MARGIN_FX,
        platform_symbol="ZZZ_NO_SUCH_SYMBOL",
    )
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_ticker(unknown)

"""Ticker integration tests — live snapshot vs gateway.

Verifies ``fetch_ticker`` returns a populated ``Ticker`` for a live stock
contract and ``InvalidSymbolError`` for a contract the gateway does not
know. Requires ``IBKR_PORT`` / ``IBKR_ACCOUNT`` (paper gateway).

Two environment realities are handled by skipping, not failing:
  - Outside market hours (or without a live farm feed) a known contract
    has no quote, and ``fetch_ticker`` returns ``None`` by contract.
  - Without market-data subscriptions the snapshot request is rejected by
    TWS; that surfaces as ``PlatformConnectionError`` and the live-quote
    test skips (the unknown-contract test needs no market data and always
    asserts).
"""

from __future__ import annotations

import os

import pytest

from unified_trading_execution.errors import InvalidSymbolError, PlatformConnectionError
from unified_trading_execution.ibkr import IBKRAdapter
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_TICKER_SYMBOL = os.getenv("IBKR_TICKER_SYMBOL", "GOLD").strip()


def _stock(symbol: str = _TICKER_SYMBOL) -> Instrument:
    return Instrument(symbol=symbol, asset_class=AssetClass.STOCK, currency="USD")


async def test_ticker_live_quote(connected_adapter: IBKRAdapter) -> None:
    """A live stock contract returns bid/ask/last with no mark."""
    try:
        ticker = await connected_adapter.fetch_ticker(_stock())
    except PlatformConnectionError as exc:
        pytest.skip(f"no market-data snapshot available on this gateway: {exc}")
    if ticker is None:
        pytest.skip(f"no live quote for {_TICKER_SYMBOL} — market closed or unsubscribed")
    assert ticker.bid is not None and ticker.bid > 0
    assert ticker.ask is not None and ticker.ask > 0
    assert ticker.ask >= ticker.bid
    assert ticker.last is not None and ticker.last > 0
    assert ticker.mark is None, "IBKR snapshot exposes no distinct mark price"


async def test_ticker_unknown_contract_raises(connected_adapter: IBKRAdapter) -> None:
    """A contract the gateway does not know raises InvalidSymbolError."""
    unknown = Instrument(symbol="ZZZ_NO_SUCH_SYMBOL", asset_class=AssetClass.STOCK, currency="USD")
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_ticker(unknown)

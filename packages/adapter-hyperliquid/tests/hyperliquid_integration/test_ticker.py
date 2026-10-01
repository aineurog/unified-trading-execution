"""Integration: live ticker snapshots.

Gate: bid < ask, mark inside the touch band, Decimal purity.
"""

from __future__ import annotations

from decimal import Decimal

from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.instrument import Instrument

from .helpers import assert_is_decimal


async def test_btc_ticker_shape(
    connected_adapter: HyperliquidAdapter, btc_perp: Instrument
) -> None:
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    assert ticker is not None
    assert ticker.bid is not None and ticker.ask is not None
    assert ticker.bid < ticker.ask
    if ticker.mark is not None:
        # Mark is index-derived (asset ctx), not touch-bound — it may sit
        # outside a narrow top-of-book spread. Bound it to mid instead.
        mid = (ticker.bid + ticker.ask) / Decimal("2")
        assert abs(ticker.mark - mid) / mid < Decimal("0.01")
        assert_is_decimal(ticker.mark, "mark")
    assert_is_decimal(ticker.bid, "bid")
    assert_is_decimal(ticker.ask, "ask")


async def test_eth_ticker_shape(
    connected_adapter: HyperliquidAdapter, eth_perp: Instrument
) -> None:
    ticker = await connected_adapter.fetch_ticker(eth_perp)
    assert ticker is not None
    assert ticker.bid is not None and ticker.ask is not None
    assert ticker.bid < ticker.ask

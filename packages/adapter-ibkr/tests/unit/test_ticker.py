"""Unit tests for IBKRAdapter.fetch_ticker — mock-only, no live Gateway.

Covers each branch of the snapshot path:
  - full bid/ask/last quote → populated Ticker, mark always None
  - all-NaN snapshot → None (no live quote)
  - halted snapshot → None
  - partial NaN → partial Ticker (quoted sides only)
  - unknown contract (no details) → InvalidSymbolError
  - unsupported asset class → InvalidSymbolError
  - snapshot timeout / request failure → PlatformConnectionError
  - not connected → PlatformConnectionError
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from unified_trading_execution.errors import (
    InvalidSymbolError,
    PlatformConnectionError,
)
from unified_trading_execution.ibkr import IBKRAdapter
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Ticker

AAPL = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")
BOND = Instrument(symbol="US10Y", asset_class=AssetClass.BOND, currency="USD")


def _snapshot(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {"bid": float("nan"), "ask": float("nan"), "last": float("nan")}
    base.update(overrides)
    snap = SimpleNamespace(**base)
    # ib_async reports halt state on the ticker; absent in SimpleNamespace
    # unless a test sets it — mirror that default here.
    if not hasattr(snap, "halted"):
        snap.halted = 0
    return snap


def _known_contract(mock_ib: Any) -> None:
    mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[object()])


class TestFetchTicker:
    async def test_full_quote(self, adapter: IBKRAdapter, mock_ib_async_module: Any) -> None:
        """Bid/ask/last snapshot maps to a populated Ticker with mark None."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(
            return_value=[_snapshot(bid=150.25, ask=150.30, last=150.28)]
        )

        await adapter.connect()
        ticker = await adapter.fetch_ticker(AAPL)

        assert ticker == Ticker(
            bid=Decimal("150.25"),
            ask=Decimal("150.3"),
            last=Decimal("150.28"),
            mark=None,
        )
        mock_ib.reqTickersAsync.assert_awaited_once()

    async def test_all_nan_returns_none(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Known contract with no quote (closed / unsubscribed) → None."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(return_value=[_snapshot()])

        await adapter.connect()
        assert await adapter.fetch_ticker(AAPL) is None

    async def test_halted_returns_none(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Halted snapshot → None, never a stale quote."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(
            return_value=[_snapshot(bid=150.25, ask=150.30, last=150.28, halted=1)]
        )

        await adapter.connect()
        assert await adapter.fetch_ticker(AAPL) is None

    async def test_partial_nan_returns_partial_ticker(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Only quoted sides populate; NaN sides are None, not zero."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(return_value=[_snapshot(ask=150.30, last=150.28)])

        await adapter.connect()
        ticker = await adapter.fetch_ticker(AAPL)

        assert ticker is not None
        assert ticker.bid is None
        assert ticker.ask == Decimal("150.3")
        assert ticker.last == Decimal("150.28")

    async def test_empty_snapshot_list_returns_none(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Defensive: an empty snapshot list is no quote, not a failure."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(return_value=[])

        await adapter.connect()
        assert await adapter.fetch_ticker(AAPL) is None

    async def test_unknown_contract_raises_invalid_symbol(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Gateway with no contract details → InvalidSymbolError, no snapshot call."""
        mock_ib = mock_ib_async_module
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[])
        mock_ib.reqTickersAsync = AsyncMock(return_value=[_snapshot(bid=1.0)])

        await adapter.connect()
        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_ticker(AAPL)
        mock_ib.reqTickersAsync.assert_not_awaited()

    async def test_unsupported_asset_class_raises_invalid_symbol(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Asset classes with no IBKR mapping fail before any gateway call."""
        mock_ib = mock_ib_async_module

        await adapter.connect()
        with pytest.raises(InvalidSymbolError):
            await adapter.fetch_ticker(BOND)
        mock_ib.reqContractDetailsAsync.assert_not_awaited()

    async def test_nan_halted_returns_quote(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Live snapshots carry halted=NaN when unset — NaN is not halted."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(
            return_value=[_snapshot(bid=150.25, ask=150.30, last=150.28, halted=float("nan"))]
        )

        await adapter.connect()
        ticker = await adapter.fetch_ticker(AAPL)

        assert ticker is not None
        assert ticker.bid == Decimal("150.25")

    async def test_live_timeout_falls_back_to_delayed(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """No live feed (TWS 2186) → reqMarketDataType(3) + one delayed retry."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(
            side_effect=[
                TimeoutError("live needs subscription"),
                [_snapshot(bid=150.25, ask=150.30, last=150.28)],
            ]
        )

        await adapter.connect()
        ticker = await adapter.fetch_ticker(AAPL)

        assert ticker is not None
        assert ticker.bid == Decimal("150.25")
        mock_ib.reqMarketDataType.assert_called_once_with(3)
        assert mock_ib.reqTickersAsync.await_count == 2

    async def test_not_subscribed_falls_back_to_delayed(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Live 354 (not subscribed) → reqMarketDataType(3) + one delayed retry."""
        from unified_trading_execution.ibkr.errors import map_ibkr_error

        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(
            side_effect=[
                map_ibkr_error(354, "Requested market data is not subscribed"),
                [_snapshot(bid=150.25, ask=150.30, last=150.28)],
            ]
        )

        await adapter.connect()
        ticker = await adapter.fetch_ticker(AAPL)

        assert ticker is not None
        assert ticker.bid == Decimal("150.25")
        mock_ib.reqMarketDataType.assert_called_once_with(3)
        assert mock_ib.reqTickersAsync.await_count == 2

    async def test_snapshot_timeout_raises_connection_error(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Live and delayed snapshots both timing out → PlatformConnectionError."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(side_effect=TimeoutError("slow farm"))

        await adapter.connect()
        with pytest.raises(PlatformConnectionError, match="timed out"):
            await adapter.fetch_ticker(AAPL)
        assert mock_ib.reqTickersAsync.await_count == 2

    async def test_snapshot_failure_raises_connection_error(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """Request-level snapshot failure → PlatformConnectionError."""
        mock_ib = mock_ib_async_module
        _known_contract(mock_ib)
        mock_ib.reqTickersAsync = AsyncMock(side_effect=RuntimeError("farm down"))

        await adapter.connect()
        with pytest.raises(PlatformConnectionError, match="failed to fetch IBKR ticker"):
            await adapter.fetch_ticker(AAPL)

    async def test_not_connected_raises_connection_error(
        self, adapter: IBKRAdapter, mock_ib_async_module: Any
    ) -> None:
        """fetch_ticker requires a live connection like every other read."""
        with pytest.raises(PlatformConnectionError, match="not connected"):
            await adapter.fetch_ticker(AAPL)


class TestFetchTickerEngine:
    async def test_engine_delegates_to_adapter(self, adapter: IBKRAdapter) -> None:
        """IBKREngine.fetch_ticker is a thin passthrough (sync engine auto-proxies)."""
        from unified_trading_execution.ibkr.engine import IBKREngine

        expected = Ticker(bid=Decimal("150.25"), ask=None, last=None, mark=None)
        adapter.fetch_ticker = AsyncMock(return_value=expected)  # type: ignore[method-assign]

        engine = IBKREngine(adapter)
        assert await engine.fetch_ticker(AAPL) is expected

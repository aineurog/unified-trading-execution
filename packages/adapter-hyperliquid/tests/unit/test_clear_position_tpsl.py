"""Unit tests for clear_position_tpsl."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Position

_TEST_ADDRESS = "0x" + "00" * 20
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(private_key=_TEST_KEY, wallet_address=_TEST_ADDRESS)


def _instrument(**kwargs: object) -> Instrument:
    """Build an Instrument with given overrides."""
    defaults = {
        "symbol": "BTC",
        "quote_currency": "USDC",
        "asset_class": AssetClass.FUTURES,
        "currency": "USDC",
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


def _position(**kwargs: object) -> Position:
    """Build a Position with given overrides."""
    now = datetime.now(UTC)
    defaults = {
        "instrument": _instrument(),
        "quantity": Decimal("1.0"),
        "average_entry_price": Decimal("60000"),
        "updated_at": now,
        "position_id": "BTC:oneWay",
    }
    defaults.update(kwargs)
    return Position(**defaults)


class TestClearPositionTpsl:
    async def test_clear_stop_loss_only(self):
        """Clear only stop loss, preserving take profit."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        # Mock position exists
        adapter.fetch_positions = AsyncMock(return_value=[_position()])

        # Mock existing TP/SL entries
        adapter._position_tpsl_entries = AsyncMock(
            return_value={
                "take_profit": [{"oid": 123}],
                "stop_loss": [{"oid": 456}],
            }
        )

        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", stop_loss=True
        )

        # Verify only stop loss was cancelled
        assert adapter._run_exchange.call_count == 1
        call_args = adapter._run_exchange.call_args
        assert call_args[0][0] == adapter._exchange.cancel
        assert call_args[0][1] == "BTC"
        assert call_args[0][2] == 456

    async def test_clear_take_profit_only(self):
        """Clear only take profit, preserving stop loss."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter.fetch_positions = AsyncMock(return_value=[_position()])

        adapter._position_tpsl_entries = AsyncMock(
            return_value={
                "take_profit": [{"oid": 123}],
                "stop_loss": [{"oid": 456}],
            }
        )

        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", take_profit=True
        )

        # Verify only take profit was cancelled
        assert adapter._run_exchange.call_count == 1
        call_args = adapter._run_exchange.call_args
        assert call_args[0][2] == 123

    async def test_clear_both(self):
        """Clear both take profit and stop loss."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter.fetch_positions = AsyncMock(return_value=[_position()])

        adapter._position_tpsl_entries = AsyncMock(
            return_value={
                "take_profit": [{"oid": 123}],
                "stop_loss": [{"oid": 456}],
            }
        )

        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", take_profit=True, stop_loss=True
        )

        # Verify both were cancelled
        assert adapter._run_exchange.call_count == 2

    async def test_clear_on_flat_position_returns_quietly(self):
        """Clear on flat position returns quietly."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        # Mock no position (flat)
        adapter.fetch_positions = AsyncMock(return_value=[])

        # Mock _run_exchange to track calls
        exchange_call_count = 0

        async def mock_run_exchange(*args, **kwargs):
            nonlocal exchange_call_count
            exchange_call_count += 1
            return {"status": "ok"}

        adapter._run_exchange = mock_run_exchange

        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", take_profit=True, stop_loss=True
        )

        # Verify no cancels were attempted
        assert exchange_call_count == 0

    async def test_clear_on_zero_quantity_position_returns_quietly(self):
        """Clear on zero quantity position returns quietly."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter.fetch_positions = AsyncMock(return_value=[_position(quantity=Decimal("0"))])

        # Mock _run_exchange to track calls
        exchange_call_count = 0

        async def mock_run_exchange(*args, **kwargs):
            nonlocal exchange_call_count
            exchange_call_count += 1
            return {"status": "ok"}

        adapter._run_exchange = mock_run_exchange

        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", take_profit=True, stop_loss=True
        )

        # Verify no cancels were attempted
        assert exchange_call_count == 0

    async def test_clear_with_no_flags_raises(self):
        """Clear with no flags raises ValueError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        with pytest.raises(
            ValueError, match="at least one of take_profit or stop_loss must be true"
        ):
            await adapter.clear_position_tpsl(_instrument(), "BTC:oneWay")

    async def test_spot_raises(self):
        """Spot instrument raises InvalidSymbolError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        with pytest.raises(InvalidSymbolError, match="has no position TP/SL"):
            await adapter.clear_position_tpsl(
                _instrument(asset_class=AssetClass.SPOT, quote_currency="USDC"),
                "BTC/USDC:oneWay",
                take_profit=True,
            )

    async def test_clear_handles_concurrent_vanish(self):
        """Clear handles leg vanishing concurrently."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter.fetch_positions = AsyncMock(return_value=[_position()])

        adapter._position_tpsl_entries = AsyncMock(
            return_value={
                "take_profit": [{"oid": 123}],
                "stop_loss": [{"oid": 456}],
            }
        )

        # Simulate OrderNotFoundError for one leg
        call_count = 0

        async def mock_cancel(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                from unified_trading_execution.errors import OrderNotFoundError
                raise OrderNotFoundError("Order not found")
            return {"status": "ok"}

        adapter._run_exchange = mock_cancel

        # Should not raise, just continue
        await adapter.clear_position_tpsl(
            _instrument(), "BTC:oneWay", take_profit=True, stop_loss=True
        )

        # Verify both were attempted despite one failing
        assert call_count == 2

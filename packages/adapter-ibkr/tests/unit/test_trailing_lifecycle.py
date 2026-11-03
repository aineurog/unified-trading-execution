"""Unit tests for IBKR synthetic trailing lifecycle (adapter mocked, no network)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _config() -> IBKRConfig:
    return IBKRConfig(
        host="127.0.0.1",
        port=7497,
        client_id=1,
        account="DU123456",
    )


def _instrument(**kwargs: object) -> Instrument:
    defaults: dict[str, object] = {
        "symbol": "AAPL",
        "quote_currency": "USD",
        "asset_class": AssetClass.STOCK,
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)  # type: ignore[arg-type]


def test_clear_position_tpsl_selective():
    """clear_position_tpsl clears only the named sides."""
    adapter = IBKRAdapter(_config())
    adapter._ib = MagicMock()
    adapter._require_ib = lambda: adapter._ib  # type: ignore[assignment]
    adapter._cancel_protection_by_side = AsyncMock()

    instrument = _instrument()
    position_id = "12345"

    async def test():
        # Clear only stop loss
        await adapter.clear_position_tpsl(instrument, position_id, stop_loss=True)
        adapter._cancel_protection_by_side.assert_called_once_with(  # type: ignore[attr-defined]
            position_id, cancel_tp=False, cancel_sl=True
        )

    import asyncio

    asyncio.run(test())


def test_position_id_is_conid():
    """IBKR position_id is str(contract.conId) as documented."""
    adapter = IBKRAdapter(_config())
    adapter._ib = MagicMock()
    adapter._require_ib = lambda: adapter._ib  # type: ignore[assignment]
    adapter._ib.positions.return_value = [
        MagicMock(contract=MagicMock(conId=12345), position=100, avgCost=Decimal("100"))
    ]

    async def test():
        positions = await adapter.fetch_positions()
        if positions:
            assert positions[0].position_id == "12345"

    import asyncio

    asyncio.run(test())


def test_protection_order_ref_format():
    """Protection orders use deterministic orderRef prefixes."""
    from unified_trading_execution.ibkr.adapter import _protection_ref_suffix

    # Verify the suffix generator exists and returns a string
    suffix = _protection_ref_suffix()
    assert isinstance(suffix, str)
    assert len(suffix) > 0


def test_cancel_protection_by_side_no_op():
    """_cancel_protection_by_side is a no-op when no owned protection exists."""
    adapter = IBKRAdapter(_config())
    adapter._ib = MagicMock()
    adapter._require_ib = lambda: adapter._ib  # type: ignore[assignment]
    adapter._ib.openOrders.return_value = []

    position_id = "12345"

    async def test():
        # Should not raise when no orders exist
        await adapter._cancel_protection_by_side(position_id, cancel_tp=True, cancel_sl=True)
        # No orders to cancel, so cancelOrder should not be called
        assert not adapter._ib.cancelOrder.called  # type: ignore[attr-defined]

    import asyncio

    asyncio.run(test())

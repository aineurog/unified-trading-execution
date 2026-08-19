"""Hedging-mode integration tests (implementation plan, Step 21).

MT5 hedging accounts keep separate position legs per direction; the adapter
nets them into one ``Position`` per instrument (D-1) before publishing, and
``UnifiedOrder.position_id`` closes a specific leg (D-2).

The test is skipped when the demo account is netting-mode (opposing orders
net to zero, so no legs exist to exercise).

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars;
broker symbol defaults to ``EURUSD`` (override via ``MT5_SYMBOL``).
"""

from __future__ import annotations

import asyncio
import os

import pytest

from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import (
    build_unified_order,
    cleanup_adapter,
    position_for_symbol,
    random_client_id,
    spec_qty,
)

_BROKER_SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD").strip()
_INSTRUMENT = Instrument(symbol="EUR", quote_currency="USD", asset_class=AssetClass.MARGIN_FX)


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
        symbol_alias_table={"EUR/USD": _BROKER_SYMBOL},
    )


async def _raw_legs() -> list:
    """Raw MT5 position tuples on the broker symbol — one entry per leg."""
    import MetaTrader5 as mt5

    positions = await asyncio.to_thread(mt5.positions_get, symbol=_BROKER_SYMBOL)
    return list(positions or ())


async def test_hedging_nets_and_closes_leg(connected_adapter: MT5Adapter) -> None:
    """Opposing legs net to zero; the BUY leg closes by position_id."""
    qty = await spec_qty(connected_adapter, _INSTRUMENT)
    buy_cid = random_client_id("hedge-buy")
    sell_cid = random_client_id("hedge-sell")
    try:
        await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.BUY,
                qty,
                client_order_id=buy_cid,
            )
        )
        await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.SELL,
                qty,
                client_order_id=sell_cid,
            )
        )

        legs = await _raw_legs()
        if len(legs) < 2:
            pytest.skip("account is netting-mode — opposing orders net to zero")

        # fetch_positions() nets the two legs to a flat position.
        positions = await connected_adapter.fetch_positions()
        position = position_for_symbol(positions, _INSTRUMENT)
        assert position is not None
        assert position.quantity == 0

        # Close the BUY leg by position_id — the SELL leg must survive.
        buy_leg = next(leg for leg in legs if leg.type == 0)
        await connected_adapter.place_order(
            build_unified_order(
                _INSTRUMENT,
                OrderType.MARKET,
                OrderSide.SELL,
                qty,
                client_order_id=random_client_id("hedge-close"),
                position_id=str(buy_leg.ticket),
            )
        )

        remaining = await _raw_legs()
        assert len(remaining) == 1, f"expected one surviving leg, got {len(remaining)}"
        assert remaining[0].type == 1  # POSITION_TYPE_SELL

        positions = await connected_adapter.fetch_positions()
        position = position_for_symbol(positions, _INSTRUMENT)
        assert position is not None
        assert position.quantity == -qty
    finally:
        await cleanup_adapter(connected_adapter)

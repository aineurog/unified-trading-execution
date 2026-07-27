"""Quickstart example — async API.

Place a limit order on Bybit testnet via the async Engine.
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal

import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter


async def main() -> None:
    engine = ute.Engine(
        adapter=BybitAdapter(
            api_key=os.environ["BYBIT_API_KEY"],
            api_secret=os.environ["BYBIT_API_SECRET"],
            testnet=True,
        ),
        state_store=ute.SQLiteStateStore("./ute_data/bybit_testnet.db"),
    )
    await engine.connect()

    btc = ute.Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=ute.AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
        broker_symbol_override=None,
    )
    spec = await engine.fetch_instrument_spec(btc)
    print(f"BTC/USDT: tick={spec.tick_size}, min_qty={spec.min_qty}")

    order = ute.UnifiedOrder(
        instrument=btc,
        order_type=ute.OrderType.LIMIT,
        side=ute.OrderSide.BUY,
        quantity=Decimal("0.001"),
        price=Decimal("50000"),
        time_in_force=ute.TimeInForce.GTC,
    )
    result = await engine.place_order(order)
    print(f"Order placed: {result.client_order_id} — {result.status}")

    positions = await engine.get_all_positions()
    print(f"Open positions: {len(positions)}")

    await engine.ashutdown()


if __name__ == "__main__":
    asyncio.run(main())

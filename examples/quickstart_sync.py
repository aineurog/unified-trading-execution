"""Quickstart example — sync API.

Same workflow as quickstart_async.py, using the blocking SyncEngine.
"""

from __future__ import annotations

import os
from decimal import Decimal

import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter


def main() -> None:
    engine = ute.SyncEngine(
        adapter=BybitAdapter(
            api_key=os.environ["BYBIT_API_KEY"],
            api_secret=os.environ["BYBIT_API_SECRET"],
            testnet=True,
        ),
        state_store=ute.SQLiteStateStore("./ute_data/bybit_testnet.db"),
    )
    engine.connect()

    btc = ute.Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=ute.AssetClass.SPOT,
    )
    spec = engine.fetch_instrument_spec(btc)
    print(f"BTC/USDT: tick={spec.tick_size}, min_qty={spec.min_qty}")

    order = ute.UnifiedOrder(
        instrument=btc,
        order_type=ute.OrderType.LIMIT,
        side=ute.OrderSide.BUY,
        quantity=Decimal("0.001"),
        price=Decimal("50000"),
        time_in_force=ute.TimeInForce.GTC,
    )
    result = engine.place_order(order)
    print(f"Order placed: {result.client_order_id} — {result.status}")

    positions = engine.get_all_positions()
    print(f"Open positions: {len(positions)}")

    engine.shutdown()


if __name__ == "__main__":
    main()

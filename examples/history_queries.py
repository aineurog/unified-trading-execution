"""Example: query order and fill history."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import unified_trading_execution as ute


async def main() -> None:
    store: ute.StateStore = ...

    # Query all orders from the last 24 hours.
    yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)
    recent_orders = await store.query_orders(start=yesterday)
    for order in recent_orders:
        print(f"{order.client_order_id}: {order.status} {order.filled_quantity}/{order.quantity}")

    # Query fills for a specific instrument.
    btc = ute.Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=ute.AssetClass.SPOT,
    )
    btc_fills = await store.query_fills(instrument=btc, start=yesterday)
    total_filled = sum(fill.fill_quantity for fill in btc_fills)
    print(f"Total BTC filled in last 24h: {total_filled}")


if __name__ == "__main__":
    asyncio.run(main())

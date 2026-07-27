"""Example: subscribe to the event bus for fill events."""

from __future__ import annotations

import asyncio
import os

import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter


async def main() -> None:
    event_bus = ute.EventBus()

    # Subscribe to fill events.
    def on_fill(event: ute.FillEvent) -> None:
        fill = event.fill
        print(
            f"Fill: {fill.client_order_id} "
            f"qty={fill.fill_quantity} @ {fill.fill_price} "
            f"fee={fill.fee_amount} {fill.fee_currency}"
        )

    event_bus.subscribe(ute.FillEvent, on_fill)

    adapter = BybitAdapter(
        api_key=os.environ["BYBIT_API_KEY"],
        api_secret=os.environ["BYBIT_API_SECRET"],
        testnet=True,
        event_bus=event_bus,
    )
    await adapter.connect()

    # Place an order — when filled, on_fill will be called.
    btc = ute.Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=ute.AssetClass.SPOT,
    )
    order = ute.UnifiedOrder(
        instrument=btc,
        order_type=ute.OrderType.MARKET,
        side=ute.OrderSide.BUY,
        quantity=ute.Decimal("0.001"),
        time_in_force=ute.TimeInForce.IOC,
    )
    result = await adapter.place_order(order)
    print(f"Order: {result.client_order_id} — {result.status}")

    # Keep running to receive fills.
    await asyncio.sleep(5)
    await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

# Unified Trading Execution

A modular, plug-and-play order execution library for Python, providing a single consistent interface for placing, managing, and tracking orders across multiple trading platforms — crypto exchanges, retail/institutional brokers, and additional platforms across all asset classes.

```python
import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter
```

## Status

**v1 in development.** Two generations planned:
- **v1** — open-source core with Bybit and cTrader adapters.
- **v2** — additional adapters (MT5, IBKR) and feature modules, built strictly on top of v1's interfaces.

## Installation

```bash
# Core only (types, engine, risk checks, state mirror)
pip install unified-trading-execution

# Core plus a platform adapter and its vendor SDK
pip install "unified-trading-execution[binance]"
pip install "unified-trading-execution[bybit]"
pip install "unified-trading-execution[ctrader]"
pip install "unified-trading-execution[hyperliquid]"
pip install "unified-trading-execution[ibkr]"

# MetaTrader 5 (Windows)
pip install "unified-trading-execution[metatrader]"

# Every adapter
pip install "unified-trading-execution[all]"
```

Adapter modules import their vendor SDK when loaded, so install the matching
extra (or the vendor package yourself) before importing them. cTrader has no
vendor SDK dependency.

Requires Python 3.11+.

## Quickstart (async)

```python
import asyncio
import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter


async def main():
    # State store is backed by SQLite by default
    engine = ute.Engine(
        adapter=BybitAdapter(api_key="...", api_secret="...", testnet=True),
        state_store=ute.SQLiteStateStore("path/to/store.db"),
    )
    await engine.connect()

    btc = ute.Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=ute.AssetClass.SPOT,
    )
    await engine.fetch_instrument_spec(btc)

    order = ute.UnifiedOrder(
        instrument=btc,
        order_type=ute.OrderType.LIMIT,
        side=ute.OrderSide.BUY,
        quantity=ute.Decimal("0.001"),
        price=ute.Decimal("50000"),
        time_in_force=ute.TimeInForce.GTC,
    )
    result = await engine.place_order(order)
    print(f"Order {result.client_order_id}: {result.status}")

    await engine.ashutdown()


asyncio.run(main())
```

## Quickstart (sync)

```python
import unified_trading_execution as ute
from unified_trading_execution.bybit import BybitAdapter

engine = ute.SyncEngine(
    adapter=BybitAdapter(api_key="...", api_secret="...", testnet=True),
    state_store=ute.SQLiteStateStore("path/to/store.db"),
)
engine.connect()

btc = ute.Instrument(
    symbol="BTC",
    quote_currency="USDT",
    asset_class=ute.AssetClass.SPOT,
)
engine.fetch_instrument_spec(btc)

order = ute.UnifiedOrder(
    instrument=btc,
    order_type=ute.OrderType.LIMIT,
    side=ute.OrderSide.BUY,
    quantity=ute.Decimal("0.001"),
    price=ute.Decimal("50000"),
    time_in_force=ute.TimeInForce.GTC,
)
result = engine.place_order(order)
print(f"Order {result.client_order_id}: {result.status}")

engine.shutdown()
```

## Documentation

Full documentation at [docs site] — installation, core concepts, platform guides, API reference.

## License

Apache License 2.0. See [LICENSE](LICENSE).

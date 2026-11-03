# IBKR Adapter

Unified Trading Execution adapter for Interactive Brokers (IBKR) via TWS/Gateway.

## What is covered

| Market | Asset class | Example symbols |
|---|---|---|
| Stocks | `STK` | AAPL, MSFT, TSLA |
| Futures | `FUT` | ES, NQ, CL |
| Options | `OPT` | AAPL 20250120 C150 |
| Forex | `CASH` | EURUSD, GBPUSD |
| CFDs | `CFD` | Indices, commodities |

**Note:** Works with IBKR paper trading accounts for development. Production use requires a live IBKR account with appropriate permissions.

## Installation

```bash
pip install unified-trading-execution[ibkr]
```

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | `str` | `"127.0.0.1"` | TWS/Gateway host |
| `port` | `int` | `7497` | Paper trading port (7497 for TWS, 4002 for Gateway) |
| `client_id` | `int` | *(required)* | Unique client ID per connection |
| `account` | `str` | *(required)* | IBKR account ID (e.g., "DU123456") |
| `read_only` | `bool` | `False` | Prevents order placement if True |
| `default_exchange` | `str \| None` | `None` | Default exchange for orders (e.g., "SMART", "IDEALPRO") |
| `platform_name` | `str` | `"ibkr"` | Human-readable label for logs and events |

## Quickstart

| Async | Sync |
|---|---|
|
```python
import asyncio

from unified_trading_execution.ibkr import IBKREngine, IBKRConfig
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

async def main():
    engine = IBKREngine(IBKRConfig(
        host="127.0.0.1",
        port=7497,
        client_id=1,
        account="DU123456",
    ))
    await engine.connect()

    aapl = Instrument(
        symbol="AAPL",
        quote_currency="USD",
        asset_class=AssetClass.STOCK,
    )
    await engine.fetch_instrument_spec(aapl)

    result = await engine.place_order(UnifiedOrder(
        instrument=aapl,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=100,
        time_in_force=TimeInForce.GTC,
    ))
    print(f"Order {result.client_order_id} -> {result.status.value}")

    await engine.ashutdown()

asyncio.run(main())
```
|
```python
from unified_trading_execution.ibkr import SyncIBKREngine, IBKRConfig
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

engine = SyncIBKREngine(IBKRConfig(
    host="127.0.0.1",
    port=7497,
    client_id=1,
    account="DU123456",
))
engine.connect()

aapl = Instrument(
    symbol="AAPL",
    quote_currency="USD",
    asset_class=AssetClass.STOCK,
)
engine.fetch_instrument_spec(aapl)

result = engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=100,
    time_in_force=TimeInForce.GTC,
))
print(f"Order {result.client_order_id} -> {result.status.value}")

engine.shutdown()
```
|

**One import, one object.** `IBKREngine` and `SyncIBKREngine` combine the engine and adapter into a single class. Construct one with an `IBKRConfig`, call any method — order lifecycle, position protection, market data, reconciliation — everything is on the engine.

If you need lower-level access, the raw `IBKRAdapter` and `Engine` are still available. See [advanced usage](#advanced-usage).

Numeric inputs (`quantity`, `price`, `stop_price`) accept plain numbers:

```python
# These all work
UnifiedOrder(..., quantity=100)
UnifiedOrder(..., price="175.50")
UnifiedOrder(..., stop_price=170.25)
```

## Trailing stops

IBKR trailing is **synthetic-only**. Native TRAIL orders are not supported. All trailing uses position protection via `modify_position_tpsl`:

- Position ID is `str(contract.conId)` — globally unique per contract
- Protection orders use deterministic `orderRef` prefixes (`<conId>-tp-`, `<conId>-sl-`)
- TP and SL orders are OCA-paired when both exist
- Every reprice is cancel-and-replace (gap behavior)
- Minimum reprice interval is 2.0 seconds

```python
from decimal import Decimal

from unified_trading_execution.trailing import (
    TrailingOffsetType,
    TrailingRequest,
    TrailingSpec,
)

state = await engine.trailing.attach(
    TrailingRequest(
        aapl,
        TrailingSpec(
            offset_type=TrailingOffsetType.PERCENT,
            offset=Decimal("1.0"),
        ),
        position_id="265598",  # str(conId)
    )
)
print(state.mode.value, state.phase.value)  # synthetic armed

await engine.trailing.cancel(state.trailing_id)

active = await engine.trailing.list(active_only=True)
current = await engine.trailing.get(state.trailing_id)
```

The sync `SyncIBKREngine` exposes the same `trailing` surface without `await`.

### Trailing configuration

```python
from unified_trading_execution.trailing import TrailingConfig

engine = IBKREngine(
    config,
    trailing_config=TrailingConfig(
        poll_interval_seconds=2.0,  # Reprice interval
    ),
)
```

See the [platform guide](../../../docs/platforms/ibkr.md) for detailed documentation on trailing stops, including:
- Native versus synthetic execution
- Position leg ownership
- Restart and recovery behavior
- Caveats and limitations

## Position protection

IBKR does not support native TP/SL attachment to entry orders. Protection is implemented as separate OCA-pair orders placed on the position contract.

```python
from unified_trading_execution.types.order import TpSlAttachment

# Set TP/SL
await engine.modify_position_tpsl(
    instrument,
    position_id="265598",  # str(conId)
    take_profit=TpSlAttachment(trigger_price=Decimal("200")),
    stop_loss=TpSlAttachment(trigger_price=Decimal("190")),
)

# Read TP/SL
tp, sl = await engine.get_position_tpsl(instrument, position_id="265598")

# Clear TP/SL selectively
await engine.clear_position_tpsl(
    instrument,
    position_id="265598",
    take_profit=True,
    stop_loss=True,
)
```

**Behavior:**
- Cancels existing owned protection on both sides before placing new orders
- Reads existing protection to preserve unmentioned side
- Places new TP as LMT order, SL as STP or STP LMT
- OCA-pairs TP and SL when both exist
- Transmits atomically (only last order transmits)

## Market data

```python
# Fetch ticker
ticker = await engine.fetch_ticker(instrument)

# Fetch candles (7 core timeframes supported)
from unified_trading_execution.adapter import CandleTimeframe

candles = await engine.fetch_candles(
    instrument,
    CandleTimeframe.H1,
    limit=100,
)
```

**Supported timeframes:** M1, M5, M15, M30, H1, H4, D1

## Order operations

### Supported order types

| Order type | Notes |
|---|---|
| `MARKET` | Market orders |
| `LIMIT` | Limit orders |
| `STOP` | Stop orders |
| `STOP_LIMIT` | Stop-limit orders |

`TimeInForce.DAY` is not supported by IBKR and raises `UnsupportedOrderTypeError`.

### Placing an order

```python
# Market buy
result = await engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=100,
    time_in_force=TimeInForce.GTC,
))

# Limit sell
result = await engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.LIMIT,
    side=OrderSide.SELL,
    quantity=100,
    price=175.50,
    time_in_force=TimeInForce.GTC,
))

# Stop-loss
result = await engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.STOP,
    side=OrderSide.SELL,
    quantity=100,
    stop_price=170.00,
    time_in_force=TimeInForce.GTC,
))
```

The engine generates a UUID7 `client_order_id` if you do not supply one.

### Modifying an order

```python
from unified_trading_execution.types.order import OrderModification

updated = await engine.modify_order(OrderModification(
    client_order_id=result.client_order_id,
    price=176.00,
    quantity=150,
))
```

IBKR supports modifying quantity and price for open orders. Time-in-force changes are not supported.

### Cancelling an order

```python
cancelled = await engine.cancel_order(result.client_order_id)
```

Raises `OrderNotFoundError` if IBKR does not know the order.

### Querying an order

```python
order = await engine.get_order("your-client-order-id")
if order is None:
    print("Order not found")
else:
    print(f"Status: {order.status.value}, filled: {order.filled_quantity}")
```

## Instrument metadata

```python
spec = await engine.fetch_instrument_spec(aapl)
# InstrumentSpec(
#     tick_size=0.01,
#     lot_size=1,
#     min_qty=1,
#     max_qty=1000000,
#     min_notional=1,
#     price_precision=2,
#     qty_precision=0,
#     max_leverage=None,  # None for stocks
# )
```

Results are cached in memory and invalidated on contract events.

## Error handling

The adapter translates IBKR errors into the common exception hierarchy:

- `InvalidSymbolError`: Contract not found or invalid
- `OrderNotFoundError`: Order or position not found
- `PlatformConnectionError`: Connection to TWS/Gateway failed
- `PlatformError`: Other IBKR errors with translated messages

## Rate limits

IBKR does not have explicit API rate limits like crypto exchanges. The adapter uses a 2.0-second minimum interval for ticker polling and reprice operations to respect IBKR's fair usage guidelines.

## Testing

Unit tests:
```bash
uv run pytest packages/adapter-ibkr/tests/unit/
```

Integration tests require a live IBKR paper trading account:
```bash
uv run pytest packages/adapter-ibkr/tests/ibkr_integration/
```

## Documentation

See [docs/platforms/ibkr.md](../../../docs/platforms/ibkr.md) for complete documentation including:
- Credentials and setup
- Connection lifecycle
- Order operations
- Trailing stops (synthetic-only)
- Position protection
- Error handling
- Rate limits
- Advanced usage

## Advanced usage

### Raw adapter access

For direct access to the IBKR adapter without the engine:

```python
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig

adapter = IBKRAdapter(IBKRConfig(
    host="127.0.0.1",
    port=7497,
    client_id=1,
    account="DU123456",
))
await adapter.connect()
# Use adapter methods directly
await adapter.disconnect()
```

### Custom state store

Provide a custom state store path:

```python
from unified_trading_execution.state import StateStore

engine = IBKREngine(
    config,
    state_store=StateStore(path="./custom/state.db"),
)
```

### Reference price callback

Provide a custom reference price for risk checks:

```python
def get_reference_price(instrument):
    # Custom logic to fetch reference price
    return Decimal("100.0")

engine = IBKREngine(config, get_reference_price=get_reference_price)
```

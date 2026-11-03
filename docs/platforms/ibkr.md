# IBKR Adapter

The IBKR adapter provides a unified interface for trading on Interactive Brokers via the TWS/Gateway TCP socket connection using the `ib_async` package. It implements the `Adapter` ABC, translating every platform-specific detail (contract details, order codes, error messages, ticker data) into the engine's canonical types so your trading logic never needs to know it is talking to IBKR.

## What is covered

| Market | Asset class | Example symbols |
|---|---|---|
| Stocks | `STK` | AAPL, MSFT, TSLA |
| Futures | `FUT` | ES, NQ, CL |
| Options | `OPT` | AAPL 20250120 C150 |
| Forex | `CASH` | EURUSD, GBPUSD |
| CFDs | `CFD` | Indices, commodities |
| Crypto | `CRYPTO` | BTC, ETH |

**Note:** This adapter works with IBKR's paper trading accounts for development. Production use requires a live IBKR account with appropriate permissions.

## Credentials and setup

You need an IBKR paper trading account for development. IBKR does not issue API keys — access is granted through TWS or IB Gateway:

1. Install TWS or IB Gateway and log in with your paper trading account.
2. Open **Global Configuration → API → Settings**.
3. Enable **Enable ActiveX and Socket Clients** and note the socket port.
4. Add your machine to the trusted IP list if the connection is rejected.
5. Keep TWS or IB Gateway running while the adapter is connected.

### The `IBKRConfig` dataclass

All configuration goes through an immutable `IBKRConfig` instance:

```python
from unified_trading_execution.ibkr import IBKRConfig

config = IBKRConfig(
    host="127.0.0.1",
    port=7497,
    client_id=1,
    account="DU123456",
)
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | `str` | `"127.0.0.1"` | TWS/Gateway host |
| `port` | `int` | `4002` | Paper Gateway port (7497 for paper TWS, 4001 live Gateway, 7496 live TWS) |
| `client_id` | `int` | `1` | Unique client ID per connection |
| `account` | `str \| None` | `None` | IBKR account ID (e.g., "DU123456"); `None` uses the Gateway default |
| `default_exchange` | `str` | `"SMART"` | Fallback routing exchange (e.g., "SMART", "IDEALPRO") |
| `default_currency` | `str` | `"USD"` | Fallback contract currency when the instrument omits it |
| `timeout_seconds` | `float` | `10.0` | Connection and blocking request timeout |
| `readonly` | `bool` | `False` | Connects in read only mode and blocks order placement |
| `instrument_spec_cache_ttl` | `float \| None` | `86400.0` | Seconds a cached spec is trusted; `None` caches indefinitely |

### Quickstart

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

## Connection lifecycle

The adapter connects to TWS or IB Gateway via TCP socket. Ensure TWS/Gateway is running with API connections enabled before calling `connect()`.

### `connect()`

Opens the TCP connection to TWS/Gateway and starts event streams:

```python
# Async
await engine.connect()
assert engine.adapter.is_connected

# Sync
engine.connect()
assert engine.adapter.is_connected
```

`connect()` is idempotent — calling it on an already-connected adapter is a no-op.

### `disconnect()`

Closes the TCP connection gracefully and clears internal caches:

```python
# Async
await engine.disconnect()

# Sync
engine.disconnect()
```

`disconnect()` is also idempotent.

### `shutdown()`

Ordered teardown: disconnects the adapter, closes the state store, and stops the background event loop (sync). After shutdown the engine is permanently unusable.

```python
# Async
await engine.ashutdown()

# Sync
engine.shutdown()
```

### Connection requirements

**Important:** Only one TWS/Gateway instance can be connected per `client_id`. If you run multiple engines, use different `client_id` values.

**Paper trading ports:**
- TWS: 7497
- IB Gateway: 4002

**Live trading ports:**
- TWS: 7496
- IB Gateway: 4001

## Order operations

Orders go through the engine's risk-check pipeline (symbol validity, quantity bounds, price sanity, duplicate detection, rate limiting) before reaching the platform. You never call `adapter.place_order()` directly — use `engine.place_order()`.

### Supported order types

| Order type | Notes |
|---|---|
| `MARKET` | Market orders |
| `LIMIT` | Limit orders |
| `STOP` | Stop orders |
| `STOP_LIMIT` | Stop-limit orders |

Supported time-in-force values are `GTC`, `DAY`, `IOC`, `FOK` and `GTD`. A `GTD` order must carry `expire_at`. `MARKET` orders on crypto are forced to `IOC`; every other asset class uses the requested value.

### Placing an order

```python
# Async / Sync — identical API, one calls await, the other doesn't

# Market buy
result = engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=100,
    time_in_force=TimeInForce.GTC,
))

# Limit sell
result = engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.LIMIT,
    side=OrderSide.SELL,
    quantity=100,
    price=175.50,
    time_in_force=TimeInForce.GTC,
))

# Stop-loss
result = engine.place_order(UnifiedOrder(
    instrument=aapl,
    order_type=OrderType.STOP,
    side=OrderSide.SELL,
    quantity=100,
    stop_price=170.00,
    time_in_force=TimeInForce.GTC,
))
```

The engine generates a UUID7 `client_order_id` if you do not supply one. The adapter uses `client_order_id` as the `orderRef` field in IBKR orders for tracking.

### TP/SL attachment

IBKR does not support native TP/SL attachment to entry orders. Instead, TP/SL protection is implemented as separate OCA-pair orders placed on the position contract. Use `modify_position_tpsl` to set or update protection on an open position. See [Position protection](#position-protection) for details.

### Modifying an order

```python
from unified_trading_execution.types.order import OrderModification

updated = engine.modify_order(OrderModification(
    client_order_id=result.client_order_id,
    price=176.00,
    quantity=150,
))
```

The adapter locates the order by `client_order_id` (sent as IBKR's `orderRef`). It supports modifying `price`, `stop_price` and `quantity` on open orders. `OrderModification` has no field for time in force, so a modification cannot change it.

### Cancelling an order

```python
cancelled = engine.cancel_order(result.client_order_id)
```

Raises `OrderNotFoundError` if IBKR does not know the order.

### Querying an order

```python
order = engine.get_order("your-client-order-id")
if order is None:
    print("Order not found")
else:
    print(f"Status: {order.status.value}, filled: {order.filled_quantity}")
```

The adapter maintains a mirror of order states from IBKR callbacks.

## Trailing stops

IBKR trailing is **synthetic-only**. The adapter reports:

- `trailing_capability.supported = False`
- Native TRAIL orders are not supported
- All trailing uses position protection via `modify_position_tpsl`

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

### Position leg ids

IBKR trailing is position state, not standalone orders. Pass the position id as `position_id`:

- Position ID is `str(contract.conId)` — globally unique per contract
- The id format is the conId as a string
- Omitting it is allowed only when the symbol has a single open position

### Position leg ownership

State unit is `(instrument, position_id)` where `position_id = str(conId)`. Protection is attached to the exact position leg. Takeover detection and integrity checks use position-scoped reads.

### Restart and recovery

Trailing state is persisted in the engine's state store. On reconnect the engine rehydrates active states from a direct position snapshot, restores synthetic protection, and re-arms missing protection. IBKR position `opened_at` is `None` because IBKR has no reliable venue field for position open time, so rehydration relies on position identity checks rather than timestamps.

### Synthetic trailing behavior

Synthetic trailing keeps a normal position stop loss on the target leg. The engine's polling worker samples the resolved basis, tightens the stop on favourable moves, never loosens it on adverse moves, carries it through partial closes, and clears it when the position reaches zero. `cancel` removes the stop levels this state owns.

### IBKR trailing caveats

- **No native trailing.** IBKR has a TRAIL order type, but it is order-scoped (not position-scoped) and requires separate bookkeeping that is out of scope.
- **Cancel-and-replace gaps.** Every reprice cancels the old order and places a new one, creating a brief gap where no protection exists.
- **Minimum reprice interval.** The adapter declares a 2.0 second floor between reprice operations. The engine respects the value each adapter reports.
- **No trigger basis selection.** The adapter declares no trigger bases, so stops fire on the venue default feed. It cannot select LAST, BID or ASK.
- **Position open time unavailable.** IBKR has no reliable venue field for position open time, so `opened_at` is `None` for all positions.

## Instrument metadata

`fetch_instrument_spec` returns an `InstrumentSpec` with the trading rules for a single instrument:

```python
spec = engine.fetch_instrument_spec(aapl)
# InstrumentSpec(
#     tick_size=0.01,
#     lot_size=1,
#     min_qty=1,
#     max_qty=1000000000,
#     min_notional=0,
#     price_precision=2,
#     qty_precision=0,
# )
```

`tick_size`, `lot_size` and `min_qty` come from IBKR contract details. IBKR publishes no explicit maximum order size, so the adapter reports a large cap (`1e9`), and it reports `min_notional=0` because IBKR has no minimum notional rule.

Specs are cached per instrument for `instrument_spec_cache_ttl` seconds (default one day). The cache entry is dropped when IBKR rejects an order because the contract's rules changed, forcing the next fetch to re-query.

The adapter does not populate `max_leverage`: IBKR leverage is implied by per instrument margin requirements, not a single account setting.

## Leverage management

IBKR does not have a unified leverage setting like crypto exchanges. Leverage is implicit based on margin requirements for each instrument. The adapter does not implement leverage management methods for IBKR.

## Position protection

IBKR does not support native TP/SL attachment to entry orders. Protection is implemented as separate OCA-pair orders placed on the position contract.

### modify_position_tpsl

Set or update TP/SL on an open position:

```python
from unified_trading_execution.types.order import TpSlAttachment

await engine.modify_position_tpsl(
    instrument,
    position_id="265598",  # str(conId)
    take_profit=TpSlAttachment(trigger_price=Decimal("200")),
    stop_loss=TpSlAttachment(trigger_price=Decimal("190")),
)
```

**Behavior:**
- Cancels existing owned protection on both sides before placing new orders
- Reads existing protection to preserve unmentioned side
- Places new TP as LMT order, SL as STP or STP LMT
- OCA-pairs TP and SL when both exist
- Transmits atomically (only last order transmits)

### get_position_tpsl

Read current TP/SL on a position:

```python
tp, sl = await engine.get_position_tpsl(instrument, position_id="265598")
```

Returns `(take_profit, stop_loss)` where each is `None` if not set, or `None` if the position does not exist.

### clear_position_tpsl

Clear protection on specific sides:

```python
await engine.clear_position_tpsl(
    instrument,
    position_id="265598",
    take_profit=True,
    stop_loss=True,
)
```

**Behavior:**
- Cancels only the named sides
- Preserves unmentioned sides
- No-op when no owned protection exists
- Raises `ValueError` if both flags are False

## Market data

### Fetch ticker

```python
ticker = await engine.fetch_ticker(instrument)
```

Returns a `Ticker` with bid, ask, and last price. Ticker data requires a market data subscription in your IBKR account.

### Fetch candles

```python
from unified_trading_execution.adapter import CandleTimeframe

candles = await engine.fetch_candles(
    instrument,
    CandleTimeframe.H1,
    limit=100,
)
```

**Supported timeframes:** M1, M5, M15, M30, H1, H4, D1

The adapter uses IBKR's `reqHistoricalData` to fetch closed OHLC candles. Historical data may require additional permissions depending on the instrument and exchange.

## Error handling

The adapter translates IBKR errors into the common exception hierarchy:

- `InvalidSymbolError`: Contract not found or invalid
- `OrderNotFoundError`: Order or position not found
- `PlatformConnectionError`: Connection to TWS/Gateway failed
- `PlatformError`: Other IBKR errors with translated messages

## Rate limits

The adapter declares a 2.0 second minimum interval for ticker polling and reprice operations. The engine schedules trailing work no faster than the interval each adapter reports, so this floor is enforced centrally rather than by the adapter itself.

## Testing

Unit tests are located in `packages/adapter-ibkr/tests/unit/`:

- `test_capabilities.py`: Capability declarations, including clear protection flags
- `test_fetch_candles.py`: Candle fetching, bar timestamps, series selection
- `test_orders.py`: Order translation and TP/SL protection orders
- `test_engine.py`: Engine wiring and method forwarding
- `test_trailing_lifecycle.py`: Synthetic trailing lifecycle and position leg ids

Run unit tests:
```bash
uv run pytest packages/adapter-ibkr/tests/unit/
```

Integration tests require a live IBKR paper trading account and are located in `packages/adapter-ibkr/tests/ibkr_integration/`:

- `test_order_roundtrip.py`: Order lifecycle and TP/SL behavior
- `test_trailing.py`: Synthetic trailing integration tests

Run integration tests:
```bash
uv run pytest packages/adapter-ibkr/tests/ibkr_integration/
```

Integration tests require environment variables:
- `IBKR_HOST` (default: 127.0.0.1)
- `IBKR_PORT` (required - 7497 for TWS, 4002 for Gateway)
- `IBKR_ACCOUNT` (required - your paper trading account ID)
- `IBKR_CLIENT_ID` (default: 1)

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
from unified_trading_execution.state import SQLiteStateStore

engine = IBKREngine(
    config,
    state_store=SQLiteStateStore("./custom/state.db"),
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

# Hyperliquid Adapter

The Hyperliquid adapter provides a unified interface for trading on Hyperliquid —
perpetual derivatives markets. It implements the `Adapter` ABC, translating every
platform-specific detail (REST payloads, WebSocket streams, error codes, symbol
naming) into the engine's canonical types so your trading logic never needs to
know it is talking to Hyperliquid.

## What is covered

| Market | Asset class | Example symbols |
|---|---|---|
| Perpetuals | `FUTURES` | BTC, ETH, SOL |

Spot markets are not currently supported. Hyperliquid uses one-way position mode
only — hedge mode is not available.

## Credentials and setup

Hyperliquid uses wallet-based authentication. You need a wallet address and
private key. For development, use the testnet:

1. Generate a testnet wallet address and private key for Hyperliquid testnet.
2. The adapter requires both `wallet_address` and `private_key` for authentication.

### The `HyperliquidConfig` dataclass

All configuration goes through an immutable `HyperliquidConfig` instance:

```python
from unified_trading_execution.hyperliquid import HyperliquidConfig

config = HyperliquidConfig(
    wallet_address="your-wallet-address",
    private_key="your-private-key",
    testnet=True,
    platform_name="hyperliquid",
    account_id="hyperliquid-account",
)
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `wallet_address` | `str` | *(required)* | Hyperliquid wallet address |
| `private_key` | `str` | *(required)* | Wallet private key for signing |
| `testnet` | `bool` | `True` | `False` connects to mainnet |
| `platform_name` | `str` | `"hyperliquid"` | Human-readable label for logs and events |
| `account_id` | `str` | `"hyperliquid-account"` | Unique account label in logs, events, and the state store filename |
| `instrument_spec_cache_ttl` | `float \| None` | `86400.0` | Seconds before a cached `InstrumentSpec` is re-fetched. `None` disables TTL-based expiry, relying solely on event-driven invalidation |

### Quickstart

<table>
<tr><th>Async</th><th>Sync</th></tr>
<tr>
<td>

```python
import asyncio

from unified_trading_execution.hyperliquid import HyperliquidEngine, HyperliquidConfig
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

async def main():
    engine = HyperliquidEngine(HyperliquidConfig(
        wallet_address="...",
        private_key="...",
        testnet=True,
    ))
    await engine.connect()

    btc = Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
    )
    await engine.fetch_instrument_spec(btc)

    result = await engine.place_order(UnifiedOrder(
        instrument=btc,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=0.001,
        time_in_force=TimeInForce.IOC,
    ))
    print(f"Order {result.client_order_id} -> {result.status.value}")

    await engine.ashutdown()

asyncio.run(main())
```

</td>
<td>

```python
from unified_trading_execution.hyperliquid import (
    HyperliquidConfig, SyncHyperliquidEngine,
)
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

engine = SyncHyperliquidEngine(HyperliquidConfig(
    wallet_address="...",
    private_key="...",
    testnet=True,
))
engine.connect()

btc = Instrument(
    symbol="BTC",
    quote_currency="USDC",
    asset_class=AssetClass.FUTURES,
)
engine.fetch_instrument_spec(btc)

result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=0.001,
    time_in_force=TimeInForce.IOC,
))
print(f"Order {result.client_order_id} -> {result.status.value}")

engine.shutdown()
```

</td>
</tr>
</table>

**One import, one object.** `HyperliquidEngine` and `SyncHyperliquidEngine` combine
the engine and adapter into a single class. Construct one with a
`HyperliquidConfig`, call any method — order lifecycle, leverage management,
position mode, reconciliation, history — everything is on the engine.

If you need lower-level access, the raw `HyperliquidAdapter` and `Engine` are
still available. See [advanced usage](#advanced-usage).

Numeric inputs (`quantity`, `price`, `stop_price`) accept plain numbers:

```python
# These all work
UnifiedOrder(..., quantity=0.001)
UnifiedOrder(..., price="65000")
UnifiedOrder(..., stop_price=64500.50)
```

## Connection lifecycle

### `connect()`

Opens the REST session and WebSocket streams in sequence:

1. **order** — every order placement, modification, cancellation, and fill
2. **execution** — individual trade executions (fills)
3. **position** — position size, entry price, and side changes
4. **wallet** — per-coin balance updates

After the streams are open, `connect()` publishes `ConnectionStateEvent(connected=True)`,
starts a background connection monitor, and applies the configured margin mode.

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

Closes the WebSocket, cancels the connection monitor, clears internal caches,
and publishes `ConnectionStateEvent(connected=False)`.

```python
# Async
await engine.disconnect()

# Sync
engine.disconnect()
```

`disconnect()` is also idempotent.

### `shutdown()`

Ordered teardown: flushes all audit-trail writes, disconnects the adapter,
closes the state store, and stops the background event loop (sync). After
shutdown the engine is permanently unusable.

```python
# Async
await engine.ashutdown()

# Sync
engine.shutdown()
```

### Connection monitor

A background task runs every 15 seconds while the adapter is connected. If it
detects the WebSocket has dropped, it publishes a new `ConnectionStateEvent`.
On a transition from disconnected back to connected, the instrument registry is
re-populated and any cached `InstrumentSpec` entries for instruments whose status
left `Trading` are invalidated.

## Order operations

Orders go through the engine's risk-check pipeline (symbol validity, quantity
bounds, price sanity, duplicate detection, rate limiting) before reaching the
platform. You never call `adapter.place_order()` directly — use
`engine.place_order()`.

### Supported order types

| Order type | Hyperliquid type | Notes |
|---|---|---|
| `MARKET` | IOC limit at slippage price | Uses SDK default 5% slippage from mid price |
| `LIMIT` | `Limit` + `price` | Full `timeInForce` support (GTC, IOC, FOK) |
| `STOP` | Market + `triggerPrice` | Perpetuals only |
| `STOP_LIMIT` | Limit + `price` + `triggerPrice` | Perpetuals only |

`TimeInForce.DAY` has no Hyperliquid equivalent and raises `UnsupportedOrderTypeError`.

### Placing an order

```python
# Async / Sync — identical API, one calls await, the other doesn't

# Market buy
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=0.001,
    time_in_force=TimeInForce.IOC,
))

# Limit sell
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.LIMIT,
    side=OrderSide.SELL,
    quantity=0.001,
    price=68000,
    time_in_force=TimeInForce.GTC,
))

# Stop-loss (conditional market — perpetuals only)
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.STOP,
    side=OrderSide.SELL,
    quantity=0.001,
    stop_price=65000,
    time_in_force=TimeInForce.GTC,
))
```

The engine generates a UUID7 `client_order_id` if you do not supply one.
If the platform rejects the order, the cached `InstrumentSpec` for that
instrument is invalidated (forcing a re-fetch on the next access) before the
mapped error is re-raised.

### Take-profit and stop-loss attachment

Hyperliquid supports native TP/SL attachment at order placement. Pass
`take_profit` and/or `stop_loss` on the `UnifiedOrder`:

```python
from unified_trading_execution.types.order import TpSlAttachment

result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.LIMIT,
    side=OrderSide.BUY,
    quantity=0.001,
    price=65000,
    time_in_force=TimeInForce.GTC,
    take_profit=TpSlAttachment(
        trigger_price=70000,
        limit_price=70100,            # omit for market TP
    ),
    stop_loss=TpSlAttachment(
        trigger_price=63000,
        # no limit_price -> market execution
    ),
))
```

When at least one TP/SL attachment has a `limit_price`, the attachment is a
limit order. When both are market (no `limit_price` on either), they execute as
market orders on trigger.

**Spot restrictions:** TP/SL is not available for spot instruments.

### Trailing stops

Hyperliquid uses **synthetic trailing stops only** via position protection. Native
trailing is not supported because Hyperliquid's API does not provide
position-scoped trailing stop reads (required for takeover detection, integrity
verification, and restart recovery). The `get_position_trailing_stop` method raises
`NotImplementedError`.

```python
from decimal import Decimal

from unified_trading_execution.trailing import (
    TrailingMode,
    TrailingOffsetType,
    TrailingRequest,
    TrailingSpec,
)

state = await engine.trailing.attach(
    TrailingRequest(
        btc,
        TrailingSpec(
            offset_type=TrailingOffsetType.PRICE,
            offset=Decimal("500"),
        ),
        position_id="BTC:oneWay",
        mode=TrailingMode.AUTO,
    )
)
print(state.mode.value, state.phase.value)  # synthetic armed

await engine.trailing.cancel(state.trailing_id, clear_protection=True)

active = await engine.trailing.list(active_only=True)
current = await engine.trailing.get(state.trailing_id)
```

The sync `SyncHyperliquidEngine` exposes the same `trailing` surface without `await`.

#### Position leg ids

Hyperliquid uses one-way position mode. The position id format is
`{coin}:oneWay`, e.g. `BTC:oneWay`. Pass this as `position_id` when attaching
trailing.

#### Synthetic trailing only

Since native trailing is not supported, all trailing requests route to synthetic:

| Request | Execution |
|---|---|
| Any request (percent, price, ATR, activation, step, etc.) | Synthetic stop loss, repriced by the engine |

Synthetic trailing keeps a normal position stop loss on the target leg. The
engine's polling worker samples the resolved basis, tightens the stop on
favourable moves, never loosens it on adverse moves, carries it through partial
closes, and clears it when the position reaches zero. `cancel` with
`clear_protection=True` removes the stop levels this state owns.

#### Position protection

Trailing uses the same position protection methods as static TP/SL:

- `modify_position_tpsl` - set or update TP/SL on a position leg
- `get_position_tpsl` - read current TP/SL on a position leg
- `clear_position_tpsl` - remove named protection sides (stop, take profit, or both)

One-sided writes preserve unmentioned sides (Hyperliquid merge semantics). If you
write only a stop loss, the existing take profit is preserved, and vice versa.

#### Trigger basis

Hyperliquid triggers stop losses on mark price. The adapter uses `MARK` as the
trigger basis for all synthetic stop writes.

#### Existing protection and takeover

Before a synthetic attach writes anything, the adapter reads the position's
current TP/SL. If a stop loss or take profit already exists, you must set
`take_over_existing_protection=True`; otherwise attach raises and leaves the
venue unchanged. With takeover enabled the new values replace the old ones. This
prevents silent overwriting of protection you placed manually or via another
system.

#### Restart and recovery

Trailing state is persisted in the engine's state store. On reconnect the
engine rehydrates active states from a direct position snapshot and restores
synthetic protection. Hyperliquid position state does not include a reliable
position open timestamp, so `Position.opened_at` is set to `None`. Rehydration
relies on side and entry price verification and never creates a second active
state for the same leg.

#### Hyperliquid trailing caveats

- **No native trailing.** The venue API cannot provide position-scoped trailing
  stop reads, so native trailing is not supported.
- **Position open time is None.** Neither REST nor WebSocket position responses
  include a position open timestamp, so identity verification on restart is
  limited to side and entry price.
- **5 significant figures.** Hyperliquid enforces a 5-significant-figure limit
  on prices. The adapter rounds prices accordingly before sending to the venue.

### Reduce-only orders

Set `reduce_only=True` to place an order that only reduces an existing position:

```python
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.SELL,
    quantity=0.001,
    time_in_force=TimeInForce.GTC,
    reduce_only=True,
))
```

### Modifying an order

```python
from unified_trading_execution.types.order import OrderModification

updated = engine.modify_order(OrderModification(
    client_order_id=result.client_order_id,
    price=65500,
    quantity=0.002,
))
```

The adapter locates the order by `client_order_id`. Unsupported modification
fields raise `UnsupportedOrderTypeError`.

### Cancelling an order

```python
cancelled = engine.cancel_order(result.client_order_id)
```

Raises `OrderNotFoundError` if Hyperliquid does not know the order.

### Querying an order

```python
order = engine.get_order("your-client-order-id")
if order is None:
    print("Order not found")
else:
    print(f"Status: {order.status.value}, filled: {order.filled_quantity}")
```

The adapter queries Hyperliquid's REST endpoint.

## Instrument metadata

`fetch_instrument_spec` returns an `InstrumentSpec` with the trading rules for
a single instrument:

```python
spec = engine.fetch_instrument_spec(btc)
# InstrumentSpec(
#     tick_size=0.1,
#     lot_size=0.00001,
#     min_qty=0.00001,
#     max_qty=1000,
#     min_notional=10,
#     price_precision=0,
#     qty_precision=5,
#     max_leverage=50,
#     min_stop_distance=50,
# )
```

Results are cached for the lifetime of the adapter instance. The cache is
invalidated in three situations:

1. **TTL expiry** — controlled by `instrument_spec_cache_ttl` in `HyperliquidConfig`
   (default: one day).
2. **Platform rejection** — if a `place_order` or `modify_order` call is
   rejected by Hyperliquid with a mapped error, the cached spec is dropped so the
   next fetch re-queries fresh rules.
3. **Instrument status change** — if the instrument's status leaves `Trading`
   (detected during the periodic instrument registry refresh on reconnect),
   the cached spec is dropped.

`max_leverage` is only present for derivatives — spot instruments return
`None` for this field.

## Leverage management

The Hyperliquid adapter manages **per-instrument leverage** as *adapter-owned
user intent*. It tracks leverage and can detect and correct drift.

### Setting leverage

```python
engine.set_leverage(btc, buy_leverage=10)
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `buy_leverage` | `int` | `1` | Leverage for the instrument |
| `on_drift` | `"reapply" \| "notify" \| "halt"` | `"reapply"` | What to do when platform leverage diverges from stored intent |
| `strict_check` | `bool` | `True` | Run a pre-order leverage check before every `place_order` call |
| `block_on_open_position` | `bool` | `True` | Refuse to change leverage while the instrument has an open position |
| `auto_apply_on_connect` | `bool` | `True` | Re-apply stored leverage on every `connect()` |

`set_leverage` validates the request eagerly:

- A leverage value above the platform's maximum raises `LeverageExceedsMaxError`.
- An open position on the instrument raises `PlatformError` when
  `block_on_open_position` is enabled.

### How intent persistence works

When you call `set_leverage`, the adapter does two things:

1. **Applies the value immediately** on the platform via Hyperliquid's
   leverage update endpoint.
2. **Persists the intent** as flat key-value rows in the state store's
   `adapter_config` table:

   ```
   leverage.buy:BTC        = "10"
   leverage.policy.on_drift:BTC       = "reapply"
   leverage.policy.strict_check:BTC   = "1"
   leverage.policy.block_on_open:BTC  = "1"
   leverage.policy.auto_apply:BTC     = "1"
   ```

This means the adapter survives restarts — on the next `connect()`, stored
intent is re-applied automatically (unless `auto_apply_on_connect=False` for
that symbol).

### Reading current leverage

```python
leverage = engine.get_leverage(btc)
if leverage is not None:
    print(f"Leverage: {leverage}x")
```

Returns the leverage from the platform's current state, or `None` for instruments
that have never had a position.

### Removing stored intent

```python
engine.clear_leverage_intent(btc)
```

Removes the stored intent for the instrument. The platform leverage is not
changed; only the state store entry is deleted.

### Leverage drift detection

The adapter detects drift by comparing the platform's current leverage with the
stored intent on every `connect()` and periodically during operation. When drift
is detected, the behavior depends on the `on_drift` policy:

| Policy | Behavior |
|---|---|
| `"reapply"` | Automatically re-apply the stored leverage |
| `"notify"` | Publish a `LeverageDriftEvent` but do not change the platform |
| `"halt"` | Publish a `LeverageDriftEvent` and halt the adapter (no further orders) |

A `LeverageDriftEvent` carries the instrument, stored leverage, and platform
leverage.

## Position mode

Hyperliquid uses one-way position mode only. Hedge mode is not available. The
adapter does not expose position mode management methods.

## Position protection

### Modifying TP/SL on an open position

```python
engine.modify_position_tpsl(
    instrument,
    "BTC:oneWay",
    take_profit=TpSlAttachment(trigger_price=70000),
    stop_loss=TpSlAttachment(trigger_price=63000),
)
```

At least one of `take_profit` / `stop_loss` is required. One-sided writes
preserve unmentioned sides — if you write only a stop loss, the existing take
profit is preserved, and vice versa.

### Reading TP/SL on an open position

```python
protection = engine.get_position_tpsl(instrument, "BTC:oneWay")
if protection is not None:
    tp, sl = protection
    print(f"TP: {tp.trigger_price if tp else 'None'}, SL: {sl.trigger_price if sl else 'None'}")
```

Returns a tuple `(take_profit, stop_loss)` where each is a `TpSlAttachment` or
`None`.

### Clearing TP/SL on an open position

```python
engine.clear_position_tpsl(
    instrument,
    "BTC:oneWay",
    take_profit=True,
    stop_loss=True,
)
```

At least one of `take_profit` / `stop_loss` must be `True`. This removes the
named protection sides from the position.

## Error handling

The adapter translates Hyperliquid errors into the common exception hierarchy:

| Hyperliquid error | Mapped exception |
|---|---|
| "Order must have minimum value of $10" | `InvalidOrderError` |
| "Price too far from oracle" | `PlatformError` |
| "Not enough margin" | `PlatformError` |
| "Invalid order" | `InvalidOrderError` |
| Network/timeout errors | `PlatformConnectionError` |
| Rate limit exceeded | `RateLimitError` |

Use the mapped exception types in your code for clean error handling:

```python
from unified_trading_execution.errors import InvalidOrderError, PlatformError

try:
    result = await engine.place_order(order)
except InvalidOrderError as e:
    print(f"Order rejected: {e}")
except PlatformError as e:
    print(f"Platform error: {e}")
```

## Rate limiting

Hyperliquid enforces rate limits on API calls. The adapter tracks rate budget
and applies surcharges for calls that consume additional budget. When the rate
limit is exceeded, the adapter raises `RateLimitError`.

The rate budget is managed per IP address. The adapter uses the SDK's default
weight accounting and applies additional surcharges for certain endpoints that
consume more budget than the SDK reports.

## Testing

### Unit tests

Unit tests use mocks and do not require live credentials:

```bash
pytest packages/adapter-hyperliquid/tests/unit/
```

### Integration tests

Integration tests require testnet credentials:

```bash
export HYPERLIQUID_TESTNET_ADDRESS=your-wallet-address
export HYPERLIQUID_TESTNET_PRIVATE_KEY=your-private-key
pytest packages/adapter-hyperliquid/tests/hyperliquid_integration/
```

Integration tests are automatically skipped when credentials are missing.

## Advanced usage

### Using the raw adapter

If you need direct access to the adapter without the engine's risk checks:

```python
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.events import EventBus

adapter = HyperliquidAdapter(
    HyperliquidConfig(wallet_address="...", private_key="...", testnet=True),
    event_bus=EventBus(),
)
await adapter.connect()

# Direct adapter calls (bypass engine risk checks)
result = await adapter.place_order(order)

await adapter.disconnect()
```

### Custom state store path

By default, the engine stores state in:
```
./unified_trading_execution_data/hyperliquid_<wallet_address>.db
```

Override this with a custom path:

```python
from unified_trading_execution.state.store import SQLiteStateStore

engine = HyperliquidEngine(
    config,
    state_store=SQLiteStateStore("my_custom_path.db"),
)
```

### Reconciliation

The adapter supports full reconciliation via:

- `fetch_positions()` — live position snapshot
- `fetch_balances()` — live balance snapshot
- `fetch_open_orders()` — live open orders snapshot
- `fetch_fills()` — fill history

Use these to verify the engine's state mirror against the platform:

```python
# Reconcile positions
platform_positions = await engine.fetch_positions()
mirror_positions = engine.get_positions(btc)

# Reconcile balances
platform_balances = await engine.fetch_balances()
mirror_balances = engine.get_balance("USDC")
```

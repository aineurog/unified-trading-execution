# Bybit Adapter

The Bybit adapter provides a unified interface for trading on Bybit — spot,
linear perpetuals (USDT/USDC margined), and inverse perpetuals. It implements
the `Adapter` ABC, translating every platform-specific detail (REST payloads,
WebSocket streams, error codes, symbol naming) into the engine's canonical
types so your trading logic never needs to know it is talking to Bybit.

## What is covered

| Market | Asset class | Example symbols |
|---|---|---|
| Spot | `SPOT` | BTCUSDT, ETHUSDT, SOLUSDT |
| Linear perpetuals (USDT) | `FUTURES` | BTCUSDT, ETHUSDT |
| Linear perpetuals (USDC) | `FUTURES` | BTCPERP, ETHPERP |
| Inverse perpetuals | `FUTURES` | BTCUSD, ETHUSD |

Dated futures (contracts with expiry), options, and spot conditional orders
(Bybit `orderFilter=StopOrder`) are not currently supported.

## Credentials and setup

You need an API key and secret from Bybit. For development, use the testnet:

1. Go to [testnet.bybit.com](https://testnet.bybit.com) and create an account.
2. Navigate to **Account & Security → API Management**.
3. Create a new API key with read/write permissions for spot and derivatives.
4. Copy the key and secret.

### The `BybitConfig` dataclass

All configuration goes through an immutable `BybitConfig` instance:

```python
from unified_trading_execution.bybit import BybitConfig

config = BybitConfig(
    api_key="your-api-key",
    api_secret="your-api-secret",
    testnet=True,
    margin_mode="cross",
    account_id="bybit-main",
)
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `api_key` | `str` | *(required)* | Bybit API key |
| `api_secret` | `str` | *(required)* | Bybit API secret |
| `testnet` | `bool` | `True` | `False` connects to mainnet |
| `demo` | `bool` | `False` | Uses the demo subdomain (`api-demo`) when `True` |
| `margin_mode` | `MarginMode \| str` | `"cross"` | Account-wide margin mode, applied on every `connect()` |
| `platform_name` | `str` | `"bybit"` | Human-readable label for logs and events |
| `account_id` | `str` | `"bybit-account"` | Unique account label in logs, events, and the state store filename |
| `instrument_spec_cache_ttl` | `float \| None` | `86400.0` | Seconds before a cached `InstrumentSpec` is re-fetched. `None` disables TTL-based expiry, relying solely on event-driven invalidation |

### Quickstart

<table>
<tr><th>Async</th><th>Sync</th></tr>
<tr>
<td>

```python
import asyncio

from unified_trading_execution import Engine
from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

async def main():
    adapter = BybitAdapter(BybitConfig(
        api_key="...", api_secret="...", testnet=True,
    ))
    engine = Engine(adapter)
    await engine.connect()

    btc = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
    )
    await engine.fetch_instrument_spec(btc)

    result = engine.place_order(UnifiedOrder(
        instrument=btc,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=0.01,
        time_in_force=TimeInForce.GTC,
    ))
    print(f"Order {result.client_order_id} -> {result.status.value}")

    await engine.ashutdown()

asyncio.run(main())
```

</td>
<td>

```python
from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

engine = SyncEngine(BybitAdapter(BybitConfig(
    api_key="...", api_secret="...", testnet=True,
)))
engine.connect()

btc = Instrument(
    symbol="BTC",
    quote_currency="USDT",
    asset_class=AssetClass.FUTURES,
)
engine.fetch_instrument_spec(btc)

result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=0.01,
    time_in_force=TimeInForce.GTC,
))
print(f"Order {result.client_order_id} -> {result.status.value}")

engine.shutdown()
```

</td>
</tr>
</table>

Both examples are symmetric — an engine wraps the adapter, orders go through
the risk-check pipeline, and every operation is a single method call on the
engine.  No separate wrapper objects, no `EventBus` wiring, and no manual
`Decimal()` construction are needed.

Numeric inputs (`quantity`, `price`, `stop_price`) accept plain numbers:

```python
# These all work
UnifiedOrder(..., quantity=0.01)
UnifiedOrder(..., price="65000")
UnifiedOrder(..., stop_price=64500.50)
```

## Connection lifecycle

### `connect()`

Opens the REST session and four private WebSocket streams in sequence:

1. **order** — every order placement, modification, cancellation, and fill
2. **execution** — individual trade executions (fills)
3. **position** — position size, entry price, and side changes
4. **wallet** — per-coin balance updates

After the streams are open, `connect()` publishes `ConnectionStateEvent(connected=True)`,
starts a background connection monitor, applies the configured margin mode, and
re-applies any stored leverage and position mode intent (see [Leverage
management](#leverage-management)).

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

A background task runs every 5 seconds while the adapter is connected. If it
detects the WebSocket has dropped, it publishes a new `ConnectionStateEvent`.
On a transition from disconnected back to connected (pybit's automatic
reconnect), the instrument registry is re-populated and any cached
`InstrumentSpec` entries for instruments whose status left `Trading` are
invalidated.

## Order operations

Orders go through the engine's risk-check pipeline (symbol validity, quantity
bounds, price sanity, duplicate detection, rate limiting) before reaching the
platform.  You never call `adapter.place_order()` directly — use
`engine.place_order()`.

### Supported order types

| Order type | Bybit `orderType` | Notes |
|---|---|---|
| `MARKET` | `Market` | Always executes IOC on Bybit; `timeInForce` omitted from payload |
| `LIMIT` | `Limit` + `price` | Full `timeInForce` support (GTC, IOC, FOK) |
| `STOP` | `Market` + `triggerPrice` | Derivatives only — not available for spot |
| `STOP_LIMIT` | `Limit` + `price` + `triggerPrice` | Derivatives only — not available for spot |

`TimeInForce.DAY` has no Bybit equivalent and raises `UnsupportedOrderTypeError`.

### Placing an order

```python
# Async / Sync — identical API, one calls await, the other doesn't

# Market buy
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=0.01,
    time_in_force=TimeInForce.GTC,
))

# Limit sell
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.LIMIT,
    side=OrderSide.SELL,
    quantity=0.01,
    price=68000,
    time_in_force=TimeInForce.GTC,
))

# Stop-loss (conditional market — derivatives only)
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.STOP,
    side=OrderSide.SELL,
    quantity=0.01,
    stop_price=65000,
    time_in_force=TimeInForce.GTC,
))
```

The engine generates a UUID7 `client_order_id` if you do not supply one.
If the platform rejects the order, the cached `InstrumentSpec` for that
instrument is invalidated (forcing a re-fetch on the next access) before the
mapped error is re-raised.

### Take-profit and stop-loss attachment

Bybit supports native TP/SL attachment at order placement. Pass
`take_profit` and/or `stop_loss` on the `UnifiedOrder`:

```python
from unified_trading_execution.types.order import TpSlAttachment

result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.LIMIT,
    side=OrderSide.BUY,
    quantity=0.01,
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

When at least one TP/SL attachment has a `limit_price`, the Bybit `tpslMode`
is `Partial` (some legs are limit orders). When both are market (no
`limit_price` on either), the mode is `Full`.

**Spot restrictions:** TP/SL on spot is only available for `LIMIT` orders.
Using it with any other order type raises `UnsupportedOrderTypeError`.

### Reduce-only orders

Set `reduce_only=True` to place an order that only reduces an existing position:

```python
result = engine.place_order(UnifiedOrder(
    instrument=btc,
    order_type=OrderType.MARKET,
    side=OrderSide.SELL,
    quantity=0.01,
    time_in_force=TimeInForce.GTC,
    reduce_only=True,
))
```

`reduce_only` cannot be combined with TP/SL on Bybit — that combination raises
`UnsupportedOrderTypeError`. `reduce_only` is also unavailable for spot.

### Modifying an order

```python
from unified_trading_execution.types.order import OrderModification

updated = engine.modify_order(OrderModification(
    client_order_id=result.client_order_id,
    price=65500,
    quantity=0.02,
))
```

The adapter locates the order by `client_order_id` (sent as Bybit's
`orderLinkId`). Unsupported modification fields raise
`UnsupportedOrderTypeError`. Spot orders cannot have TP/SL modified.

### Cancelling an order

```python
cancelled = engine.cancel_order(result.client_order_id)
```

Raises `OrderNotFoundError` if Bybit does not know the order.

### Querying an order

```python
order = engine.get_order("your-client-order-id")
if order is None:
    print("Order not found")
else:
    print(f"Status: {order.status.value}, filled: {order.filled_quantity}")
```

The adapter queries Bybit's open-orders endpoint first, then falls back to the
two-year order history so filled and cancelled orders remain findable.

## Instrument metadata

`fetch_instrument_spec` returns an `InstrumentSpec` with the trading rules for
a single instrument:

```python
spec = engine.fetch_instrument_spec(btc)
# InstrumentSpec(
#     tick_size=0.1,
#     lot_size=0.001,
#     min_qty=0.001,
#     max_qty=100,
#     min_notional=1,
#     price_precision=1,
#     qty_precision=3,
#     max_leverage=100,         # None for spot
# )
```

Results are cached for the lifetime of the adapter instance. The cache is
invalidated in three situations:

1. **TTL expiry** — controlled by `instrument_spec_cache_ttl` in `BybitConfig`
   (default: one day).
2. **Platform rejection** — if a `place_order` or `modify_order` call is
   rejected by Bybit with a mapped error, the cached spec is dropped so the
   next fetch re-queries fresh rules.
3. **Instrument status change** — if the instrument's status leaves `Trading`
   (detected during the periodic instrument registry refresh on reconnect),
   the cached spec is dropped.

`max_leverage` is only present for derivatives — spot instruments return
`None` for this field.

## Leverage management

The Bybit adapter manages **per-instrument, per-side leverage** as
*adapter-owned user intent*. Both the buy-side and sell-side leverage are
stored and tracked independently so the adapter can detect and correct drift.

### Setting leverage

```python
engine.set_leverage(btc, buy_leverage=10)
```

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `buy_leverage` | `int` | `1` | Long-side leverage |
| `sell_leverage` | `int` | *(same as buy)* | Short-side leverage. Must equal `buy_leverage` in v1 — asymmetric values raise `AsymmetricLeverageError` |
| `on_drift` | `"reapply" \| "notify" \| "halt"` | `"reapply"` | What to do when platform leverage diverges from stored intent |
| `strict_check` | `bool` | `True` | Run a pre-order leverage check before every `place_order` call |
| `block_on_open_position` | `bool` | `True` | Refuse to change leverage while the instrument has an open position |
| `auto_apply_on_connect` | `bool` | `True` | Re-apply stored leverage on every `connect()` |

`sell_leverage` is optional — it defaults to `buy_leverage`.  In v1 they must
be equal; asymmetric leverage requires hedge mode (not yet supported).

`set_leverage` validates the request eagerly:

- Spot instruments raise `InvalidSymbolError` (leverage is a derivatives-only concept).
- A leverage value above the platform's maximum raises `LeverageExceedsMaxError`.
- An open position on the instrument raises `PlatformError` when
  `block_on_open_position` is enabled.
- `buy_leverage != sell_leverage` raises `AsymmetricLeverageError`.

### How intent persistence works

When you call `set_leverage`, the adapter does two things:

1. **Applies the values immediately** on the platform via Bybit's
   `set_leverage` endpoint.
2. **Persists the intent** as flat key-value rows in the state store's
   `adapter_config` table:

   ```
   leverage.buy:BTCUSDT        = "10"
   leverage.sell:BTCUSDT       = "10"
   leverage.policy.on_drift:BTCUSDT       = "reapply"
   leverage.policy.strict_check:BTCUSDT   = "1"
   leverage.policy.block_on_open:BTCUSDT  = "1"
   leverage.policy.auto_apply:BTCUSDT     = "1"
   ```

This means the adapter survives restarts — on the next `connect()`, stored
intent is re-applied automatically (unless `auto_apply_on_connect=False` for
that symbol).

### Reading current leverage

```python
leverage = engine.get_leverage(btc)
if leverage is not None:
    buy_lev, sell_lev = leverage
    print(f"Buy: {buy_lev}x, Sell: {sell_lev}x")
```

Returns `(buy, sell)` from the platform's current state, or `None` for spot
instruments and instruments that have never had a position. In one-way mode
both sides are always equal.

### Removing stored intent

```python
engine.remove_leverage(btc)
```

This drops the stored intent from the state store but does **not** change
leverage on the platform. The adapter stops managing leverage for this
instrument; subsequent orders skip the strict leverage check for this symbol.

### Drift detection

Leverage can change outside the engine — someone uses the Bybit app, another
script calls the API directly, or the platform resets leverage on a contract
change. The adapter detects this at two points:

1. **Pre-order strict check** — runs before every `place_order` call (unless
   `strict_check=False` for that symbol). If the platform's current leverage
   differs from stored intent, the configured `on_drift` behavior executes
   immediately.

2. **Reconciliation** — `reconcile_user_intent()` is called by the engine
   during a reconciliation pass. It scans all stored leverage intents,
   compares each against the platform's current value, and handles mismatches.

The three drift behaviors:

| `on_drift` | Effect |
|---|---|
| `"reapply"` | Immediately restores the stored leverage on the platform. The order proceeds. |
| `"notify"` | Publishes a `LeverageDriftEvent` on the event bus but does not touch the platform. For the strict check, also raises `LeverageDriftError` (rejecting the order). |
| `"halt"` | Publishes a `LeverageDriftEvent`, enters an instrument-scoped halt (blocking new exposure-increasing orders on that instrument), and raises `LeverageDriftError`. |

When a subsequent reconciliation finds the platform has returned to the stored
value, the halt is cleared automatically and a `HaltClearedEvent` is published.

### Working with a standalone adapter

Leverage persistence needs the shared `StateStore`.  When using the engine,
this is handled automatically — the engine creates the state store and wires it
in.  If you construct the adapter standalone (without an engine), pass
`state_store` to the constructor:

```python
from unified_trading_execution.state.store import SQLiteStateStore

store = SQLiteStateStore(path="my_data.db")
await store.initialize()
adapter = BybitAdapter(config, state_store=store)
```

## Position mode

Bybit supports two position modes per symbol:

| Mode | Bybit value | Description |
|---|---|---|
| `one_way` | `0` | One net position per symbol (buying reduces a short, selling reduces a long) |
| `hedge` | `3` | Independent long and short positions on the same symbol |

### Setting position mode

```python
engine.set_position_mode(btc, mode="one_way")
```

Both the enum and the raw string work — `"one_way"` / `"hedge"` or `PositionMode.ONE_WAY` / `PositionMode.HEDGE`:

```python
from unified_trading_execution.bybit import PositionMode

engine.set_position_mode(btc, mode=PositionMode.HEDGE)
```

The platform enforces that no open position or open order exists on the symbol
before the switch. If that condition is not met, Bybit returns error 110030
or 110031, mapped to `PlatformError`.

Like leverage, position mode intent is persisted in the state store and
re-applied on connect.

### Reading position mode

```python
mode = engine.get_position_mode(btc)
```

Returns the mode read from the platform's `positionIdx` field, or `None` if
the symbol has no open position (mode is unreadable from the platform when
there is no position) or if the symbol is spot.

### Batch switching by coin

```python
engine.set_position_mode_for_coin(
    coin="BTC",
    category="linear",
    mode="one_way",
)
```

This calls Bybit's `switch_position_mode` with a `coin` parameter, applying
the mode to every symbol of that coin with no open positions. Newly listed
symbols of that coin inherit the mode.

This method does **not** persist per-symbol intent, so drift detection will
not apply to symbols switched this way. Use per-symbol `set_position_mode`
afterward if you want drift management on individual instruments.

## Margin mode

Margin mode is account-wide on Bybit (it applies to the entire UTA account,
not per symbol). It is a static configuration value set at construction time,
not a runtime-persisted intent.

```python
config = BybitConfig(
    api_key="...",
    api_secret="...",
    margin_mode="isolated",
)
```

The configured mode is applied on every `connect()` call. If the platform
already matches, the call is a no-op (error 110026 is suppressed). A change
from the previous mode publishes a `MarginModeChangedEvent`.

You can also call it directly at runtime:

```python
from unified_trading_execution.bybit import MarginMode

engine.set_margin_mode("isolated")       # string form
engine.set_margin_mode(MarginMode.CROSS)  # enum form
```

## Error handling

Every Bybit-specific error is translated into the common exception hierarchy
before it crosses the adapter boundary. The engine (and your code) never
receives a raw Bybit error code.

### Mapped error codes

**Rate limiting** (`RateLimitError`):

`10006`, `10018`, `10429`, `20003`, `30035`, `170005`, `170222`

**Invalid symbol** (`InvalidSymbolError`):

`10029`, `110050`, `170121`, `170221`

**Insufficient balance** (`InsufficientBalanceError`):

`110004`, `110006`, `110007`, `110012`, `110044`, `110045`, `110051`,
`110052`, `110053`, `110131`, `30256`, `170033`, `170131`

**Order not found** (`OrderNotFoundError`):

`110001`, `170143`, `170213`

**Connection / retryable** (`PlatformConnectionError`):

`10000`, `10016`, `10019`, `110079`, `110118`, `170001`, `170007`, `170032`,
`170146`, `170147`, `170191`, `170234`, `170310`, `3400214`

HTTP-layer errors are also mapped:

- `429` → `RateLimitError`
- `403` → `PlatformError`
- `5xx` → `PlatformConnectionError`
- `4xx` → `PlatformError`

Any Bybit error code not explicitly listed is mapped to `PlatformError` with
the raw `ret_code` attached so nothing is silently swallowed.

### Adapter-specific errors

These are raised by the adapter's own validation logic, before a request
reaches the platform:

| Error | When |
|---|---|
| `LeverageDriftError` | Platform leverage differs from stored intent and `on_drift` is `"notify"` or `"halt"` |
| `LeverageExceedsMaxError` | `set_leverage` value exceeds the platform's maximum for that instrument |
| `AsymmetricLeverageError` | `buy_leverage != sell_leverage` was requested (requires hedge mode) |
| `LeverageNotModifiedError` | Platform returned 110043 — leverage was already at the requested value (suppressed internally) |
| `MarginModeNotModifiedError` | Platform returned 110026 — margin mode was already at the requested value (suppressed internally) |
| `PositionModeNotModifiedError` | Platform returned 110025 — position mode was already at the requested value (suppressed internally) |

The three `*NotModifiedError` types are internal — the adapter catches them
and treats them as success. You should never see them in your own code.

## Events

The adapter publishes events on the shared `EventBus` so the engine's state
mirror, reconciliation, and your own subscribers can observe everything
without importing adapter code.

### Core events

| Event | When |
|---|---|
| `ConnectionStateEvent` | `connect()` / `disconnect()` / connection monitor detects a state change |
| `OrderPlacedEvent` | WebSocket `order` stream reports a new order |
| `OrderCancelledEvent` | A previously-seen order reaches a terminal cancelled state |
| `FillEvent` | WebSocket `execution` stream reports a trade |
| `PositionUpdateEvent` | WebSocket `position` stream reports a position change |
| `BalanceUpdateEvent` | WebSocket `wallet` stream reports a balance change |
| `HaltEnteredEvent` | Leverage or position mode drift triggers an instrument halt |
| `HaltClearedEvent` | Reconciliation confirms the platform matches stored intent and clears the halt |

### Bybit-specific events

| Event | When |
|---|---|
| `LeverageAppliedEvent` | Stored leverage intent was successfully applied on connect |
| `LeverageApplyFailedEvent` | Stored leverage intent could not be applied on connect |
| `LeverageDriftEvent` | Platform leverage differs from stored intent (carries both values and the action taken) |
| `MarginModeChangedEvent` | Margin mode was changed on the platform |
| `PositionModeAppliedEvent` | Stored position mode was successfully applied on connect |
| `PositionModeApplyFailedEvent` | Stored position mode could not be applied on connect |
| `PositionModeDriftEvent` | Platform position mode differs from stored intent |

### Subscribing to events

```python
from unified_trading_execution.bybit.events import LeverageDriftEvent

def on_drift(event: LeverageDriftEvent) -> None:
    print(
        f"Leverage drift on {event.instrument.symbol}: "
        f"stored={event.stored_buy}x, "
        f"platform={event.platform_buy}x, "
        f"action={event.action_taken}"
    )

engine.event_bus.subscribe(LeverageDriftEvent, on_drift)
```

## Reconciliation data

The adapter implements all four optional data-fetching methods so the engine
can reconcile its state mirror against the platform:

```python
# Async
positions = await adapter.fetch_positions()
balances = await adapter.fetch_balances()
orders = await adapter.fetch_open_orders()
fills = await adapter.fetch_fills()

# Sync — identical API on the engine
positions = engine.fetch_positions()
balances = engine.fetch_balances()
orders = engine.fetch_open_orders()
fills = engine.fetch_fills()
```

- **Positions** cover linear (USDT + USDC settle coins) and inverse. Spot
  holdings are excluded — they have no position concept on Bybit.
- **Balances** use the `UNIFIED` account type and return one `Balance` per
  coin. The `used` field sums `totalOrderIM`, `totalPositionIM`, `locked`,
  and `bonus` — matching Bybit's own available-balance derivation.
- **Open orders** cover all three categories (spot, linear, inverse) and key
  by `orderLinkId` (client order ID) where available, falling back to
  platform order ID for orders placed outside the engine.
- **Fills** filter to `execType=Trade` only — matching the WebSocket
  execution stream so REST snapshots and live updates are strictly
  comparable.

## Platform-specific limitations

- **No `DAY` time-in-force.** Bybit supports `GTC`, `IOC`, and `FOK` only.
  `TimeInForce.DAY` raises `UnsupportedOrderTypeError`.
- **Stop orders are derivatives-only.** `STOP` and `STOP_LIMIT` are not
  available for spot.
- **`reduce_only` is derivatives-only.** Spot does not have a reduce-only
  concept.
- **TP/SL on spot is LIMIT-only.** Attaching `take_profit` or `stop_loss`
  to a spot `MARKET` order raises `UnsupportedOrderTypeError`.
- **`reduce_only` and TP/SL are mutually exclusive.** You cannot combine
  them in a single order on Bybit.
- **Leverage is derivatives-only.** `set_leverage` on a spot instrument
  raises `InvalidSymbolError`.
- **Asymmetric leverage requires hedge mode.** `buy_leverage != sell_leverage`
  raises `AsymmetricLeverageError`. Hedge mode is not currently supported.
- **Market orders are always IOC.** Bybit executes market orders as
  immediate-or-cancel. The `timeInForce` field is omitted from the payload
  for `MARKET` and `STOP` order types.
- **Spot market orders use `marketUnit=baseCoin`.** Quantity is always
  denominated in the base asset, matching the engine's quantity convention.

## Working with multiple adapters

One engine instance manages exactly one adapter. To trade on Bybit and another
platform simultaneously, create two engine instances:

```python
# Async
bybit_engine = Engine(BybitAdapter(BybitConfig(
    api_key="...", api_secret="...", testnet=True,
)))
await bybit_engine.connect()

# ctrade_engine = Engine(CTraderAdapter(...))
# await ctrade_engine.connect()

# Sync
bybit_engine = SyncEngine(BybitAdapter(BybitConfig(
    api_key="...", api_secret="...", testnet=True,
)))
bybit_engine.connect()

# ctrade_engine = SyncEngine(CTraderAdapter(...))
# ctrade_engine.connect()
```

Each engine's state store is naturally scoped to one platform and one account.

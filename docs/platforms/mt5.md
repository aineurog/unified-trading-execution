# MetaTrader 5 Adapter

The MetaTrader 5 (MT5) adapter trades forex, CFDs, stocks, futures, metals,
crypto, bonds, and funds through a locally running MT5 terminal. It implements
the `Adapter` ABC, translating every platform detail — local IPC, polling,
symbol suffixes, error codes, server-time offsets — into the engine's canonical
types so your trading logic never needs to know it is talking to MT5.

This guide is written for **all levels**. Read the
[Quickstart](#quickstart) first if you are new; the later sections build up to
the full feature surface. Every example is shown in **both async and sync**
form, so pick the style that fits your codebase.

---

## Platform realities (read this first)

These are hard constraints of the `MetaTrader5` Python package, not design
choices:

| Reality | What it means for you |
|---|---|
| **Windows-only** | The `MetaTrader5` package ships Windows wheels only. The adapter imports it lazily so the library still installs and imports on Linux/macOS for development, but a live connection only works on Windows with a terminal installed. |
| **One terminal per process** | `mt5.initialize()` / `mt5.shutdown()` are process-wide singletons. Only **one** `MT5Adapter` may be connected per Python process. Multiple accounts need multiple terminal installations, each addressed by `MT5Config.path`. |
| **Polling, not push** | MT5 has no WebSocket or streaming API. State is discovered by polling `orders_get` / `positions_get` / `history_deals_get` / `account_info`. The adapter runs its own polling loop. |
| **Local IPC, no REST** | All calls are local round-trips to a running terminal on the same machine. There is no API key/secret — authentication is `login` (account number) + `password` + `server` (broker name). |
| **No testnet** | MT5 has no sandbox. Demo vs live is purely which credentials you pass. Test against a broker **demo** account. |

---

## Installation

```bash
# On Windows, with a MetaTrader 5 terminal installed:
pip install unified-trading-execution-metatrader[mt5]
```

The `[mt5]` extra installs the `MetaTrader5` package. Without it the adapter
is importable but raises a clear `ImportError` at `connect()`.

---

## Quickstart

One import, one object. `MT5Engine` (async) and `SyncMT5Engine` (sync)
combine the engine and adapter into a single class — order lifecycle,
positions, reconciliation, and history are all on the one object.

> **`platform_symbol` is mandatory.** MT5 broker symbols have non-standard
> suffixes (`EURUSD.m`, `EURUSDpro`, `EURUSD+`). You must tell the adapter the
> exact broker symbol via `Instrument.platform_symbol`; the adapter never
> guesses it. See [Instruments & `platform_symbol`](#instruments--platform_symbol).

<table>
<tr><th>Async</th><th>Sync</th></tr>
<tr>
<td>

```python
import asyncio

from unified_trading_execution.mt5 import MT5Engine, MT5Config
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

async def main():
    engine = MT5Engine(MT5Config(
        login=12345678,
        password="your-password",
        server="ICMarkets-Demo",
    ))
    await engine.connect()

    eurusd = Instrument(
        symbol="EUR",
        quote_currency="USD",
        asset_class=AssetClass.MARGIN_FX,
        platform_symbol="EURUSD.m",   # broker's exact symbol
    )

    result = await engine.place_order(UnifiedOrder(
        instrument=eurusd,
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
from unified_trading_execution.mt5 import SyncMT5Engine, MT5Config
from unified_trading_execution.types.enums import (
    AssetClass, OrderSide, OrderType, TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder

engine = SyncMT5Engine(MT5Config(
    login=12345678,
    password="your-password",
    server="ICMarkets-Demo",
))
engine.connect()

eurusd = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol="EURUSD.m",
)

result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
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

The async and sync APIs are **identical** — the only difference is `await`
and `ashutdown()` vs `shutdown()`. Everything below applies to both.

Numeric inputs (`quantity`, `price`, `stop_price`) accept plain numbers or
strings:

```python
UnifiedOrder(..., quantity=0.01)        # float
UnifiedOrder(..., price="1.10500")      # str
UnifiedOrder(..., stop_price=1.10000)   # float
```

---

## Configuration

All configuration goes through an immutable `MT5Config` dataclass:

```python
from unified_trading_execution.mt5 import MT5Config

config = MT5Config(
    login=12345678,
    password="your-password",
    server="ICMarkets-Demo",
    path=None,                 # terminal exe path; None = auto-detect
    poll_interval_seconds=0.5,
    instrument_spec_cache_ttl=86400.0,
    asset_class_path_map=None, # broker-specific asset-class vocabulary
)
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `login` | `int` | *(required)* | MT5 account number |
| `password` | `str` | *(required)* | Account password |
| `server` | `str` | *(required)* | Broker server name (e.g. `"ICMarkets-Demo"`) |
| `path` | `str \| None` | `None` | Terminal executable path. `None` auto-detects. Required when multiple terminals are installed. |
| `poll_interval_seconds` | `float` | `0.5` | Seconds between poll cycles |
| `instrument_spec_cache_ttl` | `float \| None` | `86400.0` | Seconds a cached `InstrumentSpec` is trusted before re-fetch. `None` caches indefinitely (relies on invalidation only) |
| `asset_class_path_map` | `Mapping[str, AssetClass] \| None` | `None` | Extra broker market-folder → `AssetClass` entries (see [Asset-class resolution](#asset-class-resolution)) |

---

## Connection lifecycle

### `connect()`

```python
# Async / Sync
await engine.connect()   # or: engine.connect()
```

`connect()` does, in order:

1. Acquires the process-global guard (fails with `PlatformConnectionError` if
   another MT5 adapter is already connected in this process).
2. `mt5.initialize(...)` via a worker thread.
3. Resolves the real account login from `account_info()` (the terminal may be
   logged into a different account than `config.login`).
4. Seeds the `platform_symbol → Instrument` and `client_order_id → ticket`
   maps from the state store, then cross-checks against `U:` order comments so
   pre-restart orders remain manageable.
5. Publishes `ConnectionStateEvent(connected=True)` and starts the polling loop.

### `disconnect()`

```python
await engine.disconnect()  # or: engine.disconnect()
```

Cancels the polling loop, calls `mt5.shutdown()`, releases the process-global
guard, and publishes `ConnectionStateEvent(connected=False)`. Idempotent.

### `shutdown()`

```python
# Async
await engine.ashutdown()

# Sync
engine.shutdown()
```

Ordered teardown: flushes audit writes, disconnects the adapter, closes the
state store, and (sync) stops the background event loop. After shutdown the
engine is permanently unusable.

### The state store

The engine persists its state mirror to a SQLite database on disk so it
survives restarts. The default path is:

```
./unified_trading_execution_data/metatrader_<login>.db
```

You can override it with `state_store=SQLiteStateStore("my_path.db")` when
constructing the engine.

---

## Instruments & `platform_symbol`

MT5 broker symbols are not standardized. The same EUR/USD pair appears as
`EURUSD.m` on IC Markets, `EURUSDpro` on Pepperstone, `EURUSD` elsewhere. The
adapter therefore **never derives** a broker symbol from
`symbol`/`quote_currency` — you supply it:

```python
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

# canonical identity + the broker's exact symbol string
eurusd = Instrument(
    symbol="EUR",
    quote_currency="USD",
    asset_class=AssetClass.MARGIN_FX,
    platform_symbol="EURUSD.m",
)
```

Missing `platform_symbol` raises `ValueError` on the first order/fetch.

You can also be deliberately imprecise about `symbol` / `quote_currency` /
`asset_class` — the adapter reconstructs the true identity from the broker's
own `symbol_info()` metadata and corrects your instrument (with a logged
warning). See [Instrument-identity correction](#instrument-identity-correction).

---

## Order operations

Orders go through the engine's risk-check pipeline (symbol validity, quantity
bounds, price sanity, duplicate detection, rate limiting) before reaching the
platform. Use `engine.place_order()`, never `adapter.place_order()`.

### Supported order types

| Unified type | MT5 order type (buy / sell) | Required fields |
|---|---|---|
| `MARKET` | `ORDER_TYPE_BUY` / `ORDER_TYPE_SELL` | `quantity` |
| `LIMIT` | `ORDER_TYPE_BUY_LIMIT` / `ORDER_TYPE_SELL_LIMIT` | `quantity`, `price` |
| `STOP` | `ORDER_TYPE_BUY_STOP` / `ORDER_TYPE_SELL_STOP` | `quantity`, `stop_price` |
| `STOP_LIMIT` | `ORDER_TYPE_BUY_STOP_LIMIT` / `ORDER_TYPE_SELL_STOP_LIMIT` | `quantity`, `price`, `stop_price` |

### Placing orders

```python
# Market buy — the adapter fetches the live ask and sends the deal at that price
result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY,
    quantity=0.01,
    time_in_force=TimeInForce.GTC,
))

# Limit sell
result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.LIMIT,
    side=OrderSide.SELL,
    quantity=0.01,
    price=1.12000,
    time_in_force=TimeInForce.GTC,
))

# Stop (conditional market) — buy stop above the market
result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.STOP,
    side=OrderSide.BUY,
    quantity=0.01,
    stop_price=1.12500,
    time_in_force=TimeInForce.GTC,
))

# Stop-limit — trigger then limit
result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.STOP_LIMIT,
    side=OrderSide.BUY,
    quantity=0.01,
    price=1.12600,       # limit price
    stop_price=1.12500,  # trigger
    time_in_force=TimeInForce.GTC,
))
```

The engine generates a UUID7 `client_order_id` if you do not supply one.

### Time in force

| `TimeInForce` | MT5 behaviour |
|---|---|
| `GTC` | Good-til-cancelled |
| `IOC` | Immediate-or-cancel (via filling mode) |
| `FOK` | Fill-or-kill (via filling mode) |
| `DAY` | `ORDER_TIME_DAY` — good until the broker's end-of-day cutoff |
| `GTD` | `ORDER_TIME_SPECIFIED` — requires `expire_at` (UTC, timezone-aware) |

```python
from datetime import datetime, timedelta, UTC

result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.LIMIT,
    side=OrderSide.BUY,
    quantity=0.01,
    price=1.10500,
    time_in_force=TimeInForce.GTD,
    expire_at=datetime.now(UTC) + timedelta(days=1),
))
```

The exact filling mode (FOK/IOC/Return) is resolved **per symbol** from
`symbol_info().filling_mode`, falling back through the supported modes. If no
compatible mode exists, `InvalidSymbolError` is raised.

### Take-profit and stop-loss

MT5 TP/SL are native **price levels** on the order (the `sl`/`tp` fields), not
separate orders:

```python
from unified_trading_execution.types.order import TpSlAttachment

result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.LIMIT,
    side=OrderSide.BUY,
    quantity=0.01,
    price=1.10500,
    time_in_force=TimeInForce.GTC,
    take_profit=TpSlAttachment(trigger_price=1.13000),
    stop_loss=TpSlAttachment(trigger_price=1.09500),
))
```

**Important:** MT5 TP/SL have no limit-price concept. Setting
`TpSlAttachment.limit_price` raises `UnsupportedOrderTypeError` — the trigger
price *is* the execution price.

### Closing a specific position leg (`position_id`)

On a **hedging** account you can hold multiple independent positions on the
same instrument. To close a *specific* leg rather than reducing net exposure,
set `position_id` to the MT5 position ticket:

```python
result = engine.place_order(UnifiedOrder(
    instrument=eurusd,
    order_type=OrderType.MARKET,
    side=OrderSide.SELL,     # opposing side
    quantity=0.01,
    time_in_force=TimeInForce.GTC,
    position_id="12345678",  # the position ticket to close
))
```

`position_id` maps to MT5's `position` field (`order_send(position=...)`).
On netting accounts (one net position per instrument) you simply place an
opposing order.

### `reduce_only`

MT5 has no reduce-only flag, so `UnifiedOrder.reduce_only` is ignored. Close or
reduce a position with an opposing order, targeting a specific leg via
`position_id` when needed.

### Modifying an order

```python
from unified_trading_execution.types.order import OrderModification

updated = engine.modify_order(OrderModification(
    client_order_id=result.client_order_id,
    price=1.10600,                       # change the limit price
    take_profit=TpSlAttachment(trigger_price=1.13100),
))
```

You can change `price`, `stop_price`, `take_profit`, and `stop_loss`. You
**cannot** change `quantity` — MT5 requires cancel-and-re-place, so
`modify_order(quantity=...)` raises `UnsupportedOrderTypeError`.

### Cancelling an order

```python
cancelled = engine.cancel_order(result.client_order_id)
```

Raises `OrderNotFoundError` if the engine has no ticket mapping for that
`client_order_id` (i.e. it was not placed by this engine).

### Querying an order

```python
order = engine.get_order("your-client-order-id")
if order is None:
    print("Order not found or no longer active")
else:
    print(f"Status: {order.status.value}, filled: {order.filled_quantity}")
```

`get_order` looks up the ticket and queries `orders_get(ticket=...)`. It
returns `None` for unknown IDs and for orders that have left the active set
(filled/cancelled/expired).

---

## Positions & account

### Legs vs net exposure

MT5 positions are fetched as **legs**. On a hedging account a single instrument
can have several legs; each leg carries its MT5 ticket as `position_id`:

```python
# Raw legs (one Position per terminal leg)
positions = engine.fetch_positions()          # list[Position]
for p in positions:
    print(p.instrument.symbol, p.quantity, p.position_id)

# Or from the state mirror, scoped to one instrument:
legs = engine.get_positions(eurusd)           # list[Position]

# Derived signed net for one instrument (None when flat)
net = engine.get_net_position(eurusd)         # Position | None
```

| Method | Returns | Meaning |
|---|---|---|
| `fetch_positions()` | `list[Position]` | Live snapshot from the terminal — one entry per leg |
| `get_positions(instrument)` | `list[Position]` | The persisted mirror's legs for one instrument |
| `get_net_position(instrument)` | `Position \| None` | Signed sum of legs (`SUM(quantity)`), cost-basis price; `None` when net zero |

`quantity` is signed: a BUY leg is `+volume`, a SELL leg is `-volume`. The
mirror never stores a synthetic flat position — a leg that disappears from
`positions_get()` is published as a zero-quantity close signal carrying its
ticket, and the mirror deletes the row.

### Balances

```python
balances = engine.fetch_balances()            # dict[str, Balance]
print(balances["USD"].free, balances["USD"].used, balances["USD"].total)

balance = engine.get_balance("USD")           # Balance | None (from mirror)
```

### Account leverage (read-only)

MT5 leverage is account-level, set by the broker back-office, and cannot be
changed through any API. Read it:

```python
leverage = engine.fetch_account_leverage()    # int, e.g. 500  (1:500)
```

There is deliberately no `set_leverage` for MT5 (the ABC has none, and MT5 has
no such call).

### Modifying TP/SL on an open position

```python
engine.modify_position_tpsl(
    position_id="12345678",                  # MT5 position ticket
    take_profit=TpSlAttachment(trigger_price=1.13000),
    stop_loss=TpSlAttachment(trigger_price=1.09500),
)
```

At least one of `take_profit` / `stop_loss` is required. This uses MT5's
`TRADE_ACTION_SLTP`. `limit_price` on either attachment raises
`UnsupportedOrderTypeError`.

---

## Instrument metadata

```python
spec = engine.fetch_instrument_spec(eurusd)
# InstrumentSpec(
#     tick_size=0.00001,
#     lot_size=0.01,
#     min_qty=0.01,
#     max_qty=100.0,
#     min_notional=0,          # MT5 has no broker-enforced notional floor
#     price_precision=5,
#     qty_precision=2,
#     max_leverage=None,       # always None for MT5 (account-level leverage)
# )
```

| MT5 field | `InstrumentSpec` field |
|---|---|
| `trade_tick_size` | `tick_size` |
| `volume_step` | `lot_size` |
| `volume_min` | `min_qty` |
| `volume_max` | `max_qty` |
| `digits` | `price_precision` |
| volume step decimals | `qty_precision` |
| — | `min_notional = 0` (MT5 has no broker-enforced floor) |
| — | `max_leverage = None` (leverage is account-level) |

`min_notional` is `0`, so the risk chain's notional check effectively skips
MT5 instruments. Specs are cached with the configured TTL and invalidated on
order rejection (invalid symbol / market closed) or a `symbol_info()` miss.

### Discovering / verifying a symbol

```python
instrument = engine.resolve_instrument("EURUSD.m")
# -> Instrument(symbol="EUR", quote_currency="USD",
#               asset_class=MARGIN_FX, platform_symbol="EURUSD.m")
```

Useful for resolving (or checking) an instrument's canonical identity without
placing an order.

---

## Reconciliation

The engine reconciles its state mirror against the terminal, keyed **per leg**
by `(instrument, position_id)`:

```python
result = engine.reconcile()
```

```python
# Raw platform snapshots (also used by reconciliation internally)
positions = engine.fetch_positions()          # list[Position] legs
balances  = engine.fetch_balances()           # dict[str, Balance]
orders    = engine.fetch_open_orders()        # dict[str, OrderRecord]
fills     = engine.fetch_fills()              # dict[str, list[FillRecord]]

# Reconciliation with an explicit lower bound (UTC) — symmetric with the
# engine's own watermark-bounded local fill query:
fills = engine.fetch_fills(since=some_datetime)
```

- `fetch_open_orders` keys by `client_order_id`, falling back to the platform
  ticket for orders placed outside the engine.
- `fetch_fills` returns only trading deals (`DEAL_TYPE_BUY`/`SELL`);
  balance in/out operations are excluded, grouped by `client_order_id`.

---

## History accessors

All history is read from the persisted state mirror (identical async/sync):

```python
# Orders
history = engine.get_order_history(instrument=eurusd)

# Fills (optionally filtered to a specific leg)
fills = engine.get_fill_history(instrument=eurusd, position_id="12345678")

# Balances, reconciliation, halts, audit
engine.get_balance_history(currency="USD")
engine.get_reconciliation_events()
engine.get_halt_events()
engine.get_audit_events()
```

Each accepts optional `start` / `end` UTC bounds.

---

## Events

The adapter publishes translated events on the engine's `EventBus`:

| Event | When |
|---|---|
| `ConnectionStateEvent` | `connect()` / `disconnect()` |
| `FillEvent` | Polling loop detects a new trading deal |
| `PositionUpdateEvent` | A leg's quantity or entry price changes, or a leg closes |
| `BalanceUpdateEvent` | Account balance changes |

```python
from unified_trading_execution.events import PositionUpdateEvent

def on_position(event: PositionUpdateEvent) -> None:
    print(event.position.instrument.symbol, event.position.quantity)

engine.event_bus.subscribe(PositionUpdateEvent, on_position)
```

---

## Error handling

Every MT5 error is translated into the common exception hierarchy before it
crosses the adapter boundary. You never receive a raw MT5 code.

| Unified exception | MT5 codes / causes |
|---|---|
| `PlatformConnectionError` (retryable) | 10004 requote, 10007 server-cancel, 10012 timeout, 10020 price-changed, 10021 no quotes, 10028 locked, 10031 no connection, 10026/10027 auto-trading disabled, `RES_E_AUTH_FAILED`, `RES_E_AUTO_TRADING_DISABLED`, IPC failures |
| `InvalidSymbolError` | 10013 invalid request, 10014 invalid volume, 10015 invalid price, 10016 invalid stops, 10018 market closed, 10022 invalid expiration, 10034 volume limit, 4301/5040 unknown symbol |
| `InsufficientBalanceError` | 10019 no money |
| `InstrumentHaltedError` | 10017 trading disabled, 10029 frozen |
| `RateLimitError` | 10024 too many requests, 10033 limit orders |
| `OrderNotFoundError` | 10035 invalid order |
| `PlatformError` (generic) | 10006 reject, 10011 error, 10023 order-changed, 10030 invalid fill, 10032 only-real, and any unmapped code |

Unmapped codes become `PlatformError` with `mt5_error_code` /
`mt5_description` attached, so nothing is silently swallowed.

---

## Platform-specific limitations

- **Windows-only** — a live connection requires a Windows machine with the
  terminal installed.
- **One adapter per process** — `mt5.initialize()` is a process singleton.
- **No quantity modification** — `modify_order(quantity=...)` raises
  `UnsupportedOrderTypeError`; cancel and re-place instead.
- **No stop-limit TP/SL** — `TpSlAttachment.limit_price` raises
  `UnsupportedOrderTypeError` (TP/SL are price levels).
- **`reduce_only` is ignored** — MT5 has no reduce-only flag.
- **No testnet** — test on a broker demo account.
- **No push updates** — state arrives by polling (default 500 ms).
- **No per-symbol leverage** — `InstrumentSpec.max_leverage` is always `None`.
- **`min_notional` is `0`** — the notional risk check is skipped for MT5.
- **`get_order` can't read `average_fill_price`** for open orders (MT5's
  `orders_get()` doesn't expose it); the field is `None` there.

---

## Advanced usage

### Asset-class resolution

MT5 never exposes a clean "asset class". The adapter derives the canonical
`AssetClass` for every symbol from a **layered, broker-agnostic classifier**:

1. **Precious-metal base currency** — a base of `XAU`/`XAG`/`XPT`/`XPD`
   resolves to `MARGIN_FX` before anything else.
2. **Market-tree path thesaurus** — any segment of `symbol_info().path`
   (case-insensitive) matched against a built-in table (`FOREX` → `MARGIN_FX`,
   `STOCKS` → `STOCK`, `INDICES`/`COMMODITIES`/`ENERGY` → `CFD`, …). Scanning
   *all* segments means an account-group root (e.g. Oanda's `PRO`) can't hide
   the meaningful folder beneath it.
3. **`trade_calc_mode` fallback** — MT5's `ENUM_SYMBOL_CALC_MODE`, used only
   when neither layer above resolves.

A symbol none of the three layers recognise raises `ValueError` — the adapter
never guesses, because a wrong asset class would silently corrupt the DB.
Brokers with non-standard folder names are accommodated with the config escape
hatch:

```python
from unified_trading_execution.types.enums import AssetClass

config = MT5Config(
    ...,
    asset_class_path_map={
        "PreciousMetals": AssetClass.MARGIN_FX,   # a broker-specific folder
    },
)
```

### Instrument-identity correction

When you place an order, the adapter reads the broker's `symbol_info()` and
reconstructs the canonical identity (`symbol`, `quote_currency`, `asset_class`)
from `currency_base`/`currency_profit`/`path`. If those differ from the
`Instrument` you supplied, it **corrects the instrument** (with a logged
warning) and caches the mapping, so the inbound polling path and your own
`Instrument` stay consistent. This means you can supply a minimal instrument
and let the broker fill in the rest — but a broker-missing `platform_symbol`
always fails fast.

### `client_order_id` in order comments

MT5 order comments are capped at 29 characters, so a 36-char UUID7
`client_order_id` can't be stored raw. The adapter packs canonical
lowercase UUIDs into a 24-char `U:<base62>` comment (`comments.py`) that
travels atomically with the order and survives in MT5 history. On
`connect()` it recovers the `client_order_id → ticket` maps by:

1. seeding from the state store (authoritative), then
2. scanning open orders and recent deals for `U:` comments.

This lets a restarted engine keep managing orders placed before the restart.
Non-canonical `client_order_id`s (custom strings, upper-case UUIDs) can't be
packed — those rely on the in-memory ticket maps alone.

### Server-time offset & deal dedup

MT5 stamps `deal.time` in the server's timezone as if it were a Unix epoch.
The adapter measures the broker's offset live from a tick (`time_msc` vs local
clock), re-measures it each poll cycle (DST-safe), and shifts deal windows and
timestamps accordingly so `fill_timestamp` comes out in real UTC. Fill
dedup uses the monotonic deal ticket (second-granular timestamps alone would
drop or duplicate same-second deals).

### Rate limits

MT5 has no rate-limit endpoint. The adapter reports a conservative estimate
(`1 request/sec`) rather than a real budget:

```python
limits = engine.get_rate_limits()
```

### Working with a standalone adapter

`MT5Engine` / `SyncMT5Engine` are the recommended path. If you need the
two-object pattern — a shared event bus/store across engines, or direct access
to raw adapter methods — use `Engine`/`SyncEngine` with `MT5Adapter`:

```python
from unified_trading_execution import Engine
from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.state.store import SQLiteStateStore

bus = EventBus()
store = SQLiteStateStore("my_mt5.db")
adapter = MT5Adapter(MT5Config(login=..., password=..., server=...))

engine = Engine(adapter, state_store=store, event_bus=bus)
await engine.connect()
await engine.ashutdown()
```

The adapter's platform-specific methods — `fetch_account_leverage`,
`resolve_instrument`, `fetch_positions`, `fetch_balances`, `fetch_open_orders`,
`fetch_fills`, `modify_position_tpsl` — are reachable on both engine classes
via attribute auto-proxy, so you never need to drop down to the adapter for
them.

---

## Multiple accounts / terminals

One process can only control one terminal at a time. To trade multiple
accounts:

- Run each account in its **own process** (or point each at a separate
  terminal installation via `MT5Config.path`), or
- Run them **sequentially** in one process — `connect()`, trade,
  `disconnect()`, then connect the next.

The state store is naturally scoped per `(platform, account)`, so separate
accounts keep separate mirrors.

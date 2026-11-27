# unified-trading-execution-binance

Partially implemented Binance adapter for
[unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
Product and instrument metadata translation, a limited REST rate-limit
observation path, the async REST client lifecycle, and core order operations
are implemented, along with private account streams and reconciliation
snapshots and market data reads. Position-level TP/SL protection is implemented
for USDⓈ-M and COIN-M Futures, including market-on-trigger and limit-trigger
attachments. Native USDⓈ-M and COIN-M trailing stops are implemented via
`TRAILING_STOP_MARKET` algo orders; Spot native trailing remains
unsupported, so `TrailingMode.AUTO` falls back to synthetic
position protection there. Futures leverage,
margin mode, and position mode are managed as persisted adapter intent.

## Installation

```bash
pip install "unified-trading-execution[binance]"
```

For a source checkout, the adapter code lives under `packages/core/src`:

```bash
pip install -e packages/core
```

The adapter pins `python-binance==1.0.37`. `connect` creates the SDK
client for the configured environment and verifies that all three REST endpoint
families resolve to it before retaining the client. `fetch_instrument_spec`
requires that connected client. Unit fixtures never reach the network; the
integration fixtures target only their explicitly selected environment.

## Public package

```python
from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    BinanceEngine,
    SyncBinanceEngine,
)

config = BinanceConfig(
    api_key="your-api-key",
    api_secret="your-api-secret",
    testnet=True,
)
engine = BinanceEngine(config)
```

`connect` creates the REST client and starts private account streams for Spot,
USDⓈ-M, and COIN-M before publishing a connected state. Pass `testnet=True` or
`demo=True` to select a sandbox; both default to production. Operations not yet
implemented raise `NotImplementedError`.

### Products, symbols, and specs

Binance publishes separate Spot, USDⓈ-M Futures, and COIN-M Futures catalogs.
`BinanceProduct` (exported from the package) identifies the family, and
`product_for_instrument` resolves it from explicit canonical fields: Spot uses
`AssetClass.SPOT`; USDⓈ-M futures set `currency` to the quote asset; COIN-M
futures use a USD quote, their margin asset in `currency`, and the base asset
as `symbol`.

`to_binance_symbol` and `from_binance_symbol` translate between a canonical
`Instrument` and one product's exchange-info row. A supplied
`platform_symbol` is authoritative; otherwise resolution requires exactly one
matching row. Perpetual and TradFi perpetual contracts map without an expiry;
dated contracts derive it from `deliveryDate`. `parse_instrument_spec` maps
PRICE_FILTER, LOT_SIZE, and the notional filters into exact `Decimal` values.
COIN-M instruments carry the venue `contractSize` as their `multiplier`.

Specs are cached per instrument and re-parsed after
`instrument_spec_cache_ttl` seconds (default one day); `None` caches
indefinitely. `max_leverage` is populated from the signed leverage brackets
when the authenticated client is available.

### REST rate-limit observations

The adapter includes a rate-limit tracker (`BinanceRateLimitTracker`) that
observes rate-limit headers from exchange-info requests. Binance applies
multiple independent limits that weight requests per endpoint, while the core
exposes only one shared budget. Therefore, this snapshot is approximate—not
complete request throttling—and observes requests routed through the adapter.
Buckets with no observed counter are left out of the snapshot rather than
guessed at.

### Error translation

The Binance error mapper translates SDK API, malformed response, timeout, and
transport errors into the core error hierarchy while retaining safe context
for unknown API responses. Metadata, order, and reconciliation REST requests
use the adapter's error and rate-limit handling.

Binance exposes both Testnet and Demo Trading API environments. `testnet=True`
selects Testnet; `demo=True` selects Demo Trading. These flags are mutually
exclusive. When both are false, the SDK selects production (live) endpoints.
Both default to `False`, so set the desired non-live environment explicitly.

`BinanceConfig` redacts credentials from its representation. Its optional
`account_id` can be used as a local account label; when omitted, the adapter
derives a stable, non-secret label from the API key.

### Order operations

The adapter supports Spot and Futures MARKET/LIMIT orders, Spot STOP/STOP_LIMIT,
and USDⓈ-M/COIN-M STOP/STOP_LIMIT via the Futures algo-order endpoints. Spot
order modification and conditional-order modification are unsupported.
Order-level ``take_profit``/``stop_loss``
attachments are supported only on Futures MARKET entries: the adapter waits
for the synchronous fill, resolves the resulting position leg, and attaches
MARK-basis position protection, failing loudly if the fill is not synchronous
or no leg remains. Spot, LIMIT/STOP, and ``reduce_only`` attachment
combinations are rejected. Position-level
TP/SL is supported separately on both Futures products. Protection
`trigger_price`/`limit_price` values must align to the symbol's `tickSize`;
`modify_position_tpsl` rejects unaligned prices with `InvalidOrderError`
before any venue call. USDⓈ-M and COIN-M
ordinary LIMIT orders can be modified by price and/or quantity.
Position TP/SL on both Futures products uses conditional algo orders. COIN-M
algo requests use python-binance's signed request transport because the library
has no public COIN-M algo-order wrappers. Changing a COIN-M protection side
cancels and replaces that side; the unmentioned side remains untouched.
Existing COIN-M regular-order protection is still readable and is cancelled
when that side is replaced or cleared; new protection is never placed through
the regular order endpoint.
Order routing references are restored from persisted orders and refreshed from
live open-order snapshots.
Valid client order IDs are sent to Binance unchanged; IDs outside Binance's
length or character constraints are rejected instead of being rewritten.

In Futures Hedge Mode, every order must target a leg with `position_id`, for
example `usd_m:BTCUSDT:LONG` or `coin_m:BTCUSD_PERP:SHORT`. The adapter maps
that leg to Binance's `positionSide`; `side` remains the order direction.
For reduce-only hedge closes, a LONG leg must use `SELL` and a SHORT leg must
use `BUY`. The adapter validates that direction and omits Binance's
`reduceOnly` parameter for LONG/SHORT legs; one-way (`BOTH`) orders retain the
parameter. When Binance reports a position-side mismatch for an order without
`position_id`, the adapter raises an actionable error explaining how to target
a hedge leg.

When a filled placement response omits its average fill price, the adapter
queries the order by its Binance order ID and requires the query response to
provide the average.

### Account streams and reconciliation

The adapter translates Spot execution and balance updates, USDⓈ-M and COIN-M
order/position and algo-order updates into core events. REST
snapshots provide balances, derivative positions, open orders (including
USDⓈ-M and COIN-M algo orders), and account trades. Adapter-owned position
protection and trailing orders are excluded from the ordinary order mirror.
Foreign `TRAILING_STOP_MARKET` algo orders are represented as core `STOP`
orders because the portable order model has no trailing order type. Reconnects
use bounded backoff; final order and fill events are deduplicated in memory.

Balance keys are product-qualified (for example, `SPOT:USDT` and
`USD_M:USDT`) because Binance maintains separate product wallets while the core
balance store keys by currency. Fill-history requests are limited to symbols
known from stored orders, positions, open orders, or stream activity; fills on
unrelated external symbols cannot be discovered automatically.

Order-routing references are a session cache: startup restores live orders,
cache misses resolve through persisted `OrderRecord`s, and terminal or
disconnected order references are evicted. Historical order-to-platform-ID
mapping for fill snapshots is read from the existing state store rather than
retained for the lifetime of the adapter.

### Trailing stops

Native trailing is implemented for USDⓈ-M and COIN-M Futures, via
`TRAILING_STOP_MARKET` conditional algo orders (`/fapi/v1/algoOrder` and
`/dapi/v1/algoOrder`).
Supported requests: `TrailingOffsetType.PERCENT` offsets in Binance's
0.1%-10% `callbackRate` range, `LAST`/`MARK` working type, and `IMMEDIATE` or
`PRICE` activation. Orders are position-leg scoped (`symbol` +
`positionSide`), carry deterministic `trl-` client algo IDs, reduce-only,
and stay out of the order mirror. Placing over an existing trail cancels
and replaces it; foreign trails on the same leg are detected for takeover
protection. COIN-M algo orders have no public python-binance wrapper, so
the adapter calls the SDK's own signed `_request_futures_coin_api`
transport for the documented `/dapi/v1/algoOrder` endpoints (Binance's
unified UM/CM migration, notice C.5) — same signing, session, and
rate-limit handling as every `futures_coin_*` method. Spot reports
native trailing as unsupported, so `TrailingMode.AUTO` falls back to
synthetic position protection there.

### Futures configuration intent

The adapter manages leverage and `MarginMode` (`CROSS`, `ISOLATED`) per
Futures product and symbol, and `PositionMode` (`ONE_WAY`, `HEDGE`) account-wide.
It persists desired settings in the StateStore, reapplies them on connect by
default, and reconciles drift using `halt` (default), `notify`, or `reapply`
policies.

Changes are refused when the affected scope has open orders, open algo orders,
or non-flat positions. The adapter does not cancel
orders or close positions. Checks are serialized with its own order operations,
but external activity can race the Binance request. Removing a setting removes
local intent only.

## Tests

Unit tests cover product resolution, symbol conversion, metadata parsing,
error and order translation, account streams, reconciliation snapshots, order
lookup, market data reads, and Futures configuration
safety/persistence/recovery. They mock SDK
client and socket construction and never reach the network:

```bash
pytest packages/adapter-binance/tests/unit/
```

The metadata integration tests read public Demo exchange-info catalogs and
perform no authenticated or trading calls. They require an explicit
`BINANCE_DEMO=true` opt-in but no credentials:

```bash
BINANCE_DEMO=true pytest packages/adapter-binance/tests/binance_integration/
```

The REST URL smoke test sends read-only `ping` requests to Spot, USDⓈ-M, and
COIN-M endpoints in production, Testnet, and Demo Trading. It needs no API
credentials and requires the explicit `BINANCE_REST_URL_SMOKE=true` opt-in:

```bash
BINANCE_REST_URL_SMOKE=true pytest packages/adapter-binance/tests/binance_integration/test_rest_urls.py
```

Account-stream integration tests verify private stream readiness and REST
reconciliation snapshots on the selected Testnet or Demo sandbox. Their
order-event cases create and cancel one passive sandbox limit order per product;
they require explicit
`BINANCE_ACCOUNT_STREAM_ORDER_INTEGRATION=true` opt-in and dedicated sandbox
credentials:

```bash
BINANCE_TESTNET=true pytest packages/adapter-binance/tests/binance_integration/test_account_streams.py
BINANCE_TESTNET=true BINANCE_TESTNET_ORDER_INTEGRATION=true BINANCE_ACCOUNT_STREAM_ORDER_INTEGRATION=true pytest packages/adapter-binance/tests/binance_integration/test_account_streams.py
BINANCE_DEMO=true BINANCE_ACCOUNT_STREAM_ORDER_INTEGRATION=true pytest packages/adapter-binance/tests/binance_integration/test_account_streams.py
```

Opt-in Futures configuration integration tests change leverage and margin mode
on both Futures products, verify drift reconciliation and reconnect reapply,
then attempt to restore the original sandbox settings. They require sandbox
credentials and `BINANCE_FUTURES_CONFIG_INTEGRATION=true`:

```bash
BINANCE_TESTNET=true BINANCE_FUTURES_CONFIG_INTEGRATION=true pytest packages/adapter-binance/tests/binance_integration/test_futures_configuration.py
BINANCE_DEMO=true BINANCE_FUTURES_CONFIG_INTEGRATION=true pytest packages/adapter-binance/tests/binance_integration/test_futures_configuration.py
```

Order integration tests submit, query, cancel, and clean up the orders they
create, so they need dedicated sandbox credentials. They run against Demo
Trading by default with `BINANCE_DEMO_API_KEY` and `BINANCE_DEMO_API_SECRET`;
set `BINANCE_DEMO_ORDER_INTEGRATION=false` to skip them. Set
`BINANCE_TESTNET=true` and `BINANCE_TESTNET_ORDER_INTEGRATION=true` to run
them on Testnet instead, using `BINANCE_SPOT_TESTNET_API_KEY`,
`BINANCE_USDM_TESTNET_API_KEY`, and `BINANCE_COINM_TESTNET_API_KEY` with
their matching secrets.

The position-protection lifecycle test covers both USDⓈ-M `BTCUSDT` and COIN-M
`BTCUSD_PERP`: it opens a small sandbox position, attaches and reads back TP/SL,
reprices and clears each side, checks open-order reconciliation, and cleans up
the protection orders and position. The COIN-M case uses a minimum valid
contract quantity and a USD notional cap. Both cases require a clean symbol and
one-way position mode; do not run them concurrently with other trades on those
symbols. Run them with the same explicit Demo or Testnet order opt-in:

```bash
BINANCE_DEMO=true pytest packages/adapter-binance/tests/binance_integration/test_position_protection.py
BINANCE_TESTNET=true BINANCE_TESTNET_ORDER_INTEGRATION=true pytest packages/adapter-binance/tests/binance_integration/test_position_protection.py
```

Copy `.env.example` to `.env` only when preparing local sandbox work. Never
commit API credentials.

## License

Apache License 2.0 — see the repository root [LICENSE](../../LICENSE).

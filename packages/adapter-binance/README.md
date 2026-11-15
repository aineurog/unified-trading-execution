# unified-trading-execution-binance

Partially implemented Binance adapter for
[unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
Product and instrument metadata translation, a limited REST rate-limit
observation path, the async REST client lifecycle, and core order operations
are implemented, along with private account streams and reconciliation
snapshots. Market data, position protection, and trailing stops remain
unimplemented. Futures leverage, margin mode, and position mode are managed as
persisted adapter intent.

## Installation

From the monorepo root:

```bash
uv pip install -e packages/core
uv pip install -e packages/adapter-binance
```

The adapter package pins `python-binance==1.0.37`. `connect` creates the SDK
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
indefinitely. `max_leverage` is left `None` because exchange info does not
publish leverage; the authenticated client branch will populate it.

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
and USDⓈ-M STOP/STOP_LIMIT via the SDK's public algo-order route. COIN-M
conditional orders, Spot order modification, conditional-order modification,
and order-level TP/SL attachments are explicitly unsupported. USDⓈ-M and
COIN-M ordinary LIMIT orders can be modified by price and/or quantity.
Order routing references are restored from persisted orders and refreshed from
live open-order snapshots.
Valid client order IDs are sent to Binance unchanged; IDs outside Binance's
length or character constraints are rejected instead of being rewritten.

When a filled placement response omits its average fill price, the adapter
queries the order by its Binance order ID and requires the query response to
provide the average.

### Account streams and reconciliation

The adapter translates Spot execution and balance updates, USDⓈ-M and COIN-M
order/position updates, and USDⓈ-M algo-order updates into core events. REST
snapshots provide balances, derivative positions, open orders (including
USDⓈ-M algo orders), and account trades. Reconnects use bounded backoff; final
order and fill events are deduplicated in memory.

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

### Futures configuration intent

The adapter manages leverage and `MarginMode` (`CROSS`, `ISOLATED`) per
Futures product and symbol, and `PositionMode` (`ONE_WAY`, `HEDGE`) account-wide.
It persists desired settings in the StateStore, reapplies them on connect by
default, and reconciles drift using `halt` (default), `notify`, or `reapply`
policies.

Changes are refused when the affected scope has open orders or non-flat
positions; USDⓈ-M algo orders are checked too. The adapter does not cancel
orders or close positions. Checks are serialized with its own order operations,
but external activity can race the Binance request. Removing a setting removes
local intent only. 

## Tests

Unit tests cover product resolution, symbol conversion, metadata parsing,
error and order translation, account streams, reconciliation snapshots, order
lookup, and Futures configuration safety/persistence/recovery. They mock SDK
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

Copy `.env.example` to `.env` only when preparing local sandbox work. Never
commit API credentials.

## License

Apache License 2.0 — see the repository root [LICENSE](../../LICENSE).

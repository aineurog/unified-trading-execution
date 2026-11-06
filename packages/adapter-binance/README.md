# unified-trading-execution-binance

Partially implemented Binance adapter for
[unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
Product and instrument metadata translation, a limited REST rate-limit
observation path, and the async REST client lifecycle are implemented. Market
data, account streams, trailing stops, and trading operations have **not** been
implemented. Unimplemented methods raise `NotImplementedError`.

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

`connect` creates the REST client and publishes a connection state event.
Pass `testnet=True` or `demo=True` to select a sandbox; both default to
production. Other unimplemented adapter operations raise
`NotImplementedError`; complete their implementation branches before use.

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
complete request throttling—and currently covers metadata requests only.
Buckets with no observed counter are left out of the snapshot rather than
guessed at.

### Error translation

The Binance error mapper translates SDK API, malformed response, timeout, and
transport errors into the core error hierarchy while retaining safe context
for unknown API responses. It is currently used for exchange-info requests;
future adapter operations should use it around their Binance I/O calls as they
are implemented.

Binance exposes both Testnet and Demo Trading API environments. `testnet=True`
selects Testnet; `demo=True` selects Demo Trading. These flags are mutually
exclusive. When both are false, the SDK selects production (live) endpoints.
Both default to `False`, so set the desired non-live environment explicitly.

`BinanceConfig` redacts credentials from its representation. Its optional
`account_id` can be used as a local account label; when omitted, the scaffold
derives a stable, non-secret label from the API key. The exchange identity
resolution behavior will be reviewed during implementation.

## Tests

Unit tests cover product resolution, symbol conversion, exchange-info
resolution, filter parsing, error translation, and adapter lookup. They mock
SDK client and socket construction and never reach the network:

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

Authenticated Testnet tests require dedicated credentials and
`BINANCE_TESTNET=true`; the integration fixtures configure Testnet and never
select Demo or production when that opt-in is used.

Copy `.env.example` to `.env` only when preparing local testnet work. Never
commit API credentials.

## License

Apache License 2.0 — see the repository root [LICENSE](../../LICENSE).

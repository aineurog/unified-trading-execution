# unified-trading-execution-binance

Scaffold for the Binance adapter in
[unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
Product and instrument metadata translation is implemented; exchange
connectivity, market data, trailing stops, and trading operations have **not**
been implemented. Unimplemented methods raise `NotImplementedError`.

## Installation

From the monorepo root:

```bash
uv pip install -e packages/core
uv pip install -e packages/adapter-binance
```

The adapter package pins `python-binance==1.0.37`. `fetch_instrument_spec`
expects an injected SDK client, but `connect` does not create one yet, so the
client lifecycle is wired in a later implementation branch. Unit fixtures
never reach the network; the integration fixtures construct the SDK client
directly.

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

This example only constructs the scaffold. `connect` fails immediately before
opening a client, starting the sync loop, or initializing persistent state.
Other unimplemented adapter operations raise `NotImplementedError`; complete
their implementation branches before use.

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
matching row. `parse_instrument_spec` maps PRICE_FILTER, LOT_SIZE, and the
notional filters into exact `Decimal` values. COIN-M instruments carry the
venue `contractSize` as their `multiplier`.

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
resolution, filter parsing, and adapter lookup. They mock SDK client and
socket construction and never reach the network:

```bash
pytest packages/adapter-binance/tests/unit/
```

The metadata integration tests read public Demo exchange-info catalogs and
perform no authenticated or trading calls. They require an explicit
`BINANCE_DEMO=true` opt-in but no credentials:

```bash
BINANCE_DEMO=true pytest packages/adapter-binance/tests/binance_integration/
```

Authenticated Testnet tests require dedicated credentials and
`BINANCE_TESTNET=true`; the integration fixtures configure Testnet and never
select Demo or production when that opt-in is used.

Copy `.env.example` to `.env` only when preparing local testnet work. Never
commit API credentials.

## License

Apache License 2.0 — see the repository root [LICENSE](../../LICENSE).

# unified-trading-execution-binance

Scaffold for the Binance adapter in
[unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
The public package and core-engine wrappers are in place, but exchange
connectivity and trading operations have **not** been implemented. Calling
those methods currently raises `NotImplementedError`.

## Installation

From the monorepo root:

```powershell
uv pip install -e packages/core
uv pip install -e packages/adapter-binance
```

The adapter package pins `python-binance==1.0.37`. Its client will be wired in
a later implementation branch; this scaffold does not construct an SDK client
or perform network I/O.

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

Binance exposes both Testnet and Demo Trading API environments. `testnet=True`
selects Testnet; `demo=True` selects Demo Trading. These flags are mutually
exclusive. When both are false, the SDK selects production (live) endpoints.
Both default to `False`, so set the desired non-live environment explicitly.
This scaffold does not construct an SDK client or contact any of these
endpoints.

`BinanceConfig` redacts credentials from its representation. Its optional
`account_id` can be used as a local account label; when omitted, the scaffold
derives a stable, non-secret label from the API key. The exchange identity
resolution behavior will be reviewed during implementation.

## Implementation status

This scaffold intentionally contains no test cases. The unit and integration
`conftest.py` files provide shared fixtures for the feature branches that add
and validate behavior. Unit fixtures mock SDK client and socket construction;
integration fixtures require dedicated credentials and an explicit
`BINANCE_TESTNET=true` opt-in, then configure Testnet regardless of other
environment selection.

Copy `.env.example` to `.env` only when preparing local testnet work. Never
commit API credentials.

## License

Apache License 2.0 — see the repository root [LICENSE](../../LICENSE).

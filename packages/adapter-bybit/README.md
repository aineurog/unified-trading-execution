# unified-trading-execution-bybit

Bybit adapter for [unified-trading-execution](https://github.com/qaisar/unified-trading-execution).  
Implements the `Adapter` ABC for Bybit spot and perpetual markets.

## Status

**Stub.** Every public method raises `NotImplementedError`.  The adapter is
importable and structurally satisfies the ABC contract so that layering-linter
checks pass from day one, but the actual HTTP / WebSocket logic has not been
written yet.

## Getting testnet API keys

1. Go to [testnet.bybit.com](https://testnet.bybit.com) and sign up / log in.
2. Navigate to **Account & Security → API Management**.
3. Create a new API key with:
   - Read / Write permissions for spot and derivatives.
   - No IP whitelist (or whitelist your public IP).
4. Copy the API key and secret.

## Setup (editable install)

```bash
# From the monorepo root, with uv:
uv pip install -e packages/core
uv pip install -e packages/adapter-bybit

# Or with plain pip:
pip install -e packages/core
pip install -e packages/adapter-bybit
```

## Running tests

```bash
# Unit tests (no network — uses mocks):
pytest packages/adapter-bybit/tests/unit/

# Integration tests (requires testnet credentials):
export BYBIT_TESTNET_API_KEY=your-key
export BYBIT_TESTNET_API_SECRET=your-secret
pytest packages/adapter-bybit/tests/integration/

# Skip integration tests automatically when credentials are missing:
pytest packages/adapter-bybit/tests/
```

## What "done" looks like (Sections 11.2 / 11.3)

- [ ] `connect()` opens authenticated WebSocket user-data streams and publishes
      `ConnectionStateEvent(connected=True)`.
- [ ] `disconnect()` closes all connections cleanly.
- [ ] `place_order()` translates every `UnifiedOrder` field into the correct
      Bybit REST params, signs the request, and returns an `OrderResult`.
- [ ] `modify_order()` / `cancel_order()` work end-to-end against testnet.
- [ ] `fetch_instrument_spec()` queries Bybit's instrument-info endpoint and
      returns a correctly populated `InstrumentSpec`.
- [ ] `supported_order_types()` returns the exact set Bybit supports.
- [ ] `get_rate_limits()` reads rate-limit state from the last HTTP response
      headers.
- [ ] All Bybit-specific errors are translated to the common exception
      hierarchy (`unified_trading_execution.errors`) before crossing the
      adapter boundary.
- [ ] Unit tests cover order translation, error translation, and WebSocket
      event parsing — each test mocks HTTP/WS responses.
- [ ] Integration tests execute all order types (market, limit, stop,
      stop-limit) against testnet and assert correct round-trip behaviour.
- [ ] Layering-linter passes: the adapter imports only from core, never from
      another adapter.

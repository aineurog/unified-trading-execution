# unified-trading-execution-bybit

Bybit adapter for [unified-trading-execution](https://github.com/qaisar/unified-trading-execution).
Implements the `Adapter` ABC for Bybit spot, linear perpetual (USDT/USDC), and
inverse perpetual markets.

## Features

- All four guaranteed order types: **Market, Limit, Stop, Stop-Limit**
- Native **take-profit and stop-loss** attachment at order placement
- **Reduce-only** orders for position closing
- Real-time **WebSocket streams** for orders, executions, positions, and wallet balances
- **Instrument metadata** fetching with TTL-based caching and automatic invalidation
- **Leverage management** with intent persistence, drift detection, and configurable
  drift policies (reapply / notify / halt)
- **Position mode** management (one-way / hedge) with drift protection
- **Margin mode** enforcement (cross / isolated) applied on every connect
- Comprehensive **error translation** — 40+ Bybit error codes mapped to the common
  exception hierarchy
- Full **reconciliation** support via `fetch_positions`, `fetch_balances`,
  `fetch_open_orders`, and `fetch_fills`

## Installation

```bash
# From the monorepo root, with uv:
uv pip install -e packages/core
uv pip install -e packages/adapter-bybit

# Or with plain pip:
pip install -e packages/core
pip install -e packages/adapter-bybit
```

The adapter depends on `unified-trading-execution` (core) and `pybit>=5.0.0`.

## Quickstart

```python
import asyncio
from decimal import Decimal

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderSide, OrderType, TimeInForce
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder


async def main():
    config = BybitConfig(
        api_key="your-api-key",
        api_secret="your-api-secret",
        testnet=True,
    )
    adapter = BybitAdapter(config, event_bus=EventBus())
    await adapter.connect()

    btc = Instrument(symbol="BTC", quote_currency="USDT", asset_class="FUTURES")

    order = UnifiedOrder(
        instrument=btc,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=Decimal("0.01"),
        time_in_force=TimeInForce.GTC,
    )
    result = await adapter.place_order(order)
    print(f"Order {result.client_order_id} → {result.status.value}")

    await adapter.disconnect()


asyncio.run(main())
```

## Getting testnet API keys

1. Go to [testnet.bybit.com](https://testnet.bybit.com) and sign up or log in.
2. Navigate to **Account & Security → API Management**.
3. Create a new API key with read/write permissions for spot and derivatives.
4. Copy the API key and secret.

## Running tests

```bash
# Unit tests (no network — uses mocks):
pytest packages/adapter-bybit/tests/unit/

# Integration tests (requires testnet credentials):
export BYBIT_TESTNET_API_KEY=your-key
export BYBIT_TESTNET_API_SECRET=your-secret
pytest packages/adapter-bybit/tests/bybit_integration/

# Skip integration tests automatically when credentials are missing:
pytest packages/adapter-bybit/tests/
```

## Documentation

The full platform guide — covering leverage management, position modes, drift
handling, error mapping, events, and every supported operation with inline
examples — is at **[docs/platforms/bybit.md](../../docs/platforms/bybit.md)**.

API reference documentation is generated from docstrings via mkdocstrings and
available in the [documentation site](https://github.com/qaisar/unified-trading-execution).

## License

Apache License 2.0 — see the [LICENSE](../../LICENSE) file at the repository root.

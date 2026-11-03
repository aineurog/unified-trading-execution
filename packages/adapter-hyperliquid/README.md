# unified-trading-execution-hyperliquid

Hyperliquid adapter for [unified-trading-execution](https://github.com/aineurog/unified-trading-execution).
Implements the `Adapter` ABC for Hyperliquid perpetual derivatives markets.

## Features

- All four guaranteed order types: **Market, Limit, Stop, Stop-Limit**
- **Position protection** (take-profit and stop-loss) via `modify_position_tpsl`, `get_position_tpsl`, and `clear_position_tpsl`
- **Trailing stops** with synthetic execution only (Hyperliquid has no native trailing primitive)
- **Reduce-only** orders for position closing
- Real-time **WebSocket streams** for orders, executions, positions, and wallet balances
- **Instrument metadata** fetching with TTL-based caching and automatic invalidation
- **Leverage management** with intent persistence, drift detection, and configurable drift policies (reapply / notify / halt)
- Comprehensive **error translation** — Hyperliquid error codes mapped to the common exception hierarchy
- Full **reconciliation** support via `fetch_positions`, `fetch_balances`, `fetch_open_orders`, and `fetch_fills`

## What is covered

| Market | Asset class | Example symbols |
|---|---|---|
| Perpetuals | `FUTURES` | BTC, ETH, SOL |
| Spot | `SPOT` | HYPE/USDC, PURR/USDC |

Hyperliquid uses one-way position mode only — hedge mode is not available.
Spot markets support order placement and market data; they have no position
leg, so position protection and trailing stops are perpetual-only.

## Trailing stops

Hyperliquid uses **synthetic trailing stops only** via position protection.
The venue offers no native trailing-stop order type, so trailing is placed and
repriced as a stop loss on the specific position leg. The
`get_position_trailing_stop` method raises `NotImplementedError`.

### Synthetic trailing

- Core places and manages stop loss orders via `modify_position_tpsl` on the specific position leg
- Supports percentage-based, price-based, and ATR-based trailing
- Supports initial stop distance, take profit, activation price, and step parameters
- AUTO mode routes to synthetic (native capability is `supported=False`)
- Position leg ownership is preserved (no silent takeover of existing protection)

### Position protection

- `modify_position_tpsl` - set or update TP/SL on a position leg
- `get_position_tpsl` - read current TP/SL on a position leg
- `clear_position_tpsl` - remove named protection sides (stop, take profit, or both)
- One-sided writes preserve unmentioned sides (Hyperliquid merge semantics)
- Spot instruments are not supported

## Installation

```bash
# From the monorepo root, with uv:
uv pip install -e packages/core
uv pip install -e packages/adapter-hyperliquid

# Or with plain pip:
pip install -e packages/core
pip install -e packages/adapter-hyperliquid
```

The adapter depends on `unified-trading-execution` (core) and `hyperliquid-python-sdk==0.24.0`.

## Quickstart

```python
import asyncio
from decimal import Decimal

from unified_trading_execution.hyperliquid import HyperliquidEngine, HyperliquidConfig
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import UnifiedOrder


async def main():
    config = HyperliquidConfig(
        wallet_address="your-wallet-address",
        private_key="your-private-key",
        testnet=True,
    )
    engine = HyperliquidEngine(config)
    await engine.connect()

    btc = Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
    )

    order = UnifiedOrder(
        instrument=btc,
        order_type=OrderType.MARKET,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.IOC,
    )
    result = await engine.place_order(order)
    print(f"Order {result.client_order_id} → {result.status.value}")

    await engine.ashutdown()


asyncio.run(main())
```

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
)
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `wallet_address` | `str` | *(required)* | Hyperliquid wallet address; also the canonical account identity |
| `private_key` | `str` | *(required)* | Wallet private key for signing |
| `testnet` | `bool` | `False` | `True` connects to the testnet |
| `default_margin_mode` | `MarginMode \| str` | `"cross"` | Margin mode seeded per traded asset on connect |
| `default_leverage` | `int` | `1` | Leverage seeded per traded asset with no explicit intent |
| `platform_name` | `str` | `"hyperliquid"` | Human-readable label for logs and events |
| `instrument_spec_cache_ttl` | `float \| None` | `86400.0` | Seconds before a cached `InstrumentSpec` is re-fetched. `None` disables TTL-based expiry, relying solely on event-driven invalidation |
| `request_timeout_seconds` | `float` | `10.0` | Timeout for each blocking SDK call |

## Running tests

```bash
# Unit tests (no network — uses mocks):
pytest packages/adapter-hyperliquid/tests/unit/

# Integration tests (requires testnet credentials):
export HYPERLIQUID_TESTNET_ADDRESS=your-wallet-address
export HYPERLIQUID_TESTNET_PRIVATE_KEY=your-private-key
pytest packages/adapter-hyperliquid/tests/hyperliquid_integration/

# Skip integration tests automatically when credentials are missing:
pytest packages/adapter-hyperliquid/tests/
```

## Documentation

The full platform guide — covering leverage management, position protection,
trailing stops, error handling, rate limiting, and every supported operation with
inline examples — is at **[docs/platforms/hyperliquid.md](../../docs/platforms/hyperliquid.md)**.

API reference documentation is generated from docstrings via mkdocstrings and
available in the [documentation site](https://github.com/aineurog/unified-trading-execution).

## License

Apache License 2.0 — see the [LICENSE](../../LICENSE) file at the repository root.

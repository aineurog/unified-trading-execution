"""MetaTrader 5 adapter — implements the Adapter ABC for MT5 brokers.

Exports:
    MT5Engine       — all-in-one async engine (recommended entry point).
    SyncMT5Engine   — all-in-one blocking engine.
    MT5Adapter      — the concrete adapter class (advanced usage).
    MT5Config       — configuration dataclass (credentials, symbol aliasing).
"""

from __future__ import annotations

from unified_trading_execution.mt5.adapter import MT5Adapter
from unified_trading_execution.mt5.config import MT5Config
from unified_trading_execution.mt5.engine import MT5Engine
from unified_trading_execution.mt5.sync_engine import SyncMT5Engine

__all__ = [
    "MT5Adapter",
    "MT5Config",
    "MT5Engine",
    "SyncMT5Engine",
]

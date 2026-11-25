"""Interactive Brokers adapter — implements the Adapter ABC for IBKR.

Exports:
    IBKREngine       — all-in-one async engine (recommended entry point).
    SyncIBKREngine   — all-in-one blocking engine.
    IBKRAdapter      — the concrete adapter class (advanced usage).
    IBKRConfig       — configuration dataclass (host, port, client ID, account).
"""

from __future__ import annotations

from unified_trading_execution.ibkr.adapter import IBKRAdapter
from unified_trading_execution.ibkr.config import IBKRConfig
from unified_trading_execution.ibkr.engine import IBKREngine
from unified_trading_execution.ibkr.sync_engine import SyncIBKREngine

__all__ = [
    "IBKRAdapter",
    "IBKRConfig",
    "IBKREngine",
    "SyncIBKREngine",
]

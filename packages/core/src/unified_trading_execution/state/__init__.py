"""StateStore ABC, SQLite implementation, halt state machine, reconciliation.

The default v1 implementation is SQLite via aiosqlite. The interface is
designed so a future Postgres or Redis backend can be swapped in with
zero changes to core logic.
"""

from __future__ import annotations

from unified_trading_execution.state.halt import HaltConfig, HaltStateMachine
from unified_trading_execution.state.reconciliation import ReconciliationResult, reconcile
from unified_trading_execution.state.store import (
    SQLiteStateStore,
    StateStore,
)

__all__ = [
    "HaltConfig",
    "HaltStateMachine",
    "ReconciliationResult",
    "SQLiteStateStore",
    "StateStore",
    "reconcile",
]

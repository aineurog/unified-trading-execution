"""Bybit adapter — implements the Adapter ABC for Bybit spot and perpetual markets.

Exports:
    BybitEngine      — all-in-one async engine (recommended entry point).
    SyncBybitEngine  — all-in-one blocking engine.
    BybitAdapter     — the concrete adapter class (advanced usage).
    BybitConfig      — configuration dataclass (API credentials, testnet switch).
    MarginMode       — account-wide margin mode enum (cross / isolated).
    PositionMode     — position mode enum (one-way / hedge).
"""

from __future__ import annotations

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.engine import BybitEngine
from unified_trading_execution.bybit.enums import MarginMode, PositionMode
from unified_trading_execution.bybit.sync_engine import SyncBybitEngine

__all__ = [
    "BybitAdapter",
    "BybitConfig",
    "BybitEngine",
    "MarginMode",
    "PositionMode",
    "SyncBybitEngine",
]

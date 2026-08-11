"""Bybit adapter — implements the Adapter ABC for Bybit spot and perpetual markets.

Exports:
    BybitAdapter     — the concrete adapter class.
    BybitConfig      — configuration dataclass (API credentials, testnet switch).
    SyncBybitAdapter — blocking facade over BybitAdapter for use with SyncEngine.
    MarginMode       — account-wide margin mode enum (cross / isolated).
    PositionMode     — position mode enum (one-way / hedge).
"""

from __future__ import annotations

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.enums import MarginMode, PositionMode
from unified_trading_execution.bybit.sync_adapter import SyncBybitAdapter

__all__ = [
    "BybitAdapter",
    "BybitConfig",
    "MarginMode",
    "PositionMode",
    "SyncBybitAdapter",
]

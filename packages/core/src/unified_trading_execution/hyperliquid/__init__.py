"""Hyperliquid adapter — implements the Adapter ABC for Hyperliquid spot and perp markets.

Exports:
    HyperliquidEngine      — all-in-one async engine (recommended entry point).
    SyncHyperliquidEngine  — all-in-one blocking engine.
    HyperliquidAdapter     — the concrete adapter class (advanced usage).
    HyperliquidConfig      — configuration dataclass (wallet, key, testnet switch).
    MarginMode             — per-asset margin mode enum (cross / isolated).
    PositionMode           — position mode enum (one-way only; venue has no hedge mode).
"""

from __future__ import annotations

from unified_trading_execution.hyperliquid.adapter import HyperliquidAdapter
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.engine import HyperliquidEngine
from unified_trading_execution.hyperliquid.enums import MarginMode, PositionMode
from unified_trading_execution.hyperliquid.sync_engine import SyncHyperliquidEngine

__all__ = [
    "HyperliquidAdapter",
    "HyperliquidConfig",
    "HyperliquidEngine",
    "MarginMode",
    "PositionMode",
    "SyncHyperliquidEngine",
]

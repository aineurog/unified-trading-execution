"""Bybit adapter — implements the Adapter ABC for Bybit spot and perpetual markets.

Exports:
    BybitAdapter   — the concrete adapter class.
    BybitConfig    — configuration dataclass (API credentials, testnet switch).
    LeverageConfig — leverage behavioral policy (nested inside BybitConfig).
    MarginMode     — account-wide margin mode enum (cross / isolated).
"""

from __future__ import annotations

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig, LeverageConfig
from unified_trading_execution.bybit.margin import MarginMode

__all__ = ["BybitAdapter", "BybitConfig", "LeverageConfig", "MarginMode"]

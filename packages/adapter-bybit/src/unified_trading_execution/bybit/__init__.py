"""Bybit adapter — implements the Adapter ABC for Bybit spot and perpetual markets.

Exports:
    BybitAdapter — the concrete adapter class, importable and instantiable
    immediately (all methods raise NotImplementedError until the dev
    implements them per Section 17.10).
    BybitConfig   — configuration dataclass (API credentials, testnet switch).
"""

from __future__ import annotations

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig

__all__ = ["BybitAdapter", "BybitConfig"]

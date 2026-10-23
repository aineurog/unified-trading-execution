"""Trailing-stop subsystem.

Public contract types and enums.  Stop math, capability queries, persistence,
and lifecycle management live in sibling modules and are imported directly.
"""

from unified_trading_execution.trailing.types import (
    ACTIVE_TRAILING_PHASES,
    TERMINAL_TRAILING_PHASES,
    CandleTimeframe,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)

__all__ = [
    # Constants
    "ACTIVE_TRAILING_PHASES",
    "TERMINAL_TRAILING_PHASES",
    # Types
    "CandleTimeframe",
    "ProtectionLevel",
    "ProtectionLevelType",
    "TrailingActivation",
    "TrailingBasis",
    "TrailingMode",
    "TrailingOffsetType",
    "TrailingPhase",
    "TrailingRequest",
    "TrailingSpec",
    "TrailingState",
]

"""Adapter-specific enums for the Bybit adapter.

``MarginMode`` and ``PositionMode`` are Bybit-specific concepts (no other v1
platform shares them in the same form), so they live in the adapter package,
not in core.
"""

from __future__ import annotations

from enum import StrEnum


class MarginMode(StrEnum):
    """Bybit account-wide margin mode.

    Set once for the entire UTA account — not per symbol.
    """

    CROSS = "cross"
    ISOLATED = "isolated"


class PositionMode(StrEnum):
    """Bybit per-symbol position mode.

    ONE_WAY (mode=0) — Merged Single: one net position per symbol.
    HEDGE   (mode=3) — Both Sides: independent long and short positions.
    """

    ONE_WAY = "one_way"  # Bybit mode=0
    HEDGE = "hedge"  # Bybit mode=3

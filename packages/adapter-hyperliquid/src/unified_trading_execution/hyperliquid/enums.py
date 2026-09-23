"""Adapter-specific enums for the Hyperliquid adapter.

``MarginMode`` and ``PositionMode`` are Hyperliquid-specific concepts, so they
live in the adapter package, not in core (Bybit precedent).
"""

from __future__ import annotations

from enum import StrEnum


class MarginMode(StrEnum):
    """Hyperliquid per-asset margin mode.

    Hyperliquid keeps cross/isolated per asset (``isCross`` on
    ``updateLeverage``), not account-wide — unlike Bybit's static
    account-wide mode.  Never a bool.
    """

    CROSS = "cross"
    ISOLATED = "isolated"


class PositionMode(StrEnum):
    """Hyperliquid position mode — one-way only.

    The venue has no hedge mode (``assetPositions[].type`` is always
    ``"oneWay"``; ``updateIsolatedMargin.isBuy`` is a documented no-op
    "until hedge mode is introduced").  Any future venue hedge mode is a
    new enum member plus routing rework, not a flag flip.
    """

    ONE_WAY = "one_way"

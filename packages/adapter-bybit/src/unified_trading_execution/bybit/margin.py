"""Margin mode and leverage configuration for the Bybit adapter (Sections 3, 4.3).

Leverage is an adapter-only concern (Section 1.1): the shape differs across
platforms, so nothing about it lives in core types.  ``MarginMode`` and
``LeverageConfig`` are the Bybit adapter's own vocabulary, published from the
adapter package and consumed only there.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class MarginMode(StrEnum):
    """Bybit margin mode per symbol (Section 3.1)."""

    CROSS = "cross"
    ISOLATED = "isolated"


@dataclass(frozen=True, slots=True)
class LeverageConfig:
    """Per-adapter configuration for leverage behavior (Section 4.3).

    All fields carry their documented defaults so a plain ``LeverageConfig()``
    is a safe, conservative starting point.
    """

    # Behavior when reconciliation detects leverage drift from stored intent.
    on_drift: Literal["reapply", "notify", "halt"] = "reapply"

    # Reapply stored leverage intent to the platform on connect.
    auto_apply_on_connect: bool = True

    # Query the platform for current leverage before every order dispatch and
    # reject the order if it differs from stored intent.  When False (default),
    # trust that connect-time reapply and reconciliation keep leverage in sync
    # — no per-order network call.
    strict_check: bool = False

    # Block leverage changes when the instrument has an open position.
    block_on_open_position: bool = True

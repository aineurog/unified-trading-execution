"""Margin mode enum for the Bybit adapter.

``MarginMode`` is Bybit-specific — margin mode is an account-wide setting on
the Bybit Unified Trading Account (set via ``POST /v5/account/set-margin-mode``,
no symbol parameter).  It lives in the adapter package, not in core, because
no other v1 platform shares this concept in the same form.
"""

from __future__ import annotations

from enum import StrEnum


class MarginMode(StrEnum):
    """Bybit account-wide margin mode.

    Set once for the entire UTA account — not per symbol.
    """

    CROSS = "cross"
    ISOLATED = "isolated"

"""Hyperliquid native error → unified exception hierarchy translation.

Every Hyperliquid-specific failure (the venue returns HTTP 200 with per-item
``error`` strings — parse, never status-code-switch) must be translated into
one of the common exception types from
``unified_trading_execution.errors`` before it crosses the adapter boundary.
Core must never receive a raw Hyperliquid error.
"""

from __future__ import annotations

from unified_trading_execution.errors import UteError


def map_hyperliquid_error(*, message: str = "") -> UteError:
    """Map a native Hyperliquid ``error`` string to the unified hierarchy.

    Covers the tick/min-notional family → ``InvalidOrderError``, the
    insufficient-margin/spot-balance family → ``InsufficientBalanceError``,
    reduce-only misuse → ``UnsupportedOrderTypeError``, post-only-bbo →
    CANCELLED outcome (not an error), OI-cap family → dedicated Rejected
    mapping, never-placed cancel → ``OrderNotFoundError``; unknown strings
    stay a generic ``PlatformError`` with full context, and unenumerated
    suffixes are never silently guessed.
    """
    raise NotImplementedError


def map_order_status_name(status: str) -> str:
    """Map a native order-status name to its canonical handling bucket.

    Every ``*Rejected`` variant maps to the ``InvalidOrderError`` bucket —
    never generic.  Unenumerated names stay generic, never silently guessed.
    """
    raise NotImplementedError

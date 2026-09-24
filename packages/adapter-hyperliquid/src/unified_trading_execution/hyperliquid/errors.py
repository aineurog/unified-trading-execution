"""Hyperliquid native error → unified exception hierarchy translation.

Every Hyperliquid-specific failure (the venue returns HTTP 200 with per-item
``error`` strings — parse, never status-code-switch) must be translated into
one of the common exception types from
``unified_trading_execution.errors`` before it crosses the adapter boundary.
Core must never receive a raw Hyperliquid error.

Source of truth for the wire strings: the venue error-responses reference
(order errors Tick/MinTradeNtl/MinTradeSpotNtl/PerpMargin/ReduceOnly/
BadAloPx/IocCancel/BadTriggerPx/MarketOrderNoLiquidity/the four
open-interest-cap errors/InsufficientSpotBalance/Oracle/PerpMaxPosition,
cancel error MissingOrder).  Prefixes are stored without trailing periods so
a venue-present or venue-absent period both match; interpolated suffixes
(``{quote_token}``, ``{bbo}``) match by prefix.

Batch pre-validation note: some payload-deterministic failures are returned
once for the entire batch rather than per item.  The adapter raises on the
first item and never partial-applies.
"""

from __future__ import annotations

from collections.abc import Callable

from unified_trading_execution.errors import (
    InsufficientBalanceError,
    InvalidOrderError,
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
    UteError,
)


class OpenInterestCapError(InvalidOrderError):
    """Order rejected by the venue open-interest cap.

    Dedicated subtype of ``InvalidOrderError``: still catchable as an
    invalid order, but distinguishable where retrying is futile and a halt
    may be warranted.
    """


# Cancel-path message; the venue appends " asset=<id>" (observed live —
# the reference shows the bare sentence), so this matches by prefix.
_MISSING_ORDER_PREFIX = "Order was never placed, already canceled, or filled."

_POST_ONLY_CANCEL_PREFIX = "Post only order would have immediately matched"

_IOC_NO_MATCH_MESSAGE = "Order could not immediately match against any resting orders."


def _platform_error(message: str) -> PlatformError:
    return PlatformError(message, platform_error={"message": message})


# (prefix without trailing period, factory).  First match wins — keep
# interpolated/specific prefixes before any general one that could shadow.
_ERROR_PREFIX_TABLE: tuple[tuple[str, Callable[[str], UteError]], ...] = (
    ("Price must be divisible by tick size", InvalidOrderError),
    ("Order must have minimum value of $10", InvalidOrderError),
    ("Order must have minimum value of 10 ", InvalidOrderError),
    ("Insufficient margin to place order", InsufficientBalanceError),
    ("Reduce only order would increase position", UnsupportedOrderTypeError),
    ("Invalid TP/SL price", InvalidOrderError),
    ("(Spot-only) Order has insufficient spot balance to trade", InsufficientBalanceError),
    ("Order price too far from oracle", InvalidOrderError),
    (
        "Order would cause position to exceed margin tier limit at current leverage",
        InvalidOrderError,
    ),
    ("Order would increase open interest while open interest is capped", OpenInterestCapError),
    (
        "Order rejected due to price more aggressive than oracle while at open interest cap",
        OpenInterestCapError,
    ),
    ("Order would increase open interest too quickly", OpenInterestCapError),
    ("No liquidity available for market order", _platform_error),
)


def is_cancelled_outcome(*, message: str = "") -> bool:
    """True when the message reports a benign cancel outcome, not a failure.

    A post-only order that would have matched immediately and an IOC order
    that matched nothing are both cancelled by the venue without resting or
    filling.  Callers translate these to a CANCELLED outcome instead of
    raising.
    """
    return message.startswith(_POST_ONLY_CANCEL_PREFIX) or message == _IOC_NO_MATCH_MESSAGE


def map_hyperliquid_error(*, message: str = "") -> UteError:
    """Map a native Hyperliquid ``error`` string to the unified hierarchy.

    Cancel-path ``MissingOrder`` maps to ``OrderNotFoundError``; every other
    known string maps via the prefix table; unknown (or empty) strings stay
    a generic ``PlatformError`` carrying the raw message as context — never
    silently guessed.
    """
    if message.startswith(_MISSING_ORDER_PREFIX):
        return OrderNotFoundError(message)
    for prefix, factory in _ERROR_PREFIX_TABLE:
        if message.startswith(prefix):
            return factory(message)
    return _platform_error(message or "unknown Hyperliquid error")

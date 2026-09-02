from __future__ import annotations

from enum import StrEnum


class OrderType(StrEnum):
    """Guaranteed portable order types — every adapter must implement all four."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(StrEnum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    DAY = "DAY"
    GTD = "GTD"  # Good-Til-Date — requires expire_at on UnifiedOrder


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


# Statuses that represent an order still working/live on the platform.  Used
# to scope reconciliation orphan detection to open orders only, so terminal
# orders (FILLED/CANCELLED/REJECTED/EXPIRED) are never mistaken for orphans.
LIVE_ORDER_STATUSES: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.PENDING,
        OrderStatus.OPEN,
        OrderStatus.PARTIALLY_FILLED,
    }
)


class AssetClass(StrEnum):
    SPOT = "SPOT"
    MARGIN_FX = "MARGIN_FX"
    CFD = "CFD"
    FUTURES = "FUTURES"
    OPTION = "OPTION"
    STOCK = "STOCK"
    BOND = "BOND"
    FUND = "FUND"


class OptionRight(StrEnum):
    CALL = "CALL"
    PUT = "PUT"


class HaltState(StrEnum):
    ACTIVE = "ACTIVE"
    HALTED = "HALTED"


class FillReason(StrEnum):
    """Why a fill happened — the platform's own classification of the deal.

    A portable superset; each adapter maps its native reason codes into these.
    ``UNKNOWN`` is the fallback for a platform that does not classify deals.
    """

    CLIENT = "CLIENT"  # manually placed / client terminal (MT5 CLIENT/MOBILE/WEB)
    EXPERT = "EXPERT"  # expert advisor / automated strategy
    DEALER = "DEALER"  # dealer / desk
    STOP_LOSS = "STOP_LOSS"  # stop-loss triggered
    TAKE_PROFIT = "TAKE_PROFIT"  # take-profit triggered
    STOP_OUT = "STOP_OUT"  # margin stop-out
    ROLLOVER = "ROLLOVER"  # swap / rollover
    MARGIN = "MARGIN"  # variation-margin call
    SPLIT = "SPLIT"  # corporate action (split)
    UNKNOWN = "UNKNOWN"


class FillEntry(StrEnum):
    """Whether a fill opened or closed exposure (the platform's deal direction)."""

    IN = "IN"  # opened a position
    OUT = "OUT"  # closed a position
    INOUT = "INOUT"  # reversed (close one way, open the other)
    OUT_BY = "OUT_BY"  # closed by an opposite deal
    UNKNOWN = "UNKNOWN"


class HaltClearMode(StrEnum):
    AUTOMATIC = "AUTOMATIC"
    MANUAL = "MANUAL"

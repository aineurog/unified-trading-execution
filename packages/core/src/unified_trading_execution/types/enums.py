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


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


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


class HaltClearMode(StrEnum):
    AUTOMATIC = "AUTOMATIC"
    MANUAL = "MANUAL"

"""Unit tests for the Hyperliquid error-string table (no network).

Every native string below is verbatim from the venue error-responses
reference; each maps to exactly one unified type.
"""

from __future__ import annotations

import pytest

from unified_trading_execution.errors import (
    InsufficientBalanceError,
    InvalidOrderError,
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
    UteError,
)
from unified_trading_execution.hyperliquid.errors import (
    OpenInterestCapError,
    is_cancelled_outcome,
    map_hyperliquid_error,
)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Price must be divisible by tick size.", InvalidOrderError),
        ("Price must be divisible by tick size", InvalidOrderError),
        ("Order must have minimum value of $10.", InvalidOrderError),
        ("Order must have minimum value of 10 USDC.", InvalidOrderError),
        ("Insufficient margin to place order.", InsufficientBalanceError),
        ("Reduce only order would increase position.", UnsupportedOrderTypeError),
        ("Invalid TP/SL price.", InvalidOrderError),
        ("(Spot-only) Order has insufficient spot balance to trade", InsufficientBalanceError),
        ("Order price too far from oracle", InvalidOrderError),
        (
            "Order would cause position to exceed margin tier limit at current leverage",
            InvalidOrderError,
        ),
        (
            "Order would increase open interest while open interest is capped",
            OpenInterestCapError,
        ),
        (
            "Order rejected due to price more aggressive than oracle while at open interest cap",
            OpenInterestCapError,
        ),
        ("Order would increase open interest too quickly", OpenInterestCapError),
        ("No liquidity available for market order.", PlatformError),
        ("Order was never placed, already canceled, or filled.", OrderNotFoundError),
        # Live cancel errors append the asset id (undocumented suffix).
        ("Order was never placed, already canceled, or filled. asset=3", OrderNotFoundError),
    ],
)
def test_error_table(message: str, expected: type[UteError]) -> None:
    assert type(map_hyperliquid_error(message=message)) is expected


def test_open_interest_cap_is_invalid_order() -> None:
    assert issubclass(OpenInterestCapError, InvalidOrderError)
    assert isinstance(
        map_hyperliquid_error(
            message="Order would increase open interest too quickly",
        ),
        InvalidOrderError,
    )


@pytest.mark.parametrize(
    "message",
    [
        "Post only order would have immediately matched, bbo was 100.",
        "Order could not immediately match against any resting orders.",
    ],
)
def test_cancelled_outcomes(message: str) -> None:
    assert is_cancelled_outcome(message=message) is True


def test_real_errors_are_not_cancelled_outcomes() -> None:
    assert is_cancelled_outcome(message="Price must be divisible by tick size.") is False
    assert is_cancelled_outcome(message="") is False


@pytest.mark.parametrize("message", ["", "Something entirely new"])
def test_unknown_stays_generic_platform_error(message: str) -> None:
    error = map_hyperliquid_error(message=message)
    assert type(error) is PlatformError
    assert error.platform_error == {"message": message or "unknown Hyperliquid error"}

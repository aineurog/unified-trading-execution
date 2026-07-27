"""Common exception hierarchy — defined once in core, translated into by every adapter.

Core logic (retry rules, risk decisions) is written once against these types
and works correctly for every current and future adapter.
"""

from __future__ import annotations


class UteError(Exception):
    """Base exception for all Unified Trading Execution errors."""


# ---- Order / dispatch errors ----

class InsufficientBalanceError(UteError):
    """Account has insufficient balance to place this order."""


class InvalidSymbolError(UteError):
    """The symbol/instrument is not recognised or tradable on this platform."""


class RateLimitError(UteError):
    """Request rate-limit exceeded — either platform-reported or self-throttled."""


class OrderNotFoundError(UteError):
    """The requested order (by client_order_id) was not found on the platform."""


class UnsupportedOrderTypeError(UteError):
    """The requested order type is not supported by this adapter."""


class DuplicateOrderIdError(UteError):
    """A user-supplied client_order_id collides with an existing non-terminal order."""


# ---- Connection errors ----

class ConnectionError(UteError):
    """Connection to the platform failed or was lost."""


# ---- Halt errors (Section 6.4) ----

class InstrumentHaltedError(UteError):
    """An instrument is halted — new exposure-increasing orders are blocked."""


class AccountHaltedError(UteError):
    """The account is halted — new orders are blocked account-wide."""


# ---- Engine lifecycle ----

class EngineShutdownError(UteError):
    """Called a method on an engine that has been shut down."""


# ---- Catch-all ----

class PlatformError(UteError):
    """A platform-native error that does not map to a more specific exception.

    The raw platform error is carried as context so nothing is silently swallowed.
    """

    def __init__(self, message: str, platform_error: object | None = None) -> None:
        super().__init__(message)
        self.platform_error = platform_error

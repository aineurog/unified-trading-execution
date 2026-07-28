"""Unit tests for the common exception hierarchy — Section 9.3."""

from __future__ import annotations

import pytest

from unified_trading_execution.errors import (
    UteError,
    AccountHaltedError,
    PlatformConnectionError,
    DuplicateOrderIdError,
    EngineShutdownError,
    InstrumentHaltedError,
    InsufficientBalanceError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformError,
    RateLimitError,
    UnsupportedOrderTypeError,
)


# ---- Hierarchy ----

def test_all_errors_inherit_from_ute_error():
    assert issubclass(InsufficientBalanceError, UteError)
    assert issubclass(InvalidSymbolError, UteError)
    assert issubclass(RateLimitError, UteError)
    assert issubclass(OrderNotFoundError, UteError)
    assert issubclass(UnsupportedOrderTypeError, UteError)
    assert issubclass(DuplicateOrderIdError, UteError)
    assert issubclass(PlatformConnectionError, UteError)
    assert issubclass(InstrumentHaltedError, UteError)
    assert issubclass(AccountHaltedError, UteError)
    assert issubclass(PlatformError, UteError)
    assert issubclass(EngineShutdownError, UteError)


def test_ute_error_inherits_from_exception():
    assert issubclass(UteError, Exception)


# ---- Construction and message ----

@pytest.mark.parametrize("exc_cls", [
    InsufficientBalanceError,
    InvalidSymbolError,
    RateLimitError,
    OrderNotFoundError,
    UnsupportedOrderTypeError,
    DuplicateOrderIdError,
    PlatformConnectionError,
    InstrumentHaltedError,
    AccountHaltedError,
    EngineShutdownError,
])
def test_error_constructs_with_message(exc_cls):
    e = exc_cls("something went wrong")
    assert str(e) == "something went wrong"
    assert e.args == ("something went wrong",)


# ---- PlatformError — carries raw error ----

def test_platform_error_constructs_with_message_only():
    e = PlatformError("generic platform failure")
    assert str(e) == "generic platform failure"
    assert e.platform_error is None


def test_platform_error_carries_raw_error():
    raw = ValueError("native exchange error code 9999")
    e = PlatformError("order rejected", platform_error=raw)
    assert e.platform_error is raw
    assert str(e) == "order rejected"


# ---- Catchability ----

def test_can_catch_by_ute_error():
    try:
        raise InsufficientBalanceError("not enough margin")
    except UteError as e:
        assert "not enough margin" in str(e)


def test_can_catch_by_specific_type():
    try:
        raise OrderNotFoundError("abc-123")
    except OrderNotFoundError as e:
        assert "abc-123" in str(e)


def test_specific_error_not_caught_by_sibling():
    """Instances of one subtype should not be caught by a different subtype."""
    caught = False
    try:
        raise RateLimitError("too many requests")
    except OrderNotFoundError:
        caught = True
    except RateLimitError:
        pass
    assert not caught


# ---- DuplicateOrderIdError — spec-accurate ----

def test_duplicate_order_id_error_message():
    e = DuplicateOrderIdError("client_order_id 'xyz' already exists (FILLED)")
    assert "xyz" in str(e)
    assert "FILLED" in str(e)


# ---- InstrumentHaltedError / AccountHaltedError ----

def test_instrument_halted_error():
    e = InstrumentHaltedError("BTC/USDT halted: position quantity mismatch")
    assert "BTC/USDT" in str(e)


def test_account_halted_error():
    e = AccountHaltedError("account halted: balance mismatch on USDT")
    assert "USDT" in str(e)

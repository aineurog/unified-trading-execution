"""Unit tests for all shared enums — Section 17.1."""

from __future__ import annotations

from enum import StrEnum

from unified_trading_execution.types.enums import (
    AssetClass,
    HaltClearMode,
    HaltState,
    OptionRight,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

# ---- OrderType ----


def test_order_type_values():
    assert OrderType.MARKET == "MARKET"
    assert OrderType.LIMIT == "LIMIT"
    assert OrderType.STOP == "STOP"
    assert OrderType.STOP_LIMIT == "STOP_LIMIT"


def test_order_type_is_str_enum():
    assert issubclass(OrderType, StrEnum)
    assert issubclass(OrderType, str)


# ---- OrderSide ----


def test_order_side_values():
    assert OrderSide.BUY == "BUY"
    assert OrderSide.SELL == "SELL"


# ---- TimeInForce ----


def test_time_in_force_values():
    assert TimeInForce.GTC == "GTC"
    assert TimeInForce.IOC == "IOC"
    assert TimeInForce.FOK == "FOK"
    assert TimeInForce.DAY == "DAY"


# ---- OrderStatus ----


def test_order_status_values():
    assert OrderStatus.PENDING == "PENDING"
    assert OrderStatus.OPEN == "OPEN"
    assert OrderStatus.PARTIALLY_FILLED == "PARTIALLY_FILLED"
    assert OrderStatus.FILLED == "FILLED"
    assert OrderStatus.CANCELLED == "CANCELLED"
    assert OrderStatus.REJECTED == "REJECTED"
    assert OrderStatus.EXPIRED == "EXPIRED"


# ---- AssetClass ----


def test_asset_class_values():
    assert AssetClass.SPOT == "SPOT"
    assert AssetClass.MARGIN_FX == "MARGIN_FX"
    assert AssetClass.CFD == "CFD"
    assert AssetClass.FUTURES == "FUTURES"
    assert AssetClass.OPTION == "OPTION"
    assert AssetClass.STOCK == "STOCK"
    assert AssetClass.BOND == "BOND"
    assert AssetClass.FUND == "FUND"


# ---- OptionRight ----


def test_option_right_values():
    assert OptionRight.CALL == "CALL"
    assert OptionRight.PUT == "PUT"


# ---- HaltState ----


def test_halt_state_values():
    assert HaltState.ACTIVE == "ACTIVE"
    assert HaltState.HALTED == "HALTED"


# ---- HaltClearMode ----


def test_halt_clear_mode_values():
    assert HaltClearMode.AUTOMATIC == "AUTOMATIC"
    assert HaltClearMode.MANUAL == "MANUAL"

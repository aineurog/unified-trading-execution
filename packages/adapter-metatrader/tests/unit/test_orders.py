"""Unit tests for MT5 order translation (orders.py).

Tests cases:
    - build_mt5_request: all 8 order type permutations (4 types × 2 sides)
    - MARKET BUY/SELL get ORDER_TYPE_BUY/SELL
    - LIMIT BUY/SELL get ORDER_TYPE_BUY_LIMIT/SELL_LIMIT
    - STOP/STOP_LIMIT type mapping
    - TP/SL are set as native price fields (sl, tp) on the request
    - TpSlAttachment.limit_price raises UnsupportedOrderTypeError
    - build_mt5_modify_request: quantity change raises UnsupportedOrderTypeError
    - build_mt5_cancel_request: TRADE_ACTION_REMOVE with correct ticket
    - build_mt5_sltp_request: TRADE_ACTION_SLTP with correct position_id
    - parse_mt5_result: maps retcode to OrderResult with status
    - parse_order_record: handles None / empty tuple
    - _select_filling: selects best available filling mode per symbol
    - GTD orders set expiration fields on the request
"""

from __future__ import annotations

import pytest


class TestBuildMT5Request:
    """UnifiedOrder → MqlTradeRequest translation."""

    def test_market_buy(self) -> None:
        """MARKET BUY → ORDER_TYPE_BUY."""
        ...

    def test_market_sell(self) -> None:
        """MARKET SELL → ORDER_TYPE_SELL."""
        ...

    def test_limit_buy(self) -> None:
        """LIMIT BUY → ORDER_TYPE_BUY_LIMIT."""
        ...

    def test_limit_sell(self) -> None:
        """LIMIT SELL → ORDER_TYPE_SELL_LIMIT."""
        ...

    def test_stop_buy(self) -> None:
        """STOP BUY → ORDER_TYPE_BUY_STOP."""
        ...

    def test_stop_sell(self) -> None:
        """STOP SELL → ORDER_TYPE_SELL_STOP."""
        ...

    def test_stop_limit_buy(self) -> None:
        """STOP_LIMIT BUY → ORDER_TYPE_BUY_STOP_LIMIT."""
        ...

    def test_stop_limit_sell(self) -> None:
        """STOP_LIMIT SELL → ORDER_TYPE_SELL_STOP_LIMIT."""
        ...

    def test_tp_sl_as_native_fields(self) -> None:
        """Take profit and stop loss are set as sl/tp fields on the request."""
        ...

    def test_tpsl_attachment_limit_price_unsupported(self) -> None:
        """TpSlAttachment with limit_price raises UnsupportedOrderTypeError."""
        ...

    def test_gtd_sets_expiration_fields(self) -> None:
        """GTD orders include type_time and expiration in the request."""
        ...


class TestBuildMT5ModifyRequest:
    """OrderModification → TRADE_ACTION_MODIFY translation."""

    def test_price_change(self) -> None:
        """Modifying price sets the PRICE field."""
        ...

    def test_stop_price_change(self) -> None:
        """Modifying stop_price sets the STOPLIMIT field."""
        ...

    def test_quantity_change_unsupported(self) -> None:
        """Modifying quantity raises UnsupportedOrderTypeError."""
        ...


class TestBuildMT5CancelRequest:
    """Cancel → TRADE_ACTION_REMOVE translation."""

    def test_cancel_request(self) -> None:
        """Cancel request has TRADE_ACTION_REMOVE and correct ticket."""
        ...


class TestBuildMT5SltpRequest:
    """TP/SL → TRADE_ACTION_SLTP translation."""

    def test_sltp_request(self) -> None:
        """SLTP request has TRADE_ACTION_SLTP and correct position_id."""
        ...


class TestParseMT5Result:
    """OrderSendResult → OrderResult parsing."""

    def test_placed_result(self) -> None:
        """TRADE_RETCODE_PLACED → OrderResult."""
        ...

    def test_done_result(self) -> None:
        """TRADE_RETCODE_DONE → OrderResult with filled status."""
        ...

    def test_rejected_result_raises(self) -> None:
        """TRADE_RETCODE_REJECT raises via error mapping."""
        ...


class TestSelectFilling:
    """Filling mode selection per symbol info and TIF."""

    def test_selects_ideal_mode_when_available(self) -> None:
        """Preferred filling mode matches TIF and is supported."""
        ...

    def test_falls_back_when_ideal_unsupported(self) -> None:
        """Fallback chain when ideal filling mode is not in bitmask."""
        ...

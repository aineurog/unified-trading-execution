"""Unit tests for IBKR order translation (orders.py).

Tests cases:
    - build_ibkr_orders: maps market, limit, stop, and stop-limit orders correctly
    - client_order_id is correctly mapped to orderRef for idempotency
    - TP/SL attachments generate parent-child bracket orders with parentId
    - apply_ibkr_modification: supports updating quantity, limit price, and stop price
    - parse_ibkr_trade: maps ib_async Trade objects to unified OrderResult
"""

from __future__ import annotations


class TestBuildIBKROrders:
    """UnifiedOrder → IBKR Order translation."""

    def test_market_buy(self) -> None:
        """MARKET BUY → ACTION: BUY, TYPE: Mkt."""
        ...

    def test_market_sell(self) -> None:
        """MARKET SELL → ACTION: SELL, TYPE: Mkt."""
        ...

    def test_limit_buy(self) -> None:
        """LIMIT BUY → ACTION: BUY, TYPE: Lmt."""
        ...

    def test_limit_sell(self) -> None:
        """LIMIT SELL → ACTION: SELL, TYPE: Lmt."""
        ...

    def test_stop_orders(self) -> None:
        """STOP and STOP_LIMIT map to STP and STP LMT with auxPrice."""
        ...

    def test_client_order_id_mapped_to_order_ref(self) -> None:
        """client_order_id is populated in orderRef for state tracking."""
        ...

    def test_bracket_orders_for_tpsl(self) -> None:
        """TP/SL attachments create linked child orders via parentId."""
        ...


class TestApplyIBKRModification:
    """OrderModification → IBKR Order mutation."""

    def test_price_change(self) -> None:
        """Modifying price updates lmtPrice."""
        ...

    def test_stop_price_change(self) -> None:
        """Modifying stop_price updates auxPrice."""
        ...

    def test_quantity_change_supported(self) -> None:
        """Modifying quantity updates totalQuantity (supported natively)."""
        ...


class TestParseIBKRTrade:
    """ib_async Trade → OrderResult parsing."""

    def test_parsed_trade_result(self) -> None:
        """Trade status, filled quantity, and price map to OrderResult."""
        ...

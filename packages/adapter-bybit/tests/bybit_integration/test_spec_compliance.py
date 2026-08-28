"""Spec-compliance integration tests — valid values pass, invalid values raise.

Every order quantity/price is derived from the instrument's live
``InstrumentSpec`` so tests reflect the platform's actual filters.  Invalid
values deliberately violate one filter at a time and must raise the mapped
unified exception (Section 11.2).  Raw Bybit ret-codes that are not yet in
``errors.py`` map to ``PlatformError`` — tests assert the mapped class and the
observed native ret-code is surfaced for an ``errors.py`` follow-up.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal
from typing import Any, cast

import pytest

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.errors import OrderNotFoundError, UteError
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec

from .conftest import cleanup_open_orders
from .helpers import (
    build_unified_order,
    invalid_values_from_spec,
    random_client_id,
    valid_price_from_spec,
    valid_qty_from_spec,
)


async def _spec(
    adapter: BybitAdapter,
    instrument: Instrument,
) -> InstrumentSpec:
    spec = await adapter.fetch_instrument_spec(instrument)
    return cast(InstrumentSpec, spec)


def _valid_price(spec: InstrumentSpec, reference: Decimal) -> Decimal:
    price = valid_price_from_spec(spec, reference)
    return price if price and price > 0 else Decimal("1")


def _valid_qty(spec: InstrumentSpec, reference: Decimal) -> Decimal:
    qty = valid_qty_from_spec(spec, reference)
    return qty if qty > 0 else Decimal("1")


async def test_valid_limit_accepted(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """A spec-compliant LIMIT order places cleanly and returns a valid result."""
    spec = await _spec(connected_adapter, traded_instrument)
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        _valid_qty(spec, reference_price),
        client_order_id=random_client_id("spec-valid"),
        price=_valid_price(spec, reference_price),
    )
    try:
        result = await connected_adapter.place_order(order)
        assert result.platform_order_id is not None
        assert result.client_order_id == order.client_order_id
    finally:
        # Order may have filled instantly (e.g. market-priced limit) — suppress
        # OrderNotFoundError so cleanup doesn't mask the real assertion above.
        with contextlib.suppress(OrderNotFoundError):
            assert order.client_order_id is not None
            await connected_adapter.cancel_order(order.client_order_id)
        await cleanup_open_orders(connected_adapter)


async def test_invalid_values_rejected(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """Each deliberately-invalid value is rejected by the platform/adapter."""
    spec = await _spec(connected_adapter, traded_instrument)
    for case in invalid_values_from_spec(spec, reference_price):
        order = build_unified_order(
            traded_instrument,
            case.order_type,
            OrderSide.BUY,
            case.quantity if case.quantity is not None else _valid_qty(spec, reference_price),
            client_order_id=random_client_id("spec-invalid"),
            price=case.price,
        )
        try:
            await connected_adapter.place_order(order)
        except UteError as exc:
            # Native ret-code surfaces for an errors.py follow-up (some codes
            # map to PlatformError until added to the map).
            print(f"[{case.name}] rejected as {type(exc).__name__}: {exc}")
        else:
            pytest.fail(
                f"{case.name} for {traded_instrument.symbol} was NOT rejected "
                f"(qty={order.quantity}, price={order.price})"
            )


async def test_invalid_values_do_not_corrupt_valid_path(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
    reference_price: Decimal,
) -> None:
    """Rejections leave the adapter usable — a follow-up valid order places."""
    spec = await _spec(connected_adapter, traded_instrument)
    for case in invalid_values_from_spec(spec, reference_price):
        order = build_unified_order(
            traded_instrument,
            case.order_type,
            OrderSide.BUY,
            case.quantity if case.quantity is not None else _valid_qty(spec, reference_price),
            client_order_id=random_client_id("spec-invalid"),
            price=case.price,
        )
        with pytest.raises(UteError):
            await connected_adapter.place_order(order)

    valid = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        _valid_qty(spec, reference_price),
        client_order_id=random_client_id("spec-after-invalid"),
        price=_valid_price(spec, reference_price),
    )
    try:
        result = await connected_adapter.place_order(valid)
        assert result.platform_order_id is not None
    finally:
        with contextlib.suppress(OrderNotFoundError):
            assert valid.client_order_id is not None
            await connected_adapter.cancel_order(valid.client_order_id)
        await cleanup_open_orders(connected_adapter)


def test_supported_order_types_declared(
    connected_adapter: BybitAdapter,
) -> None:
    """Declared capability set is exactly the four guaranteed types."""
    assert connected_adapter.supported_order_types() == frozenset(
        {
            OrderType.MARKET,
            OrderType.LIMIT,
            OrderType.STOP,
            OrderType.STOP_LIMIT,
        }
    )


async def test_supported_order_types_accepted_live(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
    linear_reference_price: Decimal,
) -> None:
    """Each declared order type is actually accepted on a live linear symbol."""
    spec = await _spec(connected_adapter, linear_instrument)
    qty = _valid_qty(spec, linear_reference_price)
    price = _valid_price(spec, linear_reference_price)

    for order_type in connected_adapter.supported_order_types():
        kwargs: dict[str, Any] = {
            "client_order_id": random_client_id(f"capacity-{order_type.value}"),
        }
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            kwargs["price"] = price
        if order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            kwargs["stop_price"] = price * Decimal("1.5")
        order = build_unified_order(
            linear_instrument,
            order_type,
            OrderSide.BUY,
            qty,
            **kwargs,
        )
        try:
            result = await connected_adapter.place_order(order)
            assert result.platform_order_id is not None
        finally:
            with contextlib.suppress(Exception):
                await connected_adapter.cancel_order(order.client_order_id)
    await cleanup_open_orders(connected_adapter)

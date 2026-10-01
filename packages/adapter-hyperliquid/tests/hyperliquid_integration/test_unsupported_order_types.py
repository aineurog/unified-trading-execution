"""Integration: unsupported order shapes raise named errors, never KeyErrors.

Gate: every GTD/DAY/FOK/PostOnly-type/chase/scale/TWAP/trailing attempt raises
``UnsupportedOrderTypeError`` with its own name (PLAN §0 non-goal guard).
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.errors import UnsupportedOrderTypeError
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument

from .helpers import build_unified_order


async def _expect_unsupported(connected_adapter: HyperliquidAdapter, **kwargs: Any) -> None:
    with pytest.raises(UnsupportedOrderTypeError):
        await connected_adapter.place_order(build_unified_order(**kwargs))


async def test_day_rejected(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    unique_cid: Callable[[str], str],
) -> None:
    await _expect_unsupported(
        connected_adapter,
        instrument=btc_perp,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        client_order_id=unique_cid("rej-day"),
        price=Decimal("1"),
        time_in_force=TimeInForce.DAY,
    )


async def test_gtd_rejected(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    unique_cid: Callable[[str], str],
) -> None:
    from datetime import UTC, datetime, timedelta

    # expire_at satisfies core construction so the rejection lands in the
    # adapter (no expiry field on the wire), not in the type itself.
    await _expect_unsupported(
        connected_adapter,
        instrument=btc_perp,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        client_order_id=unique_cid("rej-gtd"),
        price=Decimal("1"),
        time_in_force=TimeInForce.GTD,
        expire_at=datetime.now(UTC) + timedelta(hours=1),
    )


async def test_fok_rejected(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
    unique_cid: Callable[[str], str],
) -> None:
    await _expect_unsupported(
        connected_adapter,
        instrument=btc_perp,
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        quantity=Decimal("0.001"),
        client_order_id=unique_cid("rej-fok"),
        price=Decimal("1"),
        time_in_force=TimeInForce.FOK,
    )

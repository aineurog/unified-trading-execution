"""Unit tests for position opened_at field."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from unified_trading_execution.hyperliquid.streams import translate_position
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _instrument(**kwargs: object) -> Instrument:
    """Build an Instrument with given overrides."""
    defaults = {
        "symbol": "BTC",
        "quote_currency": "USDC",
        "asset_class": AssetClass.FUTURES,
        "currency": "USDC",
        "multiplier": 1,
    }
    defaults.update(kwargs)
    return Instrument(**defaults)


def test_position_opened_at_is_none():
    """Hyperliquid positions have opened_at=None (venue does not provide this)."""
    entry = {
        "type": "oneWay",
        "position": {
            "coin": "BTC",
            "szi": "1.0",
            "entryPx": "60000",
            "liquidationPx": "30000",
            "marginUsed": "500",
            "unrealizedPnl": "1000",
        },
    }

    timestamp = datetime.now(UTC)
    position = translate_position(entry, instrument=_instrument(), timestamp=timestamp)

    assert position.opened_at is None
    assert position.position_id == "BTC:oneWay"
    assert position.quantity == Decimal("1.0")

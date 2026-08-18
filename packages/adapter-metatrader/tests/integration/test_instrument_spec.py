"""Instrument spec integration tests (implementation plan, Step 20).

Fetches trading rules for known symbols from a live MT5 demo account and
verifies the unified ``InstrumentSpec`` is fully populated and cached.
Also proves an unknown symbol raises ``InvalidSymbolError``.

Requires the ``MT5_LOGIN`` / ``MT5_PASSWORD`` / ``MT5_SERVER`` env vars.
Broker symbols default to ``EURUSD`` / ``XAUUSD`` and can be overridden via
``MT5_SYMBOL`` / ``MT5_SYMBOL_XAU``.
"""

from __future__ import annotations

import os

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_BROKER_SYMBOLS = {
    "EUR/USD": os.getenv("MT5_SYMBOL", "EURUSD").strip(),
    "XAU/USD": os.getenv("MT5_SYMBOL_XAU", "XAUUSD").strip(),
}
_EURUSD = Instrument(symbol="EUR", quote_currency="USD", asset_class=AssetClass.MARGIN_FX)
_XAUUSD = Instrument(symbol="XAU", quote_currency="USD", asset_class=AssetClass.MARGIN_FX)


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    """Override the shared config with the pair aliases under test."""
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
        symbol_alias_table=_BROKER_SYMBOLS,
    )


async def _assert_populated(spec) -> None:
    assert spec.tick_size > 0
    assert spec.lot_size > 0
    assert spec.min_qty > 0
    assert spec.max_qty >= spec.min_qty
    assert spec.min_notional == 0  # D-5: MT5 has no broker-enforced notional floor
    assert spec.price_precision >= 0
    assert spec.qty_precision >= 0
    # D-9: leverage is account-level for MT5 — no symbol-level max.
    assert spec.max_leverage is None


async def test_spec_fields_populated(connected_adapter: MT5Adapter) -> None:
    """EUR/USD spec carries valid, populated trading rules."""
    spec = await connected_adapter.fetch_instrument_spec(_EURUSD)
    await _assert_populated(spec)


async def test_second_symbol_spec_populated(connected_adapter: MT5Adapter) -> None:
    """XAU/USD spec is populated independently of EUR/USD."""
    spec = await connected_adapter.fetch_instrument_spec(_XAUUSD)
    await _assert_populated(spec)


async def test_unknown_symbol_raises_invalid_symbol(connected_adapter: MT5Adapter) -> None:
    """A symbol the broker does not list raises InvalidSymbolError."""
    unknown = Instrument(symbol="ZZZ", quote_currency="ZZZ", asset_class=AssetClass.MARGIN_FX)
    with pytest.raises(InvalidSymbolError):
        await connected_adapter.fetch_instrument_spec(unknown)


async def test_spec_cached_across_calls(connected_adapter: MT5Adapter) -> None:
    """Repeated fetches of the same instrument return the cached spec."""
    first = await connected_adapter.fetch_instrument_spec(_EURUSD)
    second = await connected_adapter.fetch_instrument_spec(_EURUSD)
    assert first is second


"""Instrument-spec integration tests — live spec vs platform, cache paths.

Verifies ``fetch_instrument_spec`` returns an ``InstrumentSpec`` consistent
with the platform's real priceFilter/lotSizeFilter, that repeat calls are
served from the cache, and that reconnect / explicit invalidation revert to a
fresh fetch (Section 17.3).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec

from .helpers import assert_is_decimal


async def _live_listing(
    adapter: BybitAdapter,
    instrument: Instrument,
) -> dict[str, Any]:
    """Fetch the raw Bybit instrument listing for ``instrument``."""
    category = adapter._instrument_to_category(instrument)
    symbol = f"{instrument.symbol}{instrument.quote_currency}"
    data, _ = await adapter._run_request(
        adapter._session.get_instruments_info,
        category=category,
        symbol=symbol,
    )
    listings: list[dict[str, Any]] = (data.get("result") or {}).get("list") or []
    assert listings, f"No instrument listing for {symbol}"
    return cast(dict[str, Any], listings[0])


def _assert_spec_matches_live(spec: InstrumentSpec, listing: dict[str, Any], category: str) -> None:
    """Assert the unified spec is consistent with the platform filters."""
    lot_filter = listing.get("lotSizeFilter") or {}
    price_filter = listing.get("priceFilter") or {}

    assert_is_decimal(spec.tick_size, "tick_size")
    assert_is_decimal(spec.lot_size, "lot_size")
    assert_is_decimal(spec.min_qty, "min_qty")
    assert_is_decimal(spec.max_qty, "max_qty")
    assert_is_decimal(spec.min_notional, "min_notional")
    assert spec.tick_size > 0
    assert spec.lot_size > 0
    assert spec.price_precision == -int(spec.tick_size.as_tuple().exponent)
    assert spec.qty_precision == -int(spec.lot_size.as_tuple().exponent)

    expected_lot = Decimal(
        str(lot_filter.get("basePrecision" if category == "spot" else "qtyStep", "1"))
    )
    assert spec.lot_size == expected_lot, f"lot_size {spec.lot_size} != live {expected_lot}"
    assert spec.tick_size == Decimal(str(price_filter.get("tickSize", "1")))
    assert spec.min_qty == Decimal(str(lot_filter.get("minOrderQty", "0")))
    assert spec.max_qty == Decimal(str(lot_filter.get("maxOrderQty", "0")))
    assert spec.min_notional == Decimal(str(lot_filter.get("minNotionalValue", "0")))


async def test_spec_matches_live_instrument(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    listing = await _live_listing(connected_adapter, traded_instrument)
    category = connected_adapter._instrument_to_category(traded_instrument)
    spec = await connected_adapter.fetch_instrument_spec(traded_instrument)
    _assert_spec_matches_live(spec, listing, category)


async def test_spec_cached_across_repeated_calls(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    first = await connected_adapter.fetch_instrument_spec(traded_instrument)
    second = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert first is second, "repeated fetch_instrument_spec must hit the cache"


async def test_direct_invalidation_reverts_to_fresh(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    listing = await _live_listing(connected_adapter, traded_instrument)
    category = connected_adapter._instrument_to_category(traded_instrument)

    first = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert traded_instrument in connected_adapter._instrument_specs

    connected_adapter._invalidate_instrument_spec(traded_instrument)
    assert traded_instrument not in connected_adapter._instrument_specs

    fresh = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert fresh is not first  # a genuinely fresh fetch, not the cached object
    _assert_spec_matches_live(fresh, listing, category)
    assert traded_instrument in connected_adapter._instrument_specs


async def test_invalidation_of_uncached_is_noop(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    connected_adapter._invalidate_instrument_spec(traded_instrument)
    assert traded_instrument not in connected_adapter._instrument_specs


async def test_reconnect_refreshes_registry(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    assert connected_adapter._instruments, "registry should be populated on connect"

    await connected_adapter.disconnect()
    await connected_adapter.connect()

    assert connected_adapter._instruments, "registry must be repopulated on reconnect"
    category = connected_adapter._instrument_to_category(traded_instrument)
    symbol = f"{traded_instrument.symbol}{traded_instrument.quote_currency}"
    resolved = connected_adapter._resolve_instrument(symbol, category)
    assert resolved == traded_instrument


async def test_reconnect_spec_still_available(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    before = await connected_adapter.fetch_instrument_spec(traded_instrument)

    await connected_adapter.disconnect()
    await connected_adapter.connect()

    after = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert after == before, "spec must still resolve after a reconnect"

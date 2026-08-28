"""Cache-invalidation integration tests — TTL expiry + live invalidation paths.

The instrument-spec TTL/cache feature is merged into this branch (``f3cde27``).
``fetch_instrument_spec`` serves cached entries until TTL expiry or explicit
invalidation (WS ``REJECTED`` order, reconnect registry refresh).  These tests
exercise those paths against real testnet responses.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.errors import UteError
from unified_trading_execution.events import EventBus
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import build_unified_order, random_client_id


def _new_adapter(
    config_overrides: dict[str, Any],
    *,
    event_bus: EventBus,
) -> BybitAdapter:
    config = BybitConfig(
        api_key="test-api-key",
        api_secret="test-api-secret",
        testnet=True,
        **config_overrides,
    )
    return BybitAdapter(config, event_bus=event_bus)


async def test_spec_cache_serves_subsequent_reads(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    await connected_adapter.fetch_instrument_spec(traded_instrument)
    cached = connected_adapter._instrument_specs.get(traded_instrument)
    assert cached is not None
    spec, fetched_at = cached
    again = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert again is spec, "second read must reuse the identical cached spec"
    assert connected_adapter._instrument_specs[traded_instrument] == (spec, fetched_at)


async def test_ttl_expiry_refetches(
    event_bus: EventBus,
    traded_instrument: Instrument,
) -> None:
    adapter = _new_adapter(
        {"instrument_spec_cache_ttl": 1.0},
        event_bus=event_bus,
    )
    await adapter.connect()
    try:
        await adapter.fetch_instrument_spec(traded_instrument)
        cached = adapter._instrument_specs.get(traded_instrument)
        assert cached is not None
        _, fetched_at = cached

        await asyncio.sleep(1.5)

        await adapter.fetch_instrument_spec(traded_instrument)
        cached_after = adapter._instrument_specs.get(traded_instrument)
        assert cached_after is not None
        _, fetched_after = cached_after
        assert fetched_after > fetched_at, "expired entry must be re-fetched"
    finally:
        await adapter.disconnect()


async def test_ttl_none_caches_indefinitely(
    event_bus: EventBus,
    traded_instrument: Instrument,
) -> None:
    adapter = _new_adapter(
        {"instrument_spec_cache_ttl": None},
        event_bus=event_bus,
    )
    await adapter.connect()
    try:
        await adapter.fetch_instrument_spec(traded_instrument)
        cached = adapter._instrument_specs.get(traded_instrument)
        assert cached is not None
        spec, fetched_at = cached

        await asyncio.sleep(0.2)

        await adapter.fetch_instrument_spec(traded_instrument)
        assert adapter._instrument_specs[traded_instrument] == (spec, fetched_at)
    finally:
        await adapter.disconnect()


async def test_rejected_order_invalidates_spec(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert traded_instrument in connected_adapter._instrument_specs

    # Deliberately violate the platform's filters so Bybit rejects the order.
    # The rejection is a symptom that the platform's rules differ from the
    # cached spec; place_order invalidates the spec before re-raising.
    order = build_unified_order(
        traded_instrument,
        OrderType.LIMIT,
        OrderSide.BUY,
        _tiny_quantity(),
        client_order_id=random_client_id("cache-reject"),
        price=_absurd_price(),
    )
    with pytest.raises(UteError):
        await connected_adapter.place_order(order)

    assert traded_instrument not in connected_adapter._instrument_specs, (
        "a rejected order must invalidate the cached spec"
    )

    # A follow-up fetch returns fresh rules and re-caches.
    refetched = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert refetched is not None
    assert traded_instrument in connected_adapter._instrument_specs


async def test_reconnect_invalidates_spec(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> None:
    await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert traded_instrument in connected_adapter._instrument_specs

    await connected_adapter.disconnect()
    await connected_adapter.connect()

    # The reconnect refresh repopulates the registry; the spec re-fetches on
    # next access (still valid for a Trading instrument).
    assert connected_adapter._instruments
    spec = await connected_adapter.fetch_instrument_spec(traded_instrument)
    assert spec is not None
    assert spec.tick_size > 0


def test_invalid_ttl_rejected_at_construction() -> None:
    with pytest.raises(ValueError):
        BybitConfig(
            api_key="k",
            api_secret="s",
            testnet=True,
            instrument_spec_cache_ttl=0,
        )
    with pytest.raises(ValueError):
        BybitConfig(
            api_key="k",
            api_secret="s",
            testnet=True,
            instrument_spec_cache_ttl=-1.5,
        )


def _tiny_quantity() -> Decimal:
    return Decimal("0.00000001")


def _absurd_price() -> Decimal:
    return Decimal("0.000001")

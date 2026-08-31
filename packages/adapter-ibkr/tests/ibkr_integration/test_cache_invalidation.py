"""Spec cache — hit, TTL, and invalidation."""

from __future__ import annotations

import asyncio

import pytest

from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _stock() -> Instrument:
    return Instrument(symbol="AAPL", asset_class=AssetClass.STOCK, currency="USD")


async def test_spec_cache_hit(connected_adapter: IBKRAdapter) -> None:
    inst = _stock()
    spec = await connected_adapter.fetch_instrument_spec(inst)
    cached = connected_adapter._spec_cache.get(inst)
    assert cached is not None
    spec2 = await connected_adapter.fetch_instrument_spec(inst)
    assert spec2 is spec


async def test_ttl_expiry(ibkr_config: IBKRConfig) -> None:
    from unified_trading_execution.events import EventBus

    cfg = IBKRConfig(
        host=ibkr_config.host,
        port=ibkr_config.port,
        client_id=ibkr_config.client_id + 1,
        account=ibkr_config.account,
        instrument_spec_cache_ttl=1.0,
    )
    adapter = IBKRAdapter(cfg, event_bus=EventBus())
    await adapter.connect()
    try:
        inst = _stock()
        await adapter.fetch_instrument_spec(inst)
        _, fetched_at = adapter._spec_cache[inst]
        await asyncio.sleep(1.4)
        await adapter.fetch_instrument_spec(inst)
        _, fetched_after = adapter._spec_cache[inst]
        assert fetched_after > fetched_at
    finally:
        await adapter.disconnect()


async def test_invalidate_on_demand(connected_adapter: IBKRAdapter) -> None:
    inst = _stock()
    await connected_adapter.fetch_instrument_spec(inst)
    assert inst in connected_adapter._spec_cache
    connected_adapter._invalidate_spec_cache(inst)
    assert inst not in connected_adapter._spec_cache
    spec = await connected_adapter.fetch_instrument_spec(inst)
    assert spec.tick_size > 0


async def test_invalid_ttl_rejected() -> None:
    with pytest.raises(ValueError):
        IBKRConfig(host="127.0.0.1", port=4002, client_id=1, instrument_spec_cache_ttl=0)
    with pytest.raises(ValueError):
        IBKRConfig(host="127.0.0.1", port=4002, client_id=1, instrument_spec_cache_ttl=-1)

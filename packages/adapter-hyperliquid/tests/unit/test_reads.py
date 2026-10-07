"""Unit tests for reads (transport mocked, no network)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.hyperliquid import HyperliquidAdapter
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _perp() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


def _spot() -> Instrument:
    return Instrument(symbol="HYPE", quote_currency="USDC", asset_class=AssetClass.SPOT)


def _connected(adapter: HyperliquidAdapter) -> MagicMock:
    exchange = MagicMock()
    adapter._exchange = exchange
    adapter._connected = True
    return exchange


async def test_fetch_instrument_spec_perp_caches(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.meta.return_value = {
        "universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]
    }
    first = await adapter.fetch_instrument_spec(_perp())
    assert first.tick_size == Decimal("0.1")
    assert first.lot_size == Decimal("0.00001")
    assert first.min_qty == Decimal("0.00001")
    assert first.min_notional == Decimal("10")
    assert first.max_leverage == Decimal("40")
    assert (first.price_precision, first.qty_precision) == (1, 5)
    second = await adapter.fetch_instrument_spec(_perp())
    assert second == first
    assert exchange.info.meta.call_count == 1


async def test_fetch_instrument_spec_spot(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.name_to_coin = {"HYPE/USDC": "@107", "@107": "@107"}
    exchange.info.spot_meta.return_value = {
        "universe": [{"name": "@107", "index": 107, "tokens": [150, 0]}],
        "tokens": [{"index": 0, "name": "USDC"}, {"index": 150, "name": "HYPE", "szDecimals": 3}],
    }
    spec = await adapter.fetch_instrument_spec(_spot())
    assert spec.lot_size == Decimal("0.001")
    assert spec.max_leverage is None


async def test_fetch_instrument_spec_unknown(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.meta.return_value = {"universe": []}

    with pytest.raises(InvalidSymbolError):
        await adapter.fetch_instrument_spec(_perp())


async def test_fetch_ticker(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.l2_snapshot.return_value = {
        "levels": [[{"px": "99", "sz": "1", "n": 1}], [{"px": "101", "sz": "1", "n": 1}]]
    }
    exchange.info.meta_and_asset_ctxs.return_value = (
        {"universe": [{"name": "BTC"}]},
        [{"markPx": "100"}],
    )
    ticker = await adapter.fetch_ticker(_perp())
    assert ticker is not None
    assert (ticker.bid, ticker.ask, ticker.mark) == (Decimal("99"), Decimal("101"), Decimal("100"))
    assert ticker.last is None


async def test_fetch_ticker_empty_book(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.l2_snapshot.return_value = {"levels": [[], []]}
    assert await adapter.fetch_ticker(_perp()) is None


async def test_fetch_ticker_spot_resolves_alias(adapter: HyperliquidAdapter) -> None:
    """Spot ctxs key by venue alias (@107), not the pair spelling."""
    exchange = _connected(adapter)
    exchange.info.name_to_coin = {"HYPE/USDC": "@107", "@107": "@107"}
    exchange.info.l2_snapshot.return_value = {
        "levels": [[{"px": "88.9", "sz": "1", "n": 1}], [{"px": "89.1", "sz": "1", "n": 1}]]
    }
    exchange.info.spot_meta_and_asset_ctxs.return_value = (
        {"universe": [{"name": "@107", "index": 107, "tokens": [150, 0]}], "tokens": []},
        [{"coin": "@107", "markPx": "89.0"}],
    )
    ticker = await adapter.fetch_ticker(_spot())
    assert ticker is not None
    assert (ticker.bid, ticker.ask, ticker.mark) == (
        Decimal("88.9"),
        Decimal("89.1"),
        Decimal("89.0"),
    )


async def test_fetch_positions_skips_flat(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {
        "time": 1724361546645,
        "assetPositions": [
            {"type": "oneWay", "position": {"coin": "BTC", "szi": "0.5", "entryPx": "100"}},
            {"type": "oneWay", "position": {"coin": "ETH", "szi": "0", "entryPx": "0"}},
        ],
    }
    positions = await adapter.fetch_positions()
    assert len(positions) == 1
    assert positions[0].quantity == Decimal("0.5")
    assert positions[0].position_id == "BTC:oneWay"


async def test_fetch_balances(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.spot_user_state.return_value = {
        "balances": [{"coin": "USDC", "total": "10", "hold": "4", "entryNtl": "0"}]
    }
    balances = await adapter.fetch_balances()
    assert balances["USDC"].free == Decimal("6")


async def test_fetch_open_orders_keys(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.name_to_coin = {}
    exchange.info.open_orders.return_value = [
        {
            "coin": "BTC",
            "side": "B",
            "limitPx": "1",
            "sz": "1",
            "oid": 1,
            "timestamp": 1,
            "cloid": "0x" + "ab" * 16,
        }
    ]
    exchange.info.frontend_open_orders.return_value = [
        {
            "coin": "BTC",
            "side": "A",
            "limitPx": "2",
            "sz": "1",
            "oid": 2,
            "timestamp": 1,
        }
    ]
    orders = await adapter.fetch_open_orders()
    assert set(orders) == {"0x" + "ab" * 16, "2"}
    # Unmapped cloids keep their raw id in the record — the rewrite fires
    # only on map resolution (contrast the hashed-cid test below).
    assert orders["0x" + "ab" * 16].client_order_id == "0x" + "ab" * 16


async def test_fetch_open_orders_excludes_bracket_children(adapter: HyperliquidAdapter) -> None:
    """A bracket's TP/SL children must not mask its parent entry order."""
    from unified_trading_execution.hyperliquid.orders import (
        SL_CLOID_SUFFIX,
        TP_CLOID_SUFFIX,
        client_order_id_to_cloid,
    )

    exchange = _connected(adapter)
    exchange.info.name_to_coin = {}
    client_id = "0x" + "ab" * 16
    adapter._client_coins[client_id] = ("BTC", False)

    def entry(oid: int, cloid: str, price: int, order_type: str) -> dict[str, object]:
        return {
            "coin": "BTC",
            "side": "B",
            "limitPx": str(price),
            "sz": "0.01",
            "oid": oid,
            "timestamp": oid,
            "origSz": "0.01",
            "cloid": cloid,
            "orderType": order_type,
        }

    bracket = [
        entry(11, client_order_id_to_cloid(client_id), 50000, "Limit"),
        entry(
            12,
            client_order_id_to_cloid(f"{client_id}:{TP_CLOID_SUFFIX}"),
            60000,
            "Take Profit Market",
        ),
        entry(
            13,
            client_order_id_to_cloid(f"{client_id}:{SL_CLOID_SUFFIX}"),
            40000,
            "Stop Market",
        ),
    ]
    exchange.info.open_orders.return_value = bracket
    exchange.info.frontend_open_orders.return_value = bracket
    result = await adapter.fetch_open_orders()
    assert set(result) == {client_id}
    assert (result[client_id].platform_order_id, result[client_id].price) == (
        "11",
        Decimal("50000"),
    )


async def test_fetch_open_orders_restores_hashed_client_id(adapter: HyperliquidAdapter) -> None:
    """A non-hex client id hashes to a venue cloid — the record must carry the
    original id back, agreeing with its dict key, not the venue hex."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    exchange = _connected(adapter)
    exchange.info.name_to_coin = {}
    client_id = "my-order-123"
    adapter._client_coins[client_id] = ("BTC", False)
    hashed = client_order_id_to_cloid(client_id)
    assert hashed != client_id  # non-hex ids hash — the case under test
    exchange.info.open_orders.return_value = [
        {
            "coin": "BTC",
            "side": "B",
            "limitPx": "50000",
            "sz": "0.01",
            "oid": 7,
            "timestamp": 7,
            "origSz": "0.01",
            "cloid": hashed,
            "orderType": "Limit",
        }
    ]
    exchange.info.frontend_open_orders.return_value = []
    result = await adapter.fetch_open_orders()
    assert set(result) == {client_id}
    assert result[client_id].client_order_id == client_id


async def test_fetch_fills_attribution_and_since(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.name_to_coin = {}
    adapter._oid_clients["5"] = ("parent-1", None, None)
    fill = {
        "coin": "BTC",
        "px": "100",
        "sz": "0.01",
        "side": "B",
        "time": 2000,
        "dir": "Open Long",
        "hash": "0xh",
        "oid": 5,
        "fee": "0.01",
        "feeToken": "USDC",
        "tid": 9,
    }
    old = dict(fill)
    old.update({"hash": "0xo", "tid": 8, "time": 1000, "oid": 6})
    exchange.info.user_fills.return_value = [fill, old, fill]
    all_fills = await adapter.fetch_fills()
    assert set(all_fills) == {"parent-1", "6"}
    assert len(all_fills["parent-1"]) == 1  # deduped
    since = datetime(1970, 1, 1, 0, 0, 2, tzinfo=UTC)
    exchange.info.user_fills_by_time.return_value = [fill, old]
    windowed = await adapter.fetch_fills(since=since)
    assert set(windowed) == {"parent-1"}
    exchange.info.user_fills_by_time.assert_called_once()
    args, _ = exchange.info.user_fills_by_time.call_args
    assert args[1] == 2000


async def test_fetch_fills_recovers_position_legs_after_restart(
    adapter: HyperliquidAdapter,
) -> None:
    """Fresh maps (post-restart): open position legs re-anchor fills with reason."""
    from unified_trading_execution.hyperliquid.orders import position_tpsl_cloid
    from unified_trading_execution.types.enums import FillReason

    exchange = _connected(adapter)
    assert adapter._oid_clients == {} and adapter._child_parents == {}
    tp_raw = position_tpsl_cloid("BTC:oneWay", "take_profit")
    exchange.info.user_state.return_value = {"assetPositions": [{"position": {"coin": "BTC"}}]}
    exchange.info.open_orders.return_value = [{"cloid": tp_raw, "oid": 77}]
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [
        {
            "coin": "BTC",
            "px": "100",
            "sz": "0.01",
            "side": "A",
            "time": 2000,
            "dir": "Close Long",
            "hash": "0xh",
            "oid": 77,
            "fee": "0.01",
            "feeToken": "USDC",
            "tid": 9,
        }
    ]
    fills = await adapter.fetch_fills()
    assert set(fills) == {tp_raw}
    (record,) = fills[tp_raw]
    assert record.reason == FillReason.TAKE_PROFIT


async def test_fetch_fills_keeps_zero_tid(adapter: HyperliquidAdapter) -> None:
    """tid == 0 is a real id, not a missing one — the fill must survive."""
    exchange = _connected(adapter)
    exchange.info.name_to_coin = {}
    dust = {
        "coin": "BTC",
        "px": "100",
        "sz": "0.01",
        "side": "B",
        "time": 2000,
        "dir": "Spot Dust Conversion",
        "hash": "0x" + "0" * 64,
        "oid": 7,
        "fee": "0",
        "feeToken": "USDC",
        "tid": 0,
    }
    exchange.info.user_fills.return_value = [dust]
    fills = await adapter.fetch_fills()
    assert sum(len(v) for v in fills.values()) == 1


async def test_spec_cache_ttl_expiry(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.meta.return_value = {
        "universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]
    }
    adapter._spec_ttl = 0.0
    await adapter.fetch_instrument_spec(_perp())
    await adapter.fetch_instrument_spec(_perp())
    assert exchange.info.meta.call_count == 2


def _rest_fill(*, oid: object = 77, tid: int = 9) -> dict[str, object]:
    return {
        "coin": "BTC",
        "px": "100",
        "sz": "0.01",
        "side": "B",
        "time": 2000,
        "dir": "Open Long",
        "hash": "0xh",
        "oid": oid,
        "fee": "0.01",
        "feeToken": "USDC",
        "tid": tid,
    }


def _oid_envelope(cloid: str) -> dict[str, object]:
    return {"order": {"order": {"coin": "BTC", "cloid": cloid}}}


async def test_fetch_fills_resolves_unknown_oid(adapter: HyperliquidAdapter) -> None:
    """Issue 16B: an ack-missed oid resolves via orderStatus-by-oid to our cid."""
    from unified_trading_execution.hyperliquid.orders import client_order_id_to_cloid

    exchange = _connected(adapter)
    adapter._client_coins["my-order-123"] = ("BTC", False)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [_rest_fill()]
    exchange.info.query_order_by_oid.return_value = _oid_envelope(
        client_order_id_to_cloid("my-order-123")
    )
    fills = await adapter.fetch_fills()
    assert set(fills) == {"my-order-123"}
    assert adapter._oid_clients["77"][0] == "my-order-123"
    await adapter.fetch_fills()  # indexed now — no second resolve read
    assert exchange.info.query_order_by_oid.call_count == 1


async def test_fetch_fills_caches_unresolvable_oid(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [_rest_fill()]
    exchange.info.query_order_by_oid.return_value = {"status": "unknownOid"}
    assert set(await adapter.fetch_fills()) == {"77"}
    assert "77" in adapter._oid_unresolvable
    await adapter.fetch_fills()
    assert exchange.info.query_order_by_oid.call_count == 1  # never re-paid


async def test_fetch_fills_transport_failure_retries(adapter: HyperliquidAdapter) -> None:
    from hyperliquid.utils.error import ServerError

    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [_rest_fill()]
    exchange.info.query_order_by_oid.side_effect = ServerError(500, "boom")
    assert set(await adapter.fetch_fills()) == {"77"}
    assert "77" not in adapter._oid_unresolvable  # transient — retry next read
    await adapter.fetch_fills()
    assert exchange.info.query_order_by_oid.call_count == 2


async def test_fetch_fills_non_numeric_oid_skips_query(adapter: HyperliquidAdapter) -> None:
    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [_rest_fill(oid="abc")]
    assert set(await adapter.fetch_fills()) == {"abc"}
    assert exchange.info.query_order_by_oid.call_count == 0


async def test_fetch_fills_seed_skips_oid_resolution(adapter: HyperliquidAdapter) -> None:
    """Startup seeding consumes ids only — no per-oid reads, however many unknowns."""
    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills.return_value = [_rest_fill(), _rest_fill(oid=78, tid=10)]
    await adapter.fetch_fills(resolve_unknown_oids=False)
    assert exchange.info.query_order_by_oid.call_count == 0
    assert adapter._oid_unresolvable == set()


async def test_fetch_fills_gap_window_scopes_unknowns(adapter: HyperliquidAdapter) -> None:
    """Gap cover reads the outage window only — bounded unknowns per call."""
    exchange = _connected(adapter)
    exchange.info.user_state.return_value = {}
    exchange.info.open_orders.return_value = []
    exchange.info.frontend_open_orders.return_value = []
    exchange.info.user_fills_by_time.return_value = [_rest_fill(), _rest_fill(oid=78, tid=10)]
    exchange.info.query_order_by_oid.return_value = {"status": "unknownOid"}
    await adapter._absorb_fills(publish_unseen=True, since=datetime.now(UTC))
    assert exchange.info.user_fills_by_time.call_count == 1
    assert exchange.info.user_fills.call_count == 0
    assert exchange.info.query_order_by_oid.call_count == 2

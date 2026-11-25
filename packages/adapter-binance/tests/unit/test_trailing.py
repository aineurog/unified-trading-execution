"""Unit coverage for Binance Futures native trailing stops."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from unified_trading_execution.binance import BinanceAdapter
from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.binance.protection import parse_position_id
from unified_trading_execution.binance.trailing import (
    TRAILING_CLIENT_ID_PREFIX,
    TRAILING_ORDER_TYPE,
    build_trailing_algo_payload,
    is_trailing_client_id,
    parse_trailing_entry,
    trailing_client_id,
)
from unified_trading_execution.errors import (
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.trailing.types import (
    NativeTrailingRequest,
    TrailingActivation,
    TrailingBasis,
    TrailingOffsetType,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
)
from unified_trading_execution.types.instrument import Instrument

_TIMESTAMP = 1_684_738_540_559
_POSITION_ID = "usd_m:BTCUSDT:BOTH"


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        currency="USDT",
        asset_class=AssetClass.FUTURES,
        multiplier=1,
        platform_symbol="BTCUSDT",
    )


def _coin_m_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        currency="BTC",
        asset_class=AssetClass.FUTURES,
        multiplier=100,
        platform_symbol="BTCUSD_PERP",
    )


def _spot_instrument() -> Instrument:
    return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)


def _leg():
    return parse_position_id(BinanceProduct.USD_M, _POSITION_ID)


def _request(**overrides: Any) -> NativeTrailingRequest:
    kwargs: dict[str, Any] = {
        "instrument": _instrument(),
        "position_id": _POSITION_ID,
        "side": OrderSide.BUY,
        "quantity": Decimal("0.001"),
        "offset": Decimal("5"),
        "offset_type": TrailingOffsetType.PERCENT,
        "basis": TrailingBasis.MARK,
        "activation_price": None,
        "client_id": "trail-1",
    }
    kwargs.update(overrides)
    return NativeTrailingRequest(**kwargs)


def _metadata() -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "status": "TRADING",
        "contractType": "PERPETUAL",
        "marginAsset": "USDT",
    }


def _algo_entry(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "algoId": 555,
        "clientAlgoId": "trl-u-BTCUSDT-BOTH",
        "algoStatus": "NEW",
        "symbol": "BTCUSDT",
        "side": "SELL",
        "positionSide": "BOTH",
        "type": TRAILING_ORDER_TYPE,
        "algoType": "CONDITIONAL",
        "quantity": "0.001",
        "callbackRate": "5",
        "workingType": "MARK_PRICE",
    }
    entry.update(overrides)
    return entry


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_trailing_client_id_is_deterministic_and_reserved() -> None:
    leg = _leg()
    assert trailing_client_id(leg) == "trl-u-BTCUSDT-BOTH"
    assert is_trailing_client_id(trailing_client_id(leg))
    assert TRAILING_CLIENT_ID_PREFIX == "trl-"


def test_build_trailing_payload_market_immediate() -> None:
    payload = build_trailing_algo_payload(
        leg=_leg(),
        leg_quantity=Decimal("0.001"),
        request_side=OrderSide.BUY,
        offset=Decimal("5"),
        activation_price=None,
        basis=TrailingBasis.MARK,
    )

    assert payload["type"] == TRAILING_ORDER_TYPE
    assert payload["algoType"] == "CONDITIONAL"
    assert payload["side"] == "SELL"
    assert payload["positionSide"] == "BOTH"
    assert payload["quantity"] == "0.001"
    assert payload["callbackRate"] == "5"
    assert payload["clientAlgoId"] == "trl-u-BTCUSDT-BOTH"
    assert payload["newOrderRespType"] == "RESULT"
    assert payload["reduceOnly"] == "true"
    assert payload["workingType"] == "MARK_PRICE"
    assert "activatePrice" not in payload
    assert "closePosition" not in payload
    assert "triggerPrice" not in payload


def test_build_trailing_payload_activation_and_last_basis() -> None:
    payload = build_trailing_algo_payload(
        leg=_leg(),
        leg_quantity=Decimal("-0.002"),
        request_side=OrderSide.SELL,
        offset=Decimal("0.5"),
        activation_price=Decimal("60000"),
        basis=TrailingBasis.LAST,
    )

    assert payload["side"] == "BUY"
    assert payload["quantity"] == "0.002"
    assert payload["callbackRate"] == "0.5"
    assert payload["activatePrice"] == "60000"
    assert payload["workingType"] == "CONTRACT_PRICE"


@pytest.mark.parametrize("offset", [Decimal("0.05"), Decimal("10.5"), Decimal("0")])
def test_build_trailing_payload_rejects_out_of_band_callback_rate(
    offset: Decimal,
) -> None:
    with pytest.raises(UnsupportedOrderTypeError, match="callbackRate"):
        build_trailing_algo_payload(
            leg=_leg(),
            leg_quantity=Decimal("0.001"),
            request_side=OrderSide.BUY,
            offset=offset,
            activation_price=None,
            basis=None,
        )


def test_build_trailing_payload_rejects_unmappable_basis() -> None:
    with pytest.raises(UnsupportedOrderTypeError, match="cannot trigger"):
        build_trailing_algo_payload(
            leg=_leg(),
            leg_quantity=Decimal("0.001"),
            request_side=OrderSide.BUY,
            offset=Decimal("5"),
            activation_price=None,
            basis=TrailingBasis.BID,
        )


# ---------------------------------------------------------------------------
# Entry parsing
# ---------------------------------------------------------------------------


def test_parse_trailing_entry_reads_callback_rate() -> None:
    result = parse_trailing_entry(_algo_entry())

    assert result is not None
    assert result.native_id == "trl-u-BTCUSDT-BOTH"
    assert result.confirmed_offset == Decimal("5")
    assert result.current_stop is None
    assert result.activated is True
    assert result.position_scoped is True


def test_parse_trailing_entry_ignores_non_trailing_and_closed() -> None:
    assert parse_trailing_entry(_algo_entry(type="STOP_MARKET")) is None
    assert parse_trailing_entry(_algo_entry(algoStatus="FINISHED")) is None
    assert parse_trailing_entry(_algo_entry(algoStatus="CANCELED")) is None
    assert parse_trailing_entry(_algo_entry(orderType="TRAILING_STOP_MARKET")) is not None


def test_parse_trailing_entry_filters_position_side() -> None:
    assert parse_trailing_entry(_algo_entry(positionSide="LONG"), position_side="BOTH") is None
    assert parse_trailing_entry(_algo_entry(positionSide="BOTH"), position_side="BOTH") is not None


def test_parse_trailing_entry_falls_back_to_algo_id() -> None:
    entry = _algo_entry()
    entry.pop("clientAlgoId")
    result = parse_trailing_entry(entry)
    assert result is not None
    assert result.native_id == "555"


def test_parse_trailing_entry_rejects_missing_identity() -> None:
    entry = _algo_entry()
    entry.pop("clientAlgoId")
    entry.pop("algoId")
    with pytest.raises(PlatformError, match="no clientAlgoId"):
        parse_trailing_entry(entry)


def test_parse_trailing_entry_rejects_bad_callback_rate() -> None:
    with pytest.raises(PlatformError, match="callbackRate"):
        parse_trailing_entry(_algo_entry(callbackRate="abc"))


# ---------------------------------------------------------------------------
# Capability
# ---------------------------------------------------------------------------


def test_trailing_capability_usd_m_supported() -> None:
    adapter = BinanceAdapter.__new__(BinanceAdapter)
    cap = adapter.trailing_capability(_instrument())

    assert cap.supported is True
    assert cap.offset_types == frozenset({TrailingOffsetType.PERCENT})
    assert cap.bases == frozenset({TrailingBasis.LAST, TrailingBasis.MARK})
    assert cap.activations == frozenset({TrailingActivation.IMMEDIATE, TrailingActivation.PRICE})
    assert cap.position_scoped is True
    assert cap.supports_step is False
    assert cap.supports_take_profit is False


def test_trailing_capability_coin_m_supported() -> None:
    adapter = BinanceAdapter.__new__(BinanceAdapter)
    cap = adapter.trailing_capability(_coin_m_instrument())
    assert cap.supported is True
    assert cap.offset_types == frozenset({TrailingOffsetType.PERCENT})
    assert cap.position_scoped is True


def test_trailing_capability_spot_unsupported() -> None:
    adapter = BinanceAdapter.__new__(BinanceAdapter)
    assert adapter.trailing_capability(_spot_instrument()).supported is False


# ---------------------------------------------------------------------------
# Adapter lifecycle
# ---------------------------------------------------------------------------


async def _connected_adapter(adapter: BinanceAdapter, binance_client_mock: AsyncMock) -> None:
    await adapter.connect()
    adapter._exchange_info_cache[BinanceProduct.USD_M] = [_metadata()]


def _position_entry() -> dict[str, Any]:
    return {
        "symbol": "BTCUSDT",
        "positionAmt": "0.001",
        "positionSide": "BOTH",
        "entryPrice": "60000",
        "updateTime": _TIMESTAMP,
    }


async def test_place_trailing_stop_places_algo_order(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_position_information.return_value = [_position_entry()]
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    binance_client_mock.futures_create_algo_order.return_value = {
        "algoId": 777,
        "clientAlgoId": "trl-u-BTCUSDT-BOTH",
        "algoStatus": "NEW",
    }

    result = await adapter.place_trailing_stop(_request())

    assert result.native_id == "trl-u-BTCUSDT-BOTH"
    assert result.confirmed_offset == Decimal("5")
    assert result.position_scoped is True

    binance_client_mock.futures_create_algo_order.assert_awaited_once()
    payload = binance_client_mock.futures_create_algo_order.await_args.kwargs
    assert payload["type"] == TRAILING_ORDER_TYPE
    assert payload["callbackRate"] == "5"
    assert payload["clientAlgoId"] == "trl-u-BTCUSDT-BOTH"
    assert payload["quantity"] == "0.001"
    assert payload["side"] == "SELL"
    assert payload["positionSide"] == "BOTH"
    assert payload["reduceOnly"] == "true"
    assert payload["workingType"] == "MARK_PRICE"


async def test_place_trailing_stop_replaces_existing_trail(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_position_information.return_value = [_position_entry()]
    existing = _algo_entry(algoId=111)
    binance_client_mock.futures_get_open_algo_orders.side_effect = [
        [existing],  # cancel scan
        [],  # post-cancel verify
        [],  # (unused extra safety)
    ]
    binance_client_mock.futures_create_algo_order.return_value = {
        "algoId": 777,
        "clientAlgoId": "trl-u-BTCUSDT-BOTH",
        "algoStatus": "NEW",
    }

    await adapter.place_trailing_stop(_request())

    binance_client_mock.futures_cancel_algo_order.assert_awaited_once_with(
        symbol="BTCUSDT", algoId=111
    )
    binance_client_mock.futures_create_algo_order.assert_awaited_once()


async def test_place_trailing_stop_payload_failure_keeps_existing_trail(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    """Resolve and validate the replacement before canceling the old trail."""
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_position_information.return_value = [_position_entry()]
    binance_client_mock.futures_get_open_algo_orders.return_value = [_algo_entry(algoId=111)]

    with pytest.raises(UnsupportedOrderTypeError, match="callbackRate"):
        await adapter.place_trailing_stop(_request(offset=Decimal("20")))

    binance_client_mock.futures_cancel_algo_order.assert_not_awaited()
    binance_client_mock.futures_create_algo_order.assert_not_awaited()


async def test_place_trailing_stop_rejects_non_percent_offset(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    with pytest.raises(UnsupportedOrderTypeError, match="percent offsets"):
        await adapter.place_trailing_stop(_request(offset_type=TrailingOffsetType.PRICE))


def _coin_m_metadata() -> dict[str, object]:
    return {
        "symbol": "BTCUSD_PERP",
        "baseAsset": "BTC",
        "quoteAsset": "USD",
        "status": "TRADING",
        "contractType": "PERPETUAL",
        "marginAsset": "BTC",
        "contractSize": "100",
        "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.1"}],
    }


def _coin_m_position_entry() -> dict[str, object]:
    return {
        "symbol": "BTCUSD_PERP",
        "pair": "BTCUSD",
        "positionAmt": "1",
        "positionSide": "BOTH",
        "entryPrice": "64912",
        "updateTime": _TIMESTAMP,
    }


def _coin_m_algo_entry(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "algoId": 556,
        "clientAlgoId": "trl-c-BTCUSD_PERP-BOTH",
        "algoStatus": "NEW",
        "symbol": "BTCUSD_PERP",
        "side": "SELL",
        "positionSide": "BOTH",
        "orderType": TRAILING_ORDER_TYPE,
        "algoType": "CONDITIONAL",
        "quantity": "1",
        "callbackRate": "5.00",
        "activatePrice": "64000",
        "triggerPrice": "60800",
        "workingType": "MARK_PRICE",
        "reduceOnly": True,
        "isActivated": False,
    }
    entry.update(overrides)
    return entry


async def _coin_m_connected_adapter(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    adapter._exchange_info_cache[BinanceProduct.COIN_M] = [_coin_m_metadata()]
    binance_client_mock.futures_coin_position_information.return_value = [_coin_m_position_entry()]


async def test_place_trailing_stop_coin_m_uses_dapi_algo_endpoint(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _coin_m_connected_adapter(adapter, binance_client_mock)

    async def _coin_api(*args: Any, **kwargs: Any) -> Any:
        return [] if args[0] == "get" else {}

    binance_client_mock._request_futures_coin_api.side_effect = _coin_api

    result = await adapter.place_trailing_stop(
        _request(
            instrument=_coin_m_instrument(),
            position_id="coin_m:BTCUSD_PERP:BOTH",
        )
    )

    assert result.native_id == "trl-c-BTCUSD_PERP-BOTH"
    call = binance_client_mock._request_futures_coin_api.await_args_list[-1]
    assert call.args[0] == "post"
    assert call.args[1] == "algoOrder"
    assert call.args[2] is True
    payload = call.kwargs["data"]
    assert payload["type"] == TRAILING_ORDER_TYPE
    assert payload["clientAlgoId"] == "trl-c-BTCUSD_PERP-BOTH"
    assert payload["callbackRate"] == "5"
    assert payload["quantity"] == "1"
    assert payload["reduceOnly"] == "true"
    # Immediate activation omits activatePrice; the venue accepts the
    # omission on /dapi/v1/algoOrder (verified live on Demo).
    assert "activatePrice" not in payload
    assert "newOrderRespType" not in payload


async def test_place_trailing_stop_coin_m_bad_tick_metadata_keeps_existing_trail(
    adapter: BinanceAdapter,
    binance_client_mock: AsyncMock,
) -> None:
    """A failed tick-size lookup must not cancel the trail it would replace."""
    await _coin_m_connected_adapter(adapter, binance_client_mock)
    binance_client_mock._request_futures_coin_api.return_value = [_coin_m_algo_entry()]
    entry = _coin_m_metadata()
    del entry["filters"]
    adapter._exchange_info_cache[BinanceProduct.COIN_M] = [entry]

    with pytest.raises(PlatformError, match="Malformed Binance filters"):
        await adapter.place_trailing_stop(
            _request(
                instrument=_coin_m_instrument(),
                position_id="coin_m:BTCUSD_PERP:BOTH",
                activation_price=Decimal("64000"),
            )
        )

    calls = binance_client_mock._request_futures_coin_api.await_args_list
    assert [call.args[0] for call in calls if call.args and call.args[0] == "delete"] == []


async def test_coin_m_open_algo_orders_dispatch(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _coin_m_connected_adapter(adapter, binance_client_mock)
    binance_client_mock._request_futures_coin_api.return_value = [_coin_m_algo_entry()]

    result = await adapter.get_position_trailing_stop(
        _coin_m_instrument(), "coin_m:BTCUSD_PERP:BOTH"
    )

    assert result is not None
    assert result.native_id == "trl-c-BTCUSD_PERP-BOTH"
    assert result.current_stop == Decimal("60800")
    call = binance_client_mock._request_futures_coin_api.await_args_list[-1]
    assert call.args[:2] == ("get", "openAlgoOrders")
    assert call.kwargs["data"] == {"symbol": "BTCUSD_PERP"}


async def test_place_trailing_stop_fails_on_flat_leg(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_position_information.return_value = [
        {**_position_entry(), "positionAmt": "0"}
    ]
    with pytest.raises(OrderNotFoundError, match="No open usd_m position"):
        await adapter.place_trailing_stop(_request())


async def test_get_position_trailing_stop_reads_foreign_trail(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    foreign = _algo_entry(clientAlgoId="web_foreign123", algoId=999)
    binance_client_mock.futures_get_open_algo_orders.return_value = [
        _algo_entry(type="STOP_MARKET"),
        foreign,
    ]

    result = await adapter.get_position_trailing_stop(_instrument(), _POSITION_ID)

    assert result is not None
    assert result.native_id == "web_foreign123"
    assert result.confirmed_offset == Decimal("5")


async def test_get_position_trailing_stop_none_when_flat_or_absent(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_get_open_algo_orders.return_value = []
    assert await adapter.get_position_trailing_stop(_instrument(), _POSITION_ID) is None


async def test_get_trailing_stop_by_client_and_algo_id(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_get_open_algo_orders.return_value = [_algo_entry()]

    by_client = await adapter.get_trailing_stop("trl-u-BTCUSDT-BOTH", _instrument())
    by_algo = await adapter.get_trailing_stop("555", _instrument())
    missing = await adapter.get_trailing_stop("trl-u-BTCUSDT-BOTH-x", _instrument())

    assert by_client is not None and by_client.native_id == "trl-u-BTCUSDT-BOTH"
    assert by_algo is not None and by_algo.native_id == "trl-u-BTCUSDT-BOTH"
    assert missing is None


async def test_get_trailing_stop_unsupported_product_returns_none(
    adapter: BinanceAdapter,
) -> None:
    assert await adapter.get_trailing_stop("x", _spot_instrument()) is None


async def test_get_trailing_stop_coin_m(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _coin_m_connected_adapter(adapter, binance_client_mock)
    binance_client_mock._request_futures_coin_api.return_value = [_coin_m_algo_entry()]

    result = await adapter.get_trailing_stop("trl-c-BTCUSD_PERP-BOTH", _coin_m_instrument())

    assert result is not None
    assert result.native_id == "trl-c-BTCUSD_PERP-BOTH"


async def test_cancel_trailing_stop_cancels_and_verifies(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_get_open_algo_orders.side_effect = [
        [_algo_entry()],
        [],
    ]

    await adapter.cancel_trailing_stop("trl-u-BTCUSDT-BOTH", _instrument())

    binance_client_mock.futures_cancel_algo_order.assert_awaited_once_with(
        symbol="BTCUSDT", algoId=555
    )


async def test_cancel_trailing_stop_idempotent_when_absent(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _connected_adapter(adapter, binance_client_mock)
    binance_client_mock.futures_get_open_algo_orders.return_value = []

    await adapter.cancel_trailing_stop("trl-u-BTCUSDT-BOTH", _instrument())

    binance_client_mock.futures_cancel_algo_order.assert_not_awaited()


async def test_cancel_trailing_stop_coin_m(
    adapter: BinanceAdapter, binance_client_mock: AsyncMock
) -> None:
    await _coin_m_connected_adapter(adapter, binance_client_mock)
    state = {"cancelled": False}

    async def _side_effect(*args: Any, **kwargs: Any) -> Any:
        if args[0] == "get":
            return [] if state["cancelled"] else [_coin_m_algo_entry()]
        state["cancelled"] = True
        return {"code": "200", "msg": "success"}

    binance_client_mock._request_futures_coin_api.side_effect = _side_effect

    await adapter.cancel_trailing_stop("trl-c-BTCUSD_PERP-BOTH", _coin_m_instrument())

    calls = binance_client_mock._request_futures_coin_api.await_args_list
    delete_calls = [c for c in calls if c.args[0] == "delete"]
    assert len(delete_calls) == 1
    assert delete_calls[0].args[1] == "algoOrder"
    assert delete_calls[0].kwargs["data"] == {"symbol": "BTCUSD_PERP", "algoId": 556}

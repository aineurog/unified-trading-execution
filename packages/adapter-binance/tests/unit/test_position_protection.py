"""Binance position TP/SL protection via USDⓈ-M conditional algo orders."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from binance.exceptions import BinanceAPIException  # type: ignore[import-untyped]

from unified_trading_execution.binance import BinanceAdapter, BinanceProduct
from unified_trading_execution.binance.protection import (
    PositionLeg,
    ProtectionSide,
    algo_order_type,
    build_algo_order_payload,
    build_coin_m_order_payload,
    close_order_side,
    is_protection_client_id,
    parse_position_id,
    parse_protection_entry,
    protection_client_id,
    protection_side_for_type,
    working_type,
)
from unified_trading_execution.errors import (
    InvalidOrderError,
    OrderNotFoundError,
    PlatformError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import OrderPlacedEvent
from unified_trading_execution.trailing.types import TrailingBasis
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import TpSlAttachment, UnifiedOrder

_POSITION_ID = "usd_m:BTCUSDT:LONG"
_COIN_M_POSITION_ID = "coin_m:BTCUSD_PERP:BOTH"
_TP_ID = "tpsl-u-BTCUSDT-LONG-tp"
_SL_ID = "tpsl-u-BTCUSDT-LONG-sl"
_COIN_M_TP_ID = "tpsl-c-BTCUSD_PERP-BOTH-tp"
_COIN_M_SL_ID = "tpsl-c-BTCUSD_PERP-BOTH-sl"


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT",
    )


def _usd_m_instrument() -> Instrument:
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


def _exchange_info() -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
            {
                "filterType": "LOT_SIZE",
                "stepSize": "0.001",
                "minQty": "0.001",
                "maxQty": "1000",
            },
        ],
    }


def _position_entry(
    *,
    amount: str = "0.5",
    side: str = "LONG",
) -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "positionAmt": amount,
        "positionSide": side,
        "entryPrice": "60000",
        "updateTime": 1_700_000_000_000,
    }


def _coin_m_position_entry(*, amount: str = "1") -> dict[str, object]:
    return {
        "symbol": "BTCUSD_PERP",
        "pair": "BTCUSD",
        "positionAmt": amount,
        "positionSide": "BOTH",
        "entryPrice": "64912",
        "updateTime": 1_700_000_000_000,
    }


def _algo_entry(
    client_algo_id: str,
    *,
    order_type: str = "STOP_MARKET",
    trigger: str = "55000",
    price: str | None = None,
    status: str = "NEW",
) -> dict[str, object]:
    entry: dict[str, object] = {
        "algoId": 9001,
        "clientAlgoId": client_algo_id,
        "algoType": "CONDITIONAL",
        "type": order_type,
        "symbol": "BTCUSDT",
        "side": "SELL",
        "positionSide": "LONG",
        "algoStatus": status,
        "triggerPrice": trigger,
    }
    if price is not None:
        entry["price"] = price
    return entry


def _coin_m_order_entry(
    client_order_id: str = _COIN_M_SL_ID,
    *,
    order_type: str = "STOP_MARKET",
    trigger: str = "32456",
    price: str | None = None,
    status: str = "NEW",
) -> dict[str, object]:
    entry: dict[str, object] = {
        "orderId": 1234,
        "clientOrderId": client_order_id,
        "status": status,
        "type": order_type,
        "executedQty": "0",
        "avgPrice": "0",
        "time": 1_700_000_000_000,
        "updateTime": 1_700_000_000_000,
        "symbol": "BTCUSD_PERP",
        "side": "SELL",
        "positionSide": "BOTH",
        "stopPrice": trigger,
        "closePosition": True,
        "workingType": "CONTRACT_PRICE",
        "priceProtect": True,
    }
    if price is not None:
        entry["price"] = price
    return entry


def _algo_response(client_algo_id: str) -> dict[str, object]:
    return {
        "algoId": 9001,
        "clientAlgoId": client_algo_id,
        "algoType": "CONDITIONAL",
        "algoStatus": "NEW",
        "symbol": "BTCUSDT",
        "createTime": 1_700_000_000_000,
        "updateTime": 1_700_000_000_000,
    }


def _wire_usdm(adapter: BinanceAdapter, client: AsyncMock) -> None:
    adapter._client = client
    adapter._connected = True
    adapter._exchange_info_cache[BinanceProduct.USD_M] = [_exchange_info()]
    client.futures_position_information.return_value = [_position_entry()]
    client.futures_get_open_algo_orders.return_value = []
    client.futures_create_algo_order.return_value = _algo_response(_SL_ID)
    client.futures_cancel_algo_order.return_value = {"code": "200", "msg": "success"}


def _wire_coinm(adapter: BinanceAdapter, client: AsyncMock) -> None:
    adapter._client = client
    adapter._connected = True
    adapter._exchange_info_cache[BinanceProduct.COIN_M] = [
        {
            "symbol": "BTCUSD_PERP",
            "pair": "BTCUSD",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "contractSize": 100,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                {
                    "filterType": "LOT_SIZE",
                    "stepSize": "1",
                    "minQty": "1",
                    "maxQty": "1000",
                },
            ],
        }
    ]
    client.futures_coin_position_information.return_value = [_coin_m_position_entry()]
    client.futures_coin_get_open_orders.return_value = []


def _api_error(code: int, status: int, message: str) -> BinanceAPIException:
    return BinanceAPIException(
        SimpleNamespace(text=message),
        status,
        f'{{"code": {code}, "msg": "{message}"}}',
    )


class TestParsePositionId:
    def test_valid_long_leg(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, _POSITION_ID)

        assert leg == PositionLeg(
            product=BinanceProduct.USD_M,
            symbol="BTCUSDT",
            position_side="LONG",
        )

    @pytest.mark.parametrize("side", ["BOTH", "SHORT"])
    def test_one_way_and_short_sides(self, side: str) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, f"usd_m:BTCUSDT:{side}")
        assert leg.position_side == side

    def test_wrong_product_rejected(self) -> None:
        with pytest.raises(PlatformError, match="not usd_m"):
            parse_position_id(BinanceProduct.USD_M, "coin_m:BTCUSD_PERP:LONG")

    def test_unknown_product_rejected(self) -> None:
        with pytest.raises(PlatformError, match="unknown product"):
            parse_position_id(BinanceProduct.USD_M, "bogus:BTCUSDT:LONG")

    @pytest.mark.parametrize(
        "position_id",
        ["usd_m:BTCUSDT", "usd_m::LONG", "usd_m:BTCUSDT:LONG:extra", "usd_m:BTCUSDT:MIDDLE"],
    )
    def test_malformed_ids_rejected(self, position_id: str) -> None:
        with pytest.raises(PlatformError):
            parse_position_id(BinanceProduct.USD_M, position_id)


class TestProtectionClientId:
    def test_derives_deterministic_ids(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, _POSITION_ID)

        assert protection_client_id(leg, ProtectionSide.TAKE_PROFIT) == _TP_ID
        assert protection_client_id(leg, ProtectionSide.STOP_LOSS) == _SL_ID

    def test_id_fits_venue_limits(self) -> None:
        leg = PositionLeg(
            product=BinanceProduct.USD_M,
            symbol="A" * 20,
            position_side="SHORT",
        )
        client_id = protection_client_id(leg, ProtectionSide.TAKE_PROFIT)

        assert len(client_id) <= 36
        assert ":" not in client_id
        assert is_protection_client_id(client_id)

    def test_prefix_detection(self) -> None:
        assert is_protection_client_id(_TP_ID)
        assert not is_protection_client_id("tpsl:BTCUSDT:LONG:tp")
        assert not is_protection_client_id("binance-usd_m:123")
        assert not is_protection_client_id("tpslx:BTCUSDT:LONG:tp")

    def test_coin_m_ids_are_product_scoped(self) -> None:
        leg = parse_position_id(BinanceProduct.COIN_M, _COIN_M_POSITION_ID)

        assert protection_client_id(leg, ProtectionSide.STOP_LOSS) == _COIN_M_SL_ID
        assert protection_client_id(leg, ProtectionSide.STOP_LOSS) != _SL_ID


class TestProtectionClientIdReservation:
    async def test_regular_order_cannot_use_protection_namespace(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        with pytest.raises(InvalidOrderError, match="reserved for position protection"):
            await adapter.place_order(
                UnifiedOrder(
                    instrument=_usd_m_instrument(),
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    quantity=Decimal("0.001"),
                    time_in_force=TimeInForce.GTC,
                    client_order_id=_SL_ID,
                )
            )

        binance_client_mock.futures_create_order.assert_not_awaited()
        binance_client_mock.futures_create_algo_order.assert_not_awaited()


class TestPayloadBuilders:
    def test_close_order_side_follows_position_sign(self) -> None:
        assert close_order_side(Decimal("0.5")) == "SELL"
        assert close_order_side(Decimal("-0.5")) == "BUY"
        with pytest.raises(PlatformError):
            close_order_side(Decimal("0"))

    @pytest.mark.parametrize(
        ("side", "market", "expected"),
        [
            (ProtectionSide.STOP_LOSS, True, "STOP_MARKET"),
            (ProtectionSide.STOP_LOSS, False, "STOP"),
            (ProtectionSide.TAKE_PROFIT, True, "TAKE_PROFIT_MARKET"),
            (ProtectionSide.TAKE_PROFIT, False, "TAKE_PROFIT"),
        ],
    )
    def test_algo_order_type_mapping(
        self, side: ProtectionSide, market: bool, expected: str
    ) -> None:
        assert algo_order_type(side, market=market) == expected

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("STOP_MARKET", ProtectionSide.STOP_LOSS),
            ("STOP", ProtectionSide.STOP_LOSS),
            ("TAKE_PROFIT_MARKET", ProtectionSide.TAKE_PROFIT),
            ("TAKE_PROFIT", ProtectionSide.TAKE_PROFIT),
            ("TRAILING_STOP_MARKET", None),
            ("LIMIT", None),
        ],
    )
    def test_protection_side_for_type(self, raw: str, expected: object) -> None:
        assert protection_side_for_type(raw) == expected

    def test_working_type_mapping(self) -> None:
        assert working_type(None) is None
        assert working_type(TrailingBasis.LAST) == "CONTRACT_PRICE"
        assert working_type(TrailingBasis.MARK) == "MARK_PRICE"
        with pytest.raises(UnsupportedOrderTypeError):
            working_type(TrailingBasis.BID)

    def test_market_stop_loss_payload_uses_close_position(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, _POSITION_ID)
        payload = build_algo_order_payload(
            product=BinanceProduct.USD_M,
            symbol="BTCUSDT",
            position_side=leg.position_side,
            leg_quantity=Decimal("0.5"),
            attachment=TpSlAttachment(trigger_price=Decimal("55000")),
            side=ProtectionSide.STOP_LOSS,
            trigger_basis=None,
            client_algo_id=_SL_ID,
        )

        assert payload["type"] == "STOP_MARKET"
        assert payload["side"] == "SELL"
        assert payload["positionSide"] == "LONG"
        assert payload["closePosition"] == "true"
        assert payload["triggerPrice"] == "55000"
        assert payload["clientAlgoId"] == _SL_ID
        assert payload["algoType"] == "CONDITIONAL"
        assert "quantity" not in payload
        assert "price" not in payload
        assert "reduceOnly" not in payload
        assert "workingType" not in payload

    def test_limit_take_profit_snapshots_quantity(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, "usd_m:BTCUSDT:BOTH")
        payload = build_algo_order_payload(
            product=BinanceProduct.USD_M,
            symbol="BTCUSDT",
            position_side=leg.position_side,
            leg_quantity=Decimal("-0.25"),
            attachment=TpSlAttachment(trigger_price=Decimal("70000"), limit_price=Decimal("69900")),
            side=ProtectionSide.TAKE_PROFIT,
            trigger_basis=TrailingBasis.MARK,
            client_algo_id=_TP_ID,
        )

        assert payload["type"] == "TAKE_PROFIT"
        assert payload["side"] == "BUY"
        assert payload["price"] == "69900"
        assert payload["quantity"] == "0.25"
        assert payload["timeInForce"] == "GTC"
        assert payload["reduceOnly"] == "true"
        assert payload["workingType"] == "MARK_PRICE"
        assert "closePosition" not in payload

    def test_hedge_mode_omits_reduce_only(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, _POSITION_ID)
        payload = build_algo_order_payload(
            product=BinanceProduct.USD_M,
            symbol="BTCUSDT",
            position_side=leg.position_side,
            leg_quantity=Decimal("0.5"),
            attachment=TpSlAttachment(trigger_price=Decimal("55000"), limit_price=Decimal("54900")),
            side=ProtectionSide.STOP_LOSS,
            trigger_basis=TrailingBasis.LAST,
            client_algo_id=_SL_ID,
        )

        assert payload["type"] == "STOP"
        assert "reduceOnly" not in payload
        assert payload["workingType"] == "CONTRACT_PRICE"

    def test_unmappable_trigger_basis_rejected(self) -> None:
        leg = parse_position_id(BinanceProduct.USD_M, _POSITION_ID)
        with pytest.raises(UnsupportedOrderTypeError):
            build_algo_order_payload(
                product=BinanceProduct.USD_M,
                symbol="BTCUSDT",
                position_side=leg.position_side,
                leg_quantity=Decimal("0.5"),
                attachment=TpSlAttachment(trigger_price=Decimal("55000")),
                side=ProtectionSide.STOP_LOSS,
                trigger_basis=TrailingBasis.MID,
                client_algo_id=_SL_ID,
            )

    def test_coin_m_market_protection_matches_verified_regular_order_payload(self) -> None:
        leg = parse_position_id(BinanceProduct.COIN_M, _COIN_M_POSITION_ID)
        payload = build_coin_m_order_payload(
            symbol="BTCUSD_PERP",
            position_side=leg.position_side,
            leg_quantity=Decimal("1"),
            attachment=TpSlAttachment(trigger_price=Decimal("32456.0")),
            side=ProtectionSide.STOP_LOSS,
            trigger_basis=None,
            client_order_id=_COIN_M_SL_ID,
        )

        assert payload == {
            "symbol": "BTCUSD_PERP",
            "side": "SELL",
            "positionSide": "BOTH",
            "type": "STOP_MARKET",
            "stopPrice": "32456.0",
            "newClientOrderId": _COIN_M_SL_ID,
            "priceProtect": "TRUE",
            "closePosition": "true",
            "workingType": "CONTRACT_PRICE",
        }

    def test_coin_m_limit_trigger_uses_regular_order_fields(self) -> None:
        leg = parse_position_id(BinanceProduct.COIN_M, _COIN_M_POSITION_ID)
        payload = build_coin_m_order_payload(
            symbol="BTCUSD_PERP",
            position_side=leg.position_side,
            leg_quantity=Decimal("1"),
            attachment=TpSlAttachment(
                trigger_price=Decimal("38560.6"),
                limit_price=Decimal("35347.2"),
            ),
            side=ProtectionSide.STOP_LOSS,
            trigger_basis=TrailingBasis.MARK,
            client_order_id=_COIN_M_SL_ID,
        )

        assert payload == {
            "symbol": "BTCUSD_PERP",
            "side": "SELL",
            "positionSide": "BOTH",
            "type": "STOP",
            "stopPrice": "38560.6",
            "newClientOrderId": _COIN_M_SL_ID,
            "price": "35347.2",
            "quantity": "1",
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "TRUE",
        }


class TestParseProtectionEntry:
    def test_market_stop_loss_entry(self) -> None:
        order = parse_protection_entry(_algo_entry(_SL_ID))

        assert order is not None
        assert order.side is ProtectionSide.STOP_LOSS
        assert order.trigger_price == Decimal("55000")
        assert order.limit_price is None
        assert order.status == "NEW"

    def test_limit_take_profit_entry_reads_price(self) -> None:
        order = parse_protection_entry(
            _algo_entry(_TP_ID, order_type="TAKE_PROFIT", trigger="70000", price="69900")
        )

        assert order is not None
        assert order.side is ProtectionSide.TAKE_PROFIT
        assert order.limit_price == Decimal("69900")

    def test_coin_m_regular_order_fields_are_parsed(self) -> None:
        order = parse_protection_entry(_coin_m_order_entry())

        assert order is not None
        assert order.side is ProtectionSide.STOP_LOSS
        assert order.trigger_price == Decimal("32456")

    def test_non_protection_ids_ignored(self) -> None:
        assert parse_protection_entry(_algo_entry("user-order-1")) is None
        assert parse_protection_entry({"algoStatus": "NEW"}) is None

    def test_terminal_status_ignored(self) -> None:
        assert parse_protection_entry(_algo_entry(_SL_ID, status="FINISHED")) is None
        assert parse_protection_entry(_algo_entry(_SL_ID, status="CANCELED")) is None

    def test_trailing_type_rejected(self) -> None:
        with pytest.raises(PlatformError, match="unsupported type"):
            parse_protection_entry(_algo_entry(_SL_ID, order_type="TRAILING_STOP_MARKET"))

    def test_missing_type_rejected(self) -> None:
        entry = _algo_entry(_SL_ID)
        del entry["type"]
        with pytest.raises(PlatformError, match="no type"):
            parse_protection_entry(entry)

    def test_invalid_trigger_rejected(self) -> None:
        with pytest.raises(PlatformError, match="invalid triggerPrice"):
            parse_protection_entry(_algo_entry(_SL_ID, trigger="abc"))

    def test_limit_type_requires_valid_price(self) -> None:
        with pytest.raises(PlatformError, match="invalid price"):
            parse_protection_entry(_algo_entry(_TP_ID, order_type="TAKE_PROFIT", trigger="70000"))


class TestPositionProtectionCapability:
    def test_usd_m_supported(self, adapter: BinanceAdapter) -> None:
        capability = adapter.position_protection_capability(_usd_m_instrument())

        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases == frozenset({TrailingBasis.LAST, TrailingBasis.MARK})
        assert capability.reprice_gap is True

    @pytest.mark.parametrize("instrument", [_spot_instrument()], ids=("spot",))
    def test_other_products_unsupported(
        self, adapter: BinanceAdapter, instrument: Instrument
    ) -> None:
        capability = adapter.position_protection_capability(instrument)

        assert capability.supported is False
        assert capability.preserves_unmentioned_side is False

    def test_coin_m_supported(self, adapter: BinanceAdapter) -> None:
        capability = adapter.position_protection_capability(_coin_m_instrument())

        assert capability.supported is True
        assert capability.can_set_stop is True
        assert capability.can_set_take_profit is True
        assert capability.can_clear_stop is True
        assert capability.can_clear_take_profit is True
        assert capability.preserves_unmentioned_side is True
        assert capability.trigger_bases == frozenset({TrailingBasis.LAST, TrailingBasis.MARK})
        assert capability.reprice_gap is True


class TestModifyPositionTpsl:
    async def test_requires_at_least_one_side(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        with pytest.raises(ValueError, match="at least one"):
            await adapter.modify_position_tpsl(_usd_m_instrument(), _POSITION_ID)

    @pytest.mark.parametrize("instrument", [_spot_instrument()], ids=("spot",))
    async def test_unsupported_products_rejected(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock, instrument: Instrument
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                instrument, _POSITION_ID, stop_loss=TpSlAttachment(trigger_price=Decimal("1"))
            )

    @pytest.mark.parametrize(
        "attachment",
        [
            TpSlAttachment(trigger_price=Decimal("55000.05")),
            TpSlAttachment(trigger_price=Decimal("55000"), limit_price=Decimal("54999.95")),
        ],
        ids=("off_tick_trigger", "off_tick_limit"),
    )
    async def test_off_tick_prices_rejected_before_venue_calls(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
        attachment: TpSlAttachment,
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        with pytest.raises(InvalidOrderError, match="not aligned to tick size"):
            await adapter.modify_position_tpsl(
                _usd_m_instrument(),
                _POSITION_ID,
                stop_loss=attachment,
            )

        binance_client_mock.futures_position_information.assert_not_awaited()
        binance_client_mock.futures_create_algo_order.assert_not_awaited()

    async def test_coin_m_off_tick_trigger_rejected(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)

        with pytest.raises(InvalidOrderError, match="not aligned to tick size"):
            await adapter.modify_position_tpsl(
                _coin_m_instrument(),
                _COIN_M_POSITION_ID,
                stop_loss=TpSlAttachment(trigger_price=Decimal("32456.05")),
            )

        binance_client_mock.futures_coin_position_information.assert_not_awaited()

    async def test_stop_loss_write_cancels_then_places(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_get_open_algo_orders.side_effect = [
            [_algo_entry(_SL_ID)],
            [],
        ]

        await adapter.modify_position_tpsl(
            _usd_m_instrument(),
            _POSITION_ID,
            stop_loss=TpSlAttachment(trigger_price=Decimal("55000")),
        )

        binance_client_mock.futures_cancel_algo_order.assert_awaited_once_with(
            symbol="BTCUSDT",
            algoId=9001,
        )
        binance_client_mock.futures_create_algo_order.assert_awaited_once()
        payload = binance_client_mock.futures_create_algo_order.await_args.kwargs
        assert payload["type"] == "STOP_MARKET"
        assert payload["closePosition"] == "true"
        assert payload["triggerPrice"] == "55000"
        assert payload["clientAlgoId"] == _SL_ID
        assert payload["positionSide"] == "LONG"

    async def test_coin_m_market_stop_uses_regular_order_lifecycle(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        existing = _coin_m_order_entry()
        binance_client_mock.futures_coin_get_open_orders.side_effect = [[existing], []]
        binance_client_mock.futures_coin_create_order.return_value = _coin_m_order_entry()

        await adapter.modify_position_tpsl(
            _coin_m_instrument(),
            _COIN_M_POSITION_ID,
            stop_loss=TpSlAttachment(trigger_price=Decimal("32456")),
            trigger_basis=TrailingBasis.MARK,
        )

        binance_client_mock.futures_coin_cancel_order.assert_awaited_once_with(
            symbol="BTCUSD_PERP",
            orderId=1234,
        )
        payload = binance_client_mock.futures_coin_create_order.await_args.kwargs
        assert payload == {
            "symbol": "BTCUSD_PERP",
            "side": "SELL",
            "positionSide": "BOTH",
            "type": "STOP_MARKET",
            "stopPrice": "32456",
            "newClientOrderId": _COIN_M_SL_ID,
            "workingType": "MARK_PRICE",
            "priceProtect": "TRUE",
            "closePosition": "true",
        }
        binance_client_mock.futures_coin_position_information.assert_awaited_once_with(
            symbol="BTCUSD_PERP"
        )
        binance_client_mock._request_futures_coin_api.assert_not_awaited()

    async def test_coin_m_reprice_preserves_unmentioned_protection_side(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        stop = _coin_m_order_entry()
        take_profit = _coin_m_order_entry(
            _COIN_M_TP_ID,
            order_type="TAKE_PROFIT_MARKET",
            trigger="70000",
        )
        binance_client_mock.futures_coin_get_open_orders.side_effect = [
            [stop, take_profit],
            [stop],
        ]
        binance_client_mock.futures_coin_create_order.return_value = _coin_m_order_entry(
            _COIN_M_TP_ID,
            order_type="TAKE_PROFIT_MARKET",
            trigger="71000",
        )

        await adapter.modify_position_tpsl(
            _coin_m_instrument(),
            _COIN_M_POSITION_ID,
            take_profit=TpSlAttachment(trigger_price=Decimal("71000")),
        )

        binance_client_mock.futures_coin_cancel_order.assert_awaited_once_with(
            symbol="BTCUSD_PERP",
            orderId=1234,
        )
        assert binance_client_mock.futures_coin_create_order.await_args.kwargs[
            "newClientOrderId"
        ].endswith("-tp")

    async def test_coin_m_limit_attachment_uses_regular_order_payload(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        binance_client_mock.futures_coin_create_order.return_value = _coin_m_order_entry(
            order_type="STOP",
            trigger="38560.6",
            price="35347.2",
        )

        await adapter.modify_position_tpsl(
            _coin_m_instrument(),
            _COIN_M_POSITION_ID,
            stop_loss=TpSlAttachment(
                trigger_price=Decimal("38560.6"),
                limit_price=Decimal("35347.2"),
            ),
        )

        assert binance_client_mock.futures_coin_create_order.await_args.kwargs == {
            "symbol": "BTCUSD_PERP",
            "side": "SELL",
            "positionSide": "BOTH",
            "type": "STOP",
            "stopPrice": "38560.6",
            "newClientOrderId": _COIN_M_SL_ID,
            "price": "35347.2",
            "quantity": "1",
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "workingType": "CONTRACT_PRICE",
            "priceProtect": "TRUE",
        }

    async def test_unmentioned_side_is_preserved(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        await adapter.modify_position_tpsl(
            _usd_m_instrument(),
            _POSITION_ID,
            take_profit=TpSlAttachment(trigger_price=Decimal("70000")),
        )

        binance_client_mock.futures_cancel_algo_order.assert_not_awaited()
        payload = binance_client_mock.futures_create_algo_order.await_args.kwargs
        assert payload["type"] == "TAKE_PROFIT_MARKET"
        assert payload["clientAlgoId"] == _TP_ID
        assert _SL_ID not in str(binance_client_mock.futures_cancel_algo_order.await_args_list)

    async def test_flat_position_rejected(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_position_information.return_value = [
            _position_entry(amount="0")
        ]

        with pytest.raises(OrderNotFoundError, match="No open USD"):
            await adapter.modify_position_tpsl(
                _usd_m_instrument(),
                _POSITION_ID,
                stop_loss=TpSlAttachment(trigger_price=Decimal("55000")),
            )
        binance_client_mock.futures_create_algo_order.assert_not_awaited()

    async def test_coin_m_flat_position_rejected(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        binance_client_mock.futures_coin_position_information.return_value = [
            _coin_m_position_entry(amount="0")
        ]

        with pytest.raises(OrderNotFoundError, match="No open COIN-M"):
            await adapter.modify_position_tpsl(
                _coin_m_instrument(),
                _COIN_M_POSITION_ID,
                stop_loss=TpSlAttachment(trigger_price=Decimal("32456")),
            )

        binance_client_mock.futures_coin_get_open_orders.assert_not_awaited()
        binance_client_mock.futures_coin_create_order.assert_not_awaited()

    async def test_position_id_symbol_mismatch_rejected(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        with pytest.raises(PlatformError, match="addresses ETHUSDT"):
            await adapter.modify_position_tpsl(
                _usd_m_instrument(),
                "usd_m:ETHUSDT:LONG",
                stop_loss=TpSlAttachment(trigger_price=Decimal("55000")),
            )

    async def test_unmappable_trigger_basis_fails_before_writes(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.modify_position_tpsl(
                _usd_m_instrument(),
                _POSITION_ID,
                stop_loss=TpSlAttachment(trigger_price=Decimal("55000")),
                trigger_basis=TrailingBasis.BID,
            )
        binance_client_mock.futures_cancel_algo_order.assert_not_awaited()
        binance_client_mock.futures_create_algo_order.assert_not_awaited()

    async def test_non_live_algo_status_rejected(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        response = _algo_response(_SL_ID)
        response["algoStatus"] = "CANCELED"
        binance_client_mock.futures_create_algo_order.return_value = response

        with pytest.raises(PlatformError, match="not accepted"):
            await adapter.modify_position_tpsl(
                _usd_m_instrument(),
                _POSITION_ID,
                stop_loss=TpSlAttachment(trigger_price=Decimal("55000")),
            )


class TestGetPositionTpsl:
    async def test_flat_position_returns_none(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_position_information.return_value = []

        assert await adapter.get_position_tpsl(_usd_m_instrument(), _POSITION_ID) is None

    async def test_reads_both_sides(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_get_open_algo_orders.return_value = [
            _algo_entry(_SL_ID),
            _algo_entry(_TP_ID, order_type="TAKE_PROFIT_MARKET", trigger="70000"),
            _algo_entry("user-order", order_type="STOP_MARKET", trigger="1"),
        ]

        result = await adapter.get_position_tpsl(_usd_m_instrument(), _POSITION_ID)

        assert result is not None
        take_profit, stop_loss = result
        assert take_profit is not None
        assert take_profit.trigger_price == Decimal("70000")
        assert take_profit.limit_price is None
        assert stop_loss is not None
        assert stop_loss.trigger_price == Decimal("55000")

    async def test_coin_m_reads_regular_open_order_response_shape(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        binance_client_mock.futures_coin_get_open_orders.return_value = [_coin_m_order_entry()]

        result = await adapter.get_position_tpsl(
            _coin_m_instrument(),
            _COIN_M_POSITION_ID,
        )

        binance_client_mock.futures_coin_get_open_orders.assert_awaited_once_with(
            symbol="BTCUSD_PERP"
        )
        assert result == (
            None,
            TpSlAttachment(trigger_price=Decimal("32456")),
        )

    async def test_other_leg_protection_ignored(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_get_open_algo_orders.return_value = [
            _algo_entry("tpsl-u-BTCUSDT-SHORT-sl"),
        ]

        result = await adapter.get_position_tpsl(_usd_m_instrument(), _POSITION_ID)

        assert result == (None, None)

    async def test_limit_attachment_reads_limit_price(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_get_open_algo_orders.return_value = [
            _algo_entry(_TP_ID, order_type="TAKE_PROFIT", trigger="70000", price="69900"),
        ]

        result = await adapter.get_position_tpsl(_usd_m_instrument(), _POSITION_ID)

        assert result is not None
        take_profit, _ = result
        assert take_profit is not None
        assert take_profit.limit_price == Decimal("69900")

    @pytest.mark.parametrize("instrument", [_spot_instrument()], ids=("spot",))
    async def test_unsupported_products_return_none(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
        instrument: Instrument,
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)

        assert await adapter.get_position_tpsl(instrument, _POSITION_ID) is None


class TestClearPositionTpsl:
    async def test_requires_a_named_side(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        with pytest.raises(ValueError, match="at least one"):
            await adapter.clear_position_tpsl(_usd_m_instrument(), _POSITION_ID)

    @pytest.mark.parametrize("instrument", [_spot_instrument()], ids=("spot",))
    async def test_unsupported_products_rejected(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock, instrument: Instrument
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.clear_position_tpsl(instrument, _POSITION_ID, stop_loss=True)

    async def test_clears_only_named_side_including_legacy_ids(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        binance_client_mock.futures_get_open_algo_orders.side_effect = [
            [_algo_entry(_TP_ID, order_type="TAKE_PROFIT_MARKET")],
            [],
            [_algo_entry(_SL_ID)],
            [],
        ]

        await adapter.clear_position_tpsl(
            _usd_m_instrument(), _POSITION_ID, take_profit=True, stop_loss=True
        )

        assert [
            call.kwargs for call in binance_client_mock.futures_cancel_algo_order.await_args_list
        ] == [
            {"symbol": "BTCUSDT", "algoId": 9001},
            {"symbol": "BTCUSDT", "algoId": 9001},
        ]
        # Flat or not, clear never needs the position read.
        binance_client_mock.futures_position_information.assert_not_awaited()

    async def test_coin_m_clear_cancels_matching_regular_order(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        binance_client_mock.futures_coin_get_open_orders.side_effect = [
            [_coin_m_order_entry()],
            [],
        ]

        await adapter.clear_position_tpsl(
            _coin_m_instrument(),
            _COIN_M_POSITION_ID,
            stop_loss=True,
        )

        binance_client_mock.futures_coin_cancel_order.assert_awaited_once_with(
            symbol="BTCUSD_PERP",
            orderId=1234,
        )
        binance_client_mock.futures_coin_position_information.assert_not_awaited()


class TestProtectionMirrorExclusion:
    async def test_snapshot_skips_protection_algo_orders(
        self, adapter: BinanceAdapter, binance_client_mock: AsyncMock
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        entry = _algo_entry(_SL_ID)
        entry.update(
            {
                "side": "SELL",
                "quantity": "0.5",
                "executedQty": "0",
                "createTime": 1_700_000_000_000,
            }
        )

        order = await adapter._translate_snapshot_order(
            BinanceProduct.USD_M, entry, conditional=True
        )

        assert order is None
        assert _SL_ID not in adapter._order_refs

    async def test_coin_m_regular_snapshot_skips_adapter_protection_before_translation(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)

        order = await adapter._translate_snapshot_order(
            BinanceProduct.COIN_M,
            _coin_m_order_entry(),
        )

        assert order is None
        assert _COIN_M_SL_ID not in adapter._order_refs

    async def test_stream_update_skips_protection_events(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
        event_bus,
    ) -> None:
        _wire_usdm(adapter, binance_client_mock)
        published: list[object] = []
        event_bus.subscribe(OrderPlacedEvent, published.append)

        await adapter._handle_order_update(
            BinanceProduct.USD_M,
            {
                "s": "BTCUSDT",
                "i": 9001,
                "caid": _SL_ID,
                "algoStatus": "NEW",
                "o": "STOP_MARKET",
                "S": "SELL",
                "q": "0.5",
                "T": 1_700_000_000_000,
            },
            datetime.now(UTC),
            conditional=True,
        )

        assert published == []
        assert _SL_ID not in adapter._order_refs
        assert not any(identity.endswith("9001") for identity in adapter._open_order_ids)

    async def test_coin_m_regular_stream_update_is_excluded_from_order_mirror(
        self,
        adapter: BinanceAdapter,
        binance_client_mock: AsyncMock,
        event_bus,
    ) -> None:
        _wire_coinm(adapter, binance_client_mock)
        published: list[object] = []
        event_bus.subscribe(OrderPlacedEvent, published.append)

        await adapter._handle_user_stream_message(
            BinanceProduct.COIN_M,
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1_700_000_000_000,
                "o": {
                    "s": "BTCUSD_PERP",
                    "i": 1234,
                    "c": _COIN_M_SL_ID,
                    "X": "NEW",
                    "o": "STOP_MARKET",
                    "S": "SELL",
                    "q": "0",
                    "T": 1_700_000_000_000,
                },
            },
        )

        assert published == []
        assert _COIN_M_SL_ID not in adapter._order_refs

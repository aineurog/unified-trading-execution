"""Bybit native trailing payload translation."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import FailedRequestError

from unified_trading_execution.adapter import TrailingCapability
from unified_trading_execution.bybit import BybitAdapter
from unified_trading_execution.bybit.orders import build_native_trailing_stop_payload
from unified_trading_execution.errors import (
    PlatformConnectionError,
    PlatformError,
    TrailingNotSupportedError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.trailing.manager import resolve_routing
from unified_trading_execution.trailing.math import respects_min_stop_distance
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    NativeTrailingRequest,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.types.enums import AssetClass, OptionRight, OrderSide
from unified_trading_execution.types.instrument import Instrument


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        multiplier=1,
    )


def _inverse_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        currency="BTC",
        multiplier=1,
    )


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
    )


def _option_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.OPTION,
        currency="BTC",
        expiry=date(2026, 12, 25),
        strike=Decimal("100000"),
        option_right=OptionRight.CALL,
        multiplier=1,
    )


def _request(
    *,
    basis: TrailingBasis | None,
    activation_price: Decimal | None = None,
    offset_type: TrailingOffsetType = TrailingOffsetType.PRICE,
    offset: Decimal = Decimal("500"),
) -> NativeTrailingRequest:
    return NativeTrailingRequest(
        instrument=_instrument(),
        position_id="BTCUSDT:0",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        offset=offset,
        offset_type=offset_type,
        basis=basis,
        activation_price=activation_price,
        client_id="trail-1",
    )


def _payload(request: NativeTrailingRequest) -> dict[str, object]:
    return build_native_trailing_stop_payload(
        request,
        category="linear",
        symbol="BTCUSDT",
        position_idx=0,
    )


def _trailing_spec(
    *,
    offset_type: TrailingOffsetType = TrailingOffsetType.PRICE,
    offset: Decimal = Decimal("500"),
    basis: TrailingBasis | None = None,
) -> TrailingSpec:
    if offset_type is TrailingOffsetType.ATR:
        return TrailingSpec(
            offset_type=offset_type,
            offset=offset,
            basis=basis,
            atr_period=14,
            atr_timeframe=CandleTimeframe.H1,
        )
    return TrailingSpec(offset_type=offset_type, offset=offset, basis=basis)


def _route(
    adapter: BybitAdapter,
    *,
    instrument: Instrument | None = None,
    spec: TrailingSpec | None = None,
    mode: TrailingMode = TrailingMode.AUTO,
) -> tuple[TrailingMode, str]:
    resolved_instrument = instrument or _instrument()
    request = TrailingRequest(
        resolved_instrument,
        spec or _trailing_spec(),
        mode=mode,
    )
    return resolve_routing(
        request,
        protection=adapter.position_protection_capability(resolved_instrument),
        native=adapter.trailing_capability(resolved_instrument),
        market_data=adapter.market_data_capability(resolved_instrument),
    )


class TestBuildNativeTrailingStopPayload:
    @pytest.mark.parametrize(
        ("basis", "venue_basis"),
        [
            (TrailingBasis.LAST, "LastPrice"),
            (TrailingBasis.MARK, "MarkPrice"),
        ],
    )
    def test_request_basis_reaches_sl_trigger_by(
        self,
        basis: TrailingBasis,
        venue_basis: str,
    ) -> None:
        payload = _payload(_request(basis=basis))

        assert payload["slTriggerBy"] == venue_basis

    @pytest.mark.parametrize(
        "basis",
        [None, TrailingBasis.BID, TrailingBasis.ASK, TrailingBasis.MID],
    )
    def test_unmappable_basis_uses_the_venue_default(
        self,
        basis: TrailingBasis | None,
    ) -> None:
        payload = _payload(_request(basis=basis))

        assert "slTriggerBy" not in payload

    def test_price_activation_sets_active_price(self) -> None:
        payload = _payload(_request(basis=TrailingBasis.LAST, activation_price=Decimal("51000")))

        assert payload["activePrice"] == "51000"

    def test_immediate_activation_omits_active_price(self) -> None:
        payload = _payload(_request(basis=TrailingBasis.LAST))

        assert "activePrice" not in payload

    def test_trail_only_payload_has_no_static_protection(self) -> None:
        payload = _payload(_request(basis=TrailingBasis.MARK))

        assert payload["trailingStop"] == "500"
        assert "takeProfit" not in payload
        assert "stopLoss" not in payload
        assert "tpslMode" not in payload
        assert "tpTriggerBy" not in payload

    @pytest.mark.parametrize(
        "offset_type",
        [TrailingOffsetType.PERCENT, TrailingOffsetType.ATR],
    )
    def test_non_price_offset_raises(self, offset_type: TrailingOffsetType) -> None:
        with pytest.raises(UnsupportedOrderTypeError, match="price offsets only"):
            _payload(_request(basis=TrailingBasis.LAST, offset_type=offset_type))


class TestTrailingCapability:
    def test_linear_perpetual_declares_native_price_trailing(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_supported(adapter.trailing_capability(_instrument()))

    def test_inverse_perpetual_declares_native_price_trailing(
        self,
        adapter: BybitAdapter,
    ) -> None:
        self._assert_supported(adapter.trailing_capability(_inverse_instrument()))

    def test_spot_is_unsupported(self, adapter: BybitAdapter) -> None:
        self._assert_unsupported(adapter.trailing_capability(_spot_instrument()))

    def test_options_are_unsupported(self, adapter: BybitAdapter) -> None:
        self._assert_unsupported(adapter.trailing_capability(_option_instrument()))

    def test_percent_and_atr_offsets_route_synthetic(self, adapter: BybitAdapter) -> None:
        capability = adapter.trailing_capability(_instrument())

        assert TrailingOffsetType.PERCENT not in capability.offset_types
        assert TrailingOffsetType.ATR not in capability.offset_types
        assert capability.offset_types == frozenset({TrailingOffsetType.PRICE})

    def test_declared_bases_are_the_ones_the_payload_writes(
        self,
        adapter: BybitAdapter,
    ) -> None:
        capability = adapter.trailing_capability(_instrument())
        expected = {
            TrailingBasis.LAST: "LastPrice",
            TrailingBasis.MARK: "MarkPrice",
        }

        assert capability.bases == frozenset(expected)
        for basis, venue_basis in expected.items():
            payload = _payload(_request(basis=basis))
            assert payload["slTriggerBy"] == venue_basis

    def test_no_false_optional_support(self, adapter: BybitAdapter) -> None:
        capability = adapter.trailing_capability(_instrument())

        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is True

    @staticmethod
    def _assert_supported(capability: TrailingCapability) -> None:
        assert capability.supported is True
        assert capability.offset_types == frozenset({TrailingOffsetType.PRICE})
        assert capability.bases == frozenset({TrailingBasis.LAST, TrailingBasis.MARK})
        assert capability.activations == frozenset(
            {TrailingActivation.IMMEDIATE, TrailingActivation.PRICE}
        )
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is True
        assert capability.min_reprice_interval_seconds == 0.0

    @staticmethod
    def _assert_unsupported(capability: TrailingCapability) -> None:
        assert capability.supported is False
        assert capability.offset_types == frozenset()
        assert capability.bases == frozenset()
        assert capability.activations == frozenset()
        assert capability.supports_step is False
        assert capability.supports_initial_stop is False
        assert capability.supports_take_profit is False
        assert capability.position_scoped is False
        assert capability.min_reprice_interval_seconds == 0.0


_LINEAR_SPEC_RESPONSE: dict[str, object] = {
    "retCode": 0,
    "result": {
        "category": "linear",
        "list": [
            {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "contractType": "LinearPerpetual",
                "status": "Trading",
                "lotSizeFilter": {
                    "qtyStep": "0.001",
                    "minOrderQty": "0.001",
                    "maxOrderQty": "1000",
                    "minNotionalValue": "5",
                },
                "priceFilter": {"tickSize": "0.10"},
            }
        ],
    },
}


class TestMinimumStopDistance:
    async def test_core_enforces_the_distance_bybit_publishes(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = (_LINEAR_SPEC_RESPONSE, None, {})

        spec = await adapter.fetch_instrument_spec(_instrument())
        basis = Decimal("50000")

        assert spec.min_stop_distance == spec.tick_size == Decimal("0.10")
        assert not respects_min_stop_distance(
            side=OrderSide.BUY,
            candidate=basis - Decimal("0.05"),
            basis=basis,
            min_stop_distance=spec.min_stop_distance,
        )
        assert respects_min_stop_distance(
            side=OrderSide.BUY,
            candidate=basis - spec.min_stop_distance,
            basis=basis,
            min_stop_distance=spec.min_stop_distance,
        )


class TestTrailingRouting:
    @pytest.mark.parametrize("instrument", [_instrument(), _inverse_instrument()])
    def test_auto_routes_a_price_offset_native(
        self,
        adapter: BybitAdapter,
        instrument: Instrument,
    ) -> None:
        mode, reason = _route(adapter, instrument=instrument)

        assert mode is TrailingMode.NATIVE
        assert reason == "native_exact"

    @pytest.mark.parametrize(
        ("basis", "expected_mode"),
        [
            (None, TrailingMode.NATIVE),
            (TrailingBasis.LAST, TrailingMode.NATIVE),
            (TrailingBasis.MARK, TrailingMode.NATIVE),
        ],
    )
    def test_auto_routes_sampleable_bases_native(
        self,
        adapter: BybitAdapter,
        basis: TrailingBasis | None,
        expected_mode: TrailingMode,
    ) -> None:
        mode, _ = _route(adapter, spec=_trailing_spec(basis=basis))

        assert mode is expected_mode

    def test_percent_offset_routes_synthetic(self, adapter: BybitAdapter) -> None:
        mode, reason = _route(
            adapter,
            spec=_trailing_spec(
                offset_type=TrailingOffsetType.PERCENT,
                offset=Decimal("1"),
            ),
        )

        assert mode is TrailingMode.SYNTHETIC
        assert reason == "native_unsupported_offset_type"

    def test_atr_offset_routes_synthetic(self, adapter: BybitAdapter) -> None:
        mode, reason = _route(
            adapter,
            spec=_trailing_spec(offset_type=TrailingOffsetType.ATR, offset=Decimal("2")),
        )

        assert mode is TrailingMode.SYNTHETIC
        assert reason == "native_unsupported_atr"

    def test_native_mode_names_percent_as_unsupported(self, adapter: BybitAdapter) -> None:
        with pytest.raises(TrailingNotSupportedError) as excinfo:
            _route(
                adapter,
                spec=_trailing_spec(
                    offset_type=TrailingOffsetType.PERCENT,
                    offset=Decimal("1"),
                ),
                mode=TrailingMode.NATIVE,
            )

        assert "offset" in str(excinfo.value)

    def test_basis_the_adapter_cannot_sample_is_rejected(self, adapter: BybitAdapter) -> None:
        with pytest.raises(TrailingNotSupportedError) as excinfo:
            _route(adapter, spec=_trailing_spec(basis=TrailingBasis.MID))

        assert "mid" in str(excinfo.value)

    def test_spot_has_no_supported_mode(self, adapter: BybitAdapter) -> None:
        with pytest.raises(TrailingNotSupportedError) as excinfo:
            _route(adapter, instrument=_spot_instrument())

        assert "native" in str(excinfo.value)
        assert "synthetic" in str(excinfo.value)


def _positions_response(
    *,
    trailing_stop: str = "",
    active_price: str = "",
    position_idx: int = 0,
    take_profit: str = "",
    stop_loss: str = "",
) -> tuple[dict[str, object], None, dict[str, str]]:
    return (
        {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "positionIdx": position_idx,
                        "trailingStop": trailing_stop,
                        "activePrice": active_price,
                        "takeProfit": take_profit,
                        "stopLoss": stop_loss,
                    }
                ]
            },
        },
        None,
        {},
    )


def _ok() -> tuple[dict[str, object], None, dict[str, str]]:
    return {"retCode": 0, "retMsg": "OK", "result": {}}, None, {}


class TestNativeTrailingMethods:
    async def test_place_immediate_trail_writes_only_the_trail(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        result = await adapter.place_trailing_stop(_request(basis=TrailingBasis.LAST))

        assert result.native_id == "BTCUSDT:0:trailing"
        assert result.confirmed_offset == Decimal("500")
        assert result.current_stop is None
        assert result.activated is True
        assert result.position_scoped is True
        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs == {
            "category": "linear",
            "symbol": "BTCUSDT",
            "positionIdx": 0,
            "trailingStop": "500",
            "slTriggerBy": "LastPrice",
        }

    async def test_place_price_activation_sets_active_price(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()

        await adapter.place_trailing_stop(
            _request(basis=TrailingBasis.MARK, activation_price=Decimal("51000"))
        )

        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs["activePrice"] == "51000"
        assert call.kwargs["slTriggerBy"] == "MarkPrice"

    async def test_place_inverse_uses_the_inverse_category_and_idx(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        request = NativeTrailingRequest(
            instrument=_inverse_instrument(),
            position_id="BTCUSD:2",
            side=OrderSide.SELL,
            quantity=Decimal("1"),
            offset=Decimal("250"),
            offset_type=TrailingOffsetType.PRICE,
            basis=TrailingBasis.LAST,
            activation_price=None,
            client_id="trail-1",
        )

        result = await adapter.place_trailing_stop(request)

        assert result.native_id == "BTCUSD:2:trailing"
        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs["category"] == "inverse"
        assert call.kwargs["symbol"] == "BTCUSD"
        assert call.kwargs["positionIdx"] == 2

    async def test_place_leaves_existing_take_profit_and_stop_loss_untouched(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        mock_pybit_http.get_positions.return_value = _positions_response(
            take_profit="60000",
            stop_loss="45000",
        )

        await adapter.place_trailing_stop(_request(basis=TrailingBasis.LAST))

        call = mock_pybit_http.set_trading_stop.call_args
        assert "takeProfit" not in call.kwargs
        assert "stopLoss" not in call.kwargs
        assert "tpslMode" not in call.kwargs
        take_profit, stop_loss = await adapter.get_position_tpsl(_instrument(), "BTCUSDT:0")
        assert take_profit is not None
        assert take_profit.trigger_price == Decimal("60000")
        assert stop_loss is not None
        assert stop_loss.trigger_price == Decimal("45000")

    async def test_place_targets_only_the_requested_hedge_leg(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTCUSDT:2",
            side=OrderSide.SELL,
            quantity=Decimal("1"),
            offset=Decimal("250"),
            offset_type=TrailingOffsetType.PRICE,
            basis=TrailingBasis.MARK,
            activation_price=None,
            client_id="trail-1",
        )

        result = await adapter.place_trailing_stop(request)

        assert result.native_id == "BTCUSDT:2:trailing"
        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs["positionIdx"] == 2
        assert call.kwargs["trailingStop"] == "250"
        assert "takeProfit" not in call.kwargs
        assert "stopLoss" not in call.kwargs

    async def test_place_spot_raises_before_network(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        request = NativeTrailingRequest(
            instrument=_spot_instrument(),
            position_id="0",
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            offset=Decimal("500"),
            offset_type=TrailingOffsetType.PRICE,
            basis=TrailingBasis.LAST,
            activation_price=None,
            client_id="trail-1",
        )

        with pytest.raises(UnsupportedOrderTypeError):
            await adapter.place_trailing_stop(request)

        mock_pybit_http.set_trading_stop.assert_not_called()

    async def test_get_returns_offset_and_activation(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions_response(
            trailing_stop="500",
            active_price="51000",
        )

        result = await adapter.get_trailing_stop("BTCUSDT:0:trailing", _instrument())

        assert result is not None
        assert result.native_id == "BTCUSDT:0:trailing"
        assert result.confirmed_offset == Decimal("500")
        assert result.current_stop is None
        assert result.activated is True
        assert result.position_scoped is True
        call = mock_pybit_http.get_positions.call_args
        assert call.kwargs == {"category": "linear", "symbol": "BTCUSDT"}

    async def test_get_returns_none_when_both_fields_are_empty(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions_response()

        assert await adapter.get_trailing_stop("BTCUSDT:0:trailing", _instrument()) is None

    async def test_get_returns_none_when_the_position_is_missing(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = (
            {"retCode": 0, "result": {"list": []}},
            None,
            {},
        )

        assert await adapter.get_trailing_stop("BTCUSDT:0:trailing", _instrument()) is None

    async def test_get_transport_error_raises(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.side_effect = FailedRequestError(
            request="GET /v5/position/list",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.get_trailing_stop("BTCUSDT:0:trailing", _instrument())

    async def test_get_rejects_a_native_id_for_another_symbol(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        with pytest.raises(ValueError, match="does not match"):
            await adapter.get_trailing_stop("ETHUSDT:0:trailing", _instrument())

        mock_pybit_http.get_positions.assert_not_called()

    async def test_position_scoped_read_matches_position_idx(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions_response(
            trailing_stop="125",
            position_idx=2,
        )

        result = await adapter.get_position_trailing_stop(_instrument(), "BTCUSDT:2")

        assert result is not None
        assert result.native_id == "BTCUSDT:2:trailing"
        assert result.confirmed_offset == Decimal("125")
        assert result.activated is True

    async def test_position_scoped_read_ignores_the_other_hedge_leg(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = (
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {"positionIdx": 1, "trailingStop": "100"},
                        {"positionIdx": 2, "trailingStop": "200"},
                    ]
                },
            },
            None,
            {},
        )

        first = await adapter.get_position_trailing_stop(_instrument(), "BTCUSDT:1")
        second = await adapter.get_position_trailing_stop(_instrument(), "BTCUSDT:2")

        assert first is not None
        assert first.confirmed_offset == Decimal("100")
        assert second is not None
        assert second.confirmed_offset == Decimal("200")

    async def test_position_scoped_read_returns_none_without_a_trail(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.return_value = _positions_response(position_idx=1)

        assert await adapter.get_position_trailing_stop(_instrument(), "BTCUSDT:1") is None

    async def test_position_scoped_read_transport_error_raises(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_positions.side_effect = FailedRequestError(
            request="GET /v5/position/list",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.get_position_trailing_stop(_instrument(), "BTCUSDT:0")

    async def test_cancel_sends_zero_and_confirms_absence(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        mock_pybit_http.get_positions.return_value = _positions_response()

        await adapter.cancel_trailing_stop("BTCUSDT:0:trailing", _instrument())

        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs == {
            "category": "linear",
            "symbol": "BTCUSDT",
            "positionIdx": 0,
            "trailingStop": "0",
        }

    async def test_cancel_targets_only_the_named_hedge_leg(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        mock_pybit_http.get_positions.return_value = _positions_response(position_idx=2)

        await adapter.cancel_trailing_stop("BTCUSDT:2:trailing", _instrument())

        call = mock_pybit_http.set_trading_stop.call_args
        assert call.kwargs == {
            "category": "linear",
            "symbol": "BTCUSDT",
            "positionIdx": 2,
            "trailingStop": "0",
        }
        assert "takeProfit" not in call.kwargs
        assert "stopLoss" not in call.kwargs

    async def test_cancel_raises_when_the_trail_remains(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.return_value = _ok()
        mock_pybit_http.get_positions.return_value = _positions_response(trailing_stop="500")

        with pytest.raises(PlatformError, match="still present"):
            await adapter.cancel_trailing_stop("BTCUSDT:0:trailing", _instrument())

    async def test_cancel_transport_error_raises(
        self,
        adapter: BybitAdapter,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.set_trading_stop.side_effect = FailedRequestError(
            request="POST /v5/position/trading-stop",
            message="Internal Server Error",
            status_code=500,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformConnectionError):
            await adapter.cancel_trailing_stop("BTCUSDT:0:trailing", _instrument())

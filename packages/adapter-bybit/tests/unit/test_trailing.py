"""Bybit native trailing payload translation."""

from __future__ import annotations

from decimal import Decimal

import pytest

from unified_trading_execution.bybit.orders import build_native_trailing_stop_payload
from unified_trading_execution.errors import UnsupportedOrderTypeError
from unified_trading_execution.trailing.types import (
    NativeTrailingRequest,
    TrailingBasis,
    TrailingOffsetType,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        currency="USDT",
        multiplier=1,
    )


def _request(
    *,
    basis: TrailingBasis | None,
    activation_price: Decimal | None = None,
    offset_type: TrailingOffsetType = TrailingOffsetType.PRICE,
) -> NativeTrailingRequest:
    return NativeTrailingRequest(
        instrument=_instrument(),
        position_id="BTCUSDT:0",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        offset=Decimal("500"),
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

"""Unit tests for Hyperliquid native trailing stop methods."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unified_trading_execution.adapter import NativeTrailingRequest, TrailingOffsetType
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.hyperliquid import HyperliquidAdapter, HyperliquidConfig
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument

_TEST_ADDRESS = "0x" + "00" * 20
_TEST_KEY = "0x" + "11" * 32


def _config() -> HyperliquidConfig:
    return HyperliquidConfig(private_key=_TEST_KEY, wallet_address=_TEST_ADDRESS)


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


class TestPlaceTrailingStop:
    @patch("hyperliquid.utils.signing.sign_l1_action")
    @patch("hyperliquid.utils.signing.get_timestamp_ms")
    async def test_place_percent_retracement(self, mock_timestamp, mock_sign):
        """Place trailing stop with percent retracement."""
        mock_timestamp.return_value = 1234567890
        mock_sign.return_value = {"r": "0x123", "s": "0x456", "v": 27}

        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {"BTC": 1}
        adapter._exchange.wallet = MagicMock()
        adapter._exchange.vault_address = None
        adapter._exchange.expires_after = None
        adapter._exchange.base_url = "https://api.hyperliquid.xyz"

        # Mock _run_exchange to return the response directly
        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"type": "trailingStop", "data": {"oid": 77738308}},
            }
        )

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.234"),
            offset_type=TrailingOffsetType.PERCENT,
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        result = await adapter.place_trailing_stop(request)

        assert result.native_id == "77738308"
        assert result.confirmed_offset == Decimal("1.234")
        assert result.activated is True

    @patch("hyperliquid.utils.signing.sign_l1_action")
    @patch("hyperliquid.utils.signing.get_timestamp_ms")
    async def test_place_price_retracement(self, mock_timestamp, mock_sign):
        """Place trailing stop with fixed price retracement."""
        mock_timestamp.return_value = 1234567890
        mock_sign.return_value = {"r": "0x123", "s": "0x456", "v": 27}

        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {"BTC": 1}
        adapter._exchange.wallet = MagicMock()
        adapter._exchange.vault_address = None
        adapter._exchange.expires_after = None
        adapter._exchange.base_url = "https://api.hyperliquid.xyz"

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"type": "trailingStop", "data": {"oid": 77738309}},
            }
        )

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("10"),
            offset_type=TrailingOffsetType.PRICE,
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        result = await adapter.place_trailing_stop(request)

        assert result.native_id == "77738309"
        assert result.confirmed_offset == Decimal("10")

    @patch("hyperliquid.utils.signing.sign_l1_action")
    @patch("hyperliquid.utils.signing.get_timestamp_ms")
    async def test_place_with_activation_price(self, mock_timestamp, mock_sign):
        """Place trailing stop with activation price."""
        mock_timestamp.return_value = 1234567890
        mock_sign.return_value = {"r": "0x123", "s": "0x456", "v": 27}

        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {"BTC": 1}
        adapter._exchange.wallet = MagicMock()
        adapter._exchange.vault_address = None
        adapter._exchange.expires_after = None
        adapter._exchange.base_url = "https://api.hyperliquid.xyz"

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "response": {"type": "trailingStop", "data": {"oid": 77738310}},
            }
        )

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.0"),
            offset_type=TrailingOffsetType.PERCENT,
            basis=None,
            activation_price=Decimal("65000"),
            client_id="test-client",
        )

        result = await adapter.place_trailing_stop(request)

        assert result.native_id == "77738310"
        assert result.activated is False

    async def test_spot_rejected(self):
        """Spot instruments raise InvalidSymbolError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        request = NativeTrailingRequest(
            instrument=_instrument(asset_class=AssetClass.SPOT, quote_currency="USDC"),
            position_id="BTC/USDC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.0"),
            offset_type=TrailingOffsetType.PERCENT,
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        with pytest.raises(InvalidSymbolError, match="does not support native trailing"):
            await adapter.place_trailing_stop(request)

    async def test_unknown_coin_raises(self):
        """Unknown coin raises InvalidSymbolError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {}  # Empty, so coin not found

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.0"),
            offset_type=TrailingOffsetType.PERCENT,
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        with pytest.raises(InvalidSymbolError, match="Unknown coin"):
            await adapter.place_trailing_stop(request)

    async def test_unsupported_offset_type_raises(self):
        """Unsupported offset type raises ValueError."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {"BTC": 1}

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.0"),
            offset_type=TrailingOffsetType.ATR,  # Not supported
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        with pytest.raises(ValueError, match="Unsupported trailing offset type"):
            await adapter.place_trailing_stop(request)

    @patch("hyperliquid.utils.signing.sign_l1_action")
    @patch("hyperliquid.utils.signing.get_timestamp_ms")
    async def test_malformed_response_raises(self, mock_timestamp, mock_sign):
        """Malformed response raises PlatformError."""
        mock_timestamp.return_value = 1234567890
        mock_sign.return_value = {"r": "0x123", "s": "0x456", "v": 27}

        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._exchange.info.coin_to_asset = {"BTC": 1}
        adapter._exchange.wallet = MagicMock()
        adapter._exchange.vault_address = None
        adapter._exchange.expires_after = None
        adapter._exchange.base_url = "https://api.hyperliquid.xyz"

        adapter._run_exchange = AsyncMock(return_value={"status": "ok", "response": {}})

        request = NativeTrailingRequest(
            instrument=_instrument(),
            position_id="BTC:oneWay",
            side=OrderSide.SELL,
            quantity=Decimal("0.5"),
            offset=Decimal("1.0"),
            offset_type=TrailingOffsetType.PERCENT,
            basis=None,
            activation_price=None,
            client_id="test-client",
        )

        with pytest.raises(Exception, match="missing oid"):
            await adapter.place_trailing_stop(request)


class TestGetTrailingStop:
    async def test_get_existing_trailing_stop(self):
        """Read an existing trailing stop order."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "order": {
                    "orderType": "TrailingStop",
                    "retracement": {"pct": "1.234%"},
                    "activationPx": "65000",
                },
            }
        )

        result = await adapter.get_trailing_stop("77738308", _instrument())

        assert result is not None
        assert result.native_id == "77738308"
        assert result.confirmed_offset == Decimal("1.234")
        assert result.activated is False

    async def test_get_price_retracement_trailing_stop(self):
        """Read trailing stop with price retracement."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "order": {
                    "orderType": "TrailingStop",
                    "retracement": {"px": "10"},
                    "activationPx": None,
                },
            }
        )

        result = await adapter.get_trailing_stop("77738308", _instrument())

        assert result is not None
        assert result.confirmed_offset == Decimal("10")
        assert result.activated is True

    async def test_get_unknown_order_returns_none(self):
        """Unknown order id returns None."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter._run_exchange = AsyncMock(return_value={"status": "unknownOid"})

        result = await adapter.get_trailing_stop("99999999", _instrument())

        assert result is None

    async def test_get_non_trailing_order_returns_none(self):
        """Order that is not a trailing stop returns None."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "order": {"orderType": "Limit"},
            }
        )

        result = await adapter.get_trailing_stop("77738308", _instrument())

        assert result is None

    async def test_malformed_order_returns_none(self):
        """Malformed order response with empty retracement returns None."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()

        adapter._run_exchange = AsyncMock(
            return_value={
                "status": "ok",
                "order": {"orderType": "TrailingStop", "retracement": {}},
            }
        )

        result = await adapter.get_trailing_stop("77738308", _instrument())
        assert result is None


class TestCancelTrailingStop:
    async def test_cancel_trailing_stop(self):
        """Cancel a trailing stop order."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        await adapter.cancel_trailing_stop("77738308", _instrument())

        # Verify cancel was called
        assert adapter._run_exchange.called

    async def test_cancel_uses_exchange_cancel(self):
        """Cancel uses the exchange.cancel method."""
        adapter = HyperliquidAdapter(_config())
        adapter._exchange = MagicMock()
        adapter._run_exchange = AsyncMock(return_value={"status": "ok"})

        await adapter.cancel_trailing_stop("77738308", _instrument())

        # Verify the correct method was called
        adapter._run_exchange.assert_called_once()
        call_args = adapter._run_exchange.call_args
        assert call_args[0][0] == adapter._exchange.cancel
        assert call_args[0][1] == "BTC"
        assert call_args[0][2] == 77738308

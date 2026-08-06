"""Unit tests for BybitAdapter leverage and margin-mode operations (Phase 3, Step 5).

Covers set/get/remove leverage, margin-mode switching, max-leverage validation,
the open-position guard, and spot rejection.  HTTP is mocked via the shared
``mock_pybit_http`` fixture; intent persistence is verified against a real
in-memory SQLiteStateStore.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock

import pytest
from pybit.exceptions import InvalidRequestError

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import LeverageExceedsMaxError
from unified_trading_execution.bybit.margin import LeverageConfig, MarginMode
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.events import EventBus
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _linear_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="USDT",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _spot_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def _spec_response(max_leverage: str | None = "100") -> tuple[dict[str, Any], None, dict[str, str]]:
    entry: dict[str, Any] = {
        "symbol": "BTCUSDT",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "status": "Trading",
        "lotSizeFilter": {
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "maxOrderQty": "1000",
            "minNotionalValue": "5",
        },
        "priceFilter": {"tickSize": "0.10", "minPrice": "0.10", "maxPrice": "1999999.80"},
    }
    if max_leverage is not None:
        entry["leverageFilter"] = {
            "minLeverage": "1",
            "maxLeverage": max_leverage,
            "leverageStep": "0.01",
        }
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": [entry]}}, None, {})


def _position_response(
    *,
    size: str = "0",
    leverage: str = "10",
    trade_mode: str = "0",
) -> tuple[dict[str, Any], None, dict[str, str]]:
    entry = {
        "symbol": "BTCUSDT",
        "size": size,
        "leverage": leverage,
        "tradeMode": trade_mode,
    }
    return ({"retCode": 0, "retMsg": "OK", "result": {"list": [entry]}}, None, {})


def _ok() -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"retCode": 0, "retMsg": "OK", "result": {}}, None, {})


@pytest.fixture
async def store_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
    mock_pybit_http: MagicMock,
) -> AsyncIterator[tuple[BybitAdapter, SQLiteStateStore]]:
    """A BybitAdapter wired to a real in-memory state store (and mocked HTTP).

    The ``mock_pybit_http`` autouse fixture keeps pybit HTTP mocked; this
    fixture additionally provides an initialized ``SQLiteStateStore`` so
    intent persistence is exercised against the real SQL backend.
    """
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    config = BybitConfig(
        api_key=bybit_config.api_key,
        api_secret=bybit_config.api_secret,
        testnet=bybit_config.testnet,
        leverage=LeverageConfig(),
    )
    adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)
    yield adapter, store
    await store.close()


class TestSetLeverage:
    async def test_set_leverage_linear_success(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0")
        mock_pybit_http.set_leverage.return_value = _ok()

        await adapter.set_leverage(_linear_instrument(), 10)

        # One-way mode: buyLeverage == sellLeverage == 10.
        mock_pybit_http.set_leverage.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            buyLeverage="10",
            sellLeverage="10",
        )
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"

    async def test_set_leverage_spot_raises(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter

        with pytest.raises(InvalidSymbolError):
            await adapter.set_leverage(_spot_instrument(), 10)

        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    async def test_set_leverage_exceeds_max_raises(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")

        with pytest.raises(LeverageExceedsMaxError):
            await adapter.set_leverage(_linear_instrument(), 101)

        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    async def test_set_leverage_blocked_by_open_position(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0.5")

        with pytest.raises(PlatformError):
            await adapter.set_leverage(_linear_instrument(), 10)

        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    async def test_set_leverage_open_position_guard_disabled(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0.5")
        mock_pybit_http.set_leverage.return_value = _ok()
        store = SQLiteStateStore(":memory:")
        await store.initialize()
        try:
            config = BybitConfig(
                api_key=bybit_config.api_key,
                api_secret=bybit_config.api_secret,
                testnet=bybit_config.testnet,
                leverage=LeverageConfig(block_on_open_position=False),
            )
            adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)

            await adapter.set_leverage(_linear_instrument(), 10)
        finally:
            await store.close()

        mock_pybit_http.set_leverage.assert_called_once()

    async def test_set_leverage_platform_rejection_propagates(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0")
        mock_pybit_http.set_leverage.side_effect = InvalidRequestError(
            request="POST /v5/position/set-leverage",
            message="Invalid leverage",
            status_code=12222,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformError):
            await adapter.set_leverage(_linear_instrument(), 10)

        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    async def test_set_leverage_already_active_is_noop(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        """Bybit rejects set-leverage to the current value (110043) — treat as success."""
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0")
        mock_pybit_http.set_leverage.side_effect = InvalidRequestError(
            request="POST /v5/position/set-leverage",
            message="leverage not modified",
            status_code=110043,
            time="12:00:00",
            resp_headers=None,
        )

        await adapter.set_leverage(_linear_instrument(), 10)

        mock_pybit_http.set_leverage.assert_called_once()
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"


class TestGetLeverage:
    async def test_get_leverage_returns_value(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = _position_response(leverage="20")

        assert await adapter.get_leverage(_linear_instrument()) == 20

    async def test_get_leverage_spot_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter

        assert await adapter.get_leverage(_spot_instrument()) is None
        mock_pybit_http.get_positions.assert_not_called()

    async def test_get_leverage_no_position_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        mock_pybit_http.get_positions.return_value = (
            {"retCode": 0, "retMsg": "OK", "result": {"list": []}},
            None,
            {},
        )

        assert await adapter.get_leverage(_linear_instrument()) is None


class TestRemoveLeverage:
    async def test_remove_leverage_drops_intent_only(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        await store.set_adapter_config("leverage.BTCUSDT", "10")

        await adapter.remove_leverage(_linear_instrument())

        assert await store.get_adapter_config("leverage.BTCUSDT") is None
        mock_pybit_http.set_leverage.assert_not_called()


class TestRemoveMarginMode:
    async def test_remove_margin_mode_drops_intent_only(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        await store.set_adapter_config("margin_mode.BTCUSDT", "isolated")
        await store.set_adapter_config("leverage.BTCUSDT", "10")

        await adapter.remove_margin_mode(_linear_instrument())

        assert await store.get_adapter_config("margin_mode.BTCUSDT") is None
        # Leverage intent is independently managed and left untouched.
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"
        mock_pybit_http.set_margin_mode.assert_not_called()
        mock_pybit_http.set_leverage.assert_not_called()


class TestSetMarginMode:
    """Set margin mode on a UTA (unified) account.

    Margin mode is account-wide: ``set-margin-mode`` takes ``setMarginMode``
    (no symbol or leverage); leverage is applied separately via ``set_leverage``.
    """

    def _leverage_spec(self, mock_pybit_http: MagicMock) -> None:
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0")
        mock_pybit_http.set_leverage.return_value = _ok()

    async def test_set_margin_mode_cross_to_isolated(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        self._leverage_spec(mock_pybit_http)
        mock_pybit_http.set_margin_mode.return_value = _ok()

        await adapter.set_margin_mode(_linear_instrument(), MarginMode.ISOLATED, leverage=10)

        mock_pybit_http.set_margin_mode.assert_called_once_with(setMarginMode="ISOLATED_MARGIN")
        mock_pybit_http.switch_margin_mode.assert_not_called()
        mock_pybit_http.set_leverage.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            buyLeverage="10",
            sellLeverage="10",
        )
        assert await store.get_adapter_config("margin_mode.BTCUSDT") == "isolated"
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"

    async def test_set_margin_mode_isolated_to_cross(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        self._leverage_spec(mock_pybit_http)
        mock_pybit_http.set_margin_mode.return_value = _ok()

        await adapter.set_margin_mode(_linear_instrument(), MarginMode.CROSS, leverage=5)

        mock_pybit_http.set_margin_mode.assert_called_once_with(setMarginMode="REGULAR_MARGIN")
        mock_pybit_http.set_leverage.assert_called_once_with(
            category="linear",
            symbol="BTCUSDT",
            buyLeverage="5",
            sellLeverage="5",
        )
        assert await store.get_adapter_config("margin_mode.BTCUSDT") == "cross"
        assert await store.get_adapter_config("leverage.BTCUSDT") == "5"

    async def test_set_margin_mode_spot_raises(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter

        with pytest.raises(InvalidSymbolError):
            await adapter.set_margin_mode(_spot_instrument(), MarginMode.CROSS, leverage=10)

        mock_pybit_http.set_margin_mode.assert_not_called()
        mock_pybit_http.set_leverage.assert_not_called()

    async def test_set_margin_mode_platform_rejection_propagates(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        mock_pybit_http.set_margin_mode.side_effect = InvalidRequestError(
            request="POST /v5/account/set-margin-mode",
            message="Bad request",
            status_code=400,
            time="12:00:00",
            resp_headers=None,
        )

        with pytest.raises(PlatformError):
            await adapter.set_margin_mode(_linear_instrument(), MarginMode.ISOLATED, leverage=10)

        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("margin_mode.BTCUSDT") is None

    async def test_set_margin_mode_uta_already_active_is_noop(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, store = store_adapter
        self._leverage_spec(mock_pybit_http)
        mock_pybit_http.set_margin_mode.side_effect = InvalidRequestError(
            request="POST /v5/account/set-margin-mode",
            message="Cross/isolated margin mode has not been modified",
            status_code=110026,
            time="12:00:00",
            resp_headers=None,
        )

        await adapter.set_margin_mode(_linear_instrument(), MarginMode.ISOLATED, leverage=10)

        assert await store.get_adapter_config("margin_mode.BTCUSDT") == "isolated"
        assert await store.get_adapter_config("leverage.BTCUSDT") == "10"

    async def test_set_margin_mode_blocked_by_open_position(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        """Open-position guard runs before the account switch — atomic no-op."""
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0.5")
        mock_pybit_http.set_margin_mode.return_value = _ok()

        with pytest.raises(PlatformError):
            await adapter.set_margin_mode(_linear_instrument(), MarginMode.ISOLATED, leverage=10)

        # Nothing reached the platform and nothing was persisted.
        mock_pybit_http.set_margin_mode.assert_not_called()
        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("margin_mode.BTCUSDT") is None
        assert await store.get_adapter_config("leverage.BTCUSDT") is None

    async def test_set_margin_mode_open_position_guard_disabled(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        mock_pybit_http: MagicMock,
    ) -> None:
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")
        mock_pybit_http.get_positions.return_value = _position_response(size="0.5")
        mock_pybit_http.set_margin_mode.return_value = _ok()
        mock_pybit_http.set_leverage.return_value = _ok()
        store = SQLiteStateStore(":memory:")
        await store.initialize()
        try:
            config = BybitConfig(
                api_key=bybit_config.api_key,
                api_secret=bybit_config.api_secret,
                testnet=bybit_config.testnet,
                leverage=LeverageConfig(block_on_open_position=False),
            )
            adapter = BybitAdapter(config, event_bus=event_bus, state_store=store)

            await adapter.set_margin_mode(
                _linear_instrument(), MarginMode.ISOLATED, leverage=10
            )
        finally:
            await store.close()

        mock_pybit_http.set_margin_mode.assert_called_once()
        mock_pybit_http.set_leverage.assert_called_once()

    async def test_set_margin_mode_rejects_invalid_leverage_atomically(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        """Leverage cap is validated before the switch — nothing changes if invalid."""
        adapter, store = store_adapter
        mock_pybit_http.get_instruments_info.return_value = _spec_response("100")

        with pytest.raises(LeverageExceedsMaxError):
            await adapter.set_margin_mode(_linear_instrument(), MarginMode.ISOLATED, leverage=101)

        mock_pybit_http.set_margin_mode.assert_not_called()
        mock_pybit_http.set_leverage.assert_not_called()
        assert await store.get_adapter_config("margin_mode.BTCUSDT") is None
        assert await store.get_adapter_config("leverage.BTCUSDT") is None


class TestGetMarginMode:
    """Get margin mode on a UTA account — reads account-wide ``marginMode``."""

    def _mock_with_mode(self, mock_pybit_http: MagicMock, mode: str) -> None:
        mock_pybit_http.get_account_info.return_value = (
            {"result": {"marginMode": mode}},
            None,
            {},
        )

    async def test_get_margin_mode_cross(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        self._mock_with_mode(mock_pybit_http, "REGULAR_MARGIN")

        assert await adapter.get_margin_mode(_linear_instrument()) is MarginMode.CROSS
        mock_pybit_http.get_positions.assert_not_called()

    async def test_get_margin_mode_isolated(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        self._mock_with_mode(mock_pybit_http, "ISOLATED_MARGIN")

        assert await adapter.get_margin_mode(_linear_instrument()) is MarginMode.ISOLATED

    async def test_get_margin_mode_portfolio_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        self._mock_with_mode(mock_pybit_http, "PORTFOLIO_MARGIN")

        assert await adapter.get_margin_mode(_linear_instrument()) is None

    async def test_get_margin_mode_unknown_returns_none(
        self,
        store_adapter: tuple[BybitAdapter, SQLiteStateStore],
        mock_pybit_http: MagicMock,
    ) -> None:
        adapter, _ = store_adapter
        self._mock_with_mode(mock_pybit_http, "")

        assert await adapter.get_margin_mode(_linear_instrument()) is None

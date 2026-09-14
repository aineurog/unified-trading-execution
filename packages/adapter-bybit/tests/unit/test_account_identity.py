"""Unit tests for Bybit account-identity resolution (Section 17.10).

The canonical store-path identity must be the real Bybit ``userID`` from
``GET /v5/user/query-api`` — never a shared ``"bybit-account"`` constant that
would let two accounts collide on one state file.
"""

from __future__ import annotations

from typing import Any

import pytest

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.errors import PlatformConnectionError
from unified_trading_execution.events import EventBus


def _user_id_response(user_id: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
    return ({"result": {"userID": user_id}}, None, {})


class TestResolveAccountId:
    async def test_resolves_user_id_from_query_api(
        self, adapter: BybitAdapter, mock_pybit_http: Any
    ) -> None:
        mock_pybit_http.get_api_key_information.return_value = _user_id_response("123456")

        assert await adapter.resolve_account_id() == "123456"

    async def test_resolved_once_and_cached(
        self, adapter: BybitAdapter, mock_pybit_http: Any
    ) -> None:
        mock_pybit_http.get_api_key_information.return_value = _user_id_response("123456")

        first = await adapter.resolve_account_id()
        second = await adapter.resolve_account_id()

        assert first == second == "123456"
        mock_pybit_http.get_api_key_information.assert_called_once()

    async def test_explicit_config_account_id_wins(
        self, mock_pybit_http: Any, event_bus: EventBus
    ) -> None:
        adapter = BybitAdapter(
            BybitConfig(api_key="k", api_secret="s", account_id="acc-1"),
            event_bus=event_bus,
        )

        assert await adapter.resolve_account_id() == "acc-1"
        mock_pybit_http.get_api_key_information.assert_not_called()

    async def test_raises_when_user_id_missing(
        self, adapter: BybitAdapter, mock_pybit_http: Any
    ) -> None:
        mock_pybit_http.get_api_key_information.return_value = ({"result": {}}, None, {})

        with pytest.raises(PlatformConnectionError):
            await adapter.resolve_account_id()

    async def test_raises_when_user_id_empty(
        self, adapter: BybitAdapter, mock_pybit_http: Any
    ) -> None:
        mock_pybit_http.get_api_key_information.return_value = _user_id_response("")

        with pytest.raises(PlatformConnectionError):
            await adapter.resolve_account_id()


class TestAccountIdProperty:
    def test_placeholder_before_resolution(self, adapter: BybitAdapter) -> None:
        # Before resolution the property is a stable, per-key placeholder — not
        # a shared constant, so two keys never collapse onto one store path.
        assert adapter.account_id != "bybit-account"
        assert adapter.account_id.startswith("bybit-")
        assert adapter.account_id == adapter.account_id  # stable across calls

    def test_placeholder_is_key_specific(self, event_bus: EventBus) -> None:
        a = BybitAdapter(BybitConfig(api_key="key-a", api_secret="s"), event_bus=event_bus)
        b = BybitAdapter(BybitConfig(api_key="key-b", api_secret="s"), event_bus=event_bus)
        assert a.account_id != b.account_id

    async def test_reflects_resolved_id(self, adapter: BybitAdapter, mock_pybit_http: Any) -> None:
        mock_pybit_http.get_api_key_information.return_value = _user_id_response("123456")

        await adapter.resolve_account_id()

        assert adapter.account_id == "123456"

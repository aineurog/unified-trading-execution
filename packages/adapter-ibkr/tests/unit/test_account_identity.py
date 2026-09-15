"""Unit tests for IBKR account-identity resolution.

The canonical store-path identity must be the live managed account once
connected, the configured account before that, and a deterministic
per-connection placeholder otherwise — never a shared constant that would
let two connections collide on one state file.
"""

from __future__ import annotations

from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig


def _config_no_account(**overrides: object) -> IBKRConfig:
    base: dict[str, object] = {
        "host": "127.0.0.1",
        "port": 4002,
        "client_id": 999,
        "account": None,
    }
    base.update(overrides)
    return IBKRConfig(**base)  # type: ignore[arg-type]


class TestAccountIdProperty:
    def test_placeholder_before_connect(self, event_bus: EventBus) -> None:
        adapter = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        assert adapter.account_id == "ibkr-127.0.0.1-4002-999"
        assert adapter.account_id == adapter.account_id  # stable across calls

    def test_placeholder_is_connection_specific(self, event_bus: EventBus) -> None:
        a = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        b = IBKRAdapter(_config_no_account(client_id=1000), event_bus=event_bus)
        c = IBKRAdapter(_config_no_account(host="10.0.0.5"), event_bus=event_bus)
        assert a.account_id != b.account_id
        assert a.account_id != c.account_id

    def test_config_account_wins_pre_connect(self, event_bus: EventBus) -> None:
        adapter = IBKRAdapter(_config_no_account(account="DU123"), event_bus=event_bus)
        assert adapter.account_id == "DU123"

    async def test_managed_account_wins_post_connect(
        self,
        event_bus: EventBus,
        mock_ib_async_module: object,
    ) -> None:
        adapter = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        await adapter.connect()
        assert adapter.account_id == "DU_TEST"

    async def test_config_account_preferred_over_gateway_list(
        self,
        event_bus: EventBus,
        mock_ib_async_module: object,
    ) -> None:
        adapter = IBKRAdapter(_config_no_account(account="DU123"), event_bus=event_bus)
        await adapter.connect()
        assert adapter.account_id == "DU123"


class TestResolveAccountId:
    async def test_resolve_matches_account_id_pre_connect(self, event_bus: EventBus) -> None:
        adapter = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        assert await adapter.resolve_account_id() == adapter.account_id

    async def test_resolve_returns_managed_post_connect(
        self,
        event_bus: EventBus,
        mock_ib_async_module: object,
    ) -> None:
        adapter = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        await adapter.connect()
        assert await adapter.resolve_account_id() == "DU_TEST"

    async def test_resolve_never_raises(self, event_bus: EventBus) -> None:
        adapter = IBKRAdapter(_config_no_account(), event_bus=event_bus)
        # No connection, no account — must degrade to the placeholder.
        assert await adapter.resolve_account_id() == "ibkr-127.0.0.1-4002-999"

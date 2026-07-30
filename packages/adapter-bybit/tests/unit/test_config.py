from __future__ import annotations

from unified_trading_execution.bybit.config import BybitConfig


class TestBybitConfig:
    def test_defaults_to_testnet_not_demo(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s")
        assert config.testnet is True
        assert config.demo is False

    def test_testnet_no_demo(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", testnet=True, demo=False)
        assert config.testnet is True
        assert config.demo is False

    def test_mainnet_no_demo(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", testnet=False, demo=False)
        assert config.testnet is False
        assert config.demo is False

    def test_demo_mainnet(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", testnet=False, demo=True)
        assert config.testnet is False
        assert config.demo is True

    def test_demo_testnet(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", testnet=True, demo=True)
        assert config.testnet is True
        assert config.demo is True

    def test_platform_name_default(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s")
        assert config.platform_name == "bybit"

    def test_account_id_default(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s")
        assert config.account_id == "bybit-account"

    def test_custom_platform_name(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", platform_name="my-bybit")
        assert config.platform_name == "my-bybit"

    def test_custom_account_id(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s", account_id="acc-1")
        assert config.account_id == "acc-1"

    def test_config_is_frozen(self) -> None:
        config = BybitConfig(api_key="k", api_secret="s")
        import pytest
        with pytest.raises(AttributeError):
            config.api_key = "new-key"  # type: ignore[misc]

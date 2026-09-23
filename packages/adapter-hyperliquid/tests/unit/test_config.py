"""Unit tests for HyperliquidConfig validation."""

from __future__ import annotations

from typing import Any

import pytest

from unified_trading_execution.hyperliquid import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode

_TEST_ADDRESS = "0x0000000000000000000000000000000000000001"
_TEST_KEY = "0x" + "11" * 32


def _config(**kwargs: Any) -> HyperliquidConfig:
    """Build a config with valid credentials plus overrides."""
    args: dict[str, Any] = {"wallet_address": _TEST_ADDRESS, "private_key": _TEST_KEY}
    args.update(kwargs)
    return HyperliquidConfig(**args)


def test_defaults() -> None:
    config = _config()
    assert config.testnet is False
    assert config.private_key == _TEST_KEY
    assert config.default_margin_mode == MarginMode.CROSS
    assert config.default_leverage == 1
    assert config.platform_name == "hyperliquid"
    assert config.instrument_spec_cache_ttl == 86400.0
    assert config.request_timeout_seconds == 10.0


def test_key_without_prefix_allowed() -> None:
    config = _config(private_key="11" * 32)
    assert config.private_key == "11" * 32


@pytest.mark.parametrize("key", ["", "0x123", "not-a-key", "0x" + "zz" * 32, "0x" + "11" * 31])
def test_private_key_shape_rejected(key: str) -> None:
    with pytest.raises(ValueError, match="private_key"):
        _config(private_key=key)


def test_margin_mode_string_coerced() -> None:
    config = _config(default_margin_mode="isolated")
    assert config.default_margin_mode == MarginMode.ISOLATED


def test_margin_mode_invalid_string_rejected() -> None:
    with pytest.raises(ValueError, match="default_margin_mode"):
        _config(default_margin_mode="portfolio")


@pytest.mark.parametrize(
    "address",
    ["", "0x123", "not-an-address", "0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"],
)
def test_wallet_address_shape_rejected(address: str) -> None:
    with pytest.raises(ValueError, match="wallet_address"):
        _config(wallet_address=address)


def test_wallet_address_without_prefix_rejected() -> None:
    with pytest.raises(ValueError, match="wallet_address"):
        _config(wallet_address="0" * 40)


def test_default_leverage_below_one_rejected() -> None:
    with pytest.raises(ValueError, match="default_leverage"):
        _config(default_leverage=0)


@pytest.mark.parametrize("ttl", [0.0, -1.0])
def test_non_positive_ttl_rejected(ttl: float) -> None:
    with pytest.raises(ValueError, match="instrument_spec_cache_ttl"):
        _config(instrument_spec_cache_ttl=ttl)


def test_none_ttl_allowed() -> None:
    config = _config(instrument_spec_cache_ttl=None)
    assert config.instrument_spec_cache_ttl is None


@pytest.mark.parametrize("timeout", [0.0, -5.0])
def test_non_positive_timeout_rejected(timeout: float) -> None:
    with pytest.raises(ValueError, match="request_timeout_seconds"):
        _config(request_timeout_seconds=timeout)

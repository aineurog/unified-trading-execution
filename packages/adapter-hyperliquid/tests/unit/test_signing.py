"""Unit tests for signing guards (no network).

Conformance pins the SDK byte layout (L1 domain, phantom source,
action-hash inputs, trailing-zero stripping) so an SDK upgrade that
changes signing fails loudly here instead of as venue rejects.
"""

from __future__ import annotations

from typing import Any

import pytest
from eth_account import Account
from hyperliquid.utils.signing import (
    action_hash,
    construct_phantom_agent,
    float_to_wire,
    l1_payload,
    recover_agent_or_user_from_l1_action,
    sign_l1_action,
)

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.hyperliquid.signing import (
    assert_user_role_for_signing,
    build_wallet,
)

_NONCE = 1_700_000_000_000


def _order_action() -> dict[str, Any]:
    return {
        "type": "order",
        "orders": [
            {
                "a": 0,
                "b": True,
                "p": "100",
                "s": "0.01",
                "r": False,
                "t": {"limit": {"tif": "Gtc"}},
            }
        ],
        "grouping": "na",
    }


def _sign(wallet: Any, action: dict[str, Any], *, is_mainnet: bool) -> dict[str, Any]:
    return sign_l1_action(wallet, action, None, _NONCE, None, is_mainnet)


def _recover(action: dict[str, Any], signature: dict[str, Any], *, is_mainnet: bool) -> str:
    return recover_agent_or_user_from_l1_action(action, signature, None, _NONCE, None, is_mainnet)


def test_l1_domain_is_chain_1337() -> None:
    domain = l1_payload(construct_phantom_agent(b"\x00" * 32, True))["domain"]
    assert domain["chainId"] == 1337
    assert domain["name"] == "Exchange"
    assert domain["version"] == "1"


def test_phantom_source_marks_testnet() -> None:
    digest = action_hash(_order_action(), None, _NONCE, None)
    assert construct_phantom_agent(digest, True)["source"] == "a"
    assert construct_phantom_agent(digest, False)["source"] == "b"


def test_sign_recover_roundtrip() -> None:
    wallet = Account.create()
    action = _order_action()
    signature = _sign(wallet, action, is_mainnet=False)
    assert _recover(action, signature, is_mainnet=False) == wallet.address


def test_reordered_action_keys_mismatch() -> None:
    wallet = Account.create()
    action = _order_action()
    signature = _sign(wallet, action, is_mainnet=False)
    reordered = {key: action[key] for key in reversed(list(action.keys()))}
    assert _recover(reordered, signature, is_mainnet=False) != wallet.address


def test_altered_price_mismatch() -> None:
    wallet = Account.create()
    action = _order_action()
    signature = _sign(wallet, action, is_mainnet=False)
    tampered = _order_action()
    tampered["orders"][0]["p"] = "101"
    assert _recover(tampered, signature, is_mainnet=False) != wallet.address


def test_wrong_network_flag_mismatch() -> None:
    wallet = Account.create()
    action = _order_action()
    signature = _sign(wallet, action, is_mainnet=False)
    assert _recover(action, signature, is_mainnet=True) != wallet.address


def test_float_to_wire_strips_trailing_zeroes() -> None:
    assert float_to_wire(1.50) == "1.5"
    assert float_to_wire(100.0) == "100"
    with pytest.raises(ValueError, match="rounding"):
        float_to_wire(1 / 3)


def test_build_wallet_roundtrip() -> None:
    source = Account.create()
    for key in (source.key.hex(), "0x" + source.key.hex()):
        assert build_wallet(key).address == source.address


def test_build_wallet_rejects_garbage() -> None:
    with pytest.raises(PlatformError, match="private_key"):
        build_wallet("not-a-key")


@pytest.mark.parametrize("role", ["user", "agent"])
def test_user_roles_permitted(role: str) -> None:
    assert_user_role_for_signing(role)


@pytest.mark.parametrize("role", [None, "missing", ""])
def test_user_roles_rejected(role: str | None) -> None:
    with pytest.raises(PlatformError, match="approveAgent"):
        assert_user_role_for_signing(role)


@pytest.mark.parametrize("role", ["vault", "subAccount"])
def test_account_types_rejected(role: str) -> None:
    with pytest.raises(PlatformError, match="not supported"):
        assert_user_role_for_signing(role)

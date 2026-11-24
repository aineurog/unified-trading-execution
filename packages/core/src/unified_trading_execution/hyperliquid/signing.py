"""EIP-712 signing guards for Hyperliquid actions.

The SDK (``hyperliquid/utils/signing.py``) owns the cryptography and the
mutating call path (``Exchange`` methods take coin names and mint their own
millisecond nonces); this module owns the guards around it: wallet
construction and the ``approveAgent`` role assertion.  No own nonce manager
by design — concurrent same-millisecond writes can rarely collide on nonce
and be silently rejected, which engine retry/reconcile recovers with fresh
timestamps.  That transient, self-healing risk is accepted in exchange for
using the SDK's call path directly.
"""

from __future__ import annotations

from eth_account import Account
from eth_account.signers.local import LocalAccount

from unified_trading_execution.errors import PlatformError


def build_wallet(private_key: str) -> LocalAccount:
    """Build the SDK signing wallet from a configured private key.

    Single construction point so a malformed key fails loudly once at
    connect, not per order.  Accepts the same shapes as the config
    validation (64 hex chars, ``0x`` prefix optional).
    """
    try:
        wallet: LocalAccount = Account.from_key(private_key)
    except Exception as exc:
        raise PlatformError("Hyperliquid private_key is not a valid signing key") from exc
    return wallet


def assert_user_role_for_signing(user_role: str | None) -> None:
    """Assert the connected key's ``info.userRole`` permits signing.

    ``"user"`` (main key) and ``"agent"`` (approved API wallet — the
    recommended setup) may sign.  ``approveAgent`` is a user-signed
    (main-key) one-time setup op carrying a name and a bounded expiry; a
    ``None``/``"missing"`` role fails loud here instead of surfacing as
    opaque venue rejects later.  ``"vault"``/``"subAccount"`` are rejected
    as unsupported account types — vault and subaccount trading are out of
    scope, not a setup step the user can complete.
    """
    if user_role in ("user", "agent"):
        return
    if user_role in ("vault", "subAccount"):
        raise PlatformError(
            f"Hyperliquid userRole {user_role!r} is not supported — "
            "trade with a main or approved agent key instead"
        )
    raise PlatformError(
        f"Hyperliquid signing key has userRole {user_role!r} — run approveAgent "
        "with the main key (name, bounded expiry) before connecting"
    )

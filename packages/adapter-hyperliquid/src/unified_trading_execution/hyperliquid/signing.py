"""EIP-712 signing guards + nonce discipline for Hyperliquid actions.

The SDK (``hyperliquid/utils/signing.py``) owns the cryptography; this module
owns the discipline around it: L1 domain guards, the per-``Exchange``
nonce manager, and the ``approveAgent`` role assertion.
"""

from __future__ import annotations

# L1 signing domain chain id — always 1337, even on testnet.  Testnet uses
# ``source: "b"``, never the wallet network.
L1_CHAIN_ID: int = 1337

# Testnet action source marker.
TESTNET_SOURCE: str = "b"


def assert_l1_chain_id(chain_id: int) -> None:
    """Assert the signing domain uses the canonical L1 chain id.

    The venue rejects (or misattributes) actions signed under any other
    chain id; injected-provider signing is banned — raw API-wallet key only.
    """
    raise NotImplementedError


class NonceManager:
    """Per-``Exchange``-instance nonce manager.

    One manager per process (the SDK nonce state is instance-scoped):
    atomic counter with ``next = max(counter + 1, now_ms)``.  The venue
    window is generous (trailing two days to leading one day; machine
    expected NTP-synced) but a nonce must never be reused —
    duplicate/non-monotonic nonces are silent rejects.  Survives reconnects
    (in-memory counter plus wall-clock fast-forward).
    """

    def __init__(self) -> None:
        """Create a nonce manager with no previously issued nonce."""
        raise NotImplementedError

    def next_nonce(self) -> int:
        """Return the next strictly-monotonic nonce."""
        raise NotImplementedError


def assert_user_role_for_signing(user_role: str | None) -> None:
    """Assert the connected key's ``info.userRole`` permits signing.

    ``approveAgent`` is a user-signed (main-key) one-time setup op carrying
    a name and a bounded expiry; the adapter fails loud on role ``missing``.
    """
    raise NotImplementedError

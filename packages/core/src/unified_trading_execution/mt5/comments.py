"""Pack ``client_order_id`` into an MT5 order comment and recover it later.

MT5 order comments are limited to 29 characters at the client side (the
MetaTrader5 Python package rejects a longer comment with ``-2 Invalid
"comment" argument`` before the request reaches the server — measured
empirically; the platform's own slot allows 31).  A 36-character uuid7
``client_order_id`` therefore cannot be stored raw.  A UUID is exactly 16
bytes; base62 (``0-9A-Za-z``, 62 symbols ≈ 5.95 bits/char) packs those 16
bytes into 22 characters.  With a ``U:`` marker prefix the full comment is
24 characters — inside every known limit with room to spare, lossless, and
collision-free.

The comment travels atomically with ``order_send`` and survives in MT5
history (orders, positions and deals all inherit it), so a restarted
engine can rebuild its ``client_order_id → ticket`` maps by scanning MT5
itself.  It is best-effort by design: brokers may rewrite or truncate
comments, so the authoritative mapping for engine-placed orders lives in
the state store (see ``_seed_mappings_from_state_store`` in ``adapter.py``);
the comment is a redundant cross-check and the fallback for orders placed
outside the engine.

Only canonical lowercase hyphenated UUIDs are encodable.  Any other
``client_order_id`` (user-supplied custom strings, upper-case UUIDs) is
rejected with ``None`` so the round-trip is byte-identical; such orders
rely on the in-memory ticket maps alone.
"""

from __future__ import annotations

import re
import uuid

# MT5 order comment limit enforced by the trade API (measured: 29 chars).
_COMMENT_MAX_LENGTH = 29

_UUID_PREFIX = "U:"
# base62 width for exactly 16 bytes: ceil(128 / log2(62)) = 22.
_UUID_PAYLOAD_LENGTH = 22

# Canonical lowercase hyphenated UUID — guarantees a byte-identical round
# trip (no casing/variant normalization drift).
_CANONICAL_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_ALPHABET_INDEX = {char: i for i, char in enumerate(_ALPHABET)}


def encode_client_order_id(client_order_id: str) -> str | None:
    """Return the MT5 comment carrying *client_order_id*, or ``None``.

    ``None`` means the id cannot be packed losslessly (it is not a
    canonical lowercase UUID) — the caller should place the order without
    a comment and rely on the in-memory ticket maps.
    """
    if not _CANONICAL_UUID_RE.fullmatch(client_order_id):
        return None
    data = uuid.UUID(client_order_id).bytes
    comment = _UUID_PREFIX + _encode_fixed_base62(data, _UUID_PAYLOAD_LENGTH)
    if len(comment) > _COMMENT_MAX_LENGTH:
        return None
    return comment


def decode_comment(comment: object) -> str | None:
    """Recover a ``client_order_id`` from an MT5 comment, or ``None``.

    ``None`` is returned for any comment that is not ours (broker text,
    manual orders, Close By ticket notes, mangled payloads) — this must
    never raise, so any runtime value is tolerated.
    """
    if not isinstance(comment, str) or not comment.startswith(_UUID_PREFIX):
        return None
    payload = comment[len(_UUID_PREFIX) :]
    if len(payload) != _UUID_PAYLOAD_LENGTH:
        return None
    try:
        data = _decode_fixed_base62(payload, 16)
    except ValueError:
        return None
    return str(uuid.UUID(bytes=data))


def _encode_fixed_base62(data: bytes, width: int) -> str:
    """Encode *data* as a *width*-char base62 string, zero-padded.

    Padding with leading zeros keeps the value identical, so decoding to a
    fixed number of bytes (``to_bytes(width)``) reproduces *data* exactly
    even when its first byte is ``0x00``.
    """
    value = int.from_bytes(data, "big")
    chars: list[str] = []
    while value:
        value, remainder = divmod(value, 62)
        chars.append(_ALPHABET[remainder])
    encoded = "".join(reversed(chars))
    return encoded.rjust(width, _ALPHABET[0])


def _decode_fixed_base62(payload: str, byte_length: int) -> bytes:
    """Decode a *payload* of exactly ``byte_length * 8 / log2(62)`` chars.

    Raises ``ValueError`` if *payload* contains a non-base62 character or
    its value does not fit in *byte_length* bytes.
    """
    value = 0
    for char in payload:
        digit = _ALPHABET_INDEX.get(char)
        if digit is None:
            raise ValueError(f"invalid base62 character {char!r}")
        value = value * 62 + digit
    try:
        return value.to_bytes(byte_length, "big")
    except OverflowError as exc:
        raise ValueError("payload does not fit") from exc

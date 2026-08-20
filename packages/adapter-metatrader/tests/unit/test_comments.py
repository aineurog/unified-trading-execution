"""Unit tests for the MT5 comment ↔ client_order_id packing (comments.py)."""

from __future__ import annotations

import pytest
from uuid_extensions import uuid7

import unified_trading_execution.mt5.comments as comments
from unified_trading_execution.mt5.comments import (
    _COMMENT_MAX_LENGTH,
    _UUID_PAYLOAD_LENGTH,
    _UUID_PREFIX,
    decode_comment,
    encode_client_order_id,
)


class TestEncodeRoundTrip:
    def test_uuid_round_trip(self) -> None:
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        assert decode_comment(comment) == cid

    def test_comment_fits_mt5_limit(self) -> None:
        for _ in range(50):
            comment = encode_client_order_id(str(uuid7()))
            assert comment is not None
            assert len(comment) <= _COMMENT_MAX_LENGTH
            assert len(comment) == len(_UUID_PREFIX) + _UUID_PAYLOAD_LENGTH

    def test_marker_prefix(self) -> None:
        comment = encode_client_order_id(str(uuid7()))
        assert comment is not None
        assert comment.startswith(_UUID_PREFIX)

    def test_multiple_ids_encode_uniquely(self) -> None:
        comments = {encode_client_order_id(str(uuid7())) for _ in range(200)}
        assert len(comments) == 200

    def test_encode_refuses_comment_over_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A generated comment that would exceed the MT5 limit is refused."""
        monkeypatch.setattr(comments, "_COMMENT_MAX_LENGTH", 10)
        assert encode_client_order_id(str(uuid7())) is None


class TestEncodeRejects:
    def test_non_uuid_string(self) -> None:
        assert encode_client_order_id("trade-123") is None
        assert encode_client_order_id("") is None

    def test_uppercase_uuid(self) -> None:
        upper = str(uuid7()).upper()
        assert encode_client_order_id(upper) is None

    def test_plain_hex_without_hyphens(self) -> None:
        compact = str(uuid7()).replace("-", "")
        assert encode_client_order_id(compact) is None

    def test_unparseable_uuid(self) -> None:
        assert encode_client_order_id("not-a-uuid-at-all") is None
        assert encode_client_order_id("01234567-89ab-") is None


class TestDecodeRejects:
    def test_non_string(self) -> None:
        assert decode_comment(None) is None  # type: ignore[arg-type]
        assert decode_comment(42) is None  # type: ignore[arg-type]

    def test_empty_or_broker_text(self) -> None:
        assert decode_comment("") is None
        assert decode_comment("sl activated") is None
        assert decode_comment("manual") is None

    def test_wrong_prefix(self) -> None:
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        assert decode_comment("XTE:" + comment[len(_UUID_PREFIX) :]) is None

    def test_truncated_comment(self) -> None:
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        assert decode_comment(comment[:-1]) is None

    def test_appended_broker_text(self) -> None:
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        assert decode_comment(comment + " [SL]") is None

    def test_corrupted_payload_char(self) -> None:
        cid = str(uuid7())
        comment = encode_client_order_id(cid)
        assert comment is not None
        corrupted = comment[:4] + "!" + comment[5:]
        assert decode_comment(corrupted) is None

    def test_leading_zero_byte_round_trip(self) -> None:
        # Force a UUID whose first byte is 0x00 to prove padding is correct.
        cid = "00000000-1234-5678-9abc-def012345678"
        comment = encode_client_order_id(cid)
        assert comment is not None
        assert decode_comment(comment) == cid

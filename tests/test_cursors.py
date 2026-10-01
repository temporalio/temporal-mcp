import pytest

from ops_mcp.runtime.cursors import (
    MAX_TEMPORAL_CURSOR_TOKEN_BYTES,
    decode_temporal_cursor,
    encode_temporal_cursor,
)
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError


def test_temporal_cursor_round_trips_canonically() -> None:
    cursor = b"\x00opaque\xff"

    token = encode_temporal_cursor(cursor)

    assert token == "AG9wYXF1Zf8"
    assert decode_temporal_cursor(token) == cursor


@pytest.mark.parametrize("token", ("AG9wYXF1Zf8=", "not base64!"))
def test_temporal_cursor_rejects_noncanonical_or_malformed_input(token: str) -> None:
    with pytest.raises(OpsMcpError) as exc_info:
        decode_temporal_cursor(token)

    assert exc_info.value.code is ErrorCode.INVALID_ARGUMENT


def test_temporal_cursor_rejects_oversized_input_before_decode() -> None:
    with pytest.raises(OpsMcpError) as exc_info:
        decode_temporal_cursor("A" * (MAX_TEMPORAL_CURSOR_TOKEN_BYTES + 1))

    assert exc_info.value.code is ErrorCode.INVALID_ARGUMENT

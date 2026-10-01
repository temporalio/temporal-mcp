"""Canonical transport encoding for Temporal-owned pagination cursors."""

from __future__ import annotations

import base64
import binascii

from ops_mcp.runtime.errors import ErrorCode, OpsMcpError

MAX_TEMPORAL_CURSOR_TOKEN_BYTES = 8_000


def encode_temporal_cursor(cursor: bytes | None) -> str | None:
    if not cursor:
        return None
    token = base64.urlsafe_b64encode(cursor).decode("ascii").rstrip("=")
    if len(token.encode()) > MAX_TEMPORAL_CURSOR_TOKEN_BYTES:
        raise OpsMcpError(
            ErrorCode.RESULT_TOO_LARGE,
            "Temporal returned an oversized pagination cursor",
            retryable=False,
            details={"limit_bytes": MAX_TEMPORAL_CURSOR_TOKEN_BYTES},
        )
    return token


def decode_temporal_cursor(token: str | None, *, label: str = "page token") -> bytes | None:
    if token is None:
        return None
    if not isinstance(token, str):
        raise _invalid_cursor(label)
    try:
        if len(token.encode()) > MAX_TEMPORAL_CURSOR_TOKEN_BYTES:
            raise ValueError("oversized page token")
        encoded = token.encode("ascii")
        cursor = base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True
        )
        if encode_temporal_cursor(cursor) != token:
            raise ValueError("non-canonical page token")
        return cursor
    except (UnicodeError, binascii.Error, ValueError) as exc:
        raise _invalid_cursor(label) from exc


def _invalid_cursor(label: str) -> OpsMcpError:
    return OpsMcpError(
        ErrorCode.INVALID_ARGUMENT,
        f"The {label} is invalid or oversized",
        retryable=False,
    )

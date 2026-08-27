"""Signed opaque continuations for stateless, scope-bound inspection resumes."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass
from typing import Any

from ops_mcp.runtime.errors import ErrorCode, OpsMcpError

_PROCESS_SECRET = secrets.token_bytes(32)


@dataclass(frozen=True)
class ContinuationState:
    tool: str
    target_ref: str
    query: str
    sample_seed: str
    inspection_offset: int
    candidate_frame_size: int
    candidate_collection_complete: bool
    inspected_run_ledger: tuple[str, ...] = ()
    confirmed_run_ledger: tuple[str, ...] = ()
    started_at: str | None = None
    ended_at: str | None = None
    discovered_activity_task_queues: tuple[str, ...] = ()
    cumulative_inspection_failures: int = 0
    cumulative_histories_inspected: int = 0
    cumulative_group_counts: tuple[tuple[str, str, int], ...] = ()
    cumulative_incomplete: bool = False
    # Temporal's native Visibility cursor for the beginning of the current
    # candidate frame. It is protected by the outer HMAC and never exposed as
    # a standalone pagination control.
    candidate_page_token: str | None = None
    schema_version: str = "1"


class ContinuationCodec:
    def __init__(self, secret: bytes | None = None) -> None:
        configured = os.getenv("OPS_MCP_CONTINUATION_SECRET")
        self._secret = secret or (configured.encode() if configured else _PROCESS_SECRET)

    def encode(self, state: ContinuationState) -> str:
        payload = json.dumps(state.__dict__, sort_keys=True, separators=(",", ":")).encode()
        signature = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return _urlsafe(payload) + "." + _urlsafe(signature)

    def decode(self, token: str, *, tool: str, target_ref: str, query: str) -> ContinuationState:
        try:
            encoded_payload, encoded_signature = token.split(".", 1)
            payload = _decode(encoded_payload)
            signature = _decode(encoded_signature)
            expected = hmac.new(self._secret, payload, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("signature")
            raw: dict[str, Any] = json.loads(payload)
            for name in (
                "inspected_run_ledger",
                "confirmed_run_ledger",
                "discovered_activity_task_queues",
            ):
                if name in raw:
                    raw[name] = tuple(raw[name])
            if "cumulative_group_counts" in raw:
                raw["cumulative_group_counts"] = tuple(
                    tuple(item) for item in raw["cumulative_group_counts"]
                )
            state = ContinuationState(**raw)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise _invalid_token("Continuation token is invalid or has been tampered with") from exc
        if state.schema_version != "1":
            raise _invalid_token("Continuation token schema version is unsupported")
        if (
            state.tool != tool
            or state.target_ref != target_ref
            or _normalize_query(state.query) != _normalize_query(query)
        ):
            raise _invalid_token("Continuation token does not match the requested scope")
        return state

    def stable_seed(self, *, tool: str, target_ref: str, scope: str) -> str:
        payload = f"{tool}\0{target_ref}\0{_normalize_query(scope)}".encode()
        digest = hmac.new(self._secret, payload, hashlib.sha256).digest()[:12]
        return _urlsafe(digest)


def normalize_query(query: str) -> str:
    return _normalize_query(query)


def _normalize_query(query: str) -> str:
    return " ".join(query.split())


def _urlsafe(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _decode(value: str) -> bytes:
    decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    if _urlsafe(decoded) != value:
        raise ValueError("non-canonical base64")
    return decoded


def _invalid_token(message: str) -> OpsMcpError:
    return OpsMcpError(ErrorCode.INVALID_ARGUMENT, message, retryable=False)

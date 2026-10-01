"""Validated, stateless continuations for MCP-owned resume state.

The checksum detects accidental corruption. It is not an authentication or authorization
boundary: every resumed request is authorized and budgeted again.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from collections.abc import Iterable
from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)

from ops_mcp.runtime.errors import ErrorCode, OpsMcpError

MAX_CONTINUATION_TOKEN_BYTES = 8_000
MAX_CUMULATIVE_GROUPS = 64
MAX_DISCOVERED_ACTIVITY_QUEUES = 64
MAX_CURSOR_LENGTH = 4_096
MAX_QUEUE_NAME_LENGTH = 256
MAX_GROUP_DIMENSION_LENGTH = 128
MAX_GROUP_VALUES_LENGTH = 2_048
COMPOSITE_CONTINUATION_SCHEMA_VERSION = "3"
HISTORY_CONTINUATION_SCHEMA_VERSION = "history-1"
MAX_NATIVE_HISTORY_PAGE_EVENTS = 10_000
_CHECKSUM_BYTES = 16
_SAMPLE_SEED_BYTES = 12

NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
QueueName = Annotated[StrictStr, Field(min_length=1, max_length=MAX_QUEUE_NAME_LENGTH)]
GroupDimension = Annotated[StrictStr, Field(min_length=1, max_length=MAX_GROUP_DIMENSION_LENGTH)]
GroupValues = Annotated[StrictStr, Field(min_length=1, max_length=MAX_GROUP_VALUES_LENGTH)]
GroupCount = tuple[GroupDimension, GroupValues, NonNegativeInt]


class CompositeContinuationState(BaseModel):
    """Bounded composite progress state; decoded values remain caller-controlled."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: Literal[
        "inspect_workflow_task_failures",
        "inspect_activity_queue_dependencies",
        "analyze_incident_impact",
    ]
    target_ref: Annotated[StrictStr, Field(min_length=1)]
    scope_digest: Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
    sample_seed: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    candidate_page_token: (
        Annotated[StrictStr, Field(min_length=1, max_length=MAX_CURSOR_LENGTH)] | None
    ) = None
    started_at: Annotated[StrictStr, Field(min_length=1, max_length=64)] | None = None
    ended_at: Annotated[StrictStr, Field(min_length=1, max_length=64)] | None = None
    discovered_activity_task_queues: Annotated[
        tuple[QueueName, ...], Field(max_length=MAX_DISCOVERED_ACTIVITY_QUEUES)
    ] = ()
    discovered_activity_task_queues_overflow: NonNegativeInt = 0
    cumulative_candidates_observed: NonNegativeInt = 0
    cumulative_candidates_selected: NonNegativeInt = 0
    cumulative_runs_attempted: NonNegativeInt = 0
    cumulative_runs_inspected: NonNegativeInt = 0
    cumulative_inspection_failures: NonNegativeInt = 0
    cumulative_histories_inspected: NonNegativeInt = 0
    cumulative_temporal_calls: NonNegativeInt = 0
    cumulative_pages_read: NonNegativeInt = 0
    cumulative_history_events_read: NonNegativeInt = 0
    cumulative_candidate_collection_pages: NonNegativeInt = 0
    cumulative_task_queues_inspected: NonNegativeInt = 0
    cumulative_confirmed: NonNegativeInt = 0
    cumulative_group_counts: Annotated[
        tuple[GroupCount, ...], Field(max_length=MAX_CUMULATIVE_GROUPS)
    ] = ()
    cumulative_group_overflow: NonNegativeInt = 0
    cumulative_incomplete: StrictBool = False
    cumulative_sampled: StrictBool = False
    cumulative_history_window_truncated: StrictBool = False
    cumulative_exhausted_dimension: (
        Annotated[StrictStr, Field(min_length=1, max_length=64)] | None
    ) = None
    schema_version: Literal[COMPOSITE_CONTINUATION_SCHEMA_VERSION] = (
        COMPOSITE_CONTINUATION_SCHEMA_VERSION
    )

    @field_validator("sample_seed")
    @classmethod
    def validate_sample_seed(cls, value: str) -> str:
        if len(_decode(value)) != _SAMPLE_SEED_BYTES:
            raise ValueError("sample seed must encode exactly 12 bytes")
        return value

    @field_validator("candidate_page_token")
    @classmethod
    def validate_candidate_cursor(cls, value: str | None) -> str | None:
        if value is not None:
            _decode(value)
        return value

    @field_validator("discovered_activity_task_queues")
    @classmethod
    def normalize_queues(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @field_validator("cumulative_group_counts", mode="before")
    @classmethod
    def validate_groups(cls, value: object) -> tuple[GroupCount, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("group counts must be a collection")
        return normalize_group_counts(value)

    @model_validator(mode="after")
    def validate_time_window(self) -> CompositeContinuationState:
        if (self.started_at is None) != (self.ended_at is None):
            raise ValueError("both continuation timestamps are required together")
        if self.started_at is None:
            return self
        started_at = _timestamp(self.started_at)
        ended_at = _timestamp(self.ended_at)
        if started_at >= ended_at:
            raise ValueError("continuation start time must precede end time")
        return self


class HistoryContinuationState(BaseModel):
    """Resume an unreturned suffix of an over-delivered Temporal history page."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: Literal["get_workflow_history_slice"] = "get_workflow_history_slice"
    target_ref: Annotated[StrictStr, Field(min_length=1)]
    scope_digest: Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
    native_page_token: (
        Annotated[StrictStr, Field(min_length=1, max_length=MAX_CURSOR_LENGTH)] | None
    ) = None
    page_offset: Annotated[StrictInt, Field(ge=1, le=MAX_NATIVE_HISTORY_PAGE_EVENTS)]
    expected_event_id: Annotated[StrictInt, Field(ge=1)]
    schema_version: Literal[HISTORY_CONTINUATION_SCHEMA_VERSION] = (
        HISTORY_CONTINUATION_SCHEMA_VERSION
    )

    @field_validator("native_page_token")
    @classmethod
    def validate_native_cursor(cls, value: str | None) -> str | None:
        if value is not None:
            _decode(value)
        return value


class CompositeContinuationCodec:
    """Encode validated composite state without process-specific configuration."""

    def encode(self, state: CompositeContinuationState) -> str:
        validated = CompositeContinuationState.model_validate(state.model_dump())
        return _encode_envelope(validated, label="composite")

    def decode(
        self, token: str, *, tool: str, target_ref: str, query: str
    ) -> CompositeContinuationState:
        try:
            raw = _decode_envelope(token, label="Composite")
            expected_fields = set(CompositeContinuationState.model_fields)
            if set(raw) != expected_fields:
                raise ValueError("continuation fields")
            state = CompositeContinuationState.model_validate(raw)
        except OpsMcpError:
            raise
        except (
            UnicodeError,
            TypeError,
            ValueError,
            ValidationError,
            json.JSONDecodeError,
        ) as exc:
            raise _invalid_token("Composite continuation token is invalid or corrupted") from exc
        if (
            state.tool != tool
            or state.target_ref != target_ref
            or state.scope_digest != self.scope_digest(query)
        ):
            raise _invalid_token("Continuation token does not match the requested scope")
        return state

    def new_sample_seed(self) -> str:
        return _urlsafe(secrets.token_bytes(_SAMPLE_SEED_BYTES))

    def scope_digest(self, scope: str) -> str:
        """Fingerprint exact canonical scope to catch accidental cross-request reuse."""
        return _scope_digest(scope)


class HistoryContinuationCodec:
    """Codec for the exceptional intra-page history resume path."""

    def encode(self, state: HistoryContinuationState) -> str:
        validated = HistoryContinuationState.model_validate(state.model_dump())
        return _encode_envelope(validated, label="history")

    def decode(self, token: str, *, target_ref: str, scope: str) -> HistoryContinuationState:
        try:
            raw = _decode_envelope(token, label="History")
            if set(raw) != set(HistoryContinuationState.model_fields):
                raise ValueError("continuation fields")
            state = HistoryContinuationState.model_validate(raw)
        except OpsMcpError:
            raise
        except (
            UnicodeError,
            TypeError,
            ValueError,
            ValidationError,
            json.JSONDecodeError,
        ) as exc:
            raise _invalid_token("History continuation token is invalid or corrupted") from exc
        if state.target_ref != target_ref or state.scope_digest != _scope_digest(scope):
            raise _invalid_token("Continuation token does not match the requested scope")
        return state

    def scope_digest(self, scope: str) -> str:
        return _scope_digest(scope)


def normalize_group_counts(groups: Iterable[object]) -> tuple[GroupCount, ...]:
    """Bound and normalize cumulative group state after decoding or page accumulation."""
    items = tuple(groups)
    if len(items) > MAX_CUMULATIVE_GROUPS:
        raise ValueError("too many cumulative groups")
    normalized: dict[tuple[str, str], int] = {}
    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            raise ValueError("invalid cumulative group")
        dimension, serialized_values, count = item
        if (
            not isinstance(dimension, str)
            or not dimension
            or len(dimension) > MAX_GROUP_DIMENSION_LENGTH
            or not isinstance(serialized_values, str)
            or not serialized_values
            or len(serialized_values) > MAX_GROUP_VALUES_LENGTH
            or type(count) is not int
            or count < 0
        ):
            raise ValueError("invalid cumulative group")
        values = json.loads(serialized_values)
        if not isinstance(values, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in values.items()
        ):
            raise ValueError("invalid cumulative group values")
        canonical_values = json.dumps(values, sort_keys=True, separators=(",", ":"))
        key = (dimension, canonical_values)
        normalized[key] = normalized.get(key, 0) + count
    return tuple(
        (dimension, values, count) for (dimension, values), count in sorted(normalized.items())
    )


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("continuation timestamps must be timezone-aware")
    return parsed


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def canonical_scope(value: object) -> str:
    """Serialize exact typed request scope without delimiter or whitespace normalization."""
    return _canonical_json(value).decode()


def _scope_digest(scope: str) -> str:
    return hashlib.sha256(f"scope\0{scope}".encode()).hexdigest()


def _encode_envelope(state: BaseModel, *, label: str) -> str:
    payload = _canonical_json(state.model_dump(mode="json"))
    checksum = hashlib.sha256(payload).digest()[:_CHECKSUM_BYTES]
    token = _urlsafe(payload) + "." + _urlsafe(checksum)
    if len(token.encode()) > MAX_CONTINUATION_TOKEN_BYTES:
        raise OpsMcpError(
            ErrorCode.RESULT_TOO_LARGE,
            f"The {label} continuation state exceeds the safe token-size limit",
            retryable=False,
            details={"limit_bytes": MAX_CONTINUATION_TOKEN_BYTES},
        )
    return token


def _decode_envelope(token: str, *, label: str) -> dict[str, object]:
    if not isinstance(token, str) or len(token.encode()) > MAX_CONTINUATION_TOKEN_BYTES:
        raise _invalid_token(f"{label} continuation token is invalid or oversized")
    encoded_payload, encoded_checksum = token.split(".")
    payload = _decode(encoded_payload)
    checksum = _decode(encoded_checksum)
    expected = hashlib.sha256(payload).digest()[:_CHECKSUM_BYTES]
    if not secrets.compare_digest(checksum, expected):
        if len(checksum) == hashlib.sha256().digest_size and _is_legacy_v2(payload):
            raise _invalid_token(
                "Continuation token came from an earlier server version; restart pagination"
            )
        raise ValueError("checksum")
    raw = json.loads(payload)
    if isinstance(raw, dict) and raw.get("schema_version") == "2":
        raise _invalid_token(
            "Continuation token came from an earlier server version; restart pagination"
        )
    if not isinstance(raw, dict) or _canonical_json(raw) != payload:
        raise ValueError("non-canonical JSON")
    return raw


def _is_legacy_v2(payload: bytes) -> bool:
    """Recognize the old signed framing without accepting any legacy state."""
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return (
        isinstance(raw, dict)
        and raw.get("schema_version") == "2"
        and _canonical_json(raw) == payload
    )


def _urlsafe(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _decode(value: object) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValueError("base64")
    encoded = value.encode("ascii")
    decoded = base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
    if _urlsafe(decoded) != value:
        raise ValueError("non-canonical base64")
    return decoded


def _invalid_token(message: str) -> OpsMcpError:
    return OpsMcpError(ErrorCode.INVALID_ARGUMENT, message, retryable=False)

import base64
import json

import pytest

from ops_mcp.runtime import (
    MAX_CONTINUATION_TOKEN_BYTES,
    ContinuationCodec,
    ContinuationState,
    OpsMcpError,
)


def _state(codec: ContinuationCodec) -> ContinuationState:
    return ContinuationState(
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        scope_digest=codec.scope_digest("WorkflowType = 'OrderWorkflow'"),
        sample_seed="seed",
        cumulative_candidates_observed=250_000,
        cumulative_runs_inspected=100_000,
        cumulative_confirmed=42_000,
        candidate_page_token="MjAwMA",
    )


def test_signed_continuation_round_trips_and_preserves_bounded_counters() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))

    decoded = codec.decode(
        codec.encode(_state(codec)),
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query="  WorkflowType   = 'OrderWorkflow' ",
    )

    assert decoded.cumulative_runs_inspected == 100_000
    assert decoded.cumulative_confirmed == 42_000
    assert decoded.candidate_page_token == "MjAwMA"


@pytest.mark.parametrize(
    ("tool", "target", "query"),
    (
        ("other", "orders-prod", "WorkflowType = 'OrderWorkflow'"),
        ("inspect_workflow_task_failures", "other", "WorkflowType = 'OrderWorkflow'"),
        ("inspect_workflow_task_failures", "orders-prod", "ExecutionStatus = 'Running'"),
    ),
)
def test_continuation_rejects_changed_scope(tool: str, target: str, query: str) -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))

    with pytest.raises(OpsMcpError) as exc_info:
        codec.decode(codec.encode(_state(codec)), tool=tool, target_ref=target, query=query)

    assert exc_info.value.code.value == "invalid_argument"


def test_continuation_rejects_tampering() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))
    token = codec.encode(_state(codec))

    with pytest.raises(OpsMcpError):
        codec.decode(
            token[:-1] + ("A" if token[-1] != "A" else "B"),
            tool="inspect_workflow_task_failures",
            target_ref="orders-prod",
            query="WorkflowType = 'OrderWorkflow'",
        )


def test_scope_seed_is_stable_and_changes_with_scope() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))

    first = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="query")
    second = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="query")
    changed = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="other")

    assert first == second
    assert first != changed


def test_continuation_rejects_weak_secret() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        ContinuationCodec(b"too-short")


def test_token_size_is_constant_as_counters_grow() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))
    small = codec.encode(_state(codec))
    huge = codec.encode(
        ContinuationState(
            **{
                **_state(codec).__dict__,
                "cumulative_candidates_observed": 10**100,
                "cumulative_runs_inspected": 10**100,
                "cumulative_confirmed": 10**100,
            }
        )
    )

    assert len(huge) < MAX_CONTINUATION_TOKEN_BYTES
    assert len(huge) - len(small) < 400


def test_cross_process_codec_with_shared_secret_decodes_token() -> None:
    secret = b"shared-secret".ljust(32, b"!")
    first = ContinuationCodec(secret)
    second = ContinuationCodec(secret)

    decoded = second.decode(
        first.encode(_state(first)),
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query="WorkflowType = 'OrderWorkflow'",
    )

    assert decoded.candidate_page_token == "MjAwMA"


def test_token_payload_contains_scope_digest_not_raw_query() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))
    token = codec.encode(_state(codec))
    encoded_payload = token.split(".", 1)[0]
    payload = json.loads(
        base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4))
    )

    assert "scope_digest" in payload
    assert "query" not in payload
    assert "WorkflowType" not in json.dumps(payload)


def test_oversized_native_cursor_fails_safely() -> None:
    codec = ContinuationCodec(b"test-secret".ljust(32, b"!"))
    state = ContinuationState(**{**_state(codec).__dict__, "candidate_page_token": "x" * 8_000})

    with pytest.raises(OpsMcpError) as exc_info:
        codec.encode(state)

    assert exc_info.value.code.value == "result_too_large"

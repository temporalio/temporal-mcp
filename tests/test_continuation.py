import base64
import hashlib
import json

import pytest

from ops_mcp.contracts import PopulationLimits
from ops_mcp.mcp.adapters import _dependency_scope, _population_scope
from ops_mcp.runtime import (
    MAX_CONTINUATION_TOKEN_BYTES,
    CompositeContinuationCodec,
    CompositeContinuationState,
    HistoryContinuationCodec,
    HistoryContinuationState,
    OpsMcpError,
)

SCOPE = "WorkflowType = 'OrderWorkflow'"


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _token(raw: object) -> str:
    payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    return f"{_encode(payload)}.{_encode(hashlib.sha256(payload).digest()[:16])}"


def _legacy_token(raw: object) -> str:
    payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    return f"{_encode(payload)}.{_encode(hashlib.sha256(payload).digest())}"


def _state(codec: CompositeContinuationCodec) -> CompositeContinuationState:
    return CompositeContinuationState(
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        scope_digest=codec.scope_digest(SCOPE),
        sample_seed=codec.new_sample_seed(),
        candidate_page_token="MjAwMA",
        started_at="2026-09-29T10:00:00+00:00",
        ended_at="2026-09-29T11:00:00+00:00",
        cumulative_candidates_observed=250_000,
        cumulative_candidates_selected=100_000,
        cumulative_runs_attempted=100_000,
        cumulative_runs_inspected=100_000,
        cumulative_confirmed=42_000,
    )


def _decode(codec: CompositeContinuationCodec, token: str) -> CompositeContinuationState:
    return codec.decode(
        token,
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query=SCOPE,
    )


def test_composite_continuation_round_trips_across_independent_codecs() -> None:
    first = CompositeContinuationCodec()
    second = CompositeContinuationCodec()

    decoded = _decode(second, first.encode(_state(first)))

    assert decoded.cumulative_runs_inspected == 100_000
    assert decoded.cumulative_confirmed == 42_000
    assert decoded.candidate_page_token == "MjAwMA"


@pytest.mark.parametrize(
    ("tool", "target", "query"),
    (
        ("other", "orders-prod", SCOPE),
        ("inspect_workflow_task_failures", "other", SCOPE),
        ("inspect_workflow_task_failures", "orders-prod", "ExecutionStatus = 'Running'"),
    ),
)
def test_continuation_rejects_changed_scope(tool: str, target: str, query: str) -> None:
    codec = CompositeContinuationCodec()

    with pytest.raises(OpsMcpError, match="requested scope"):
        codec.decode(codec.encode(_state(codec)), tool=tool, target_ref=target, query=query)


def test_continuation_rejects_corrupted_checksum() -> None:
    codec = CompositeContinuationCodec()
    payload, checksum = codec.encode(_state(codec)).split(".")
    corrupted_checksum = ("A" if checksum[0] != "A" else "B") + checksum[1:]

    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        _decode(codec, f"{payload}.{corrupted_checksum}")


def test_continuation_rejects_noncanonical_base64_and_malformed_json() -> None:
    codec = CompositeContinuationCodec()
    payload, checksum = codec.encode(_state(codec)).split(".")
    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        _decode(codec, f"{payload}=.{checksum}")

    bad_payload = b"not-json"
    malformed = f"{_encode(bad_payload)}.{_encode(hashlib.sha256(bad_payload).digest()[:16])}"
    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        _decode(codec, malformed)


def test_continuation_rejects_noncanonical_json() -> None:
    codec = CompositeContinuationCodec()
    payload = json.dumps(_state(codec).model_dump(mode="json"), indent=2).encode()
    token = f"{_encode(payload)}.{_encode(hashlib.sha256(payload).digest()[:16])}"

    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        _decode(codec, token)


def test_continuation_rejects_oversized_input_before_decode() -> None:
    with pytest.raises(OpsMcpError, match="oversized"):
        _decode(CompositeContinuationCodec(), "x" * (MAX_CONTINUATION_TOKEN_BYTES + 1))


def test_legacy_v2_token_requests_pagination_restart() -> None:
    codec = CompositeContinuationCodec()
    raw = _state(codec).model_dump(mode="json")
    raw["schema_version"] = "2"

    with pytest.raises(OpsMcpError, match="earlier server version; restart pagination"):
        _decode(codec, _token(raw))


@pytest.mark.parametrize(
    "mutate",
    (
        lambda raw: raw.update(cumulative_runs_inspected=True),
        lambda raw: raw.update(extra="field"),
        lambda raw: raw.pop("sample_seed"),
        lambda raw: raw.update(sample_seed="not-base64!"),
        lambda raw: raw.update(candidate_page_token="not-base64!"),
        lambda raw: raw.update(candidate_page_token="x" * 4_097),
        lambda raw: raw.update(discovered_activity_task_queues=["q" * 257]),
        lambda raw: raw.update(
            discovered_activity_task_queues=[f"queue-{index:02}" for index in range(65)]
        ),
        lambda raw: raw.update(started_at="2026-09-29T10:00:00"),
        lambda raw: raw.update(ended_at="2026-09-29T09:00:00+00:00"),
    ),
)
def test_continuation_rejects_untrusted_field_violations(mutate) -> None:
    codec = CompositeContinuationCodec()
    raw = _state(codec).model_dump(mode="json")
    mutate(raw)

    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        _decode(codec, _token(raw))


def test_continuation_normalizes_harmless_queue_and_group_ordering() -> None:
    codec = CompositeContinuationCodec()
    raw = _state(codec).model_dump(mode="json")
    raw["discovered_activity_task_queues"] = ["b", "a", "a"]
    raw["cumulative_group_counts"] = [
        ["z", '{"team":"one"}', 1],
        ["a", '{"team":"two"}', 2],
        ["a", '{"team":"two"}', 3],
    ]

    decoded = _decode(codec, _token(raw))

    assert decoded.discovered_activity_task_queues == ("a", "b")
    assert decoded.cumulative_group_counts == (
        ("a", '{"team":"two"}', 5),
        ("z", '{"team":"one"}', 1),
    )


def test_random_seed_is_bounded_and_preserved() -> None:
    codec = CompositeContinuationCodec()
    first = codec.new_sample_seed()
    second = codec.new_sample_seed()

    assert first != second
    resumed = _decode(
        codec,
        codec.encode(_state(codec).model_copy(update={"sample_seed": first})),
    )
    assert resumed.sample_seed == first


def test_token_payload_contains_scope_digest_not_raw_query() -> None:
    codec = CompositeContinuationCodec()
    encoded_payload = codec.encode(_state(codec)).split(".", 1)[0]
    payload = json.loads(
        base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4))
    )

    assert "scope_digest" in payload
    assert "query" not in payload
    assert "WorkflowType" not in json.dumps(payload)


def test_oversized_encoded_composite_state_fails_safely() -> None:
    codec = CompositeContinuationCodec()
    groups = tuple(
        (
            f"dimension-{index:02}",
            json.dumps({"value": "x" * 100}, separators=(",", ":")),
            1,
        )
        for index in range(64)
    )
    state = _state(codec).model_copy(update={"cumulative_group_counts": groups})

    with pytest.raises(OpsMcpError) as exc_info:
        codec.encode(state)

    assert exc_info.value.code.value == "result_too_large"


def test_target_ref_has_no_continuation_only_length_limit() -> None:
    codec = CompositeContinuationCodec()
    target_ref = "target-" + "x" * 300
    state = _state(codec).model_copy(update={"target_ref": target_ref})

    decoded = codec.decode(
        codec.encode(state),
        tool=state.tool,
        target_ref=target_ref,
        query=SCOPE,
    )

    assert decoded.target_ref == target_ref


def test_history_continuation_round_trips_and_binds_exact_scope() -> None:
    codec = HistoryContinuationCodec()
    scope = '{"limit":51,"query":"exact  spacing"}'
    state = HistoryContinuationState(
        target_ref="orders-prod",
        scope_digest=codec.scope_digest(scope),
        native_page_token=None,
        page_offset=51,
        expected_event_id=52,
    )

    decoded = HistoryContinuationCodec().decode(
        codec.encode(state), target_ref="orders-prod", scope=scope
    )

    assert decoded.page_offset == 51
    assert decoded.expected_event_id == 52
    with pytest.raises(OpsMcpError, match="requested scope"):
        codec.decode(codec.encode(state), target_ref="orders-prod", scope=scope + " ")


def test_history_continuation_rejects_wrong_target_and_legacy_v2() -> None:
    codec = HistoryContinuationCodec()
    scope = '{"limit":51}'
    state = HistoryContinuationState(
        target_ref="orders-prod",
        scope_digest=codec.scope_digest(scope),
        native_page_token="bmF0aXZl",
        page_offset=2,
        expected_event_id=3,
    )

    with pytest.raises(OpsMcpError, match="requested scope"):
        codec.decode(codec.encode(state), target_ref="other", scope=scope)

    legacy = state.model_dump(mode="json")
    legacy["schema_version"] = "2"
    with pytest.raises(OpsMcpError, match="earlier server version; restart pagination"):
        codec.decode(_legacy_token(legacy), target_ref="orders-prod", scope=scope)


@pytest.mark.parametrize(
    "mutate",
    (
        lambda raw: raw.update(page_offset=True),
        lambda raw: raw.update(page_offset=0),
        lambda raw: raw.update(page_offset=10_001),
        lambda raw: raw.update(expected_event_id=0),
        lambda raw: raw.update(native_page_token="not-base64!"),
        lambda raw: raw.update(extra="field"),
    ),
)
def test_history_continuation_rejects_untrusted_state(mutate) -> None:
    codec = HistoryContinuationCodec()
    scope = '{"limit":51}'
    raw = HistoryContinuationState(
        target_ref="orders-prod",
        scope_digest=codec.scope_digest(scope),
        page_offset=2,
        expected_event_id=3,
    ).model_dump(mode="json")
    mutate(raw)

    with pytest.raises(OpsMcpError, match="invalid or corrupted"):
        codec.decode(_token(raw), target_ref="orders-prod", scope=scope)


def test_composite_scopes_preserve_exact_query_and_array_boundaries() -> None:
    limits = PopulationLimits(
        max_candidates=2,
        max_visibility_pages=1,
        max_runs=2,
        max_histories=2,
        max_history_events_per_run=20,
        max_task_queues=2,
        max_concurrency=1,
    )

    assert _population_scope("Memo = 'a  b'", None, None, None, (), limits) != _population_scope(
        "Memo = 'a b'", None, None, None, (), limits
    )
    assert _dependency_scope("", None, None, ("a,b",), None, (), limits) != _dependency_scope(
        "", None, None, ("a", "b"), None, (), limits
    )

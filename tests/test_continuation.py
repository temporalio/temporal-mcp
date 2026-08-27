import pytest

from ops_mcp.runtime import ContinuationCodec, ContinuationState, OpsMcpError


def _state() -> ContinuationState:
    return ContinuationState(
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query="WorkflowType = 'OrderWorkflow'",
        sample_seed="seed",
        inspection_offset=100,
        candidate_frame_size=250,
        candidate_collection_complete=True,
        inspected_run_ledger=("order-1/run-1",),
        confirmed_run_ledger=("order-1/run-1",),
        candidate_page_token="MjAwMA",
    )


def test_signed_continuation_round_trips_and_preserves_ledger() -> None:
    codec = ContinuationCodec(b"test-secret")

    decoded = codec.decode(
        codec.encode(_state()),
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query="  WorkflowType   = 'OrderWorkflow' ",
    )

    assert decoded.inspection_offset == 100
    assert decoded.inspected_run_ledger == ("order-1/run-1",)
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
    codec = ContinuationCodec(b"test-secret")

    with pytest.raises(OpsMcpError) as exc_info:
        codec.decode(codec.encode(_state()), tool=tool, target_ref=target, query=query)

    assert exc_info.value.code.value == "invalid_argument"


def test_continuation_rejects_tampering() -> None:
    codec = ContinuationCodec(b"test-secret")
    token = codec.encode(_state())

    with pytest.raises(OpsMcpError):
        codec.decode(
            token[:-1] + ("A" if token[-1] != "A" else "B"),
            tool="inspect_workflow_task_failures",
            target_ref="orders-prod",
            query="WorkflowType = 'OrderWorkflow'",
        )


def test_scope_seed_is_stable_and_changes_with_scope() -> None:
    codec = ContinuationCodec(b"test-secret")

    first = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="query")
    second = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="query")
    changed = codec.stable_seed(tool="inspect", target_ref="orders-prod", scope="other")

    assert first == second
    assert first != changed

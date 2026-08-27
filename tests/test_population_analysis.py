from datetime import UTC, datetime, timedelta

from ops_mcp.analysis.population import (
    classify_progress_evidence,
    classify_workflow_task_failures,
)
from ops_mcp.facts import (
    ActivityTaskEventAttributes,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    TaskQueueFact,
    TaskQueueKind,
    UnknownEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.workflows import PendingWorkflowTaskFact, WorkflowExecutionFact


def _history(*events: HistoryEventFact, complete: bool = True) -> HistoryReadFact:
    return HistoryReadFact(
        workflow_id="wf",
        run_id="run",
        events=events,
        reverse=False,
        pages_read=1,
        events_scanned=len(events),
        next_page_token=None if complete else b"more",
        observed_at=datetime(2026, 8, 23, tzinfo=UTC),
    )


def _workflow(*, pending: bool = False) -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id="wf",
        run_id="run",
        workflow_type="Example",
        status="RUNNING",
        task_queue="workflows",
        namespace="default",
        start_time=datetime(2026, 8, 22, tzinfo=UTC),
        execution_time=None,
        close_time=None,
        history_length=10,
        history_size_bytes=100,
        first_run_id="run",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="wf",
        root_run_id="run",
        pending_workflow_task=(
            PendingWorkflowTaskFact(
                state="SCHEDULED", attempt=1, scheduled_time=None, started_time=None
            )
            if pending
            else None
        ),
    )


def test_classifies_nondeterminism_from_workflow_task_failure() -> None:
    history = _history(
        HistoryEventFact(
            event_id=6,
            event_type="EVENT_TYPE_WORKFLOW_TASK_STARTED",
            event_time=datetime(2026, 8, 23, tzinfo=UTC),
            task_id=1,
            attributes=WorkflowTaskEventAttributes(
                phase="started", worker_build_id="fulfillment-v2"
            ),
        ),
        HistoryEventFact(
            event_id=7,
            event_type="EVENT_TYPE_WORKFLOW_TASK_FAILED",
            event_time=datetime(2026, 8, 23, tzinfo=UTC),
            task_id=1,
            attributes=WorkflowTaskEventAttributes(
                phase="failed",
                started_event_id=6,
                failure=FailureFact(message="NondeterminismError: command mismatch"),
            ),
        ),
    )

    findings = classify_workflow_task_failures(history)

    assert findings[0].code == "workflow_nondeterminism"
    assert findings[0].failure_class == "nondeterminism"
    assert findings[0].evidence == ("history:event:7",)
    assert findings[0].worker_build_id == "fulfillment-v2"
    assert findings[0].occurred_at == datetime(2026, 8, 23, tzinfo=UTC)
    assert findings[1].code == "workflow_task_processed_on_build"
    assert findings[1].failure_class is None
    assert findings[1].worker_build_id == "fulfillment-v2"
    assert "not a workflow-task failure" in findings[1].caveats[0]


def test_current_missing_poller_is_time_scoped_and_timer_wait_is_normal() -> None:
    observed = datetime(2026, 8, 23, tzinfo=UTC)
    queue = TaskQueueFact(
        task_queue="workflows",
        task_type=TaskQueueKind.WORKFLOW,
        pollers=(),
        approximate_backlog_count=1,
        approximate_backlog_age_seconds=2,
        tasks_add_rate=0,
        tasks_dispatch_rate=0,
        rate_limiting_active=False,
        observed_at=observed,
    )
    history = _history(
        HistoryEventFact(
            event_id=4,
            event_type="EVENT_TYPE_TIMER_STARTED",
            event_time=observed,
            task_id=1,
            attributes=UnknownEventAttributes(),
        )
    )

    findings = classify_progress_evidence(_workflow(pending=True), history, (queue,))

    missing = next(item for item in findings if item.code == "dispatch_blocked_now")
    assert observed.isoformat() in missing.caveats[0]
    waiting = next(item for item in findings if item.code == "waiting_on_timer")
    assert "normal" in waiting.caveats[0]


def test_activity_dependency_evidence_includes_failure_retry_details() -> None:
    observed = datetime(2026, 8, 23, tzinfo=UTC)
    history = _history(
        HistoryEventFact(
            event_id=10,
            event_type="EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            event_time=observed,
            task_id=1,
            attributes=ActivityTaskEventAttributes(
                phase="scheduled", activity_id="a1", task_queue="ledger"
            ),
        ),
        HistoryEventFact(
            event_id=11,
            event_type="EVENT_TYPE_ACTIVITY_TASK_FAILED",
            event_time=observed + timedelta(seconds=30),
            task_id=2,
            attributes=ActivityTaskEventAttributes(
                phase="failed",
                scheduled_event_id=10,
                attempt=3,
                failure=FailureFact(message="ledger unavailable"),
            ),
        ),
    )

    findings = classify_progress_evidence(_workflow(), history, ())
    failure = next(item for item in findings if item.code == "activity_failure_observed")

    assert failure.occurred_at == observed + timedelta(seconds=30)
    assert failure.failure_messages == ("ledger unavailable",)
    assert "attempt 3" in failure.summary

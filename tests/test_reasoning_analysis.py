from __future__ import annotations

from datetime import UTC, datetime

from ops_mcp.analysis.reasoning import DiagnosisAnalyzer, ResetAnalyzer
from ops_mcp.contracts.primitives import Confidence
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    ContinueAsNewEventAttributes,
    ExternalInteractionEventAttributes,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    SignalEventAttributes,
    UnknownEventAttributes,
    UpdateEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import PollerFact, TaskQueueFact, TaskQueueKind
from ops_mcp.facts.workflows import WorkflowExecutionFact

NOW = datetime(2026, 8, 23, 12, tzinfo=UTC)


def _event(event_id: int, event_type: str, attributes: object) -> HistoryEventFact:
    return HistoryEventFact(
        event_id=event_id,
        event_type=event_type,
        event_time=NOW,
        task_id=event_id,
        attributes=attributes,  # type: ignore[arg-type]
    )


def _workflow(history_length: int = 15) -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id="order-88231",
        run_id="run-1",
        workflow_type="OrderWorkflow",
        status="WORKFLOW_EXECUTION_STATUS_FAILED",
        task_queue="orders",
        namespace="default",
        start_time=NOW,
        execution_time=NOW,
        close_time=NOW,
        history_length=history_length,
        history_size_bytes=100,
        first_run_id="run-1",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="order-88231",
        root_run_id="run-1",
        observed_at=NOW,
    )


def _linear_events() -> tuple[HistoryEventFact, ...]:
    catalog_failure = FailureFact(
        message="503 Service Unavailable",
        application_type="CatalogServiceUnavailable",
    )
    return (
        _event(
            4,
            "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
            WorkflowTaskEventAttributes(
                phase="completed", scheduled_event_id=2, started_event_id=3
            ),
        ),
        _event(
            5,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled",
                activity_id="payment",
                activity_type="AuthorizePayment",
                task_queue="payments",
            ),
        ),
        _event(
            6,
            "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            ActivityTaskEventAttributes(phase="started", scheduled_event_id=5, attempt=1),
        ),
        _event(
            7,
            "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
            ActivityTaskEventAttributes(phase="completed", scheduled_event_id=5),
        ),
        _event(
            10,
            "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
            WorkflowTaskEventAttributes(
                phase="completed", scheduled_event_id=8, started_event_id=9
            ),
        ),
        _event(
            11,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled",
                activity_id="enrich",
                activity_type="EnrichProduct",
                task_queue="catalog",
            ),
        ),
        _event(
            12,
            "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            ActivityTaskEventAttributes(phase="started", scheduled_event_id=11, attempt=3),
        ),
        _event(
            13,
            "EVENT_TYPE_ACTIVITY_TASK_FAILED",
            ActivityTaskEventAttributes(
                phase="failed",
                scheduled_event_id=11,
                started_event_id=12,
                retry_state="RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED",
                failure=catalog_failure,
            ),
        ),
        _event(
            14,
            "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
            SignalEventAttributes(signal_name="address-change"),
        ),
        _event(
            15,
            "EVENT_TYPE_WORKFLOW_TASK_FAILED",
            WorkflowTaskEventAttributes(
                phase="failed",
                scheduled_event_id=14,
                started_event_id=15,
                failure=FailureFact(message="workflow wrapper failed"),
            ),
        ),
    )


def _history(events: tuple[HistoryEventFact, ...], *, complete: bool = True) -> HistoryReadFact:
    return HistoryReadFact(
        workflow_id="order-88231",
        run_id="run-1",
        events=events,
        reverse=False,
        pages_read=1,
        events_scanned=len(events),
        next_page_token=None if complete else b"more",
        observed_at=NOW,
    )


def _queue(name: str, kind: TaskQueueKind, *, pollers: bool) -> TaskQueueFact:
    return TaskQueueFact(
        task_queue=name,
        task_type=kind,
        pollers=(PollerFact("worker", NOW, 1.0),) if pollers else (),
        approximate_backlog_count=0,
        approximate_backlog_age_seconds=0,
        tasks_add_rate=0,
        tasks_dispatch_rate=0,
        rate_limiting_active=False,
        observed_at=NOW,
    )


def test_diagnosis_returns_competing_evidence_and_unknown_retry_assessment() -> None:
    analysis = DiagnosisAnalyzer().analyze(
        _workflow(),
        _history(_linear_events()),
        (
            _queue("orders", TaskQueueKind.WORKFLOW, pollers=True),
            _queue("catalog", TaskQueueKind.ACTIVITY, pollers=False),
        ),
    )

    by_code = {finding.code: finding for finding in analysis.findings}
    assert by_code["activity_retry_exhausted"].confidence is Confidence.HIGH
    assert "503 Service Unavailable" in by_code["activity_retry_exhausted"].summary
    assert "workflow_task_failed" in by_code
    assert "workflow_task_queue_current_pollers" in by_code
    assert "activity_task_queue_no_current_pollers" in by_code
    assert analysis.retry.assessment == "unknown"
    assert set(analysis.retry.missing_evidence) == {
        "dependency recovery",
        "activity idempotency or side-effect policy",
    }


def test_linear_reset_uses_exact_finish_event_and_reports_replay() -> None:
    analysis = ResetAnalyzer().analyze(
        _workflow(history_length=len(_linear_events())),
        _history(_linear_events()),
        complete=True,
    )

    assert [point.event_id for point in analysis.points] == [4, 10, 15]
    assert analysis.recommended_event_id == 10
    point = next(item for item in analysis.points if item.event_id == 10)
    assert point.event_type == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"
    assert [item.activity_type for item in point.completed_activities_preserved] == [
        "AuthorizePayment"
    ]
    assert [item.activity_type for item in point.activities_replayed] == ["EnrichProduct"]
    assert point.signals_reapplied == ("address-change",)
    assert {finding.code for finding in point.safety_findings} == {
        "activity_side_effect_safety_unknown",
        "signals_will_be_reapplied",
    }


def test_incomplete_or_complex_history_lists_points_without_recommendation() -> None:
    extra = (
        _event(
            16,
            "EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED",
            UpdateEventAttributes(phase="accepted", update_id="update-1"),
        ),
        _event(
            17,
            "EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED",
            ExternalInteractionEventAttributes(
                interaction="child_workflow_execution_started", workflow_id="child-1"
            ),
        ),
        _event(
            18,
            "EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW",
            ContinueAsNewEventAttributes(new_execution_run_id="run-2"),
        ),
        _event(
            19,
            "EVENT_TYPE_MARKER_RECORDED",
            UnknownEventAttributes(attribute_name="marker_recorded_event_attributes"),
        ),
    )
    events = _linear_events() + extra
    analysis = ResetAnalyzer().analyze(
        _workflow(history_length=len(events)), _history(events, complete=False), complete=False
    )

    assert analysis.points
    assert analysis.recommended_event_id is None
    codes = {finding.code for finding in analysis.safety_findings}
    assert {
        "incomplete_history",
        "workflow_update_present",
        "child_or_external_interaction",
        "continue_as_new_present",
        "versioning_or_marker_risk",
    } <= codes
    assert "unsafe_to_recommend" in analysis.warnings


def test_unknown_event_type_suppresses_reset_recommendation() -> None:
    events = _linear_events() + (
        _event(
            20,
            "EVENT_TYPE_UNKNOWN_999",
            UnknownEventAttributes(attribute_name="future_event_attributes"),
        ),
    )
    analysis = ResetAnalyzer().analyze(
        _workflow(history_length=len(events)), _history(events), complete=True
    )

    assert analysis.recommended_event_id is None
    assert any(item.code == "unsupported_history_event" for item in analysis.safety_findings)


def test_parallel_activity_commands_suppress_reset_recommendation() -> None:
    parallel = (
        _event(
            11,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled", activity_id="one", activity_type="ActivityOne"
            ),
        ),
        _event(
            12,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled", activity_id="two", activity_type="ActivityTwo"
            ),
        ),
        _event(
            13,
            "EVENT_TYPE_ACTIVITY_TASK_FAILED",
            ActivityTaskEventAttributes(phase="failed", scheduled_event_id=11),
        ),
    )
    events = _linear_events()[:5] + parallel
    analysis = ResetAnalyzer().analyze(
        _workflow(history_length=len(events)), _history(events), complete=True
    )

    point = next(item for item in analysis.points if item.event_id == 10)
    assert point.recommendable is False
    assert any(
        finding.code == "parallel_activities_at_reset_boundary" for finding in point.safety_findings
    )
    assert analysis.recommended_event_id is None


def test_timed_out_workflow_task_finish_is_exact_supported_boundary() -> None:
    timed_out = _event(
        20,
        "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT",
        WorkflowTaskEventAttributes(phase="timed_out", scheduled_event_id=18, started_event_id=19),
    )
    events = _linear_events() + (timed_out,)
    analysis = ResetAnalyzer().analyze(
        _workflow(history_length=len(events)), _history(events), complete=True
    )

    assert analysis.points[-1].event_id == 20
    assert analysis.points[-1].event_type == "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT"

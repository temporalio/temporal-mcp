"""Legacy protobuf-based analysis retained during adapter migration."""

from __future__ import annotations

import hashlib
import shlex
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio.api.enums.v1 import EventType, RetryState, WorkflowExecutionStatus
from temporalio.api.failure.v1 import Failure
from temporalio.api.history.v1 import HistoryEvent

from ops_mcp.models import FailureDetail

FAILURE_EVENT_TYPES = {
    EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
    EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
    EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
    EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
    EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
    EventType.EVENT_TYPE_NEXUS_OPERATION_FAILED,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
}


def enum_label(enum_type: Any, value: int, prefix: str) -> str:
    try:
        name = enum_type.Name(value)
    except (KeyError, ValueError):
        return f"UNKNOWN_{value}"
    return name.removeprefix(prefix)


def workflow_status_label(value: int) -> str:
    return enum_label(WorkflowExecutionStatus, value, "WORKFLOW_EXECUTION_STATUS_")


def isoformat(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "ToDatetime"):
        value = value.ToDatetime(tzinfo=UTC)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    return str(value)


def duration_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if hasattr(value, "ToTimedelta"):
        return value.ToTimedelta().total_seconds()
    if isinstance(value, timedelta):
        return value.total_seconds()
    return None


def flatten_failure(failure: Failure) -> tuple[str, str | None]:
    messages: list[str] = []
    error_type: str | None = None
    current: Failure | None = failure
    while current is not None:
        if current.message and current.message not in messages:
            messages.append(current.message)
        if current.HasField("application_failure_info"):
            error_type = current.application_failure_info.type or error_type
        current = current.cause if current.HasField("cause") else None
    return ": ".join(messages) or "Temporal recorded a failure without a message", error_type


def extract_failures(events: Iterable[HistoryEvent]) -> list[FailureDetail]:
    event_list = list(events)
    scheduled: dict[int, tuple[str, str]] = {}
    attempts: dict[int, int] = {}
    for event in event_list:
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            attrs = event.activity_task_scheduled_event_attributes
            scheduled[event.event_id] = (attrs.activity_id, attrs.activity_type.name)
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            attrs = event.activity_task_started_event_attributes
            attempts[event.event_id] = attrs.attempt

    failures: list[FailureDetail] = []
    for event in event_list:
        event_type = event.event_type
        if event_type not in FAILURE_EVENT_TYPES:
            continue
        failure: Failure | None = None
        activity_id: str | None = None
        activity_type: str | None = None
        attempt: int | None = None
        retry_state: str | None = None
        if event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
            attrs = event.activity_task_failed_event_attributes
            failure = attrs.failure
            activity_id, activity_type = scheduled.get(attrs.scheduled_event_id, (None, None))
            attempt = attempts.get(attrs.started_event_id)
            retry_state = enum_label(RetryState, attrs.retry_state, "RETRY_STATE_")
        elif event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            failure = event.workflow_task_failed_event_attributes.failure
        elif event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
            failure = event.workflow_execution_failed_event_attributes.failure
        elif event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
            failure = event.child_workflow_execution_failed_event_attributes.failure
        elif event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_FAILED:
            failure = event.nexus_operation_failed_event_attributes.failure

        message, error_type = (
            flatten_failure(failure)
            if failure is not None and failure.ByteSize()
            else (enum_label(EventType, event.event_type, "EVENT_TYPE_"), None)
        )
        failures.append(
            FailureDetail(
                event_id=event.event_id,
                event_type=enum_label(EventType, event.event_type, "EVENT_TYPE_"),
                message=message,
                error_type=error_type,
                activity_id=activity_id,
                activity_type=activity_type,
                attempt=attempt,
                retry_state=retry_state,
            )
        )
    return failures


def likely_cause(failures: list[FailureDetail], worker_health: dict[str, Any]) -> tuple[str, bool]:
    if not worker_health.get("pollers_healthy", False):
        return "No workflow-task pollers are currently visible on the task queue", False
    activity_failures = [f for f in failures if f.activity_type]
    if activity_failures:
        last = activity_failures[-1]
        exhausted = last.retry_state in {"MAXIMUM_ATTEMPTS_REACHED", "NON_RETRYABLE_FAILURE"}
        cause = f"Activity {last.activity_type} failed"
        if last.attempt:
            cause += f" on attempt {last.attempt}"
        if exhausted:
            cause += " after retries stopped"
        cause += f": {last.message}"
        return cause, exhausted
    workflow_task_failures = [f for f in failures if f.event_type == "WORKFLOW_TASK_FAILED"]
    if workflow_task_failures:
        return f"Workflow task failed: {workflow_task_failures[-1].message}", False
    if failures:
        return failures[-1].message, False
    return "No failure event was found in the inspected history", False


def build_reset_plan(
    workflow_id: str,
    run_id: str,
    events: list[HistoryEvent],
    reason: str | None,
) -> dict[str, Any]:
    failures = extract_failures(events)
    failed_activity = next((f for f in reversed(failures) if f.activity_type), None)
    completed_tasks = [
        event
        for event in events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    ]
    if not completed_tasks:
        raise ValueError("This run has no completed workflow task and cannot be reset safely")

    scheduled_event_id: int | None = None
    if failed_activity:
        for event in events:
            if (
                event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
                and event.activity_task_scheduled_event_attributes.activity_id
                == failed_activity.activity_id
            ):
                scheduled_event_id = event.event_id
                break

    recommended = completed_tasks[0]
    if scheduled_event_id is not None:
        before_activity = [
            event for event in completed_tasks if event.event_id < scheduled_event_id
        ]
        if before_activity:
            recommended = before_activity[-1]
    elif failures:
        before_failure = [
            event for event in completed_tasks if event.event_id < failures[-1].event_id
        ]
        if before_failure:
            recommended = before_failure[-1]

    valid_points = []
    for event in completed_tasks[-10:]:
        label = "after initial workflow task"
        next_activity = next(
            (
                candidate.activity_task_scheduled_event_attributes.activity_type.name
                for candidate in events
                if candidate.event_id > event.event_id
                and candidate.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            ),
            None,
        )
        if next_activity:
            label = f"before {next_activity}"
        valid_points.append(
            {
                "event_id": event.event_id,
                "label": label,
                "recommended": event.event_id == recommended.event_id,
            }
        )

    completed_before = []
    signals_after = []
    activities_after = []
    scheduled_by_id: dict[int, str] = {}
    for event in events:
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            scheduled_by_id[event.event_id] = (
                event.activity_task_scheduled_event_attributes.activity_type.name
            )
            if event.event_id > recommended.event_id:
                activities_after.append(scheduled_by_id[event.event_id])
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            scheduled_id = event.activity_task_completed_event_attributes.scheduled_event_id
            if event.event_id < recommended.event_id:
                completed_before.append(scheduled_by_id.get(scheduled_id, f"event {scheduled_id}"))
        elif (
            event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
            and event.event_id > recommended.event_id
        ):
            signals_after.append(event.workflow_execution_signaled_event_attributes.signal_name)

    observed_at = datetime.now(UTC)
    expires_at = observed_at + timedelta(minutes=15)
    history_length = events[-1].event_id if events else 0
    digest = hashlib.sha256(
        f"{workflow_id}:{run_id}:{history_length}:{recommended.event_id}:{observed_at.isoformat()}".encode()
    ).hexdigest()[:8]
    plan_id = f"rp-{digest}"
    reset_reason = reason or (
        f"plan {plan_id}: resume before "
        f"{failed_activity.activity_type if failed_activity else 'the last failure'}"
    )
    command = " ".join(
        [
            "temporal workflow reset",
            "--workflow-id",
            shlex.quote(workflow_id),
            "--run-id",
            shlex.quote(run_id),
            "--event-id",
            str(recommended.event_id),
            "--reason",
            shlex.quote(reset_reason),
        ]
    )
    return {
        "plan_id": plan_id,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "history_length_observed": history_length,
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "recommended_event_id": recommended.event_id,
        "valid_reset_points": valid_points,
        "already_completed_before_reset": sorted(set(completed_before)),
        "activities_that_would_replay": activities_after,
        "signals_that_would_be_reapplied": signals_after,
        "command": command,
        "read_only": True,
        "warning": (
            "Recompute after expiry or if the run history changes. This command was not executed."
        ),
    }

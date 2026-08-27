"""Pure population finding classifiers over typed Temporal facts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ops_mcp.contracts.population import FailureClass
from ops_mcp.contracts.primitives import Confidence
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    HistoryReadFact,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import TaskQueueFact, TaskQueueKind
from ops_mcp.facts.workflows import WorkflowExecutionFact


@dataclass(frozen=True)
class PopulationAnalysisFinding:
    code: str
    summary: str
    confidence: Confidence
    evidence: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()
    failure_class: FailureClass | None = None
    failure_messages: tuple[str, ...] = ()
    stack_trace_excerpt: str | None = None
    worker_build_id: str | None = None
    occurred_at: datetime | None = None


def classify_workflow_task_failures(
    history: HistoryReadFact,
) -> tuple[PopulationAnalysisFinding, ...]:
    findings: list[PopulationAnalysisFinding] = []
    starts = {
        event.event_id: event.attributes
        for event in history.events
        if isinstance(event.attributes, WorkflowTaskEventAttributes)
        and event.attributes.phase == "started"
    }
    for event in history.events:
        attrs = event.attributes
        if not isinstance(attrs, WorkflowTaskEventAttributes) or attrs.phase not in {
            "failed",
            "timed_out",
        }:
            continue
        messages = _failure_messages(attrs.failure)
        stack_trace = _failure_stack_trace(attrs.failure)
        started = starts.get(attrs.started_event_id)
        searchable = " ".join((attrs.cause or "", *messages)).casefold()
        if attrs.phase == "timed_out":
            failure_class = FailureClass.TIMEOUT
            code = "workflow_task_timeout"
        elif "nondetermin" in searchable or "non_determin" in searchable:
            failure_class = FailureClass.NONDETERMINISM
            code = "workflow_nondeterminism"
        elif "deadlock" in searchable:
            failure_class = FailureClass.DEADLOCK
            code = "workflow_deadlock"
        else:
            failure_class = FailureClass.OTHER
            code = "workflow_task_failure"
        summary = f"Workflow task {attrs.phase} at history event {event.event_id}"
        if messages:
            summary += f": {': '.join(messages)}"
        findings.append(
            PopulationAnalysisFinding(
                code=code,
                summary=summary,
                confidence=Confidence.HIGH,
                evidence=(f"history:event:{event.event_id}",),
                failure_class=failure_class,
                failure_messages=tuple(message[:4_000] for message in messages[:20]),
                stack_trace_excerpt=stack_trace[:4_000] if stack_trace else None,
                worker_build_id=started.worker_build_id if started else None,
                occurred_at=event.event_time,
            )
        )
    for event in history.events:
        attrs = event.attributes
        if (
            isinstance(attrs, WorkflowTaskEventAttributes)
            and attrs.phase == "started"
            and attrs.worker_build_id
        ):
            findings.append(
                PopulationAnalysisFinding(
                    code="workflow_task_processed_on_build",
                    summary=f"Workflow task was processed on Build ID {attrs.worker_build_id}",
                    confidence=Confidence.HIGH,
                    evidence=(f"history:event:{event.event_id}",),
                    worker_build_id=attrs.worker_build_id,
                    occurred_at=event.event_time,
                    caveats=(
                        "Build ID membership establishes exposure, not a workflow-task failure.",
                    ),
                )
            )
    return tuple(findings)


def classify_progress_evidence(
    workflow: WorkflowExecutionFact,
    history: HistoryReadFact | None,
    queue_snapshots: tuple[TaskQueueFact, ...],
) -> tuple[PopulationAnalysisFinding, ...]:
    findings: list[PopulationAnalysisFinding] = []
    if workflow.status.casefold() != "running":
        findings.append(
            PopulationAnalysisFinding(
                code="not_running_at_inspection",
                summary=f"Workflow status was {workflow.status} when inspected",
                confidence=Confidence.HIGH,
                evidence=(f"workflow:{workflow.workflow_id}:{workflow.run_id}",),
            )
        )

    workflow_queue = _queue(queue_snapshots, workflow.task_queue, TaskQueueKind.WORKFLOW)
    if workflow.pending_workflow_task is not None:
        if workflow_queue is not None and not workflow_queue.currently_has_pollers:
            findings.append(
                _queue_finding(
                    workflow_queue,
                    "dispatch_blocked_now",
                    "A workflow task is pending and no workflow-task pollers are currently visible",
                )
            )
        elif workflow_queue is not None:
            findings.append(
                _queue_finding(
                    workflow_queue,
                    "dispatch_available_now",
                    "A workflow task is pending and workflow-task pollers are currently visible",
                )
            )

    activity_queues = tuple(
        queue for queue in queue_snapshots if queue.task_type is TaskQueueKind.ACTIVITY
    )
    if workflow.pending_activities and activity_queues:
        missing = tuple(queue for queue in activity_queues if not queue.currently_has_pollers)
        if missing:
            findings.extend(
                _queue_finding(
                    queue,
                    "dispatch_blocked_now",
                    "Pending activity work exists and no activity-task pollers are "
                    "currently visible",
                )
                for queue in missing
            )

    for activity in workflow.pending_activities:
        if activity.attempt > 1 or activity.last_failure is not None:
            findings.append(
                PopulationAnalysisFinding(
                    code="activity_retry_observed",
                    summary=(
                        f"Pending activity {activity.activity_type} is on attempt "
                        f"{activity.attempt}"
                    ),
                    confidence=Confidence.HIGH,
                    evidence=(f"workflow:pending_activity:{activity.activity_id}",),
                    failure_messages=_failure_messages(activity.last_failure),
                    caveats=(
                        "Retry state does not prove that the external dependency recovered.",
                    ),
                )
            )

    if history is not None:
        scheduled_activities = {
            event.event_id: event
            for event in history.events
            if isinstance(event.attributes, ActivityTaskEventAttributes)
            and event.attributes.phase == "scheduled"
        }
        for event in history.events:
            attrs = event.attributes
            if not isinstance(attrs, ActivityTaskEventAttributes) or attrs.phase != "started":
                continue
            scheduled = scheduled_activities.get(attrs.scheduled_event_id)
            if scheduled is None:
                continue
            scheduled_attrs = scheduled.attributes
            if not isinstance(scheduled_attrs, ActivityTaskEventAttributes):
                continue
            delay = max(0.0, (event.event_time - scheduled.event_time).total_seconds())
            findings.append(
                PopulationAnalysisFinding(
                    code="historical_dispatch_delay_confirmed",
                    summary=(
                        f"Activity task on {scheduled_attrs.task_queue or 'unknown queue'} "
                        f"started {delay:.3f}s after it was scheduled"
                    ),
                    confidence=Confidence.HIGH,
                    evidence=(
                        f"history:event:{scheduled.event_id}",
                        f"history:event:{event.event_id}",
                    ),
                    caveats=(
                        "Schedule-to-start duration is historical dispatch evidence; it does "
                        "not by itself identify the cause of delay.",
                    ),
                )
            )
        for event in history.events:
            attrs = event.attributes
            if not isinstance(attrs, ActivityTaskEventAttributes) or attrs.phase not in {
                "failed",
                "timed_out",
            }:
                continue
            scheduled = scheduled_activities.get(attrs.scheduled_event_id)
            scheduled_attrs = scheduled.attributes if scheduled is not None else None
            queue = (
                scheduled_attrs.task_queue
                if isinstance(scheduled_attrs, ActivityTaskEventAttributes)
                else None
            )
            findings.append(
                PopulationAnalysisFinding(
                    code=(
                        "activity_timeout_observed"
                        if attrs.phase == "timed_out"
                        else "activity_failure_observed"
                    ),
                    summary=(
                        f"Activity task on {queue or 'unknown queue'} {attrs.phase}"
                        + (f" at attempt {attrs.attempt}" if attrs.attempt is not None else "")
                    ),
                    confidence=Confidence.HIGH,
                    evidence=(f"history:event:{event.event_id}",),
                    failure_messages=_failure_messages(attrs.failure),
                    occurred_at=event.event_time,
                    caveats=(
                        "An activity failure is durable evidence, but does not by itself prove "
                        "the cause of a broader incident.",
                    ),
                )
            )
        task_failures = tuple(
            finding
            for finding in classify_workflow_task_failures(history)
            if finding.failure_class is not None
        )
        if task_failures:
            repeated = len(task_failures) >= 2
            findings.append(
                PopulationAnalysisFinding(
                    code=(
                        "workflow_task_repeatedly_failing"
                        if repeated
                        else "workflow_task_failure_observed"
                    ),
                    summary=f"{len(task_failures)} workflow-task failure event(s) were inspected",
                    confidence=Confidence.HIGH,
                    evidence=tuple(
                        evidence for finding in task_failures for evidence in finding.evidence
                    ),
                    caveats=(
                        "Failure events show code/replay symptoms, not a universal stuck verdict.",
                    ),
                )
            )
        timer_starts = sum(
            event.event_type == "EVENT_TYPE_TIMER_STARTED" for event in history.events
        )
        timer_finishes = sum(
            event.event_type in {"EVENT_TYPE_TIMER_FIRED", "EVENT_TYPE_TIMER_CANCELED"}
            for event in history.events
        )
        if history.complete and timer_starts > timer_finishes:
            findings.append(
                PopulationAnalysisFinding(
                    code="waiting_on_timer",
                    summary="History contains an unresolved durable timer",
                    confidence=Confidence.HIGH,
                    evidence=("history:timer_events",),
                    caveats=("Waiting on a timer is normal workflow behavior.",),
                )
            )

    if not findings:
        findings.append(
            PopulationAnalysisFinding(
                code="dependency_unconfirmed",
                summary="Available Temporal evidence does not establish why progress is delayed",
                confidence=Confidence.UNKNOWN,
                caveats=(
                    "A workflow may be waiting normally for a signal or application condition.",
                    "No approved diagnostic workflow query was available for this inspection.",
                ),
            )
        )
    return tuple(findings)


def _queue(
    snapshots: tuple[TaskQueueFact, ...], name: str, kind: TaskQueueKind
) -> TaskQueueFact | None:
    return next(
        (item for item in snapshots if item.task_queue == name and item.task_type is kind),
        None,
    )


def _queue_finding(queue: TaskQueueFact, code: str, summary: str) -> PopulationAnalysisFinding:
    return PopulationAnalysisFinding(
        code=code,
        summary=f"{summary} on {queue.task_queue}",
        confidence=Confidence.HIGH,
        evidence=(f"task_queue:{queue.task_type.value}:{queue.task_queue}",),
        caveats=(
            f"Queue state was observed at {queue.observed_at.isoformat()} and is not historical.",
        ),
    )


def _failure_messages(failure: object) -> tuple[str, ...]:
    messages: list[str] = []
    current = failure
    while current is not None:
        message = getattr(current, "message", None)
        if message and message not in messages:
            messages.append(message)
        current = getattr(current, "cause", None)
    return tuple(messages)


def _failure_stack_trace(failure: object) -> str | None:
    current = failure
    while current is not None:
        stack_trace = getattr(current, "stack_trace", None)
        if stack_trace:
            return str(stack_trace)
        current = getattr(current, "cause", None)
    return None

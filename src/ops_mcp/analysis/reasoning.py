"""Pure single-workflow diagnosis and reset analysis over typed facts.

This module intentionally imports no Temporal SDK, protobuf, gateway, target, or
configuration types. Its results are deterministic for the supplied facts.
"""

from __future__ import annotations

from dataclasses import dataclass

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
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import TaskQueueFact
from ops_mcp.facts.workflows import WorkflowExecutionFact

RESET_BOUNDARY_PHASES = frozenset({"completed", "failed", "timed_out"})

# These families are normalized as UnknownEventAttributes because no richer fact is
# currently needed. They are ordinary topology, not unknown replay behavior.
BENIGN_EVENT_TYPES = frozenset(
    {
        "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
        "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED",
        "EVENT_TYPE_WORKFLOW_TASK_STARTED",
        "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
        "EVENT_TYPE_WORKFLOW_TASK_FAILED",
        "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT",
        "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
        "EVENT_TYPE_ACTIVITY_TASK_STARTED",
        "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
        "EVENT_TYPE_ACTIVITY_TASK_FAILED",
        "EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT",
        "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
        "EVENT_TYPE_WORKFLOW_EXECUTION_FAILED",
        "EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED",
        "EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED",
        "EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT",
        "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
        "EVENT_TYPE_TIMER_STARTED",
        "EVENT_TYPE_TIMER_FIRED",
        "EVENT_TYPE_TIMER_CANCELED",
        "EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED",
    }
)


@dataclass(frozen=True)
class AnalysisFinding:
    code: str
    summary: str
    confidence: Confidence
    evidence: tuple[str, ...] = ()
    counterevidence: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetryAnalysis:
    assessment: str
    rationale: str
    missing_evidence: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiagnosisAnalysis:
    findings: tuple[AnalysisFinding, ...]
    retry: RetryAnalysis


@dataclass(frozen=True)
class ActivityImpact:
    activity_id: str
    activity_type: str


@dataclass(frozen=True)
class ResetPointAnalysis:
    event_id: int
    event_type: str
    label: str
    completed_activities_preserved: tuple[ActivityImpact, ...]
    activities_replayed: tuple[ActivityImpact, ...]
    signals_reapplied: tuple[str, ...]
    updates_reapplied: tuple[str, ...]
    safety_findings: tuple[AnalysisFinding, ...]
    recommendable: bool


@dataclass(frozen=True)
class ResetAnalysis:
    points: tuple[ResetPointAnalysis, ...]
    recommended_event_id: int | None
    safety_findings: tuple[AnalysisFinding, ...]
    warnings: tuple[str, ...]


def _failure_messages(failure: FailureFact | None) -> tuple[str, ...]:
    messages: list[str] = []
    current = failure
    while current is not None:
        if current.message and current.message not in messages:
            messages.append(current.message)
        current = current.cause
    return tuple(messages)


def _failure_type(failure: FailureFact | None) -> str | None:
    current = failure
    while current is not None:
        if current.application_type:
            return current.application_type
        current = current.cause
    return None


class DiagnosisAnalyzer:
    """Produce portable Temporal findings without deciding customer policy."""

    def analyze(
        self,
        workflow: WorkflowExecutionFact,
        history: HistoryReadFact,
        queue_snapshots: tuple[TaskQueueFact, ...],
        *,
        expected_duration_seconds: float | None = None,
    ) -> DiagnosisAnalysis:
        findings: list[AnalysisFinding] = []
        scheduled: dict[int, ActivityTaskEventAttributes] = {}
        started: dict[int, ActivityTaskEventAttributes] = {}

        observed_at = workflow.observed_at or history.observed_at
        observed_duration = max(0.0, (observed_at - workflow.start_time).total_seconds())
        if expected_duration_seconds is not None and observed_duration > expected_duration_seconds:
            findings.append(
                AnalysisFinding(
                    code="workflow_duration_exceeds_expectation",
                    summary=(
                        f"Observed duration {observed_duration:.0f}s exceeds the caller-supplied "
                        f"expectation of {expected_duration_seconds:.0f}s"
                    ),
                    confidence=Confidence.HIGH,
                    evidence=("workflow:start_time", "workflow:observed_at"),
                    caveats=(
                        "Age establishes delay, not that the workflow is unable to make progress.",
                    ),
                )
            )

        for event in history.events:
            attrs = event.attributes
            if isinstance(attrs, ActivityTaskEventAttributes):
                if attrs.phase == "scheduled":
                    scheduled[event.event_id] = attrs
                elif attrs.phase == "started":
                    started[event.event_id] = attrs
                elif attrs.phase in {"failed", "timed_out"}:
                    schedule = scheduled.get(attrs.scheduled_event_id)
                    start = started.get(attrs.started_event_id)
                    activity_type = (
                        attrs.activity_type
                        or (schedule.activity_type if schedule else None)
                        or "unknown activity"
                    )
                    activity_id = attrs.activity_id or (schedule.activity_id if schedule else None)
                    attempt = attrs.attempt or (start.attempt if start else None)
                    messages = _failure_messages(attrs.failure)
                    summary = f"Activity {activity_type} {attrs.phase}"
                    if attempt:
                        summary += f" on attempt {attempt}"
                    if messages:
                        summary += f": {': '.join(messages)}"
                    code = "activity_timed_out" if attrs.phase == "timed_out" else "activity_failed"
                    if attrs.retry_state == "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED":
                        code = "activity_retry_exhausted"
                    elif attrs.retry_state == "RETRY_STATE_NON_RETRYABLE_FAILURE" or (
                        attrs.failure and attrs.failure.non_retryable
                    ):
                        code = "activity_non_retryable_failure"
                    findings.append(
                        AnalysisFinding(
                            code=code,
                            summary=summary,
                            confidence=Confidence.HIGH,
                            evidence=(f"history:event:{event.event_id}",),
                            caveats=(
                                f"activity_id={activity_id}",
                                f"retry_state={attrs.retry_state or 'unknown'}",
                                f"application_type={_failure_type(attrs.failure) or 'unknown'}",
                            ),
                        )
                    )
            elif isinstance(attrs, WorkflowTaskEventAttributes) and attrs.phase in {
                "failed",
                "timed_out",
            }:
                messages = _failure_messages(attrs.failure)
                code = (
                    "workflow_task_timed_out"
                    if attrs.phase == "timed_out"
                    else "workflow_task_failed"
                )
                failure_type = (_failure_type(attrs.failure) or "").casefold()
                cause = (attrs.cause or "").casefold()
                joined = " ".join(messages).casefold()
                if (
                    "nondetermin" in joined
                    or "nondetermin" in failure_type
                    or "non_determin" in cause
                ):
                    code = "workflow_nondeterminism"
                elif "deadlock" in joined or "deadlock" in failure_type:
                    code = "workflow_deadlock"
                summary = f"Workflow task {attrs.phase}"
                if messages:
                    summary += f": {': '.join(messages)}"
                findings.append(
                    AnalysisFinding(
                        code=code,
                        summary=summary,
                        confidence=Confidence.HIGH,
                        evidence=(f"history:event:{event.event_id}",),
                    )
                )
            elif isinstance(attrs, WorkflowCloseEventAttributes) and attrs.state != "completed":
                messages = _failure_messages(attrs.failure)
                findings.append(
                    AnalysisFinding(
                        code=f"workflow_{attrs.state}",
                        summary=(
                            f"Workflow closed as {attrs.state}"
                            + (f": {': '.join(messages)}" if messages else "")
                        ),
                        confidence=Confidence.HIGH,
                        evidence=(f"history:event:{event.event_id}",),
                    )
                )
            elif isinstance(attrs, ExternalInteractionEventAttributes):
                findings.append(
                    AnalysisFinding(
                        code="child_or_external_workflow_interaction",
                        summary=f"History contains {attrs.interaction}",
                        confidence=Confidence.HIGH,
                        evidence=(f"history:event:{event.event_id}",),
                        caveats=(
                            "The related workflow may contain additional root-cause evidence.",
                        ),
                    )
                )

        for queue in queue_snapshots:
            kind = queue.task_type.value
            if queue.currently_has_pollers:
                findings.append(
                    AnalysisFinding(
                        code=f"{kind}_task_queue_current_pollers",
                        summary=(
                            f"{len(queue.pollers)} {kind}-task poller(s) are currently visible "
                            f"on {queue.task_queue}"
                        ),
                        confidence=Confidence.HIGH,
                        evidence=(f"task_queue:{kind}:{queue.task_queue}",),
                        caveats=(
                            "This current snapshot does not establish health at failure time.",
                        ),
                    )
                )
            else:
                findings.append(
                    AnalysisFinding(
                        code=f"{kind}_task_queue_no_current_pollers",
                        summary=(
                            f"No {kind}-task pollers are currently visible on {queue.task_queue}"
                        ),
                        confidence=Confidence.HIGH,
                        evidence=(f"task_queue:{kind}:{queue.task_queue}",),
                        caveats=("This current snapshot does not establish the historical cause.",),
                    )
                )

        pending_activity_queues = {
            item.activity_task_queue
            for item in workflow.pending_activities
            if item.activity_task_queue
        }
        for queue in queue_snapshots:
            if (
                queue.task_type.value == "activity"
                and queue.task_queue in pending_activity_queues
                and not queue.currently_has_pollers
            ):
                findings.append(
                    AnalysisFinding(
                        code="current_activity_dispatch_blocker",
                        summary=(
                            f"A pending activity explicitly targets {queue.task_queue}, which "
                            "currently has no activity-task pollers"
                        ),
                        confidence=Confidence.HIGH,
                        evidence=(
                            f"workflow:pending_activity_queue:{queue.task_queue}",
                            f"task_queue:activity:{queue.task_queue}",
                        ),
                        caveats=(
                            "This current snapshot does not prove when the no-poller state began.",
                            "It does not by itself prove the historical cause of the delay.",
                        ),
                    )
                )

        if not findings:
            findings.append(
                AnalysisFinding(
                    code="no_diagnostic_finding",
                    summary="The supplied Temporal facts contain no classified failure",
                    confidence=Confidence.UNKNOWN,
                    caveats=(
                        f"workflow_status={workflow.status}",
                        "The workflow may be waiting normally or require workflow-specific state.",
                    ),
                )
            )

        retry = RetryAnalysis(
            assessment="unknown",
            rationale=(
                "Temporal history can establish how retries stopped, but not whether the external "
                "dependency recovered or replaying the operation is idempotent."
            ),
            missing_evidence=("dependency recovery", "activity idempotency or side-effect policy"),
        )
        return DiagnosisAnalysis(findings=tuple(findings), retry=retry)


class ResetAnalyzer:
    """Enumerate pinned reset boundaries and conservatively recommend linear cases."""

    def analyze(
        self,
        workflow: WorkflowExecutionFact,
        history: HistoryReadFact,
        *,
        complete: bool,
    ) -> ResetAnalysis:
        events = tuple(sorted(history.events, key=lambda item: item.event_id))
        global_findings = list(self._global_safety_findings(events, complete=complete))
        boundaries = tuple(
            event
            for event in events
            if isinstance(event.attributes, WorkflowTaskEventAttributes)
            and event.attributes.phase in RESET_BOUNDARY_PHASES
        )
        points = tuple(self._point(event, events, global_findings) for event in boundaries)
        warnings: list[str] = []
        if not complete:
            warnings.append("incomplete_history")
        if not points:
            warnings.append("no_supported_reset_points")

        recommended: int | None = None
        failed_schedule_id = self._last_failed_activity_schedule_id(events)
        if complete and not self._has_blocking_findings(global_findings) and failed_schedule_id:
            candidates = [
                point
                for point in points
                if point.event_id < failed_schedule_id and point.recommendable
            ]
            if candidates:
                recommended = candidates[-1].event_id
        if points and recommended is None:
            warnings.append("unsafe_to_recommend")

        return ResetAnalysis(
            points=points,
            recommended_event_id=recommended,
            safety_findings=tuple(global_findings),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    def _point(
        self,
        boundary: HistoryEventFact,
        events: tuple[HistoryEventFact, ...],
        global_findings: list[AnalysisFinding],
    ) -> ResetPointAnalysis:
        scheduled: dict[int, ActivityTaskEventAttributes] = {}
        completed_ids: set[int] = set()
        active_at_boundary: set[int] = set()
        active_after_boundary: set[int] = set()
        parallel_after_boundary: set[int] = set()
        signals: list[str] = []
        updates: list[str] = []

        for event in events:
            attrs = event.attributes
            if isinstance(attrs, ActivityTaskEventAttributes):
                if attrs.phase == "scheduled":
                    scheduled[event.event_id] = attrs
                    if event.event_id <= boundary.event_id:
                        active_at_boundary.add(event.event_id)
                    else:
                        active_after_boundary.add(event.event_id)
                        if len(active_after_boundary) > 1:
                            parallel_after_boundary.update(active_after_boundary)
                elif attrs.phase in {"completed", "failed", "timed_out"}:
                    if attrs.phase == "completed" and event.event_id < boundary.event_id:
                        completed_ids.add(attrs.scheduled_event_id)
                    if event.event_id <= boundary.event_id:
                        active_at_boundary.discard(attrs.scheduled_event_id)
                    else:
                        active_after_boundary.discard(attrs.scheduled_event_id)
            elif event.event_id > boundary.event_id and isinstance(attrs, SignalEventAttributes):
                signals.append(attrs.signal_name)
            elif event.event_id > boundary.event_id and isinstance(attrs, UpdateEventAttributes):
                updates.append(attrs.update_id or attrs.protocol_instance_id or "unknown update")

        preserved = tuple(
            self._impact(event_id, scheduled[event_id])
            for event_id in sorted(completed_ids)
            if event_id in scheduled
        )
        replayed = tuple(
            self._impact(event_id, attrs)
            for event_id, attrs in sorted(scheduled.items())
            if event_id > boundary.event_id
        )
        point_findings: list[AnalysisFinding] = []
        parallel_event_ids = active_at_boundary | parallel_after_boundary
        if len(active_at_boundary) > 1 or parallel_after_boundary:
            point_findings.append(
                AnalysisFinding(
                    code="parallel_activities_at_reset_boundary",
                    summary="Multiple activities were in flight at this reset boundary",
                    confidence=Confidence.HIGH,
                    evidence=tuple(
                        f"history:event:{value}" for value in sorted(parallel_event_ids)
                    ),
                    caveats=("Replay ordering and external side effects require manual review.",),
                )
            )
        if replayed:
            point_findings.append(
                AnalysisFinding(
                    code="activity_side_effect_safety_unknown",
                    summary="Temporal history cannot establish external side-effect safety",
                    confidence=Confidence.UNKNOWN,
                    evidence=tuple(f"history:event:{item.activity_id}" for item in replayed),
                    caveats=("Confirm activity idempotency before executing a reset.",),
                )
            )
        if signals:
            point_findings.append(
                AnalysisFinding(
                    code="signals_will_be_reapplied",
                    summary=f"{len(signals)} signal event(s) occur after this boundary",
                    confidence=Confidence.HIGH,
                    evidence=tuple(f"signal:{name}" for name in signals),
                )
            )
        blockers = self._has_blocking_findings([*global_findings, *point_findings])
        label = "after workflow task"
        if replayed:
            label = f"before {replayed[0].activity_type}"
        return ResetPointAnalysis(
            event_id=boundary.event_id,
            event_type=boundary.event_type,
            label=label,
            completed_activities_preserved=preserved,
            activities_replayed=replayed,
            signals_reapplied=tuple(signals),
            updates_reapplied=tuple(updates),
            safety_findings=tuple(point_findings),
            recommendable=not blockers,
        )

    def _global_safety_findings(
        self, events: tuple[HistoryEventFact, ...], *, complete: bool
    ) -> tuple[AnalysisFinding, ...]:
        findings: list[AnalysisFinding] = []
        if not complete:
            findings.append(
                AnalysisFinding(
                    code="incomplete_history",
                    summary="Complete history is required for a reset recommendation",
                    confidence=Confidence.HIGH,
                    caveats=("Valid boundaries may still be listed, but none is recommended.",),
                )
            )
        for event in events:
            attrs = event.attributes
            finding: AnalysisFinding | None = None
            if isinstance(attrs, ContinueAsNewEventAttributes):
                finding = AnalysisFinding(
                    "continue_as_new_present",
                    "The run continues as new",
                    Confidence.HIGH,
                    (f"history:event:{event.event_id}",),
                )
            elif isinstance(attrs, ExternalInteractionEventAttributes):
                finding = AnalysisFinding(
                    "child_or_external_interaction",
                    f"History contains {attrs.interaction}",
                    Confidence.HIGH,
                    (f"history:event:{event.event_id}",),
                    caveats=("Related workflow effects require manual review.",),
                )
            elif isinstance(attrs, UpdateEventAttributes):
                finding = AnalysisFinding(
                    "workflow_update_present",
                    "Workflow update semantics require manual replay review",
                    Confidence.HIGH,
                    (f"history:event:{event.event_id}",),
                )
            elif isinstance(attrs, UnknownEventAttributes):
                if event.event_type == "EVENT_TYPE_MARKER_RECORDED":
                    finding = AnalysisFinding(
                        "versioning_or_marker_risk",
                        "A marker may represent patching, versioning, or local-activity state",
                        Confidence.MEDIUM,
                        (f"history:event:{event.event_id}",),
                    )
                elif event.event_type not in BENIGN_EVENT_TYPES:
                    finding = AnalysisFinding(
                        "unsupported_history_event",
                        f"Reset effects for {event.event_type} are not modeled",
                        Confidence.HIGH,
                        (f"history:event:{event.event_id}",),
                    )
            if finding and finding.code not in {item.code for item in findings}:
                findings.append(finding)
        return tuple(findings)

    @staticmethod
    def _has_blocking_findings(findings: list[AnalysisFinding]) -> bool:
        non_blocking = {"activity_side_effect_safety_unknown", "signals_will_be_reapplied"}
        return any(item.code not in non_blocking for item in findings)

    @staticmethod
    def _last_failed_activity_schedule_id(events: tuple[HistoryEventFact, ...]) -> int | None:
        failed = [
            event.attributes.scheduled_event_id
            for event in events
            if isinstance(event.attributes, ActivityTaskEventAttributes)
            and event.attributes.phase in {"failed", "timed_out"}
        ]
        return failed[-1] if failed else None

    @staticmethod
    def _impact(event_id: int, attrs: ActivityTaskEventAttributes) -> ActivityImpact:
        return ActivityImpact(
            activity_id=attrs.activity_id or f"event-{event_id}",
            activity_type=attrs.activity_type or "unknown activity",
        )

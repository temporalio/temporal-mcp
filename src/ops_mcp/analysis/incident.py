"""Pure, conservative classifiers for historical incident evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ops_mcp.contracts.incident import IncidentImpactClassification, IncidentTimeWindow
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    HistoryReadFact,
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.visibility import WorkflowVisibilityFact


@dataclass(frozen=True)
class IncidentAnalysisFinding:
    code: str
    summary: str
    confirms_historical_impact: bool
    evidence: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()


@dataclass(frozen=True)
class IncidentRunAnalysis:
    classification: IncidentImpactClassification
    findings: tuple[IncidentAnalysisFinding, ...]


def classify_incident_impact(
    row: WorkflowVisibilityFact,
    history: HistoryReadFact,
    *,
    incident_window: IncidentTimeWindow,
    affected_activity_task_queues: frozenset[str],
    expected_duration_seconds: float | None,
    observed_at: datetime,
) -> IncidentRunAnalysis:
    """Classify only facts present in history; timing correlation never proves causation."""

    findings: list[IncidentAnalysisFinding] = []
    scheduled = {
        event.event_id: event
        for event in history.events
        if isinstance(event.attributes, ActivityTaskEventAttributes)
        and event.attributes.phase == "scheduled"
    }

    for event in history.events:
        attrs = event.attributes
        if not isinstance(attrs, ActivityTaskEventAttributes):
            continue
        scheduled_event = (
            event if attrs.phase == "scheduled" else scheduled.get(attrs.scheduled_event_id)
        )
        if scheduled_event is None:
            continue
        scheduled_attrs = scheduled_event.attributes
        if not isinstance(scheduled_attrs, ActivityTaskEventAttributes):
            continue
        queue = scheduled_attrs.task_queue
        if queue not in affected_activity_task_queues:
            continue
        if (
            attrs.phase == "started"
            and scheduled_event.event_time < incident_window.ended_at
            and event.event_time > incident_window.ended_at
        ):
            findings.append(
                IncidentAnalysisFinding(
                    code="affected_activity_dispatch_crossed_incident_end",
                    summary=(
                        f"Activity on {queue} was scheduled before the incident ended and "
                        "started only after it ended"
                    ),
                    confirms_historical_impact=True,
                    evidence=(
                        f"history:event:{scheduled_event.event_id}",
                        f"history:event:{event.event_id}",
                    ),
                    caveats=(
                        "The history proves temporal overlap with an affected queue, "
                        "not root-cause attribution.",
                    ),
                )
            )
        if (
            attrs.phase in {"failed", "timed_out"}
            and incident_window.started_at <= event.event_time < incident_window.ended_at
        ):
            findings.append(
                IncidentAnalysisFinding(
                    code=f"affected_activity_{attrs.phase}_during_incident",
                    summary=f"Activity on {queue} {attrs.phase} during the incident window",
                    confirms_historical_impact=True,
                    evidence=(
                        f"history:event:{scheduled_event.event_id}",
                        f"history:event:{event.event_id}",
                    ),
                    caveats=(
                        "Coincident failure is impact evidence, but does not by itself "
                        "prove incident causation.",
                    ),
                )
            )

    for event in history.events:
        attrs = event.attributes
        failed = (
            isinstance(attrs, WorkflowTaskEventAttributes)
            and attrs.phase in {"failed", "timed_out"}
        ) or (
            isinstance(attrs, WorkflowCloseEventAttributes)
            and attrs.state in {"failed", "timed_out"}
        )
        if failed and incident_window.started_at <= event.event_time < incident_window.ended_at:
            findings.append(
                IncidentAnalysisFinding(
                    code="workflow_failure_or_timeout_during_incident",
                    summary="A workflow failure or timeout occurred during the incident window",
                    confirms_historical_impact=False,
                    evidence=(f"history:event:{event.event_id}",),
                    caveats=(
                        "Timing alone does not connect this failure to an affected activity queue.",
                    ),
                )
            )

    duration_end = row.close_time or observed_at
    duration_seconds = max(0.0, (duration_end - row.start_time).total_seconds())
    if expected_duration_seconds is not None and duration_seconds > expected_duration_seconds:
        findings.append(
            IncidentAnalysisFinding(
                code=(
                    "open_beyond_expected_duration"
                    if row.close_time is None
                    else "closed_beyond_expected_duration"
                ),
                summary=(
                    f"Run duration {duration_seconds:.0f}s exceeded the supplied "
                    f"{expected_duration_seconds:.0f}s expectation"
                ),
                confirms_historical_impact=False,
                evidence=(f"visibility:workflow:{row.workflow_id}:{row.run_id}",),
                caveats=(
                    "Expected duration is operator-supplied policy; exceeding it does "
                    "not establish incident causation.",
                ),
            )
        )

    if any(finding.confirms_historical_impact for finding in findings):
        classification = IncidentImpactClassification.CONFIRMED
    elif history.complete:
        classification = IncidentImpactClassification.UNCONFIRMED
        if not findings:
            findings.append(
                IncidentAnalysisFinding(
                    code="no_historical_impact_evidence",
                    summary=(
                        "Complete inspected history contained no configured "
                        "incident-impact evidence"
                    ),
                    confirms_historical_impact=False,
                )
            )
    else:
        classification = IncidentImpactClassification.UNKNOWN
        findings.append(
            IncidentAnalysisFinding(
                code="history_truncated",
                summary="History inspection ended before the complete run history was read",
                confirms_historical_impact=False,
                caveats=("Absence of evidence in a truncated history is not evidence of absence.",),
            )
        )
    return IncidentRunAnalysis(classification=classification, findings=tuple(findings))

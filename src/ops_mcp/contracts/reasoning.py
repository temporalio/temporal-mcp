"""Versioned public contracts for single-workflow reasoning capabilities."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    AnswerConstraints,
    CompositeInspection,
    ContractModel,
    Finding,
    Inspection,
    SchemaVersion,
)
from ops_mcp.contracts.workflow_reads import DecodedPayloadsOutput


class DiagnoseWorkflowRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    history_event_limit: int = Field(default=2_000, ge=1, le=20_000)
    expected_duration_seconds: float | None = Field(default=None, gt=0)


class WorkflowSummary(ContractModel):
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str
    task_queue: str
    history_length: int = Field(ge=0)
    observed_at: datetime | None = None
    observed_duration_seconds: float = Field(ge=0)
    expected_duration_seconds: float | None = Field(default=None, gt=0)
    pending_workflow_task: bool = False


class PendingActivitySummary(ContractModel):
    activity_id: str
    activity_type: str
    state: str
    attempt: int = Field(ge=0)
    maximum_attempts: int = Field(ge=0)
    last_failure: str | None = None
    activity_task_queue: str | None = None
    last_heartbeat_time: datetime | None = None
    heartbeat_details: DecodedPayloadsOutput


class FailureSummary(ContractModel):
    event_id: int = Field(ge=1)
    event_type: str
    activity_id: str | None = None
    activity_type: str | None = None
    attempt: int | None = Field(default=None, ge=0)
    retry_state: str | None = None
    messages: tuple[str, ...] = ()
    application_type: str | None = None


class TaskQueueSnapshot(ContractModel):
    task_queue: str
    task_type: Literal["workflow", "activity"]
    poller_count: int = Field(ge=0)
    approximate_backlog_count: int = Field(ge=0)
    approximate_backlog_age_seconds: float = Field(ge=0)
    observed_at: datetime
    historical_health_known: Literal[False] = False


class RetryAssessment(ContractModel):
    assessment: Literal["advisable", "inadvisable", "unknown"]
    rationale: str
    missing_evidence: tuple[str, ...] = ()


class DiagnoseWorkflowResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str
    workflow: WorkflowSummary
    pending_activities: tuple[PendingActivitySummary, ...] = ()
    failures: tuple[FailureSummary, ...] = ()
    task_queues: tuple[TaskQueueSnapshot, ...] = ()
    findings: tuple[Finding, ...] = ()
    retry_assessment: RetryAssessment
    history_inspection: Inspection
    warnings: tuple[str, ...] = ()


class InspectWorkflowExecutionRequest(ContractModel):
    target_ref: str = Field(min_length=1, description="Target reference from resolve_target.")
    workflow_id: str = Field(min_length=1, description="Temporal Workflow ID.")
    run_id: str | None = Field(
        default=None,
        min_length=1,
        description="Exact Run ID. When omitted, the server resolves the current run once.",
    )
    expected_duration_seconds: float | None = Field(
        default=None,
        gt=0,
        description="Optional operator-supplied normal-duration policy.",
    )
    history_event_limit: int = Field(
        default=2_000,
        ge=1,
        le=20_000,
        description="Maximum recent/history events inspected by the composite.",
    )
    include_diagnostic_query: bool = Field(
        default=True,
        description="Execute the first approved diagnostic query when configured.",
    )


class InspectWorkflowExecutionAnswer(ContractModel):
    workflow: WorkflowSummary
    duration_state: Literal["within_expected_duration", "exceeds_expected_duration", "no_policy"]
    progress_state: Literal["running", "terminal", "unknown"]
    workflow_wait_state: Literal[
        "pending_activity", "pending_workflow_task", "durable_wait", "no_wait_observed", "unknown"
    ]
    current_dispatch_state: Literal[
        "dispatch_blocked_now", "dispatch_available_now", "no_pending_dispatch", "unknown"
    ]
    pending_activities: tuple[PendingActivitySummary, ...] = ()
    task_queues: tuple[TaskQueueSnapshot, ...] = ()
    failure_findings: tuple[Finding, ...] = ()
    retry_findings: tuple[Finding, ...] = ()
    diagnostic_query_name: str | None = None
    diagnostic_query_value: Any | None = None


class RecommendedFollowup(ContractModel):
    intent: str = Field(min_length=1)
    tool: str = Field(min_length=1)


class InspectWorkflowExecutionResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str
    answer: InspectWorkflowExecutionAnswer
    evidence_collected: tuple[str, ...]
    evidence_unavailable: tuple[str, ...]
    supported_conclusions: tuple[str, ...]
    unsupported_conclusions: tuple[str, ...]
    inspection: CompositeInspection
    continuation_token: None = None
    warnings: tuple[str, ...] = ()
    observed_at: datetime
    recommended_followups: tuple[RecommendedFollowup, ...] = ()
    population_analysis_required: Literal[False] = False
    answer_constraints: AnswerConstraints | None = None


class PlanWorkflowResetRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    operator_rationale: str | None = Field(default=None, min_length=1, max_length=500)


class ActivityReplayImpact(ContractModel):
    activity_id: str
    activity_type: str


class ReplayImpact(ContractModel):
    completed_activities_preserved: tuple[ActivityReplayImpact, ...] = ()
    activities_replayed: tuple[ActivityReplayImpact, ...] = ()
    signals_reapplied: tuple[str, ...] = ()
    updates_reapplied: tuple[str, ...] = ()


class ResetPoint(ContractModel):
    event_id: int = Field(ge=1)
    event_type: Literal[
        "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
        "EVENT_TYPE_WORKFLOW_TASK_FAILED",
        "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT",
    ]
    label: str
    replay_impact: ReplayImpact
    safety_findings: tuple[Finding, ...] = ()
    recommendable: bool


class ResetRecommendation(ContractModel):
    event_id: int = Field(ge=1)
    rationale: str


class CommandPreview(ContractModel):
    shell: Literal["sh"] = "sh"
    command: str = Field(min_length=1)
    executed: Literal[False] = False


class PlanWorkflowResetResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str
    workflow_id: str
    run_id: str
    plan_id: str
    observed_history_length: int = Field(ge=0)
    observed_at: datetime
    recheck_after: datetime
    recheck_after_is_advisory: Literal[True] = True
    valid_reset_points: tuple[ResetPoint, ...]
    safety_findings: tuple[Finding, ...] = ()
    recommendation: ResetRecommendation | None = None
    command_preview: CommandPreview | None = None
    operator_rationale: str | None = None
    history_inspection: Inspection
    inspection: CompositeInspection
    warnings: tuple[str, ...] = ()
    evidence_collected: tuple[str, ...] = ()
    evidence_unavailable: tuple[str, ...] = ()
    supported_conclusions: tuple[str, ...] = ()
    unsupported_conclusions: tuple[str, ...] = ()
    continuation_token: None = None
    answer_constraints: AnswerConstraints | None = None

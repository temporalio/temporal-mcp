"""Public contracts for bounded population diagnostics."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    AnswerConstraints,
    Confidence,
    ContractModel,
    SchemaVersion,
)
from ops_mcp.contracts.visibility import SearchAttributeProjection

NonEmptyString = Annotated[str, Field(min_length=1)]
BoundedFailureText = Annotated[str, Field(min_length=1, max_length=4_000)]


class PopulationTimeWindow(ContractModel):
    started_at_or_after: datetime
    started_before: datetime

    @model_validator(mode="after")
    def validate_order(self) -> PopulationTimeWindow:
        if self.started_at_or_after.tzinfo is None or self.started_before.tzinfo is None:
            raise ValueError("time-window values must be timezone-aware")
        if self.started_before <= self.started_at_or_after:
            raise ValueError("started_before must be after started_at_or_after")
        return self


class SamplingStrategy(StrEnum):
    DETERMINISTIC_HASH = "deterministic_hash"


class PopulationLimits(ContractModel):
    max_candidates: int = Field(ge=1, le=10_000)
    max_visibility_pages: int = Field(default=50, ge=1, le=50)
    max_runs: int = Field(ge=1, le=100)
    max_histories: int = Field(ge=1, le=100)
    max_history_events_per_run: int = Field(ge=1, le=2_000)
    max_task_queues: int = Field(ge=1, le=100)
    max_concurrency: int = Field(ge=1, le=20)

    @model_validator(mode="after")
    def validate_relationships(self) -> PopulationLimits:
        if self.max_runs > self.max_candidates:
            raise ValueError("max_runs cannot exceed max_candidates")
        if self.max_histories > self.max_runs:
            raise ValueError("max_histories cannot exceed max_runs")
        return self


class PopulationAdvancedOptions(ContractModel):
    """Optional expert overrides; omitted values retain server-owned defaults."""

    max_candidates: int | None = Field(default=None, ge=1, le=10_000)
    max_visibility_pages: int | None = Field(default=None, ge=1, le=50)
    max_runs: int | None = Field(default=None, ge=1, le=100)
    max_histories: int | None = Field(default=None, ge=1, le=100)
    max_history_events_per_run: int | None = Field(default=None, ge=1, le=2_000)
    max_task_queues: int | None = Field(default=None, ge=1, le=100)
    max_concurrency: int | None = Field(default=None, ge=1, le=20)

    def resolved(self) -> PopulationLimits:
        values = {
            "max_candidates": 2_000,
            "max_visibility_pages": 10,
            "max_runs": 100,
            "max_histories": 100,
            "max_history_events_per_run": 500,
            "max_task_queues": 20,
            "max_concurrency": 10,
        }
        values.update(self.model_dump(exclude_none=True))
        # Preserve the required relationships when only one ceiling is overridden.
        values["max_runs"] = min(values["max_runs"], values["max_candidates"])
        values["max_histories"] = min(values["max_histories"], values["max_runs"])
        return PopulationLimits(**values)


class FailureClass(StrEnum):
    NONDETERMINISM = "nondeterminism"
    DEADLOCK = "deadlock"
    TIMEOUT = "timeout"
    OTHER = "other"


class FindWorkflowTaskFailuresRequest(ContractModel):
    target_ref: NonEmptyString
    query: NonEmptyString
    time_window: PopulationTimeWindow
    failure_class: FailureClass | None = None
    limits: PopulationLimits
    sampling_strategy: Literal[SamplingStrategy.DETERMINISTIC_HASH]
    sample_seed: NonEmptyString
    inspection_offset: int = Field(default=0, ge=0, exclude=True)
    excluded_run_identities: tuple[str, ...] = Field(default=(), exclude=True)
    candidate_page_token: str | None = Field(default=None, exclude=True)
    group_by_custom_search_attributes: tuple[NonEmptyString, ...] = Field(default=(), max_length=4)

    @field_validator("group_by_custom_search_attributes")
    @classmethod
    def aggregation_fields_must_be_unique(cls, names: tuple[str, ...]) -> tuple[str, ...]:
        if len(names) != len(set(names)):
            raise ValueError("group_by_custom_search_attributes must not contain duplicates")
        return names


class FindStuckWorkflowsRequest(ContractModel):
    target_ref: NonEmptyString
    query: NonEmptyString
    time_window: PopulationTimeWindow
    minimum_open_duration_seconds: float | None = Field(default=None, gt=0)
    limits: PopulationLimits
    sampling_strategy: Literal[SamplingStrategy.DETERMINISTIC_HASH]
    sample_seed: NonEmptyString
    inspection_offset: int = Field(default=0, ge=0, exclude=True)
    excluded_run_identities: tuple[str, ...] = Field(default=(), exclude=True)
    candidate_page_token: str | None = Field(default=None, exclude=True)
    require_running_and_duration: bool = Field(default=True, exclude=True)
    activity_task_queue: NonEmptyString | None = None
    activity_task_queues: tuple[NonEmptyString, ...] = Field(default=(), max_length=20)
    group_by_custom_search_attributes: tuple[NonEmptyString, ...] = Field(default=(), max_length=4)

    @field_validator("group_by_custom_search_attributes")
    @classmethod
    def aggregation_fields_must_be_unique(cls, names: tuple[str, ...]) -> tuple[str, ...]:
        if len(names) != len(set(names)):
            raise ValueError("group_by_custom_search_attributes must not contain duplicates")
        return names

    @model_validator(mode="after")
    def queue_inputs_do_not_conflict(self) -> FindStuckWorkflowsRequest:
        if self.require_running_and_duration and self.minimum_open_duration_seconds is None:
            raise ValueError(
                "minimum_open_duration_seconds is required for the deprecated running-duration mode"
            )
        if self.activity_task_queue and self.activity_task_queues:
            raise ValueError("use activity_task_queues or deprecated activity_task_queue, not both")
        if len(self.activity_task_queues) != len(set(self.activity_task_queues)):
            raise ValueError("activity_task_queues must not contain duplicates")
        return self

    @property
    def requested_activity_task_queues(self) -> tuple[str, ...]:
        return self.activity_task_queues or (
            (self.activity_task_queue,) if self.activity_task_queue else ()
        )


class PopulationFinding(ContractModel):
    code: NonEmptyString
    summary: NonEmptyString
    confidence: Confidence
    evidence: tuple[NonEmptyString, ...] = ()
    caveats: tuple[NonEmptyString, ...] = ()
    failure_messages: tuple[BoundedFailureText, ...] = Field(default=(), max_length=20)
    stack_trace_excerpt: BoundedFailureText | None = None
    worker_build_id: NonEmptyString | None = None
    occurred_at: datetime | None = None


class InspectedRun(ContractModel):
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    workflow_type: NonEmptyString
    status: NonEmptyString
    start_time: datetime
    open_duration_seconds: float = Field(ge=0)
    findings: tuple[PopulationFinding, ...]
    inspection_error_code: str | None = None
    activity_queue_membership: Literal["confirmed", "unconfirmed", "unknown"] | None = None


class FailureClassGroup(ContractModel):
    failure_class: FailureClass
    runs: int = Field(ge=0)


class AffectedPopulationGroup(ContractModel):
    workflow_type: NonEmptyString
    custom_search_attributes: tuple[SearchAttributeProjection, ...] = ()
    confirmed_runs: int = Field(ge=1)


class AffectedPopulationAggregation(ContractModel):
    basis: Literal["observed_workflow_task_failure", "confirmed_activity_queue_membership"]
    scope: Literal["complete_query_population", "bounded_inspection"]
    dimensions: tuple[NonEmptyString, ...]
    inspected_runs: int = Field(ge=0)
    confirmed_affected_runs: int = Field(ge=0)
    groups: tuple[AffectedPopulationGroup, ...] = ()


class QueueSnapshot(ContractModel):
    task_queue: NonEmptyString
    task_type: Literal["workflow", "activity"]
    poller_count: int = Field(ge=0)
    currently_has_pollers: bool
    approximate_backlog_count: int = Field(ge=0)
    approximate_backlog_age_seconds: float = Field(ge=0)
    observed_at: datetime


class PopulationInspection(ContractModel):
    completion: Literal["complete", "partial", "sampled", "truncated", "budget_exhausted"]
    temporal_calls: int = Field(ge=0)
    pages_read: int = Field(ge=0)
    history_events_read: int = Field(ge=0)
    population_count: int = Field(ge=0)
    population_count_precision: Literal["approximate"] = "approximate"
    candidate_frame_size: int = Field(ge=0)
    candidate_count: int = Field(ge=0)
    candidate_frame_has_more: bool
    candidate_collection_pages: int = Field(ge=0)
    candidate_collection_complete: bool
    candidate_collection_stop_reason: Literal[
        "complete", "max_candidates", "max_visibility_pages", "stalled_page_token"
    ]
    sample_size: int = Field(ge=0)
    runs_inspected: int = Field(ge=0)
    histories_inspected: int = Field(ge=0)
    task_queues_inspected: int = Field(ge=0)
    sampling_strategy: SamplingStrategy
    sample_seed: NonEmptyString
    sampling_frame: Literal["complete_visibility_query", "bounded_visibility_query"]
    estimate_eligible: bool
    estimate_eligibility_reason: Literal[
        "complete_frame_uniform_sample",
        "truncated_candidate_frame",
        "incomplete_sample_inspection",
        "candidate_frame_census",
    ]
    history_direction: Literal["not_read", "forward", "reverse_recent"]
    history_window_truncated: bool
    partial_failures: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)
    exhausted_dimension: str | None = None
    limits: PopulationLimits
    # Internal handoff to the signed MCP continuation. Excluded from the wire
    # response so callers only ever receive the scope-bound continuation_token.
    candidate_next_page_token: str | None = Field(default=None, exclude=True)


class PopulationProgressGroup(ContractModel):
    dimension: NonEmptyString
    values: dict[str, str]
    confirmed: int = Field(ge=0)


class PopulationProgress(ContractModel):
    inspected: int = Field(ge=0)
    confirmed: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)
    groups: tuple[PopulationProgressGroup, ...] = ()


class PopulationNextCall(ContractModel):
    tool: NonEmptyString
    continuation_token: NonEmptyString


class WorkflowTaskFailuresAnswer(ContractModel):
    effective_query: NonEmptyString
    runs: tuple[InspectedRun, ...]
    failure_groups: tuple[FailureClassGroup, ...]
    affected_aggregation: AffectedPopulationAggregation


class FindWorkflowTaskFailuresResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: WorkflowTaskFailuresAnswer
    inspection: PopulationInspection
    warnings: tuple[NonEmptyString, ...] = ()
    continuation_token: str | None = None
    evidence_collected: tuple[NonEmptyString, ...] = ()
    evidence_unavailable: tuple[NonEmptyString, ...] = ()
    supported_conclusions: tuple[NonEmptyString, ...] = ()
    unsupported_conclusions: tuple[NonEmptyString, ...] = ()
    cumulative_inspected_identities: tuple[NonEmptyString, ...] = ()
    cumulative_confirmed_identities: tuple[NonEmptyString, ...] = ()
    page: PopulationProgress | None = None
    cumulative: PopulationProgress | None = None
    next_call: PopulationNextCall | None = None
    answer_constraints: AnswerConstraints | None = None


class InspectWorkflowTaskFailuresResponse(FindWorkflowTaskFailuresResponse):
    """Preferred composite contract; the parent remains the compatibility wire shape."""


class StuckWorkflowsAnswer(ContractModel):
    effective_query: NonEmptyString
    runs: tuple[InspectedRun, ...]
    task_queues: tuple[QueueSnapshot, ...]
    blast_radius: BlastRadiusSummary | None = None
    affected_aggregation: AffectedPopulationAggregation | None = None


class ConfirmedWorkflowTypeGroup(ContractModel):
    workflow_type: NonEmptyString
    confirmed_runs: int = Field(ge=0)


class BlastRadiusSummary(ContractModel):
    activity_task_queue: NonEmptyString
    candidate_frame_size: int = Field(ge=0)
    inspected: int = Field(ge=0)
    confirmed_affected: int = Field(ge=0)
    inspected_but_unconfirmed: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)
    uninspected: int = Field(ge=0)
    collection_complete: bool
    confirmed_groups: tuple[ConfirmedWorkflowTypeGroup, ...] = ()


class FindStuckWorkflowsResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: StuckWorkflowsAnswer
    inspection: PopulationInspection
    warnings: tuple[NonEmptyString, ...] = ()
    continuation_token: str | None = None
    evidence_collected: tuple[NonEmptyString, ...] = ()
    evidence_unavailable: tuple[NonEmptyString, ...] = ()
    supported_conclusions: tuple[NonEmptyString, ...] = ()
    unsupported_conclusions: tuple[NonEmptyString, ...] = ()
    cumulative_inspected_identities: tuple[NonEmptyString, ...] = ()
    cumulative_confirmed_identities: tuple[NonEmptyString, ...] = ()
    page: PopulationProgress | None = None
    cumulative: PopulationProgress | None = None
    next_call: PopulationNextCall | None = None
    answer_constraints: AnswerConstraints | None = None


class ActivityQueueDependenciesAnswer(ContractModel):
    effective_query: NonEmptyString
    runs: tuple[InspectedRun, ...]
    task_queues: tuple[QueueSnapshot, ...]
    blast_radius: BlastRadiusSummary | None = None
    affected_aggregation: AffectedPopulationAggregation | None = None


class InspectActivityQueueDependenciesResponse(ContractModel):
    """Preferred activity-dependency evidence contract."""

    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: ActivityQueueDependenciesAnswer
    inspection: PopulationInspection
    warnings: tuple[NonEmptyString, ...] = ()
    continuation_token: str | None = None
    evidence_collected: tuple[NonEmptyString, ...] = ()
    evidence_unavailable: tuple[NonEmptyString, ...] = ()
    supported_conclusions: tuple[NonEmptyString, ...] = ()
    unsupported_conclusions: tuple[NonEmptyString, ...] = ()
    cumulative_inspected_identities: tuple[NonEmptyString, ...] = ()
    cumulative_confirmed_identities: tuple[NonEmptyString, ...] = ()
    page: PopulationProgress | None = None
    cumulative: PopulationProgress | None = None
    next_call: PopulationNextCall | None = None
    answer_constraints: AnswerConstraints | None = None

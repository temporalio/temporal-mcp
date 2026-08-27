"""Public contracts for bounded historical incident-impact analysis."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from ops_mcp.contracts.population import (
    PopulationInspection,
    PopulationLimits,
    PopulationNextCall,
    PopulationProgress,
    SamplingStrategy,
)
from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    AnswerConstraints,
    ContractModel,
    SchemaVersion,
)
from ops_mcp.contracts.visibility import SearchAttributeProjection

NonEmptyString = Annotated[str, Field(min_length=1)]


class IncidentTimeWindow(ContractModel):
    started_at: datetime
    ended_at: datetime

    @model_validator(mode="after")
    def validate_order(self) -> IncidentTimeWindow:
        if self.started_at.tzinfo is None or self.ended_at.tzinfo is None:
            raise ValueError("incident-window values must be timezone-aware")
        if self.ended_at <= self.started_at:
            raise ValueError("ended_at must be after started_at")
        return self


class IncidentImpactClassification(StrEnum):
    CONFIRMED = "confirmed_historical_impact"
    UNCONFIRMED = "inspected_but_unconfirmed"
    UNKNOWN = "unknown_or_truncated"


class AnalyzeIncidentImpactRequest(ContractModel):
    target_ref: NonEmptyString
    query: NonEmptyString
    incident_window: IncidentTimeWindow
    affected_activity_task_queues: tuple[NonEmptyString, ...] = Field(default=(), max_length=20)
    expected_duration_seconds: float | None = Field(default=None, gt=0)
    limits: PopulationLimits
    sampling_strategy: Literal[SamplingStrategy.DETERMINISTIC_HASH]
    sample_seed: NonEmptyString
    inspection_offset: int = Field(default=0, ge=0, exclude=True)
    excluded_run_identities: tuple[str, ...] = Field(default=(), exclude=True)
    candidate_page_token: str | None = Field(default=None, exclude=True)
    group_by_custom_search_attributes: tuple[NonEmptyString, ...] = Field(default=(), max_length=4)

    @field_validator("affected_activity_task_queues", "group_by_custom_search_attributes")
    @classmethod
    def tuple_values_must_be_unique(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)):
            raise ValueError("values must not contain duplicates")
        return values


class IncidentImpactFinding(ContractModel):
    code: NonEmptyString
    summary: NonEmptyString
    confirms_historical_impact: bool
    evidence: tuple[NonEmptyString, ...] = ()
    caveats: tuple[NonEmptyString, ...] = ()


class IncidentImpactRun(ContractModel):
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    workflow_type: NonEmptyString
    status: NonEmptyString
    start_time: datetime
    classification: IncidentImpactClassification
    findings: tuple[IncidentImpactFinding, ...]
    inspection_error_code: str | None = None


class IncidentImpactGroup(ContractModel):
    workflow_type: NonEmptyString
    custom_search_attributes: tuple[SearchAttributeProjection, ...] = ()
    confirmed_runs: int = Field(ge=1)


class IncidentImpactSummary(ContractModel):
    candidate_frame_size: int = Field(ge=0)
    inspected: int = Field(ge=0)
    confirmed_historical_impact: int = Field(ge=0)
    inspected_but_unconfirmed: int = Field(ge=0)
    unknown_or_truncated: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)
    uninspected: int = Field(ge=0)
    confirmed_affected_lower_bound: int = Field(ge=0)
    exact_confirmed_count: bool
    result_scope: Literal["complete_candidate_census", "bounded_inspection"]


class IncidentImpactAnswer(ContractModel):
    effective_query: NonEmptyString
    incident_window: IncidentTimeWindow
    affected_activity_task_queues: tuple[NonEmptyString, ...]
    runs: tuple[IncidentImpactRun, ...]
    summary: IncidentImpactSummary
    confirmed_groups: tuple[IncidentImpactGroup, ...] = ()


class IncidentCandidatePopulation(ContractModel):
    candidate_query: NonEmptyString
    candidate_frame_size: int = Field(ge=0)
    complete: bool


class IncidentImpactCriteria(ContractModel):
    incident_window: IncidentTimeWindow
    activity_task_queues: tuple[NonEmptyString, ...]
    evidence_basis: tuple[NonEmptyString, ...]


class IncidentBusinessGrouping(ContractModel):
    attributes: tuple[NonEmptyString, ...] = ()
    value_semantics: Literal["raw_registered_search_attribute_values"] = (
        "raw_registered_search_attribute_values"
    )


class IncidentQueueScope(ContractModel):
    candidate_query: NonEmptyString
    affected_activity_task_queues: tuple[NonEmptyString, ...]
    queue_scope_source: Literal["explicit", "discovered"]
    discovered_activity_task_queues: tuple[NonEmptyString, ...] = ()
    excluded_discovered_queues: tuple[NonEmptyString, ...] = ()
    completion_scope: Literal["full_candidate_population", "explicit_queue_subset"]
    candidate_population_complete: bool
    incident_impact_complete: bool


class AnalyzeIncidentImpactResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: IncidentImpactAnswer
    inspection: PopulationInspection
    warnings: tuple[NonEmptyString, ...] = ()
    continuation_token: str | None = None
    evidence_collected: tuple[NonEmptyString, ...] = ()
    evidence_unavailable: tuple[NonEmptyString, ...] = ()
    supported_conclusions: tuple[NonEmptyString, ...] = ()
    unsupported_conclusions: tuple[NonEmptyString, ...] = ()
    cumulative_inspected_identities: tuple[NonEmptyString, ...] = ()
    cumulative_confirmed_identities: tuple[NonEmptyString, ...] = ()
    candidate_population: IncidentCandidatePopulation | None = None
    impact_criteria: IncidentImpactCriteria | None = None
    business_grouping: IncidentBusinessGrouping | None = None
    scope: IncidentQueueScope | None = None
    page: PopulationProgress | None = None
    cumulative: PopulationProgress | None = None
    next_call: PopulationNextCall | None = None
    answer_constraints: AnswerConstraints | None = None

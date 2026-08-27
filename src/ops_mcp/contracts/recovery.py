"""Read-only recovery evidence for a failed activity dependency."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    AnswerConstraints,
    CompositeInspection,
    ContractModel,
    SchemaVersion,
)

NonEmptyString = Annotated[str, Field(min_length=1)]


class AssessWorkflowRecoveryRequest(ContractModel):
    target_ref: NonEmptyString
    failed_workflow_id: NonEmptyString
    failed_run_id: NonEmptyString
    comparison_query: NonEmptyString
    activity_type: NonEmptyString


class RecoveryRunOutcome(ContractModel):
    workflow_id: NonEmptyString
    run_id: NonEmptyString
    outcome: Literal["success", "failure", "not_observed", "inspection_failed"]


class AssessWorkflowRecoveryAnswer(ContractModel):
    failed_workflow_id: NonEmptyString
    failed_run_id: NonEmptyString
    activity_type: NonEmptyString
    comparison_query: NonEmptyString
    failed_run_activity_outcome: Literal["success", "failure", "not_observed"]
    candidate_runs: int = Field(ge=0)
    histories_inspected: int = Field(ge=0)
    activity_successes: int = Field(ge=0)
    activity_failures: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)
    completion: Literal["complete", "sampled", "partial", "truncated"]
    outcomes: tuple[RecoveryRunOutcome, ...]
    recovery_assessment: Literal["supported", "not_supported", "unknown"]
    caveats: tuple[NonEmptyString, ...] = (
        "Subsequent activity success is recovery evidence, not a direct dependency health check.",
    )


class AssessWorkflowRecoveryResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: AssessWorkflowRecoveryAnswer
    inspection: CompositeInspection
    warnings: tuple[NonEmptyString, ...] = ()
    evidence_collected: tuple[NonEmptyString, ...] = ()
    evidence_unavailable: tuple[NonEmptyString, ...] = ()
    supported_conclusions: tuple[NonEmptyString, ...] = ()
    unsupported_conclusions: tuple[NonEmptyString, ...] = ()
    answer_constraints: AnswerConstraints
    continuation_token: None = None

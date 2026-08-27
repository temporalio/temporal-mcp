"""Versioned primitives shared by MCP tool contracts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

SchemaVersion = Literal["1"]
CURRENT_SCHEMA_VERSION: SchemaVersion = "1"
WarningMessage = Annotated[str, Field(min_length=1)]


class Completeness(StrEnum):
    """How completely a collection was inspected for its stated scope."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    TRUNCATED = "truncated"
    SAMPLED = "sampled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    UNKNOWN = "unknown"


class Confidence(StrEnum):
    """Evidence basis for a finding, not a numeric probability."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class EvidenceSource(StrEnum):
    HISTORY_EVENT = "history_event"
    WORKFLOW_DESCRIPTION = "workflow_description"
    TASK_QUEUE_SNAPSHOT = "task_queue_snapshot"
    VISIBILITY_RECORD = "visibility_record"
    QUERY_RESULT = "query_result"


class ContractModel(BaseModel):
    """Strict immutable base for shared wire-contract values."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Inspection(ContractModel):
    """Common bounded-inspection metadata approved by the contract registry."""

    completion: Completeness
    temporal_calls: int = Field(ge=0)
    pages_read: int = Field(ge=0)
    events_read: int | None = Field(default=None, ge=0)
    runs_inspected: int | None = Field(default=None, ge=0)
    limit: int | None = Field(default=None, ge=0)


class CompositeInspection(ContractModel):
    """Required common metadata for every inspect/analyze/plan response."""

    completion: Literal["complete", "partial", "sampled", "truncated", "budget_exhausted"]
    candidate_count: int = Field(ge=0)
    runs_inspected: int = Field(ge=0)
    histories_inspected: int = Field(ge=0)
    inspection_failures: int = Field(ge=0)


class AnswerConstraints(ContractModel):
    """Claims an agent may and may not make from one composite response."""

    may_claim: tuple[str, ...] = ()
    must_not_claim: tuple[str, ...] = ()


class EvidenceRef(ContractModel):
    """A minimal pointer from a finding to an observed fact.

    `source` identifies the fact family (for example, history or task queue), while `reference`
    identifies the fact within the tool response. Tool contracts may constrain both values.
    """

    source: EvidenceSource
    reference: str = Field(min_length=1)
    summary: str | None = Field(default=None, min_length=1)
    observed_at: datetime | None = None


class Finding(ContractModel):
    """Portable Temporal-semantic interpretation backed by explicit evidence."""

    code: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    confidence: Confidence
    evidence: tuple[EvidenceRef, ...] = ()
    counterevidence: tuple[EvidenceRef, ...] = ()
    caveats: tuple[str, ...] = ()

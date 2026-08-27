"""Loss-bounded internal facts for Temporal workflow visibility."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TypeAlias


class SearchAttributeSource(StrEnum):
    SYSTEM = "system"
    CUSTOM = "custom"


@dataclass(frozen=True)
class IndexedValueTypeFact:
    """A registered indexed-value type, preserving its server enum value."""

    code: int
    name: str


@dataclass(frozen=True)
class SearchAttributeFieldFact:
    name: str
    source: SearchAttributeSource
    value_type: IndexedValueTypeFact


@dataclass(frozen=True)
class StorageSchemaEntryFact:
    name: str
    storage_type: str


@dataclass(frozen=True)
class VisibilitySchemaFact:
    namespace: str
    fields: tuple[SearchAttributeFieldFact, ...]
    storage_schema: tuple[StorageSchemaEntryFact, ...]


@dataclass(frozen=True)
class WorkflowVisibilityFact:
    namespace: str
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str | None
    task_queue: str
    start_time: datetime
    execution_time: datetime | None
    close_time: datetime | None
    history_length: int
    parent_workflow_id: str | None
    parent_run_id: str | None
    root_workflow_id: str | None
    root_run_id: str | None
    search_attributes: dict[str, VisibilityScalar] = field(default_factory=dict)


@dataclass(frozen=True)
class VisibilityPageFact:
    namespace: str
    query: str
    rows: tuple[WorkflowVisibilityFact, ...]
    next_page_token: bytes | None


class VisibilityGroupBy(StrEnum):
    STATUS = "status"


class CountPrecision(StrEnum):
    """Precision documented by the pinned SDK for visibility aggregates."""

    APPROXIMATE = "approximate"


VisibilityScalar: TypeAlias = str | int | float | bool | datetime | tuple[str, ...] | None


@dataclass(frozen=True)
class VisibilityCountGroupFact:
    values: tuple[VisibilityScalar, ...]
    count: int


@dataclass(frozen=True)
class VisibilityCountFact:
    namespace: str
    query: str
    group_by: VisibilityGroupBy | None
    count: int
    groups: tuple[VisibilityCountGroupFact, ...]
    precision: CountPrecision = CountPrecision.APPROXIMATE

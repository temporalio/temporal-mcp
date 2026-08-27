"""Typed facts from DescribeWorkflowExecution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ops_mcp.facts.history import DecodedPayloadsFact, FailureFact, JsonValue


@dataclass(frozen=True)
class PendingActivityFact:
    activity_id: str
    activity_type: str
    state: str
    attempt: int
    maximum_attempts: int
    scheduled_time: datetime | None
    last_started_time: datetime | None
    next_attempt_time: datetime | None
    last_worker_identity: str | None
    last_failure: FailureFact | None
    paused: bool
    activity_task_queue: str | None = None
    last_heartbeat_time: datetime | None = None
    heartbeat_details: DecodedPayloadsFact = field(default_factory=DecodedPayloadsFact)


@dataclass(frozen=True)
class PendingChildFact:
    workflow_id: str
    run_id: str
    workflow_type: str
    initiated_event_id: int
    parent_close_policy: str


@dataclass(frozen=True)
class PendingWorkflowTaskFact:
    state: str
    attempt: int
    scheduled_time: datetime | None
    started_time: datetime | None


@dataclass(frozen=True)
class WorkflowExecutionFact:
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str
    task_queue: str
    namespace: str
    start_time: datetime
    execution_time: datetime | None
    close_time: datetime | None
    history_length: int
    history_size_bytes: int
    first_run_id: str | None
    parent_workflow_id: str | None
    parent_run_id: str | None
    root_workflow_id: str | None
    root_run_id: str | None
    memo: dict[str, DecodedPayloadsFact] = field(default_factory=dict)
    search_attributes: dict[str, JsonValue] = field(default_factory=dict)
    pending_activities: tuple[PendingActivityFact, ...] = ()
    pending_children: tuple[PendingChildFact, ...] = ()
    pending_workflow_task: PendingWorkflowTaskFact | None = None
    observed_at: datetime | None = None

"""Public schema-v1 contracts for direct workflow read tools."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    ContractModel,
    Inspection,
    SchemaVersion,
    WarningMessage,
)


class DecodedPayloadsOutput(ContractModel):
    decode_status: Literal["decoded", "decode_failed"]
    values: tuple[Any, ...] = ()


class FailureOutput(ContractModel):
    message: str
    source: str | None = None
    stack_trace: str | None = None
    application_type: str | None = None
    non_retryable: bool | None = None
    timeout_type: str | None = None
    cause: FailureOutput | None = None


class PendingActivityOutput(ContractModel):
    activity_id: str
    activity_type: str
    state: str
    attempt: int = Field(ge=0)
    maximum_attempts: int = Field(ge=0)
    scheduled_time: datetime | None
    last_started_time: datetime | None
    next_attempt_time: datetime | None
    last_worker_identity: str | None
    last_failure: FailureOutput | None
    paused: bool
    activity_task_queue: str | None
    last_heartbeat_time: datetime | None
    heartbeat_details: DecodedPayloadsOutput


class PendingChildOutput(ContractModel):
    workflow_id: str
    run_id: str
    workflow_type: str
    initiated_event_id: int = Field(ge=0)
    parent_close_policy: str


class PendingWorkflowTaskOutput(ContractModel):
    state: str
    attempt: int = Field(ge=0)
    scheduled_time: datetime | None
    started_time: datetime | None


class WorkflowDescriptionOutput(ContractModel):
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str
    task_queue: str
    namespace: str
    start_time: datetime
    execution_time: datetime | None
    close_time: datetime | None
    history_length: int = Field(ge=0)
    history_size_bytes: int = Field(ge=0)
    first_run_id: str | None
    parent_workflow_id: str | None
    parent_run_id: str | None
    root_workflow_id: str | None
    root_run_id: str | None
    memo: dict[str, DecodedPayloadsOutput]
    search_attributes: dict[str, Any]
    pending_activities: tuple[PendingActivityOutput, ...]
    pending_children: tuple[PendingChildOutput, ...]
    pending_workflow_task: PendingWorkflowTaskOutput | None
    observed_at: datetime


class DescribeWorkflowRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str | None = Field(default=None, min_length=1)


class DescribeWorkflowResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: WorkflowDescriptionOutput
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class EventIdRange(ContractModel):
    first: int = Field(ge=1)
    last: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_order(self) -> EventIdRange:
        if self.last < self.first:
            raise ValueError("event range last must be greater than or equal to first")
        return self


class HistorySliceRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    failures_only: Literal[True] | None = None
    event_id_range: EventIdRange | None = None
    last_n: int | None = Field(default=None, ge=1, le=500)
    event_types: tuple[str, ...] = ()
    page_token: str | None = Field(default=None, min_length=1)
    limit: int = Field(default=100, ge=1, le=500)

    @model_validator(mode="after")
    def validate_selector(self) -> HistorySliceRequest:
        selected = sum(
            value is not None for value in (self.failures_only, self.event_id_range, self.last_n)
        )
        if selected != 1:
            raise ValueError("exactly one history selector is required")
        if self.last_n is not None and self.page_token is not None:
            raise ValueError("last_n does not accept a page token")
        return self


class HistoryEventOutput(ContractModel):
    event_id: int = Field(ge=1)
    event_type: str
    event_time: datetime
    task_id: int
    attributes: dict[str, Any]


class HistorySliceAnswer(ContractModel):
    workflow_id: str
    run_id: str
    events: tuple[HistoryEventOutput, ...]
    inspected_event_id_range: tuple[int, int] | None
    returned_event_id_range: tuple[int, int] | None
    continuation_token: str | None = None


class HistorySliceResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: HistorySliceAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class GetWorkflowResultRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    wait: bool = False
    wait_timeout_seconds: float | None = Field(default=None, gt=0, le=30)

    @model_validator(mode="after")
    def validate_wait_timeout(self) -> GetWorkflowResultRequest:
        if self.wait_timeout_seconds is not None and not self.wait:
            raise ValueError("wait_timeout_seconds requires wait=true")
        return self


class WorkflowResultOutput(ContractModel):
    state: Literal[
        "running",
        "completed",
        "failed",
        "canceled",
        "terminated",
        "timed_out",
        "continued_as_new",
    ]
    result: DecodedPayloadsOutput | None = None
    failure: FailureOutput | None = None
    successor_run_id: str | None = None
    closing_event_id: int | None = Field(default=None, ge=1)


class WorkflowResultAnswer(ContractModel):
    workflow_id: str
    run_id: str
    result: WorkflowResultOutput


class GetWorkflowResultResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: WorkflowResultAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


SingleTaskQueueType = Annotated[Literal["workflow", "activity"], Field()]
TaskQueueTypeInput = Annotated[Literal["workflow", "activity", "both"], Field()]


class DescribeTaskQueueRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    task_queue: str = Field(min_length=1)
    task_type: TaskQueueTypeInput


class PollerOutput(ContractModel):
    identity: str
    last_access_time: datetime | None
    rate_per_second: float


class TaskQueueDescriptionOutput(ContractModel):
    task_queue: str
    task_type: SingleTaskQueueType
    pollers: tuple[PollerOutput, ...]
    poller_count: int = Field(ge=0)
    currently_has_pollers: bool
    approximate_backlog_count: int = Field(ge=0)
    approximate_backlog_age_seconds: float = Field(ge=0)
    tasks_add_rate: float
    tasks_dispatch_rate: float
    rate_limiting_active: bool
    observed_at: datetime


class TaskQueueKindResult(ContractModel):
    task_type: SingleTaskQueueType
    description: TaskQueueDescriptionOutput | None = None
    error: dict[str, Any] | None = None

    @model_validator(mode="after")
    def exactly_one_outcome(self) -> TaskQueueKindResult:
        if (self.description is None) == (self.error is None):
            raise ValueError("exactly one of description or error is required")
        return self


class TaskQueueBothOutput(ContractModel):
    task_queue: str
    task_type: Literal["both"] = "both"
    results: tuple[TaskQueueKindResult, TaskQueueKindResult]


class DescribeTaskQueueResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: TaskQueueDescriptionOutput | TaskQueueBothOutput
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()

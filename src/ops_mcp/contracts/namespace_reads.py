"""Public schema-v1 contracts for schedules, queries, and batch operations."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import Field, model_validator

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    ContractModel,
    Inspection,
    SchemaVersion,
    WarningMessage,
)


class ListSchedulesRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    name_contains: str | None = Field(default=None, min_length=1)
    shape: Literal["cron", "interval", "calendar"] | None = None
    paused: bool | None = None
    page_size: int = Field(default=100, ge=1, le=200)
    page_token: str | None = Field(default=None, min_length=1)


class ScheduleSummaryOutput(ContractModel):
    schedule_id: str
    paused: bool | None
    note: str | None
    shapes: tuple[Literal["cron", "interval", "calendar"], ...]
    recent_action_times: tuple[datetime, ...]
    next_action_times: tuple[datetime, ...]


class ListSchedulesAnswer(ContractModel):
    schedules: tuple[ScheduleSummaryOutput, ...]
    server_rows_scanned: int = Field(ge=0)
    returned: int = Field(ge=0)
    continuation_token: str | None = None


class ListSchedulesResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: ListSchedulesAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class DescribeScheduleRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    schedule_id: str = Field(min_length=1)
    upcoming_count: int = Field(default=0, ge=0, le=50)
    upcoming_start: datetime | None = None
    upcoming_end: datetime | None = None

    @model_validator(mode="after")
    def validate_upcoming_range(self) -> DescribeScheduleRequest:
        supplied = self.upcoming_start is not None or self.upcoming_end is not None
        if self.upcoming_count and (self.upcoming_start is None or self.upcoming_end is None):
            raise ValueError("upcoming_start and upcoming_end are required when count is positive")
        if not self.upcoming_count and supplied:
            raise ValueError("an upcoming range requires upcoming_count")
        if self.upcoming_start and self.upcoming_end:
            if self.upcoming_start.tzinfo is None or self.upcoming_end.tzinfo is None:
                raise ValueError("upcoming range must be timezone-aware")
            if self.upcoming_end <= self.upcoming_start:
                raise ValueError("upcoming_end must be later than upcoming_start")
            if self.upcoming_end - self.upcoming_start > timedelta(days=366):
                raise ValueError("upcoming range cannot exceed 366 days")
        return self


class ScheduleActionOutput(ContractModel):
    action_type: str
    workflow_type: str | None
    workflow_id: str | None
    task_queue: str | None


class DescribeScheduleAnswer(ContractModel):
    schedule_id: str
    action: ScheduleActionOutput
    paused: bool
    note: str | None
    limited_actions: bool
    remaining_actions: int = Field(ge=0)
    shapes: tuple[Literal["cron", "interval", "calendar"], ...]
    cron_expressions: tuple[str, ...]
    intervals_seconds: tuple[float, ...]
    calendar_count: int = Field(ge=0)
    time_zone_name: str | None
    overlap_policy: str
    catchup_window_seconds: float = Field(ge=0)
    pause_on_failure: bool
    recent_action_times: tuple[datetime, ...]
    next_action_times: tuple[datetime, ...]
    upcoming_action_times: tuple[datetime, ...]
    observed_at: datetime


class DescribeScheduleResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: DescribeScheduleAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class QueryWorkflowRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    query_name: str | None = Field(default=None, min_length=1)


class QueryWorkflowAnswer(ContractModel):
    kind: Literal["result"] = "result"
    workflow_id: str
    run_id: str
    query_name: str
    decode_status: Literal["decoded", "decode_failed"]
    value: Any | None
    observed_at: datetime


class ApprovedQueryOutput(ContractModel):
    query_name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    sensitivity: Literal["ordinary", "sensitive"]


class ListApprovedWorkflowQueriesRequest(ContractModel):
    target_ref: str = Field(
        min_length=1,
        description="Opaque target reference returned by resolve_target.",
        examples=["orders-prod"],
    )


class ListApprovedWorkflowQueriesAnswer(ContractModel):
    approved_queries: tuple[ApprovedQueryOutput, ...]
    approval_scope: Literal["target"] = "target"
    support_status: Literal["not_verified"] = "not_verified"


class ListApprovedWorkflowQueriesResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: ListApprovedWorkflowQueriesAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class QueryWorkflowDiscoveryAnswer(ContractModel):
    kind: Literal["discovery"] = "discovery"
    workflow_id: str
    run_id: str
    approved_queries: tuple[ApprovedQueryOutput, ...]
    approval_scope: Literal["target"] = "target"
    support_status: Literal["not_verified"] = "not_verified"


class QueryWorkflowResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: QueryWorkflowAnswer | QueryWorkflowDiscoveryAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class ListBatchOperationsRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    state: str | None = Field(default=None, min_length=1)
    job_type: str | None = Field(default=None, min_length=1)
    page_size: int = Field(default=25, ge=1, le=50)
    page_token: str | None = Field(default=None, min_length=1)
    include_details: bool = True


class BatchOperationOutput(ContractModel):
    job_id: str
    state: str
    operation_type: str
    start_time: datetime | None
    close_time: datetime | None
    detail_status: Literal["complete", "not_requested", "unknown", "budget_exhausted"]
    total_operation_count: int | None = Field(default=None, ge=0)
    complete_operation_count: int | None = Field(default=None, ge=0)
    failure_operation_count: int | None = Field(default=None, ge=0)
    identity: str | None = None
    reason: str | None = None
    query: str | None = None


class ListBatchOperationsAnswer(ContractModel):
    operations: tuple[BatchOperationOutput, ...]
    returned: int = Field(ge=0)
    continuation_token: str | None = None


class ListBatchOperationsResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: ListBatchOperationsAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()

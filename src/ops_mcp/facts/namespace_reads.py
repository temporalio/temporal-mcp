"""SDK-free facts for schedules, workflow queries, and batch operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from ops_mcp.facts.history import JsonValue


@dataclass(frozen=True)
class ScheduleSummaryFact:
    schedule_id: str
    paused: bool | None
    note: str | None
    shapes: tuple[Literal["cron", "interval", "calendar"], ...]
    recent_action_times: tuple[datetime, ...]
    next_action_times: tuple[datetime, ...]


@dataclass(frozen=True)
class ScheduleActionFact:
    action_type: str
    workflow_type: str | None
    workflow_id: str | None
    task_queue: str | None


@dataclass(frozen=True)
class ScheduleDescriptionFact:
    schedule_id: str
    action: ScheduleActionFact
    paused: bool
    note: str | None
    limited_actions: bool
    remaining_actions: int
    shapes: tuple[Literal["cron", "interval", "calendar"], ...]
    cron_expressions: tuple[str, ...]
    intervals_seconds: tuple[float, ...]
    calendar_count: int
    time_zone_name: str | None
    overlap_policy: str
    catchup_window_seconds: float
    pause_on_failure: bool
    recent_action_times: tuple[datetime, ...]
    next_action_times: tuple[datetime, ...]
    upcoming_action_times: tuple[datetime, ...]
    observed_at: datetime


@dataclass(frozen=True)
class QueryResultFact:
    workflow_id: str
    run_id: str
    query_name: str
    value: JsonValue | None
    decode_status: Literal["decoded", "decode_failed"]
    observed_at: datetime


@dataclass(frozen=True)
class BatchOperationSummaryFact:
    job_id: str
    state: str
    operation_type: str
    start_time: datetime | None
    close_time: datetime | None


@dataclass(frozen=True)
class BatchOperationPageFact:
    operations: tuple[BatchOperationSummaryFact, ...]
    next_page_token: bytes | None
    observed_at: datetime


@dataclass(frozen=True)
class BatchOperationDetailFact:
    job_id: str
    state: str
    operation_type: str
    start_time: datetime | None
    close_time: datetime | None
    total_operation_count: int
    complete_operation_count: int
    failure_operation_count: int
    identity: str | None
    reason: str | None
    query: str | None

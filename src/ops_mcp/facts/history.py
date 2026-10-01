"""Normalized workflow-history facts used by services and analysis.

These models deliberately contain no protobuf or Temporal SDK objects. Event families needed by
diagnosis and reset analysis have typed attributes. Unknown and presently unsupported families
remain visible through ``UnknownEventAttributes`` so analysis can refuse rather than silently
discarding history.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class DecodedPayloadsFact:
    values: tuple[JsonValue, ...] = ()
    decode_status: Literal["decoded", "decode_failed"] = "decoded"


@dataclass(frozen=True)
class FailureFact:
    message: str
    source: str | None = None
    stack_trace: str | None = None
    application_type: str | None = None
    non_retryable: bool | None = None
    timeout_type: str | None = None
    cause: FailureFact | None = None


@dataclass(frozen=True)
class WorkflowTaskEventAttributes:
    family: Literal["workflow_task"] = "workflow_task"
    phase: Literal["started", "completed", "failed", "timed_out"] = "completed"
    scheduled_event_id: int = 0
    started_event_id: int = 0
    identity: str | None = None
    worker_build_id: str | None = None
    cause: str | None = None
    failure: FailureFact | None = None


@dataclass(frozen=True)
class ActivityTaskEventAttributes:
    family: Literal["activity_task"] = "activity_task"
    phase: Literal["scheduled", "started", "completed", "failed", "timed_out"] = "scheduled"
    activity_id: str | None = None
    activity_type: str | None = None
    task_queue: str | None = None
    scheduled_event_id: int = 0
    started_event_id: int = 0
    identity: str | None = None
    attempt: int | None = None
    retry_state: str | None = None
    failure: FailureFact | None = None


@dataclass(frozen=True)
class WorkflowCloseEventAttributes:
    family: Literal["workflow_close"] = "workflow_close"
    state: Literal["completed", "failed", "canceled", "terminated", "timed_out"] = "completed"
    failure: FailureFact | None = None
    retry_state: str | None = None
    new_execution_run_id: str | None = None
    result: DecodedPayloadsFact = field(default_factory=DecodedPayloadsFact)


@dataclass(frozen=True)
class SignalEventAttributes:
    family: Literal["signal"] = "signal"
    signal_name: str = ""
    identity: str | None = None
    input: DecodedPayloadsFact = field(default_factory=DecodedPayloadsFact)


@dataclass(frozen=True)
class UpdateEventAttributes:
    family: Literal["update"] = "update"
    phase: Literal["admitted", "accepted", "rejected", "completed"] = "admitted"
    protocol_instance_id: str | None = None
    update_id: str | None = None


@dataclass(frozen=True)
class ExternalInteractionEventAttributes:
    family: Literal["child_or_external"] = "child_or_external"
    interaction: str = ""
    workflow_id: str | None = None
    run_id: str | None = None
    workflow_type: str | None = None
    initiated_event_id: int = 0


@dataclass(frozen=True)
class ContinueAsNewEventAttributes:
    family: Literal["continue_as_new"] = "continue_as_new"
    new_execution_run_id: str = ""
    workflow_type: str | None = None
    task_queue: str | None = None


@dataclass(frozen=True)
class UnknownEventAttributes:
    family: Literal["unknown"] = "unknown"
    attribute_name: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)


HistoryEventAttributes: TypeAlias = (
    WorkflowTaskEventAttributes
    | ActivityTaskEventAttributes
    | WorkflowCloseEventAttributes
    | SignalEventAttributes
    | UpdateEventAttributes
    | ExternalInteractionEventAttributes
    | ContinueAsNewEventAttributes
    | UnknownEventAttributes
)


@dataclass(frozen=True)
class HistoryEventFact:
    event_id: int
    event_type: str
    event_time: datetime
    task_id: int
    attributes: HistoryEventAttributes


@dataclass(frozen=True)
class HistoryReadFact:
    workflow_id: str
    run_id: str
    events: tuple[HistoryEventFact, ...]
    reverse: bool
    pages_read: int
    events_scanned: int
    next_page_token: bytes | None
    observed_at: datetime
    next_page_offset: int = 0
    next_expected_event_id: int | None = None

    @property
    def complete(self) -> bool:
        return self.next_page_token is None and self.next_page_offset == 0

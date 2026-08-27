"""Typed current task-queue snapshot facts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class TaskQueueKind(StrEnum):
    WORKFLOW = "workflow"
    ACTIVITY = "activity"


@dataclass(frozen=True)
class PollerFact:
    identity: str
    last_access_time: datetime | None
    rate_per_second: float


@dataclass(frozen=True)
class TaskQueueFact:
    task_queue: str
    task_type: TaskQueueKind
    pollers: tuple[PollerFact, ...]
    approximate_backlog_count: int
    approximate_backlog_age_seconds: float
    tasks_add_rate: float
    tasks_dispatch_rate: float
    rate_limiting_active: bool
    observed_at: datetime

    @property
    def currently_has_pollers(self) -> bool:
        return bool(self.pollers)

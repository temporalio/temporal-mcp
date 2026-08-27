from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Target:
    id: str
    aliases: tuple[str, ...]
    account: str
    cluster: str
    namespace: str
    address: str
    tls: bool = False
    api_key_env: str | None = None
    server_name: str | None = None


@dataclass(frozen=True)
class Principal:
    subject: str
    groups: tuple[str, ...]


@dataclass(frozen=True)
class FailureDetail:
    event_id: int
    event_type: str
    message: str
    error_type: str | None = None
    activity_id: str | None = None
    activity_type: str | None = None
    attempt: int | None = None
    retry_state: str | None = None


@dataclass
class WorkflowSnapshot:
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str
    task_queue: str
    start_time: str | None
    close_time: str | None
    history_length: int
    pending_activities: list[dict[str, Any]] = field(default_factory=list)
    parent: dict[str, str] | None = None

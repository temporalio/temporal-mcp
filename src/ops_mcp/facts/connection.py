"""Redacted facts produced by connection-stage probes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ConnectionStageStatus(StrEnum):
    REACHABLE = "reachable"
    FAILED = "failed"
    NOT_CONFIGURED = "not_configured"
    NOT_CHECKED = "not_checked"


@dataclass(frozen=True)
class ConnectionStageFact:
    stage: str
    status: ConnectionStageStatus
    latency_ms: float | None
    failure_code: str | None = None


@dataclass(frozen=True)
class ServerConnectionFact:
    outcome: ConnectionStageFact
    server_version: str | None = None
    capabilities: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class NamespaceConnectionFact:
    outcome: ConnectionStageFact
    namespace: str | None = None
    namespace_id: str | None = None
    state: str | None = None
    is_global_namespace: bool | None = None
    supports_schedules: bool | None = None

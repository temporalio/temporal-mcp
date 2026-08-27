"""Public schema-v1 contract for staged target connection checks."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    ContractModel,
    Inspection,
    SchemaVersion,
    WarningMessage,
)


class CheckConnectionRequest(ContractModel):
    target_ref: str = Field(min_length=1)
    probe_codec: bool = True


class ConnectionStageOutput(ContractModel):
    stage: Literal["server", "namespace", "codec"]
    status: Literal["reachable", "failed", "not_configured", "not_checked"]
    latency_ms: float | None = Field(default=None, ge=0)
    failure_code: str | None = None


class ServerCapabilityOutput(ContractModel):
    server_version: str | None
    capabilities: dict[str, bool]


class NamespaceIdentityOutput(ContractModel):
    namespace: str
    namespace_id: str
    state: str
    is_global_namespace: bool
    supports_schedules: bool


class CheckConnectionAnswer(ContractModel):
    overall_status: Literal["reachable", "failed", "incomplete"]
    server: ConnectionStageOutput
    namespace: ConnectionStageOutput
    codec: ConnectionStageOutput
    server_facts: ServerCapabilityOutput | None
    namespace_facts: NamespaceIdentityOutput | None


class CheckConnectionResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: str = Field(min_length=1)
    answer: CheckConnectionAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()

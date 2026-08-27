"""Orchestration for staged, redacted Temporal connection checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ops_mcp.contracts.connection import (
    CheckConnectionAnswer,
    CheckConnectionRequest,
    CheckConnectionResponse,
    ConnectionStageOutput,
    NamespaceIdentityOutput,
    ServerCapabilityOutput,
)
from ops_mcp.contracts.primitives import Completeness, Inspection
from ops_mcp.facts.connection import (
    ConnectionStageFact,
    ConnectionStageStatus,
    NamespaceConnectionFact,
    ServerConnectionFact,
)
from ops_mcp.gateways.connection import ConnectionGateway
from ops_mcp.runtime.context import RequestContext
from ops_mcp.targets.catalog import TargetCatalog
from ops_mcp.targets.models import ResolvedTarget


class CodecProbe(Protocol):
    async def probe(
        self, context: RequestContext, target: ResolvedTarget
    ) -> ConnectionStageFact: ...


@dataclass(frozen=True)
class NotConfiguredCodecProbe:
    """Foundation default; performs no I/O and assumes no codec configuration."""

    async def probe(self, context: RequestContext, target: ResolvedTarget) -> ConnectionStageFact:
        del context, target
        return ConnectionStageFact(
            stage="codec",
            status=ConnectionStageStatus.NOT_CONFIGURED,
            latency_ms=None,
        )


class ConnectionCheckService(Protocol):
    async def check_connection(
        self, context: RequestContext, request: CheckConnectionRequest
    ) -> CheckConnectionResponse: ...


class DefaultConnectionCheckService:
    def __init__(
        self,
        catalog: TargetCatalog,
        gateway: ConnectionGateway,
        codec_probe: CodecProbe | None = None,
    ) -> None:
        self._catalog = catalog
        self._gateway = gateway
        self._codec = codec_probe or NotConfiguredCodecProbe()

    async def check_connection(
        self, context: RequestContext, request: CheckConnectionRequest
    ) -> CheckConnectionResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        server = await self._gateway.probe_server(context, target)

        if server.outcome.status is ConnectionStageStatus.REACHABLE:
            namespace = await self._gateway.probe_namespace(context, target)
        else:
            namespace = NamespaceConnectionFact(
                outcome=ConnectionStageFact(
                    stage="namespace",
                    status=ConnectionStageStatus.NOT_CHECKED,
                    latency_ms=None,
                )
            )

        if request.probe_codec:
            codec = await self._codec.probe(context, target)
        else:
            codec = ConnectionStageFact(
                stage="codec",
                status=ConnectionStageStatus.NOT_CHECKED,
                latency_ms=None,
            )

        completion = (
            Completeness.UNKNOWN
            if namespace.outcome.status is ConnectionStageStatus.NOT_CHECKED
            else Completeness.COMPLETE
        )
        overall = _overall_status(server.outcome, namespace.outcome, codec)
        after = context.budget.snapshot()
        response = CheckConnectionResponse(
            target_ref=target.target_ref,
            answer=CheckConnectionAnswer(
                overall_status=overall,
                server=_stage_output(server.outcome),
                namespace=_stage_output(namespace.outcome),
                codec=_stage_output(codec),
                server_facts=_server_output(server),
                namespace_facts=_namespace_output(namespace),
            ),
            inspection=Inspection(
                completion=completion,
                temporal_calls=after.temporal_calls - before.temporal_calls,
                pages_read=after.pages_read - before.pages_read,
            ),
            warnings=(
                ("Codec probing is not configured in this proof of concept.",)
                if codec.status is ConnectionStageStatus.NOT_CONFIGURED
                else ()
            ),
        )
        context.budget.consume_output_bytes(len(response.model_dump_json().encode("utf-8")))
        return response


def _overall_status(
    server: ConnectionStageFact,
    namespace: ConnectionStageFact,
    codec: ConnectionStageFact,
) -> str:
    if any(stage.status is ConnectionStageStatus.FAILED for stage in (server, namespace, codec)):
        return "failed"
    if any(
        stage.status is ConnectionStageStatus.NOT_CHECKED for stage in (server, namespace, codec)
    ):
        return "incomplete"
    return "reachable"


def _stage_output(value: ConnectionStageFact) -> ConnectionStageOutput:
    return ConnectionStageOutput(
        stage=value.stage,
        status=value.status.value,
        latency_ms=value.latency_ms,
        failure_code=value.failure_code,
    )


def _server_output(value: ServerConnectionFact) -> ServerCapabilityOutput | None:
    if value.outcome.status is not ConnectionStageStatus.REACHABLE:
        return None
    return ServerCapabilityOutput(
        server_version=value.server_version,
        capabilities=value.capabilities,
    )


def _namespace_output(value: NamespaceConnectionFact) -> NamespaceIdentityOutput | None:
    if value.outcome.status is not ConnectionStageStatus.REACHABLE:
        return None
    assert value.namespace is not None
    assert value.namespace_id is not None
    assert value.state is not None
    assert value.is_global_namespace is not None
    assert value.supports_schedules is not None
    return NamespaceIdentityOutput(
        namespace=value.namespace,
        namespace_id=value.namespace_id,
        state=value.state,
        is_global_namespace=value.is_global_namespace,
        supports_schedules=value.supports_schedules,
    )

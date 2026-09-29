"""Temporal server and namespace connection-stage probes."""

from __future__ import annotations

from datetime import timedelta
from time import monotonic
from typing import Protocol

from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    GetSystemInfoRequest,
)
from temporalio.service import RPCError

from ops_mcp.clients import TemporalClientProvider, acquire_client
from ops_mcp.facts.connection import (
    ConnectionStageFact,
    ConnectionStageStatus,
    NamespaceConnectionFact,
    ServerConnectionFact,
)
from ops_mcp.gateways.history import translate_rpc_error
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.targets.models import ResolvedTarget


class ConnectionGateway(Protocol):
    async def probe_server(
        self, context: RequestContext, target: ResolvedTarget
    ) -> ServerConnectionFact: ...

    async def probe_namespace(
        self, context: RequestContext, target: ResolvedTarget
    ) -> NamespaceConnectionFact: ...


class SdkConnectionGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def probe_server(
        self, context: RequestContext, target: ResolvedTarget
    ) -> ServerConnectionFact:
        started = monotonic()
        try:
            client = await acquire_client(self._provider, context, target)
            context.budget.reserve_temporal_call("GetSystemInfo")
            response = await client.workflow_service.get_system_info(
                GetSystemInfoRequest(),
                retry=True,
                timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            return ServerConnectionFact(
                outcome=_failed_stage(
                    "server", started, translate_rpc_error(exc, operation="get_system_info")
                )
            )
        except OpsMcpError as exc:
            return ServerConnectionFact(outcome=_failed_stage("server", started, exc))
        except Exception:
            return ServerConnectionFact(
                outcome=_failed_stage("server", started, _unreachable_error())
            )
        capabilities = {
            field.name: bool(getattr(response.capabilities, field.name))
            for field in response.capabilities.DESCRIPTOR.fields
        }
        return ServerConnectionFact(
            outcome=_reachable_stage("server", started),
            server_version=response.server_version or None,
            capabilities=capabilities,
        )

    async def probe_namespace(
        self, context: RequestContext, target: ResolvedTarget
    ) -> NamespaceConnectionFact:
        started = monotonic()
        try:
            client = await acquire_client(self._provider, context, target)
            context.budget.reserve_temporal_call("DescribeNamespace")
            response = await client.workflow_service.describe_namespace(
                DescribeNamespaceRequest(namespace=target.namespace),
                retry=True,
                timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            return NamespaceConnectionFact(
                outcome=_failed_stage(
                    "namespace", started, translate_rpc_error(exc, operation="describe_namespace")
                )
            )
        except OpsMcpError as exc:
            return NamespaceConnectionFact(outcome=_failed_stage("namespace", started, exc))
        except Exception:
            return NamespaceConnectionFact(
                outcome=_failed_stage("namespace", started, _unreachable_error())
            )
        info = response.namespace_info
        state_value = info.DESCRIPTOR.fields_by_name["state"].enum_type.values_by_number.get(
            info.state
        )
        return NamespaceConnectionFact(
            outcome=_reachable_stage("namespace", started),
            namespace=info.name,
            namespace_id=info.id,
            state=state_value.name if state_value else "NAMESPACE_STATE_UNSPECIFIED",
            is_global_namespace=response.is_global_namespace,
            supports_schedules=info.supports_schedules,
        )


def _reachable_stage(stage: str, started: float) -> ConnectionStageFact:
    return ConnectionStageFact(
        stage=stage,
        status=ConnectionStageStatus.REACHABLE,
        latency_ms=max(0.0, (monotonic() - started) * 1000),
    )


def _failed_stage(stage: str, started: float, error: OpsMcpError) -> ConnectionStageFact:
    return ConnectionStageFact(
        stage=stage,
        status=ConnectionStageStatus.FAILED,
        latency_ms=max(0.0, (monotonic() - started) * 1000),
        failure_code=error.code.value,
    )


def _unreachable_error() -> OpsMcpError:
    return OpsMcpError(
        ErrorCode.TEMPORAL_UNREACHABLE,
        "Temporal is currently unreachable",
        retryable=True,
    )

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.api.enums.v1 import NamespaceState
from temporalio.api.namespace.v1 import NamespaceInfo
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceResponse,
    GetSystemInfoResponse,
)

from ops_mcp.contracts.connection import CheckConnectionRequest
from ops_mcp.facts.connection import (
    ConnectionStageFact,
    ConnectionStageStatus,
    NamespaceConnectionFact,
    ServerConnectionFact,
)
from ops_mcp.gateways.connection import SdkConnectionGateway
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.services.connection import DefaultConnectionCheckService
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_gateway_probes_redacted_system_and_namespace_facts() -> None:
    system = GetSystemInfoResponse(server_version="1.31.0")
    system.capabilities.supports_schedules = True
    namespace = DescribeNamespaceResponse(
        namespace_info=NamespaceInfo(
            name="default",
            id="namespace-id",
            state=NamespaceState.NAMESPACE_STATE_REGISTERED,
            owner_email="must-not-leak@example.com",
            supports_schedules=True,
        ),
        is_global_namespace=False,
    )
    service = _WorkflowService(system, namespace)
    gateway = SdkConnectionGateway(_Provider(SimpleNamespace(workflow_service=service)))
    context = _context()

    server_fact = await gateway.probe_server(context, _target())
    namespace_fact = await gateway.probe_namespace(context, _target())

    assert server_fact.server_version == "1.31.0"
    assert server_fact.capabilities["supports_schedules"] is True
    assert namespace_fact.namespace_id == "namespace-id"
    assert not hasattr(namespace_fact, "owner_email")
    assert context.budget.snapshot().temporal_calls == 2
    assert service.timeouts[0] <= timedelta(seconds=30)


@pytest.mark.asyncio
async def test_expected_server_failure_is_structured_and_stops_namespace_probe() -> None:
    gateway = _ConnectionGateway(
        ServerConnectionFact(
            outcome=ConnectionStageFact(
                "server", ConnectionStageStatus.FAILED, 2.0, "temporal_unreachable"
            )
        )
    )
    service = DefaultConnectionCheckService(_Catalog(), gateway)

    response = await service.check_connection(
        _context(), CheckConnectionRequest(target_ref="orders-prod")
    )

    assert response.answer.overall_status == "failed"
    assert response.answer.server.failure_code == "temporal_unreachable"
    assert response.answer.namespace.status == "not_checked"
    assert response.answer.codec.status == "not_configured"
    assert gateway.namespace_calls == 0
    serialized = response.model_dump_json()
    assert "localhost:7233" not in serialized


@pytest.mark.asyncio
async def test_successful_stages_with_codec_disabled_are_incomplete() -> None:
    server = ServerConnectionFact(
        outcome=ConnectionStageFact("server", ConnectionStageStatus.REACHABLE, 1.0),
        server_version="1.31.0",
        capabilities={"supports_schedules": True},
    )
    namespace = NamespaceConnectionFact(
        outcome=ConnectionStageFact("namespace", ConnectionStageStatus.REACHABLE, 1.0),
        namespace="default",
        namespace_id="id",
        state="NAMESPACE_STATE_REGISTERED",
        is_global_namespace=False,
        supports_schedules=True,
    )
    service = DefaultConnectionCheckService(_Catalog(), _ConnectionGateway(server, namespace))

    response = await service.check_connection(
        _context(), CheckConnectionRequest(target_ref="orders-prod", probe_codec=False)
    )

    assert response.answer.overall_status == "incomplete"
    assert response.answer.codec.status == "not_checked"
    assert response.answer.namespace_facts
    assert response.inspection.temporal_calls == 2


class _Provider:
    def __init__(self, client: object) -> None:
        self.client = client

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> object:
        return self.client


class _WorkflowService:
    def __init__(self, system: object, namespace: object) -> None:
        self.system = system
        self.namespace = namespace
        self.timeouts: list[timedelta] = []

    async def get_system_info(self, request: object, **kwargs: object) -> object:
        self.timeouts.append(kwargs["timeout"])
        if isinstance(self.system, Exception):
            raise self.system
        return self.system

    async def describe_namespace(self, request: object, **kwargs: object) -> object:
        self.timeouts.append(kwargs["timeout"])
        return self.namespace


class _Catalog:
    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        assert target_ref == "orders-prod"
        return _target()


class _ConnectionGateway:
    def __init__(
        self,
        server: ServerConnectionFact,
        namespace: NamespaceConnectionFact | None = None,
    ) -> None:
        self.server = server
        self.namespace = namespace
        self.namespace_calls = 0

    async def probe_server(
        self, context: RequestContext, target: ResolvedTarget
    ) -> ServerConnectionFact:
        context.budget.reserve_temporal_call("system")
        return self.server

    async def probe_namespace(
        self, context: RequestContext, target: ResolvedTarget
    ) -> NamespaceConnectionFact:
        self.namespace_calls += 1
        context.budget.reserve_temporal_call("namespace")
        assert self.namespace
        return self.namespace


def _context() -> RequestContext:
    return RequestContext(
        "request-1",
        Principal("tester", ("test",)),
        BasicExecutionBudget(deadline=datetime.now(UTC) + timedelta(seconds=30)),
    )


def _target() -> ResolvedTarget:
    descriptor = ConnectionDescriptor("localhost:7233", "default", api_key_env="SECRET")
    return ResolvedTarget("orders-prod", (), "demo", "local", "default", descriptor)

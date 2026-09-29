"""Application composition with explicit process and request lifetimes."""

from __future__ import annotations

import json
import os
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ops_mcp.clients import CachingTemporalClientProvider, TemporalClientProvider
from ops_mcp.gateways import (
    SdkBatchGateway,
    SdkConnectionGateway,
    SdkHistoryGateway,
    SdkQueryGateway,
    SdkScheduleGateway,
    SdkTaskQueueGateway,
    SdkVisibilityGateway,
    SdkWorkflowGateway,
)
from ops_mcp.runtime import DemoIdentityProvider, InvalidConfigurationError, RequestContextFactory
from ops_mcp.services import (
    ConfiguredQueryApprovalPolicy,
    ConnectionCheckService,
    DefaultConnectionCheckService,
    DefaultNamespaceReadService,
    DefaultPopulationDiagnosticService,
    DefaultReasoningService,
    DefaultTargetService,
    DefaultVisibilityService,
    DefaultWorkflowReadService,
    NamespaceReadService,
    PopulationDiagnosticService,
    ReasoningService,
    SearchAttributeSchemaResolver,
    TargetService,
    VisibilityService,
    WorkflowReadService,
)
from ops_mcp.targets import YamlTargetCatalog


class ShutdownableClientProvider(TemporalClientProvider, Protocol):
    async def shutdown(self) -> None: ...


@dataclass(frozen=True)
class ServiceBundle:
    connection: ConnectionCheckService
    targets: TargetService
    visibility: VisibilityService
    workflow_reads: WorkflowReadService
    namespace_reads: NamespaceReadService
    reasoning: ReasoningService
    population: PopulationDiagnosticService


@dataclass
class CompositionRoot:
    contexts: RequestContextFactory
    services: ServiceBundle
    client_provider: ShutdownableClientProvider

    async def shutdown(self) -> None:
        await self.client_provider.shutdown()


def build_composition_root(
    config_path: str | Path | None = None,
    *,
    query_approvals: Mapping[str, Collection[str]] | None = None,
) -> CompositionRoot:
    approvals = _resolve_query_approvals(query_approvals)
    catalog = YamlTargetCatalog.load(config_path or _default_target_config_path())
    provider = CachingTemporalClientProvider()
    visibility_gateway = SdkVisibilityGateway(provider)
    schema_resolver = SearchAttributeSchemaResolver(visibility_gateway)
    workflow_gateway = SdkWorkflowGateway(provider)
    history_gateway = SdkHistoryGateway(provider)
    task_queue_gateway = SdkTaskQueueGateway(provider)
    schedule_gateway = SdkScheduleGateway(provider)
    query_gateway = SdkQueryGateway(provider)
    batch_gateway = SdkBatchGateway(provider)
    workflow_reads = DefaultWorkflowReadService(
        catalog,
        workflow_gateway,
        history_gateway,
        task_queue_gateway,
    )
    namespace_reads = DefaultNamespaceReadService(
        catalog,
        schedule_gateway,
        query_gateway,
        batch_gateway,
        ConfiguredQueryApprovalPolicy(approvals),
    )
    reasoning = DefaultReasoningService(
        catalog,
        workflow_gateway,
        history_gateway,
        task_queue_gateway,
        namespace_reads=namespace_reads,
    )
    population = DefaultPopulationDiagnosticService(
        catalog,
        visibility_gateway,
        workflow_gateway,
        history_gateway,
        task_queue_gateway,
        schema_resolver,
    )
    return CompositionRoot(
        contexts=RequestContextFactory(DemoIdentityProvider()),
        services=ServiceBundle(
            connection=DefaultConnectionCheckService(
                catalog,
                SdkConnectionGateway(provider),
            ),
            targets=DefaultTargetService(catalog),
            visibility=DefaultVisibilityService(
                catalog, visibility_gateway, schema_resolver=schema_resolver
            ),
            workflow_reads=workflow_reads,
            namespace_reads=namespace_reads,
            reasoning=reasoning,
            population=population,
        ),
        client_provider=provider,
    )


def _default_target_config_path() -> Path:
    configured = os.getenv("OPS_MCP_TARGET_CONFIG")
    if configured:
        return Path(configured)
    checkout_example = Path(__file__).resolve().parents[3] / "config" / "targets.example.yaml"
    if checkout_example.is_file():
        return checkout_example
    raise InvalidConfigurationError(
        "OPS_MCP_TARGET_CONFIG must point to a target configuration file when ops-mcp is "
        "run outside its source checkout"
    )


def _resolve_query_approvals(
    explicit: Mapping[str, Collection[str]] | None,
) -> dict[str, frozenset[str]]:
    if explicit is not None:
        return _validate_query_approvals(explicit, require_lists=False)

    serialized = os.getenv("OPS_MCP_QUERY_APPROVALS_JSON")
    if serialized is None:
        return {}
    try:
        parsed = json.loads(serialized)
    except json.JSONDecodeError as exc:
        raise ValueError("OPS_MCP_QUERY_APPROVALS_JSON must be valid JSON") from exc
    return _validate_query_approvals(parsed, require_lists=True)


def _validate_query_approvals(
    value: object,
    *,
    require_lists: bool,
) -> dict[str, frozenset[str]]:
    if not isinstance(value, Mapping):
        raise ValueError("query approvals must map target_ref strings to query-name lists")

    normalized: dict[str, frozenset[str]] = {}
    for target_ref, names in value.items():
        if not isinstance(target_ref, str) or not target_ref:
            raise ValueError("query approvals contain an invalid target_ref")
        if require_lists and not isinstance(names, list):
            raise ValueError("OPS_MCP_QUERY_APPROVALS_JSON values must be query-name lists")
        if isinstance(names, (str, bytes)) or not isinstance(names, Collection):
            raise ValueError("query approvals must map each target_ref to query-name strings")
        if any(not isinstance(name, str) or not name for name in names):
            raise ValueError("query approvals contain an invalid query name")
        if len(set(names)) != len(names):
            raise ValueError("query approvals contain duplicate query names")
        normalized[target_ref] = frozenset(names)
    return normalized

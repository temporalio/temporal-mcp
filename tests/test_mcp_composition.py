from __future__ import annotations

import json
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from ops_mcp.contracts import (
    Completeness,
    Inspection,
    ResolvedTargetAnswer,
    ResolveTargetResponse,
)
from ops_mcp.mcp import CompositionRoot, ServiceBundle, build_composition_root, create_mcp
from ops_mcp.runtime import (
    ErrorCode,
    OpsMcpError,
    Principal,
    RequestContext,
    RequestContextFactory,
)


class _Identity:
    def current_principal(self) -> Principal:
        return Principal(subject="demo-user", groups=("demo-support",))


class _Provider:
    def __init__(self) -> None:
        self.shutdown_calls = 0

    async def get_client(self, *args: object) -> Any:
        raise AssertionError("adapter tests must not acquire Temporal clients")

    async def invalidate(self, *args: object) -> None:
        return None

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class _RecordingServices:
    def __init__(self, *, successful_resolve: bool = False) -> None:
        self.calls: list[tuple[str, RequestContext, object]] = []
        self.successful_resolve = successful_resolve

    def resolve_target(self, context: RequestContext, request: object) -> ResolveTargetResponse:
        self._record("resolve_target", context, request)
        if self.successful_resolve:
            return ResolveTargetResponse(
                answer=ResolvedTargetAnswer(
                    target_ref="orders-prod",
                    display_name="orders prod",
                    account="demo",
                    cluster="local",
                    namespace="default",
                    resolved_from="orders prod",
                ),
                inspection=Inspection(
                    completion=Completeness.COMPLETE,
                    temporal_calls=0,
                    pages_read=0,
                ),
            )
        self._stop()

    async def check_connection(self, context: RequestContext, request: object) -> Any:
        self._record("check_connection", context, request)
        self._stop()

    async def filter_workflows(self, context: RequestContext, request: object) -> Any:
        self._record("filter_workflows", context, request)
        self._stop()

    async def count_workflows(self, context: RequestContext, request: object) -> Any:
        self._record("count_workflows", context, request)
        self._stop()

    async def get_visibility_schema(self, context: RequestContext, request: object) -> Any:
        self._record("get_visibility_schema", context, request)
        self._stop()

    async def describe_workflow(self, context: RequestContext, request: object) -> Any:
        self._record("describe_workflow", context, request)
        self._stop()

    async def get_workflow_history_slice(self, context: RequestContext, request: object) -> Any:
        self._record("get_workflow_history_slice", context, request)
        self._stop()

    async def get_workflow_result(self, context: RequestContext, request: object) -> Any:
        self._record("get_workflow_result", context, request)
        self._stop()

    async def describe_task_queue(self, context: RequestContext, request: object) -> Any:
        self._record("describe_task_queue", context, request)
        self._stop()

    async def diagnose_workflow(self, context: RequestContext, request: object) -> Any:
        self._record("diagnose_workflow", context, request)
        self._stop()

    async def inspect_workflow_execution(self, context: RequestContext, request: object) -> Any:
        self._record("inspect_workflow_execution", context, request)
        self._stop()

    async def plan_workflow_reset(self, context: RequestContext, request: object) -> Any:
        self._record("plan_workflow_reset", context, request)
        self._stop()

    async def find_workflow_task_failures(self, context: RequestContext, request: object) -> Any:
        self._record("find_workflow_task_failures", context, request)
        self._stop()

    async def find_stuck_workflows(self, context: RequestContext, request: object) -> Any:
        self._record("find_stuck_workflows", context, request)
        self._stop()

    async def analyze_incident_impact(self, context: RequestContext, request: object) -> Any:
        self._record("analyze_incident_impact", context, request)
        self._stop()

    async def assess_workflow_recovery(self, context: RequestContext, request: object) -> Any:
        self._record("assess_workflow_recovery", context, request)
        self._stop()

    async def list_schedules(self, context: RequestContext, request: object) -> Any:
        self._record("list_schedules", context, request)
        self._stop()

    async def describe_schedule(self, context: RequestContext, request: object) -> Any:
        self._record("describe_schedule", context, request)
        self._stop()

    async def query_workflow(self, context: RequestContext, request: object) -> Any:
        self._record("query_workflow", context, request)
        self._stop()

    def list_approved_workflow_queries(self, context: RequestContext, request: object) -> Any:
        self._record("list_approved_workflow_queries", context, request)
        self._stop()

    async def list_batch_operations(self, context: RequestContext, request: object) -> Any:
        self._record("list_batch_operations", context, request)
        self._stop()

    def _record(self, name: str, context: RequestContext, request: object) -> None:
        self.calls.append((name, context, request))

    def _stop(self) -> None:
        raise OpsMcpError(
            ErrorCode.TARGET_NOT_AUTHORIZED,
            "not authorized",
            retryable=False,
        )


def _root(services: _RecordingServices) -> tuple[CompositionRoot, _Provider]:
    provider = _Provider()
    bundle = ServiceBundle(
        connection=services,
        targets=services,
        visibility=services,
        describe_workflow=services,
        workflow_history=services,
        workflow_result=services,
        task_queue=services,
        namespace_reads=services,
        diagnose_workflow=services,
        plan_workflow_reset=services,
        population=services,
    )
    root = CompositionRoot(
        contexts=RequestContextFactory(_Identity(), default_timeout=timedelta(seconds=5)),
        services=bundle,
        client_provider=provider,
    )
    return root, provider


async def test_registers_all_generalized_tools_with_typed_schemas() -> None:
    root, _ = _root(_RecordingServices())
    server = create_mcp(root)

    tools = await server.list_tools()

    assert {tool.name for tool in tools} == {
        "check_connection",
        "resolve_target",
        "filter_workflows",
        "search_workflow_runs",
        "count_workflows",
        "get_visibility_schema",
        "describe_workflow",
        "get_workflow_history_slice",
        "get_workflow_result",
        "describe_task_queue",
        "diagnose_workflow",
        "inspect_workflow_execution",
        "plan_workflow_reset",
        "find_workflow_task_failures",
        "inspect_workflow_task_failures",
        "find_stuck_workflows",
        "inspect_activity_queue_dependencies",
        "assess_workflow_recovery",
        "analyze_incident_impact",
        "list_schedules",
        "describe_schedule",
        "query_workflow",
        "list_approved_workflow_queries",
        "list_batch_operations",
    }
    assert all(tool.annotations and tool.annotations.readOnlyHint for tool in tools)
    assert all(tool.outputSchema and "schema_version" in str(tool.outputSchema) for tool in tools)
    for tool in tools:
        for schema in tool.inputSchema.get("properties", {}).values():
            assert schema.get("description"), tool.name
            assert schema.get("examples"), tool.name

    preferred_paginated = {
        "search_workflow_runs",
        "get_workflow_history_slice",
        "list_schedules",
        "list_batch_operations",
        "inspect_workflow_task_failures",
        "inspect_activity_queue_dependencies",
        "analyze_incident_impact",
    }
    schemas = {tool.name: tool.inputSchema for tool in tools}
    for name in preferred_paginated:
        properties = schemas[name]["properties"]
        assert "continuation_token" in properties
        assert "page_token" not in properties

    for name in (
        "inspect_workflow_task_failures",
        "inspect_activity_queue_dependencies",
        "analyze_incident_impact",
    ):
        properties = schemas[name]["properties"]
        assert "advanced_options" not in properties
        assert "limits" not in properties
        assert "sampling_strategy" not in properties
        assert "sample_seed" not in properties
    assert "candidate_query" in schemas["analyze_incident_impact"]["properties"]
    assert "query" not in schemas["analyze_incident_impact"]["properties"]

    composites = {
        "inspect_workflow_execution",
        "inspect_workflow_task_failures",
        "inspect_activity_queue_dependencies",
        "assess_workflow_recovery",
        "analyze_incident_impact",
        "plan_workflow_reset",
    }
    for name in composites:
        serialized = json.dumps(next(tool.outputSchema for tool in tools if tool.name == name))
        for field in (
            "candidate_count",
            "runs_inspected",
            "histories_inspected",
            "inspection_failures",
            "evidence_collected",
            "evidence_unavailable",
            "supported_conclusions",
            "unsupported_conclusions",
            "continuation_token",
            "warnings",
        ):
            assert field in serialized, (name, field)


async def test_each_adapter_calls_exactly_one_service_with_fresh_context() -> None:
    services = _RecordingServices()
    root, _ = _root(services)
    server = create_mcp(root)
    calls = (
        ("check_connection", {"target_ref": "orders-prod"}),
        ("resolve_target", {"human_name": "orders prod"}),
        (
            "filter_workflows",
            {
                "target_ref": "orders-prod",
                "query": "",
                "fields": ["workflow_id"],
            },
        ),
        (
            "search_workflow_runs",
            {"target_ref": "orders-prod", "query": ""},
        ),
        ("count_workflows", {"target_ref": "orders-prod", "query": ""}),
        ("get_visibility_schema", {"target_ref": "orders-prod"}),
        (
            "describe_workflow",
            {"target_ref": "orders-prod", "workflow_id": "order-1"},
        ),
        (
            "get_workflow_history_slice",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
                "failures_only": True,
            },
        ),
        (
            "get_workflow_result",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
            },
        ),
        (
            "describe_task_queue",
            {
                "target_ref": "orders-prod",
                "task_queue": "orders",
                "task_type": "workflow",
            },
        ),
        (
            "diagnose_workflow",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
            },
        ),
        (
            "inspect_workflow_execution",
            {"target_ref": "orders-prod", "workflow_id": "order-1"},
        ),
        (
            "plan_workflow_reset",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
            },
        ),
        (
            "find_workflow_task_failures",
            {
                "target_ref": "orders-prod",
                "query": "WorkflowType = 'OrderWorkflow'",
                "time_window": {
                    "started_at_or_after": "2026-08-23T00:00:00Z",
                    "started_before": "2026-08-24T00:00:00Z",
                },
                "limits": {
                    "max_candidates": 10,
                    "max_runs": 5,
                    "max_histories": 3,
                    "max_history_events_per_run": 500,
                    "max_task_queues": 5,
                    "max_concurrency": 2,
                },
                "sampling_strategy": "deterministic_hash",
                "sample_seed": "test-seed",
            },
        ),
        (
            "inspect_workflow_task_failures",
            {"target_ref": "orders-prod", "query": "WorkflowType = 'OrderWorkflow'"},
        ),
        (
            "find_stuck_workflows",
            {
                "target_ref": "orders-prod",
                "query": "WorkflowType = 'OrderWorkflow'",
                "time_window": {
                    "started_at_or_after": "2026-08-23T00:00:00Z",
                    "started_before": "2026-08-24T00:00:00Z",
                },
                "minimum_open_duration_seconds": 300,
                "limits": {
                    "max_candidates": 10,
                    "max_runs": 5,
                    "max_histories": 3,
                    "max_history_events_per_run": 500,
                    "max_task_queues": 5,
                    "max_concurrency": 2,
                },
                "sampling_strategy": "deterministic_hash",
                "sample_seed": "test-seed",
            },
        ),
        (
            "inspect_activity_queue_dependencies",
            {"target_ref": "orders-prod", "query": "ExecutionStatus = 'Running'"},
        ),
        (
            "analyze_incident_impact",
            {
                "target_ref": "orders-prod",
                "candidate_query": "WorkflowType = 'OrderWorkflow'",
                "started_at": "2026-08-23T00:00:00Z",
                "ended_at": "2026-08-23T00:40:00Z",
                "affected_activity_task_queues": ["ledger-activity"],
                "expected_duration_seconds": 600,
            },
        ),
        (
            "assess_workflow_recovery",
            {
                "target_ref": "orders-prod",
                "failed_workflow_id": "order-1",
                "failed_run_id": "run-1",
                "comparison_query": "WorkflowType = 'OrderWorkflow'",
                "activity_type": "EnrichProduct",
            },
        ),
        ("list_schedules", {"target_ref": "orders-prod"}),
        (
            "describe_schedule",
            {"target_ref": "orders-prod", "schedule_id": "nightly"},
        ),
        (
            "query_workflow",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
                "query_name": "blocked_on",
            },
        ),
        ("list_approved_workflow_queries", {"target_ref": "orders-prod"}),
        ("list_batch_operations", {"target_ref": "orders-prod"}),
    )

    for tool_name, arguments in calls:
        with pytest.raises(ToolError) as exc_info:
            await server.call_tool(tool_name, arguments)
        payload = _error_payload(exc_info.value)
        assert payload["code"] == "target_not_authorized"

    expected_services = [
        {
            "search_workflow_runs": "filter_workflows",
            "inspect_workflow_task_failures": "find_workflow_task_failures",
            "inspect_activity_queue_dependencies": "find_stuck_workflows",
        }.get(name, name)
        for name, _ in calls
    ]
    assert [call[0] for call in services.calls] == expected_services
    assert len({call[1].request_id for call in services.calls}) == len(calls)
    assert all(call[1].principal.subject == "demo-user" for call in services.calls)


async def test_adapter_returns_structured_contract_response() -> None:
    root, _ = _root(_RecordingServices(successful_resolve=True))
    server = create_mcp(root)

    _, structured = await server.call_tool("resolve_target", {"human_name": "orders prod"})

    assert structured["schema_version"] == "1"
    assert structured["answer"]["namespace"] == "default"


async def test_adapter_maps_request_validation_without_calling_service() -> None:
    services = _RecordingServices(successful_resolve=True)
    root, _ = _root(services)
    server = create_mcp(root)

    with pytest.raises(ToolError) as exc_info:
        await server.call_tool("resolve_target", {"human_name": ""})

    payload = _error_payload(exc_info.value)
    assert payload["code"] == "invalid_argument"
    assert services.calls == []


async def test_enum_validation_reports_allowed_values() -> None:
    services = _RecordingServices(successful_resolve=True)
    root, _ = _root(services)
    server = create_mcp(root)

    with pytest.raises(ToolError) as exc_info:
        await server.call_tool(
            "describe_task_queue",
            {"target_ref": "orders-prod", "task_queue": "ledger", "task_type": "wrong"},
        )

    message = str(exc_info.value)
    assert "'workflow' or 'activity'" in message
    assert services.calls == []


async def test_mcp_lifespan_shuts_down_process_scoped_provider() -> None:
    root, provider = _root(_RecordingServices())
    server = create_mcp(root)
    lifespan = cast(
        Any,
        server.settings.lifespan,
    )
    manager = cast(AbstractAsyncContextManager[CompositionRoot], lifespan(server))

    async with manager as yielded:
        assert yielded is root

    assert provider.shutdown_calls == 1


async def test_default_query_allowlist_denies_without_temporal_io() -> None:
    root = build_composition_root()
    server = create_mcp(root)

    with pytest.raises(ToolError) as exc_info:
        await server.call_tool(
            "query_workflow",
            {
                "target_ref": "orders-prod",
                "workflow_id": "order-1",
                "run_id": "run-1",
                "query_name": "blocked_on",
            },
        )

    assert _error_payload(exc_info.value)["code"] == "query_not_approved"
    await root.shutdown()


async def test_query_allowlist_loads_from_server_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OPS_MCP_QUERY_APPROVALS_JSON",
        json.dumps({"orders-prod": ["blocked_on", "progress"]}),
    )
    root = build_composition_root()

    policy = cast(Any, root.services.namespace_reads)._query_policy
    assert policy.is_approved("orders-prod", "blocked_on")
    assert not policy.is_approved("orders-prod", "unapproved")
    await root.shutdown()


@pytest.mark.parametrize(
    "serialized",
    (
        "not-json",
        "[]",
        '{"orders-prod":"blocked_on"}',
        '{"orders-prod":[""]}',
        '{"orders-prod":["blocked_on","blocked_on"]}',
    ),
)
def test_invalid_query_allowlist_stops_startup(
    monkeypatch: pytest.MonkeyPatch,
    serialized: str,
) -> None:
    monkeypatch.setenv("OPS_MCP_QUERY_APPROVALS_JSON", serialized)

    with pytest.raises(ValueError, match="query|OPS_MCP_QUERY_APPROVALS_JSON"):
        build_composition_root()


async def test_explicit_query_allowlist_overrides_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPS_MCP_QUERY_APPROVALS_JSON", "not-json")
    root = build_composition_root(query_approvals={"orders-prod": ["explicit_query"]})

    policy = cast(Any, root.services.namespace_reads)._query_policy
    assert policy.is_approved("orders-prod", "explicit_query")
    assert not policy.is_approved("orders-prod", "blocked_on")
    await root.shutdown()


def test_generalized_entrypoint_is_default_and_session_one_remains_explicit() -> None:
    project = Path("pyproject.toml").read_text(encoding="utf-8")

    assert 'ops-mcp = "ops_mcp.mcp:main"' in project
    assert 'ops-mcp-session-one = "ops_mcp.server:main"' in project
    assert 'ops-mcp-generalized = "ops_mcp.mcp:main"' in project
    assert 'ops-mcp-direct-reads = "ops_mcp.mcp:main"' in project
    assert 'ops-mcp-poc = "ops_mcp.mcp:main"' in project


def test_invalid_transport_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    from ops_mcp.mcp import app

    monkeypatch.setenv("OPS_MCP_TRANSPORT", "websocket")
    with pytest.raises(SystemExit, match="OPS_MCP_TRANSPORT"):
        app.main()


def _error_payload(error: ToolError) -> dict[str, object]:
    # FastMCP's in-process helper prefixes ToolError messages with the tool name. The wire error
    # content retains the JSON object after this framework-owned prefix.
    _, separator, payload = str(error).partition(": ")
    assert separator
    return cast(dict[str, object], json.loads(payload))

"""FastMCP application for the generalized read-only tool surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal, cast

from mcp.server.fastmcp import FastMCP

from ops_mcp.mcp.adapters import register_direct_read_tools
from ops_mcp.mcp.composition import CompositionRoot, build_composition_root
from ops_mcp.runtime import InvalidConfigurationError


def create_mcp(root: CompositionRoot | None = None) -> FastMCP[CompositionRoot]:
    application = root or build_composition_root()

    @asynccontextmanager
    async def lifespan(_: FastMCP[CompositionRoot]) -> AsyncIterator[CompositionRoot]:
        try:
            yield application
        finally:
            await application.shutdown()

    server = FastMCP[CompositionRoot](
        "Temporal Operations",
        instructions=(
            "Routing tiers: first resolve a configured target once and reuse target_ref; never "
            "infer a target from workflow IDs, task queues, namespaces, or query text. Tier 1: "
            "use inspect_workflow_execution for one exact workflow and stop unless the operator "
            "asks for blast radius or its response requires population evidence. Tier 2: use "
            "inspect_workflow_task_failures, inspect_activity_queue_dependencies, or "
            "analyze_incident_impact for explicit population questions; follow continuation_token "
            "and distinguish page from cumulative totals. Tier 3: use building-block reads only "
            "for novel questions, enrichment, or a reported evidence gap. Use "
            "assess_workflow_recovery to gather exact later-run activity outcomes. Use "
            "plan_workflow_reset only after sufficient recovery evidence and only when the "
            "operator asks for a reset plan. All tools are read-only."
            " Do not abandon a suitable composite merely because building blocks could reproduce "
            "it. Do not broaden an exact-run question into population analysis without operator "
            "intent or a stated evidence requirement."
        ),
        stateless_http=True,
        json_response=True,
        lifespan=lifespan,
    )
    register_direct_read_tools(server, application)
    _enrich_input_schemas(server)
    return server


_ARGUMENT_EXAMPLES: dict[str, object] = {
    "target_ref": "orders-prod",
    "human_name": "orders prod",
    "probe_codec": False,
    "query": "WorkflowType = 'OrderWorkflow'",
    "candidate_query": "WorkflowType = 'OrderWorkflow'",
    "comparison_query": "WorkflowType = 'OrderWorkflow' AND StartTime > '2026-08-24T15:00:00Z'",
    "failed_workflow_id": "order-88231",
    "failed_run_id": "01a0268c-ba90-7f27-ac62-754c4a5c892c",
    "activity_type": "EnrichProduct",
    "fields": ["WorkflowId", "RunId", "ExecutionStatus"],
    "custom_search_attributes": ["Team"],
    "group_by_custom_search_attributes": ["Team"],
    "business_grouping": ["Team", "Domain"],
    "page_size": 100,
    "continuation_token": "signed-opaque-token",
    "workflow_id": "order-88231",
    "run_id": "01a0268c-ba90-7f27-ac62-754c4a5c892c",
    "failures_only": True,
    "event_id_range": {"first": 10, "last": 30},
    "first": 10,
    "last": 30,
    "last_n": 50,
    "event_types": ["EVENT_TYPE_WORKFLOW_TASK_FAILED"],
    "limit": 100,
    "wait": False,
    "wait_timeout_seconds": 5,
    "task_queue": "ledger",
    "task_type": "activity",
    "expected_duration_seconds": 600,
    "minimum_open_duration_seconds": 600,
    "history_event_limit": 2000,
    "include_diagnostic_query": True,
    "operator_rationale": "Recover after the catalog dependency became healthy",
    "started_at": "2026-08-24T14:20:00Z",
    "ended_at": "2026-08-24T15:00:00Z",
    "started_at_or_after": "2026-08-24T14:20:00Z",
    "started_before": "2026-08-24T15:00:00Z",
    "time_window": {
        "started_at_or_after": "2026-08-24T14:20:00Z",
        "started_before": "2026-08-24T15:00:00Z",
    },
    "incident_window": {
        "started_at": "2026-08-24T14:20:00Z",
        "ended_at": "2026-08-24T15:00:00Z",
    },
    "failure_class": "nondeterminism",
    "activity_task_queue": "ledger",
    "activity_task_queues": ["ledger"],
    "affected_activity_task_queues": ["ledger"],
    "max_candidates": 2000,
    "max_visibility_pages": 10,
    "max_runs": 100,
    "max_histories": 100,
    "max_history_events_per_run": 500,
    "max_task_queues": 20,
    "max_concurrency": 10,
    "limits": {
        "max_candidates": 2000,
        "max_runs": 100,
        "max_histories": 100,
        "max_history_events_per_run": 500,
        "max_visibility_pages": 10,
        "max_task_queues": 20,
        "max_concurrency": 10,
    },
    "sampling_strategy": "deterministic_hash",
    "sample_seed": "server-generated-seed",
    "group_by": "status",
    "name_contains": "nightly",
    "shape": "cron",
    "paused": False,
    "schedule_id": "nightly-reconciliation",
    "upcoming_count": 5,
    "upcoming_start": "2026-08-25T00:00:00Z",
    "upcoming_end": "2026-09-01T00:00:00Z",
    "query_name": "blocked_on",
    "state": "running",
    "job_type": "reset",
    "include_details": True,
}


def _enrich_input_schemas(server: FastMCP[CompositionRoot]) -> None:
    """Guarantee usable descriptions and examples for every public argument.

    FastMCP stores the generated JSON Schema on each registered Tool. Keeping
    this final normalization in one place also covers migration aliases while
    their deliberately old Python signatures remain available.
    """

    for tool in server._tool_manager._tools.values():  # noqa: SLF001
        _enrich_schema_node(tool.parameters)


def _enrich_schema_node(node: object) -> None:
    if not isinstance(node, dict):
        return
    for name, schema in node.get("properties", {}).items():
        schema.setdefault(
            "description",
            f"Input `{name}`; nested execution controls override the documented server default.",
        )
        example = _ARGUMENT_EXAMPLES.get(name)
        if example is not None:
            schema.setdefault("examples", [example])
        _enrich_schema_node(schema)
    for schema in node.get("$defs", {}).values():
        _enrich_schema_node(schema)
    for keyword in ("anyOf", "oneOf", "allOf"):
        for schema in node.get(keyword, ()):
            _enrich_schema_node(schema)


def main() -> None:
    transport = os.getenv("OPS_MCP_TRANSPORT", "stdio")
    if transport not in {"stdio", "streamable-http"}:
        raise SystemExit("OPS_MCP_TRANSPORT must be 'stdio' or 'streamable-http'")
    if transport == "streamable-http" and os.getenv("OPS_MCP_ALLOW_INSECURE_HTTP") != "1":
        raise SystemExit(
            "streamable-http is unauthenticated in this prototype; set "
            "OPS_MCP_ALLOW_INSECURE_HTTP=1 only for a loopback-only demo"
        )
    continuation_secret = os.getenv("OPS_MCP_CONTINUATION_SECRET")
    if transport == "streamable-http" and (
        continuation_secret is None or len(continuation_secret.encode()) < 32
    ):
        raise SystemExit(
            "OPS_MCP_CONTINUATION_SECRET must contain at least 32 bytes for stable "
            "streamable-http continuations"
        )
    try:
        server = create_mcp()
    except (InvalidConfigurationError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    server.run(transport=cast(Literal["stdio", "streamable-http"], transport))


if __name__ == "__main__":
    main()

from __future__ import annotations

import os
from functools import lru_cache
from typing import Annotated, Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from ops_mcp.config import (
    AuthorizationError,
    ConfigurationError,
    TargetRegistry,
    default_config_path,
    demo_principal,
)
from ops_mcp.temporal_gateway import TemporalGateway

READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)

mcp = FastMCP(
    "Temporal Operations",
    instructions=(
        "Read-only Temporal operations tools. Resolve a human target first, then pass its "
        "target_ref to other tools. Reset plans are analysis only and are never executed."
    ),
    stateless_http=True,
    json_response=True,
)

gateway = TemporalGateway()


@lru_cache(maxsize=1)
def registry() -> TargetRegistry:
    return TargetRegistry.load(default_config_path())


def _target(target_ref: str):
    return registry().authorize(demo_principal(), target_ref)


def _tool_error(exc: Exception) -> ToolError:
    return ToolError(str(exc))


@mcp.tool(annotations=READ_ONLY, structured_output=True)
async def resolve_target(human_name: str) -> dict[str, Any]:
    """Resolve a human environment name to a caller-authorized Temporal target.

    The response deliberately omits network addresses, TLS material, and credentials.
    Use target_ref in all subsequent Temporal tools.
    """
    try:
        target = registry().resolve(demo_principal(), human_name)
        return {
            "target_ref": target.id,
            "account": target.account,
            "cluster": target.cluster,
            "namespace": target.namespace,
            "resolved_from": human_name,
            "caller_group": demo_principal().groups[0],
        }
    except (AuthorizationError, ConfigurationError) as exc:
        raise _tool_error(exc) from exc


@mcp.tool(annotations=READ_ONLY, structured_output=True)
async def filter_workflows(
    target_ref: str,
    query: str = "",
    fields: list[str] | None = None,
    page_size: Annotated[int, Field(ge=1, le=200)] = 50,
    page_token: str | None = None,
) -> dict[str, Any]:
    """Find workflow runs using a Temporal visibility query.

    Only requested fields are returned, with a bounded page size and an opaque page token.
    Supported fields: workflow_id, run_id, workflow_type, status, task_queue, start_time,
    close_time, history_length, and parent_workflow_id.
    """
    selected_fields = fields or [
        "workflow_id",
        "run_id",
        "workflow_type",
        "status",
        "start_time",
        "close_time",
    ]
    try:
        return await gateway.filter_workflows(
            _target(target_ref), query, selected_fields, page_size, page_token
        )
    except Exception as exc:
        raise _tool_error(exc) from exc


@mcp.tool(annotations=READ_ONLY, structured_output=True)
async def count_workflows(
    target_ref: str,
    query: str = "",
    group_by: Literal["status", "workflow_type"] | None = None,
) -> dict[str, Any]:
    """Count workflow runs matching a visibility query, optionally grouped."""
    try:
        return await gateway.count_workflows(_target(target_ref), query, group_by)
    except Exception as exc:
        raise _tool_error(exc) from exc


@mcp.tool(annotations=READ_ONLY, structured_output=True)
async def diagnose_workflow(
    target_ref: str,
    workflow_id: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Diagnose one run from its state, failures, retries, and worker health.

    If run_id is omitted Temporal resolves the current run. History inspection is capped and
    the response reports the inspected size and whether it was truncated.
    """
    try:
        return await gateway.diagnose_workflow(_target(target_ref), workflow_id, run_id)
    except Exception as exc:
        raise _tool_error(exc) from exc


@mcp.tool(annotations=READ_ONLY, structured_output=True)
async def plan_workflow_reset(
    target_ref: str,
    workflow_id: str,
    run_id: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Create, but never execute, a time-limited reset plan for one exact run.

    Returns valid workflow-task reset points, a recommendation, replay impact, signals that
    would be reapplied, and a reviewable Temporal CLI command. Requires an explicit run_id.
    """
    try:
        return await gateway.plan_workflow_reset(_target(target_ref), workflow_id, run_id, reason)
    except Exception as exc:
        raise _tool_error(exc) from exc


def main() -> None:
    transport = os.getenv("OPS_MCP_TRANSPORT", "stdio")
    if transport not in {"stdio", "streamable-http"}:
        raise SystemExit("OPS_MCP_TRANSPORT must be 'stdio' or 'streamable-http'")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()

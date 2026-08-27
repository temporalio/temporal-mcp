from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from ops_mcp.config import TargetRegistry, default_config_path
from ops_mcp.models import Principal
from ops_mcp.temporal_gateway import TemporalGateway


def show(tool_name: str, result: dict[str, Any]) -> None:
    print(f"\n## {tool_name}")
    print(json.dumps(result, indent=2, sort_keys=True))


async def run() -> None:
    registry = TargetRegistry.load(default_config_path())
    principal = Principal(subject="demo", groups=("demo-support",))
    target = registry.resolve(principal, "orders prod")
    show(
        "resolve_target",
        {
            "target_ref": target.id,
            "account": target.account,
            "cluster": target.cluster,
            "namespace": target.namespace,
        },
    )

    gateway = TemporalGateway()
    filtered = await gateway.filter_workflows(
        target,
        "OrderId = '88231'",
        ["workflow_id", "run_id", "status", "start_time", "close_time"],
        10,
        None,
    )
    show("filter_workflows", filtered)
    if filtered["returned"] != 1:
        raise RuntimeError("Expected exactly one seeded OrderId 88231 run")
    workflow = filtered["workflows"][0]

    diagnosis = await gateway.diagnose_workflow(target, workflow["workflow_id"], workflow["run_id"])
    show("diagnose_workflow", diagnosis)

    ten_minutes_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    count = await gateway.count_workflows(
        target,
        (
            "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Completed' "
            f"AND CloseTime > '{ten_minutes_ago}'"
        ),
        None,
    )
    show("count_workflows", count)

    plan = await gateway.plan_workflow_reset(
        target, workflow["workflow_id"], workflow["run_id"], None
    )
    show("plan_workflow_reset", plan)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()

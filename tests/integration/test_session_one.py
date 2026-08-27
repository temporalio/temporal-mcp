import os

import pytest

from ops_mcp.config import TargetRegistry
from ops_mcp.models import Principal
from ops_mcp.temporal_gateway import TemporalGateway

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_INTEGRATION") != "1",
    reason="set OPS_MCP_INTEGRATION=1 with the demo server, worker, and seed running",
)


@pytest.mark.asyncio
async def test_session_one_end_to_end() -> None:
    registry = TargetRegistry.load("config/targets.example.yaml")
    target = registry.resolve(Principal(subject="demo", groups=("demo-support",)), "orders prod")
    gateway = TemporalGateway()

    filtered = await gateway.filter_workflows(
        target,
        "OrderId = '88231'",
        ["workflow_id", "run_id", "status"],
        10,
        None,
    )
    assert filtered["returned"] == 1
    failed = filtered["workflows"][0]
    assert failed["workflow_id"] == "order-88231"
    assert failed["status"] == "FAILED"

    diagnosis = await gateway.diagnose_workflow(target, failed["workflow_id"], failed["run_id"])
    assert diagnosis["worker_health"]["pollers_healthy"] is True
    assert diagnosis["history_inspection"]["truncated"] is False
    assert any(
        failure["activity_type"] == "enrich_product"
        and failure["attempt"] == 3
        and "503 Service Unavailable" in failure["message"]
        for failure in diagnosis["failures"]
    )

    count = await gateway.count_workflows(
        target,
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Completed'",
        None,
    )
    assert count["count"] >= 12

    plan = await gateway.plan_workflow_reset(target, failed["workflow_id"], failed["run_id"], None)
    assert plan["read_only"] is True
    assert plan["recommended_event_id"] > 0
    assert "authorize_payment" in plan["already_completed_before_reset"]
    assert "enrich_product" in plan["activities_that_would_replay"]
    assert "temporal workflow reset" in plan["command"]

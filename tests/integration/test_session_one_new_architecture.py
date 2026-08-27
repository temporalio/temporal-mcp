"""Live Session 1 acceptance through the composed typed services.

The Temporal dev server, demo worker, and seeded workflows are intentionally
independent processes. This test only connects to them when explicitly enabled.
"""

from __future__ import annotations

import os

import pytest

from ops_mcp.contracts import (
    CountWorkflowsRequest,
    DiagnoseWorkflowRequest,
    FilterWorkflowsRequest,
    PlanWorkflowResetRequest,
    ResolveTargetRequest,
    WorkflowField,
)
from ops_mcp.contracts.primitives import Completeness
from ops_mcp.mcp import build_composition_root

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_INTEGRATION") != "1",
    reason=(
        "set OPS_MCP_INTEGRATION=1 after starting the dev server, demo worker, "
        "and demo seed as separate processes"
    ),
)


@pytest.mark.asyncio
async def test_session_one_through_live_composed_services() -> None:
    root = build_composition_root("config/targets.example.yaml")
    try:
        resolved = root.services.targets.resolve_target(
            root.contexts.create(), ResolveTargetRequest(human_name="orders prod")
        )
        assert resolved.target_ref == "orders-prod"
        assert "address" not in resolved.model_dump_json()

        filtered = await root.services.visibility.filter_workflows(
            root.contexts.create(),
            FilterWorkflowsRequest(
                target_ref=resolved.target_ref,
                query="OrderId = '88231'",
                fields=(
                    WorkflowField.WORKFLOW_ID,
                    WorkflowField.RUN_ID,
                    WorkflowField.STATUS,
                ),
                page_size=10,
            ),
        )
        assert filtered.answer.returned == 1
        failed = filtered.answer.workflows[0]
        assert failed.workflow_id == "order-88231"
        assert failed.run_id
        assert failed.status == "FAILED"

        diagnosis = await root.services.diagnose_workflow.diagnose_workflow(
            root.contexts.create(),
            DiagnoseWorkflowRequest(
                target_ref=resolved.target_ref,
                workflow_id=failed.workflow_id,
                run_id=failed.run_id,
            ),
        )
        assert diagnosis.history_inspection.completion is Completeness.COMPLETE
        assert diagnosis.retry_assessment.assessment == "unknown"
        assert any(
            finding.code == "activity_retry_exhausted"
            and "503 Service Unavailable" in finding.summary
            for finding in diagnosis.findings
        )
        assert diagnosis.task_queues
        assert all(queue.observed_at is not None for queue in diagnosis.task_queues)
        assert all(queue.historical_health_known is False for queue in diagnosis.task_queues)
        assert any(
            "does not establish health at failure time" in caveat
            for finding in diagnosis.findings
            for caveat in finding.caveats
        )

        count = await root.services.visibility.count_workflows(
            root.contexts.create(),
            CountWorkflowsRequest(
                target_ref=resolved.target_ref,
                query="WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Completed'",
            ),
        )
        assert count.answer.count >= 12
        assert count.answer.precision == "approximate"

        plan = await root.services.plan_workflow_reset.plan_workflow_reset(
            root.contexts.create(),
            PlanWorkflowResetRequest(
                target_ref=resolved.target_ref,
                workflow_id=failed.workflow_id,
                run_id=failed.run_id,
            ),
        )
        assert plan.history_inspection.completion is Completeness.COMPLETE
        assert plan.recommendation is not None
        recommended = next(
            point
            for point in plan.valid_reset_points
            if point.event_id == plan.recommendation.event_id
        )
        assert recommended.event_type == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"
        assert [
            activity.activity_type
            for activity in recommended.replay_impact.completed_activities_preserved
        ] == ["authorize_payment"]
        assert [
            activity.activity_type for activity in recommended.replay_impact.activities_replayed
        ] == ["enrich_product"]
        assert plan.recheck_after > plan.observed_at
        assert plan.command_preview is not None
        assert plan.command_preview.executed is False
        assert "\n" not in plan.command_preview.command
        assert f"--event-id {recommended.event_id}" in plan.command_preview.command
    finally:
        await root.shutdown()

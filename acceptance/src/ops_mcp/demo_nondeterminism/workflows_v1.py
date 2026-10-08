"""Pre-deploy workflow definition that schedules shipping work on one branch."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

from ops_mcp.demo_nondeterminism.ground_truth import ACTIVITY_TASK_QUEUE


@workflow.defn(name="FulfillmentWorkflow")
class FulfillmentWorkflowV1:
    def __init__(self) -> None:
        self._deployment_probes = 0

    @workflow.run
    async def run(self, shipping_branch: bool) -> None:
        if shipping_branch:
            workflow.start_activity(
                "PrepareShippingLabel",
                start_to_close_timeout=timedelta(hours=1),
                task_queue=ACTIVITY_TASK_QUEUE,
            )
        await workflow.wait_condition(lambda: False)

    @workflow.signal(name="deployment_probe")
    async def deployment_probe(self) -> None:
        self._deployment_probes += 1

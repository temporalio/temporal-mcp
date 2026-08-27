"""Post-deploy definition with the unsafe shipping activity removal."""

from __future__ import annotations

from temporalio import workflow


@workflow.defn(name="FulfillmentWorkflow")
class FulfillmentWorkflowV2:
    def __init__(self) -> None:
        self._deployment_probes = 0

    @workflow.run
    async def run(self, shipping_branch: bool) -> None:
        # Intentionally unsafe fixture change: v1 histories on this branch contain an
        # ActivityTaskScheduled command that this code no longer emits.
        if shipping_branch:
            pass
        await workflow.wait_condition(lambda: False)

    @workflow.signal(name="deployment_probe")
    async def deployment_probe(self) -> None:
        self._deployment_probes += 1

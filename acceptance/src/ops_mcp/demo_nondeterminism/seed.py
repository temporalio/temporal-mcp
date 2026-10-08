"""Start v1 histories before the Session 3 deploy boundary."""

from __future__ import annotations

import asyncio

from temporalio.exceptions import WorkflowAlreadyStartedError

from ops_mcp.demo.connection import connect
from ops_mcp.demo_nondeterminism.ground_truth import (
    AFFECTED_ORDINALS,
    EXPOSED_RUNS,
    TASK_QUEUE,
    TOTAL_CANDIDATES,
    workflow_id,
)
from ops_mcp.demo_nondeterminism.workflows_v1 import FulfillmentWorkflowV1


async def seed() -> None:
    client = await connect()
    semaphore = asyncio.Semaphore(50)

    async def start(index: int) -> bool:
        async with semaphore:
            try:
                await client.start_workflow(
                    FulfillmentWorkflowV1.run,
                    index in AFFECTED_ORDINALS,
                    id=workflow_id(index),
                    task_queue=TASK_QUEUE,
                )
            except WorkflowAlreadyStartedError:
                return False
            return True

    created = await asyncio.gather(
        *(asyncio.create_task(start(index)) for index in range(TOTAL_CANDIDATES))
    )
    await asyncio.sleep(5)
    print(
        f"Session 3 v1 fixture: {sum(created)} started, "
        f"{TOTAL_CANDIDATES - sum(created)} already existed",
        flush=True,
    )
    print(
        f"Ground truth after deployment: {EXPOSED_RUNS} exposed; "
        f"{len(AFFECTED_ORDINALS)} should become non-deterministic",
        flush=True,
    )
    print("Stop the v1 worker, start the v2 worker, then run the expose command.", flush=True)


def main() -> None:
    asyncio.run(seed())


if __name__ == "__main__":
    main()

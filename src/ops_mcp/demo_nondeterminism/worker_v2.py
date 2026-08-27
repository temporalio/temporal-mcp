from __future__ import annotations

import asyncio

from temporalio.worker import Worker

from ops_mcp.demo.connection import connect
from ops_mcp.demo_nondeterminism.ground_truth import TASK_QUEUE, V2_BUILD_ID
from ops_mcp.demo_nondeterminism.workflows_v2 import FulfillmentWorkflowV2


async def run_worker() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[FulfillmentWorkflowV2],
        build_id=V2_BUILD_ID,
    )
    print(f"Session 3 v2 worker polling {TASK_QUEUE!r} as {V2_BUILD_ID!r}", flush=True)
    await worker.run()


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass

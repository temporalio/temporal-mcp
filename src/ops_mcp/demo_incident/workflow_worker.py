from __future__ import annotations

import asyncio

from temporalio.worker import Worker

from ops_mcp.demo.connection import connect
from ops_mcp.demo_incident.workflows import WORKFLOWS

WORKFLOW_QUEUE = "session4-incident-workflows"


async def run_worker() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=WORKFLOW_QUEUE,
        workflows=list(WORKFLOWS),
        activities=[],
    )
    print(
        f"Session 4 workflow worker polling {WORKFLOW_QUEUE!r}; activity workers are separate",
        flush=True,
    )
    await worker.run()


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass

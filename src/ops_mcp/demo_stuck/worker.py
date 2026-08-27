"""Workflow-task-only worker for the Session 2 fixture."""

from __future__ import annotations

import asyncio

from temporalio.worker import Worker

from ops_mcp.demo.connection import connect
from ops_mcp.demo_stuck.workflows import TASK_QUEUE, WORKFLOWS


async def run_worker() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=list(WORKFLOWS),
        activities=[],
    )
    print(
        f"Session 2 workflow worker polling {TASK_QUEUE!r}; no activity worker is running",
        flush=True,
    )
    await worker.run()


def main() -> None:
    asyncio.run(run_worker())

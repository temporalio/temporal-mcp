from __future__ import annotations

import asyncio

from temporalio.worker import Worker

from ops_mcp.demo.connection import connect
from ops_mcp.demo_incident.activities import process_ledger_entry
from ops_mcp.demo_incident.ground_truth import CORE_QUEUE, PAYMENTS_QUEUE


async def run_workers() -> None:
    client = await connect()
    workers = tuple(
        Worker(
            client,
            task_queue=queue,
            workflows=[],
            activities=[process_ledger_entry],
            max_concurrent_activities=20,
        )
        for queue in (PAYMENTS_QUEUE, CORE_QUEUE)
    )
    print(
        f"Session 4 recovery workers polling {PAYMENTS_QUEUE!r} and {CORE_QUEUE!r}",
        flush=True,
    )
    await asyncio.gather(*(worker.run() for worker in workers))


def main() -> None:
    try:
        asyncio.run(run_workers())
    except KeyboardInterrupt:
        pass

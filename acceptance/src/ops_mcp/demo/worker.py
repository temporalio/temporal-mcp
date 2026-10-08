from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from ops_mcp.demo.activities import authorize_payment, enrich_product
from ops_mcp.demo.connection import connect
from ops_mcp.demo.workflows import OrderWorkflow

TASK_QUEUE = "orders-demo"


async def run() -> None:
    client = await connect()
    with ThreadPoolExecutor(max_workers=8) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[OrderWorkflow],
            activities=[authorize_payment, enrich_product],
            activity_executor=executor,
        )
        print(f"Session 1 worker polling task queue {TASK_QUEUE!r}", flush=True)
        await worker.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

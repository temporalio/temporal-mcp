from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from temporalio.client import WorkflowFailureError
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

from ops_mcp.demo.connection import connect
from ops_mcp.demo.models import OrderInput
from ops_mcp.demo.worker import TASK_QUEUE
from ops_mcp.demo.workflows import OrderWorkflow

ORDER_ID = SearchAttributeKey.for_keyword("OrderId")


async def seed() -> None:
    client = await connect()
    suffix = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    failed = await client.start_workflow(
        OrderWorkflow.run,
        OrderInput(order_id="88231", catalog_should_fail=True),
        id="order-88231",
        task_queue=TASK_QUEUE,
        search_attributes=TypedSearchAttributes([SearchAttributePair(ORDER_ID, "88231")]),
    )
    print(f"Started failed-order fixture: workflow_id={failed.id} run_id={failed.result_run_id}")
    try:
        await failed.result()
    except WorkflowFailureError:
        print("Order 88231 failed as expected after three catalog attempts")

    handles = []
    for index in range(12):
        order_id = f"recovered-{suffix}-{index + 1:02d}"
        handles.append(
            await client.start_workflow(
                OrderWorkflow.run,
                OrderInput(order_id=order_id),
                id=f"order-{order_id}",
                task_queue=TASK_QUEUE,
                search_attributes=TypedSearchAttributes([SearchAttributePair(ORDER_ID, order_id)]),
            )
        )
    await asyncio.gather(*(handle.result() for handle in handles))
    print("Started and completed 12 recovered orders")
    print("Use visibility query: OrderId = '88231'")


def main() -> None:
    asyncio.run(seed())


if __name__ == "__main__":
    main()

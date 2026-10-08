from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ops_mcp.demo.activities import authorize_payment, enrich_product
    from ops_mcp.demo.models import OrderInput


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order: OrderInput) -> dict[str, str]:
        payment = await workflow.execute_activity(
            authorize_payment,
            order,
            start_to_close_timeout=timedelta(seconds=10),
        )
        enrichment = await workflow.execute_activity(
            enrich_product,
            order,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=order.retry_interval_seconds),
                backoff_coefficient=1.0,
                maximum_attempts=3,
            ),
        )
        return {"payment": payment, "enrichment": enrichment}

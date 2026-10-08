from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from ops_mcp.demo_incident.ground_truth import CORE_QUEUE, PAYMENTS_QUEUE
from ops_mcp.demo_incident.models import IncidentWorkItem


class _LedgerWorkflowMixin:
    async def _run_ledger_work(self, item: IncidentWorkItem) -> dict[str, object]:
        queue = PAYMENTS_QUEUE if item.domain == "payments" else CORE_QUEUE
        return await workflow.execute_activity(
            "ProcessLedgerEntry",
            item,
            task_queue=queue,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=1,
                maximum_interval=timedelta(seconds=1),
                maximum_attempts=25,
            ),
        )


@workflow.defn
class SettlementWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


@workflow.defn
class RefundWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


@workflow.defn
class PayoutWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


@workflow.defn
class LedgerIngestionWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


@workflow.defn
class BalanceSnapshotWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


@workflow.defn
class AuditExportWorkflow(_LedgerWorkflowMixin):
    @workflow.run
    async def run(self, item: IncidentWorkItem) -> dict[str, object]:
        return await self._run_ledger_work(item)


WORKFLOWS = (
    SettlementWorkflow,
    RefundWorkflow,
    PayoutWorkflow,
    LedgerIngestionWorkflow,
    BalanceSnapshotWorkflow,
    AuditExportWorkflow,
)

WORKFLOW_BY_NAME = {value.__name__: value for value in WORKFLOWS}

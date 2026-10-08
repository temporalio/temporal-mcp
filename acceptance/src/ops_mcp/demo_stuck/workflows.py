"""Workflow-only Session 2 fixtures that intentionally build an activity backlog."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

TASK_QUEUE = "ledger"
QUERY_NAME = "__stack_trace"
ACTIVITY_NAME = "FetchLedgerBatch"


class _BlockedOnLedgerMixin:
    async def _run_blocked(self, activity_count: int) -> None:
        handles = [
            workflow.start_activity(
                ACTIVITY_NAME,
                start_to_close_timeout=timedelta(hours=12),
                task_queue=TASK_QUEUE,
            )
            for _ in range(activity_count)
        ]
        await asyncio.gather(*handles)


@workflow.defn
class PaymentReconciliationWorkflow(_BlockedOnLedgerMixin):
    @workflow.run
    async def run(self, activity_count: int) -> None:
        await self._run_blocked(activity_count)


@workflow.defn
class PaymentSettlementWorkflow(_BlockedOnLedgerMixin):
    @workflow.run
    async def run(self, activity_count: int) -> None:
        await self._run_blocked(activity_count)


@workflow.defn
class LedgerCompactionWorkflow(_BlockedOnLedgerMixin):
    @workflow.run
    async def run(self, activity_count: int) -> None:
        await self._run_blocked(activity_count)


@workflow.defn
class BalanceSnapshotWorkflow(_BlockedOnLedgerMixin):
    @workflow.run
    async def run(self, activity_count: int) -> None:
        await self._run_blocked(activity_count)


WORKFLOWS = (
    PaymentReconciliationWorkflow,
    PaymentSettlementWorkflow,
    LedgerCompactionWorkflow,
    BalanceSnapshotWorkflow,
)

"""Seed the exact Session 2 workflow population and activity backlog."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from temporalio.exceptions import WorkflowAlreadyStartedError

from ops_mcp.demo.connection import connect
from ops_mcp.demo_stuck.workflows import (
    TASK_QUEUE,
    BalanceSnapshotWorkflow,
    LedgerCompactionWorkflow,
    PaymentReconciliationWorkflow,
    PaymentSettlementWorkflow,
)

MAIN_WORKFLOW_ID = "session2-payment-reconciliation-main"
TOTAL_WORKFLOWS = 1_830
TOTAL_ACTIVITIES = 4_200
EXTRA_ACTIVITY_WORKFLOWS = TOTAL_ACTIVITIES - (TOTAL_WORKFLOWS * 2)


@dataclass(frozen=True)
class Population:
    workflow: Any
    prefix: str
    count: int


POPULATIONS: Sequence[Population] = (
    Population(PaymentReconciliationWorkflow, "payment-reconciliation", 800),
    Population(PaymentSettlementWorkflow, "payment-settlement", 500),
    Population(LedgerCompactionWorkflow, "ledger-compaction", 300),
    Population(BalanceSnapshotWorkflow, "balance-snapshot", 230),
)


async def seed() -> None:
    client = await connect()
    semaphore = asyncio.Semaphore(100)
    started = 0
    existing = 0
    ordinal = 0

    async def start(workflow: Any, workflow_id: str, activity_count: int) -> bool:
        async with semaphore:
            try:
                await client.start_workflow(
                    workflow.run,
                    activity_count,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
            except WorkflowAlreadyStartedError:
                return False
            return True

    pending: list[asyncio.Task[bool]] = []
    for population in POPULATIONS:
        for index in range(population.count):
            workflow_id = (
                MAIN_WORKFLOW_ID
                if ordinal == 0
                else f"session2-{population.prefix}-{index + 1:04d}"
            )
            activity_count = 3 if ordinal < EXTRA_ACTIVITY_WORKFLOWS else 2
            pending.append(
                asyncio.create_task(start(population.workflow, workflow_id, activity_count))
            )
            ordinal += 1

    for created in await asyncio.gather(*pending):
        if created:
            started += 1
        else:
            existing += 1

    print(f"Session 2 fixture: {started} started, {existing} already existed", flush=True)
    print(f"Main workflow ID: {MAIN_WORKFLOW_ID}", flush=True)
    print(
        f"Expected running population: {TOTAL_WORKFLOWS}; expected activity backlog: "
        f"{TOTAL_ACTIVITIES}",
        flush=True,
    )


def main() -> None:
    asyncio.run(seed())


if __name__ == "__main__":
    main()

"""Create the post-deploy replay boundary for the exposed Session 3 cohort."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from ops_mcp.demo.connection import connect
from ops_mcp.demo_nondeterminism.ground_truth import (
    EXPOSED_RUNS,
    V2_BUILD_ID,
    operator_prompt,
    workflow_id,
)


async def expose() -> None:
    client = await connect()
    deploy_time = datetime.now(UTC).replace(microsecond=0)
    semaphore = asyncio.Semaphore(50)

    async def signal(index: int) -> None:
        async with semaphore:
            await client.get_workflow_handle(workflow_id(index)).signal("deployment_probe")

    await asyncio.gather(*(asyncio.create_task(signal(index)) for index in range(EXPOSED_RUNS)))
    await asyncio.sleep(5)
    deploy_value = deploy_time.isoformat().replace("+00:00", "Z")
    print(f"Signaled {EXPOSED_RUNS} runs to post-deploy build {V2_BUILD_ID}.", flush=True)
    print("Canonical customer question:", flush=True)
    print(operator_prompt(deploy_value), flush=True)
    print(
        "Ground truth is intentionally not indexed: 43 exposed runs took the changed branch. "
        "The agent must inspect histories to find them.",
        flush=True,
    )
    print(
        "Important: BuildIds records successful workflow-task processing. The 43 failed v2 "
        "replays retain fulfillment-v2 on WorkflowTaskStarted history, but are not members of "
        "the BuildIds=v2 visibility result.",
        flush=True,
    )


def main() -> None:
    asyncio.run(expose())


if __name__ == "__main__":
    main()

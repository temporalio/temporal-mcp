"""Wait until Session 4 recovery evidence has settled before starting IAI."""

from __future__ import annotations

import asyncio

from ops_mcp.demo.connection import connect
from ops_mcp.demo_incident.ground_truth import POPULATIONS, TOTAL_RUNS


async def wait_until_ready() -> None:
    client = await connect()
    results = []
    for population in POPULATIONS:
        for index in range(population.count):
            workflow_id = f"session4-{population.id_prefix}-{index + 1:04d}"
            results.append(client.get_workflow_handle(workflow_id).result())
    await asyncio.wait_for(asyncio.gather(*results, return_exceptions=True), timeout=300)
    print(
        f"Session 4 readiness barrier: all {TOTAL_RUNS} workflows reached a close event",
        flush=True,
    )


def main() -> None:
    asyncio.run(wait_until_ready())


if __name__ == "__main__":
    main()

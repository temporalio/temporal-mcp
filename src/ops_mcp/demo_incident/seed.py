"""Administrative demo seeder, not part of the read-only MCP runtime.

This setup command uses OperatorService only to create demo Search Attributes before evaluation.
Runtime composition never imports or invokes it.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import (
    AddSearchAttributesRequest,
    ListSearchAttributesRequest,
)
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes
from temporalio.exceptions import WorkflowAlreadyStartedError

from ops_mcp.demo.connection import connect
from ops_mcp.demo_incident.ground_truth import (
    EXPECTED_DURATION_SECONDS,
    PAYMENT_RUNS,
    POPULATIONS,
    TEAMS,
    TOTAL_RUNS,
    operator_prompt,
)
from ops_mcp.demo_incident.models import IncidentWorkItem
from ops_mcp.demo_incident.workflow_worker import WORKFLOW_QUEUE
from ops_mcp.demo_incident.workflows import WORKFLOW_BY_NAME


async def _ensure_search_attributes(client) -> None:
    response = await client.operator_service.list_search_attributes(
        ListSearchAttributesRequest(namespace=client.namespace)
    )
    existing = {*response.system_attributes, *response.custom_attributes}
    missing = {
        name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
        for name in ("Team", "Domain")
        if name not in existing
    }
    if missing:
        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(
                namespace=client.namespace,
                search_attributes=missing,
            )
        )


async def seed() -> None:
    client = await connect()
    await _ensure_search_attributes(client)
    incident_start = datetime.now(UTC) - timedelta(minutes=40)
    semaphore = asyncio.Semaphore(50)
    ordinal = 0

    async def start(
        workflow_type: str,
        workflow_id: str,
        team: str,
        domain: str,
        mode: str,
    ) -> bool:
        async with semaphore:
            try:
                await client.start_workflow(
                    WORKFLOW_BY_NAME[workflow_type].run,
                    IncidentWorkItem(team=team, domain=domain, mode=mode),
                    id=workflow_id,
                    task_queue=WORKFLOW_QUEUE,
                    search_attributes=TypedSearchAttributes(
                        [
                            SearchAttributePair(SearchAttributeKey.for_keyword("Team"), team),
                            SearchAttributePair(SearchAttributeKey.for_keyword("Domain"), domain),
                        ]
                    ),
                )
            except WorkflowAlreadyStartedError:
                return False
            return True

    pending: list[asyncio.Task[bool]] = []
    for population in POPULATIONS:
        for index in range(population.count):
            mode = "terminal" if ordinal % 16 == 0 else "recover"
            pending.append(
                asyncio.create_task(
                    start(
                        population.workflow_type,
                        f"session4-{population.id_prefix}-{index + 1:04d}",
                        population.team,
                        population.domain,
                        mode,
                    )
                )
            )
            ordinal += 1

    created = await asyncio.gather(*pending)
    await asyncio.sleep(EXPECTED_DURATION_SECONDS + 1)
    incident_end = datetime.now(UTC)
    start_value = incident_start.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_value = incident_end.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    print(
        f"Session 4 fixture: {sum(created)} started, {TOTAL_RUNS - sum(created)} already existed",
        flush=True,
    )
    print("Canonical customer question:", flush=True)
    print(operator_prompt(start_value, end_value), flush=True)
    print(
        f"Ground truth: {TOTAL_RUNS} incident-window candidates across {len(TEAMS)} teams; "
        f"{PAYMENT_RUNS} have Domain=payments.",
        flush=True,
    )
    print(
        "No activity worker ran before incident_end. Start the incident activity-worker module "
        "to model recovery; current pollers then become healthy while histories retain the delay.",
        flush=True,
    )


def main() -> None:
    asyncio.run(seed())


if __name__ == "__main__":
    main()

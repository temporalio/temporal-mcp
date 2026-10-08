from __future__ import annotations

import asyncio

from temporalio import activity
from temporalio.exceptions import ApplicationError

from ops_mcp.demo_incident.ground_truth import RECOVERY_FAILURE_ATTEMPTS
from ops_mcp.demo_incident.models import IncidentWorkItem


@activity.defn(name="ProcessLedgerEntry")
async def process_ledger_entry(item: IncidentWorkItem) -> dict[str, object]:
    attempt = activity.info().attempt
    await asyncio.sleep(1)
    if item.mode == "terminal":
        raise ApplicationError(
            "LedgerWriteRejected: terminal fixture failure",
            type="LedgerWriteRejected",
            non_retryable=True,
        )
    if attempt <= RECOVERY_FAILURE_ATTEMPTS:
        raise ApplicationError(
            "LedgerServiceUnavailable: recovery still in progress",
            type="LedgerServiceUnavailable",
        )
    return {
        "team": item.team,
        "domain": item.domain,
        "touches_money": item.domain == "payments",
        "attempts": attempt,
    }

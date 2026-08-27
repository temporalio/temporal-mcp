"""Stable ground truth and customer prompt for Session 4."""

from __future__ import annotations

from dataclasses import dataclass

PAYMENTS_QUEUE = "session4-ledger-payments"
CORE_QUEUE = "session4-ledger-core"
EXPECTED_DURATION_SECONDS = 5
RECOVERY_FAILURE_ATTEMPTS = 20


@dataclass(frozen=True)
class Population:
    workflow_type: str
    id_prefix: str
    team: str
    domain: str
    count: int


POPULATIONS = (
    Population("SettlementWorkflow", "settlement", "treasury", "payments", 50),
    Population("RefundWorkflow", "refund", "refunds", "payments", 25),
    Population("PayoutWorkflow", "payout", "payouts", "payments", 15),
    Population("LedgerIngestionWorkflow", "ingestion", "ledger-platform", "ledger", 30),
    Population("BalanceSnapshotWorkflow", "balance", "accounting", "ledger", 20),
    Population("AuditExportWorkflow", "audit", "compliance", "compliance", 20),
)

TOTAL_RUNS = sum(item.count for item in POPULATIONS)
PAYMENT_RUNS = sum(item.count for item in POPULATIONS if item.domain == "payments")
TEAMS = tuple(sorted({item.team for item in POPULATIONS}))
WORKFLOW_TYPES = tuple(item.workflow_type for item in POPULATIONS)


def operator_prompt(incident_start: str, incident_end: str) -> str:
    return (
        "We had a ledger incident from "
        f"{incident_start} through {incident_end}. Which teams' workflows were affected, "
        "and which ones touch money? Treat 5 seconds as the configured normal duration for "
        "this scaled fixture."
    )

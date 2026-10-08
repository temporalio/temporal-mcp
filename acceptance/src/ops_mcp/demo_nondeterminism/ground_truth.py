"""Stable ground truth for the scaled Session 3 customer story."""

from __future__ import annotations

import hashlib

TASK_QUEUE = "session3-fulfillment"
ACTIVITY_TASK_QUEUE = "session3-shipping"
WORKFLOW_TYPE = "FulfillmentWorkflow"
V1_BUILD_ID = "fulfillment-v1"
V2_BUILD_ID = "fulfillment-v2"
TOTAL_CANDIDATES = 230
EXPOSED_RUNS = 190
NONDETERMINISTIC_RUNS = 43


def workflow_id(index: int) -> str:
    return f"session3-fulfillment-{index + 1:04d}"


def _affected_ordinals() -> frozenset[int]:
    ranked = sorted(
        range(EXPOSED_RUNS),
        key=lambda index: hashlib.sha256(f"session3\0{index}".encode()).digest(),
    )
    return frozenset(ranked[:NONDETERMINISTIC_RUNS])


AFFECTED_ORDINALS = _affected_ordinals()
AFFECTED_WORKFLOW_IDS = tuple(workflow_id(index) for index in sorted(AFFECTED_ORDINALS))


def operator_prompt(deploy_time: str) -> str:
    return (
        "We shipped fulfillment build fulfillment-v2 at "
        f"{deploy_time}. I think some in-flight runs are hitting non-determinism errors. "
        "How many, and which ones?"
    )

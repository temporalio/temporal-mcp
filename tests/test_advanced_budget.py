import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from ops_mcp.runtime import (
    AdvancedInspectionBudget,
    AdvancedInspectionLimits,
    BudgetExceededError,
    bounded_fanout,
    deterministic_sample,
)


def test_advanced_budget_reservations_are_atomic() -> None:
    budget = AdvancedInspectionBudget(
        AdvancedInspectionLimits(candidates=10, runs=5, histories=3, task_queues=2, concurrency=2)
    )

    def reserve() -> bool:
        try:
            budget.consume_runs(1)
            return True
        except BudgetExceededError:
            return False

    with ThreadPoolExecutor(max_workers=10) as executor:
        successes = list(executor.map(lambda _: reserve(), range(20)))

    assert sum(successes) == 5
    assert budget.snapshot().runs == 5


async def test_bounded_fanout_preserves_order_and_concurrency_limit() -> None:
    active = 0
    maximum_active = 0
    lock = asyncio.Lock()

    async def operation(value: int) -> int:
        nonlocal active, maximum_active
        async with lock:
            active += 1
            maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.001 * (10 - value))
        async with lock:
            active -= 1
        return value * 2

    results = await bounded_fanout(tuple(range(10)), concurrency=3, operation=operation)

    assert maximum_active == 3
    assert [item.item for item in results] == list(range(10))
    assert [item.value for item in results] == [value * 2 for value in range(10)]


def test_deterministic_sample_is_order_independent() -> None:
    values = tuple(f"run-{value}" for value in range(100))

    first = deterministic_sample(values, size=10, seed="fixed", identity=lambda item: item)
    second = deterministic_sample(
        tuple(reversed(values)), size=10, seed="fixed", identity=lambda item: item
    )

    assert first == second
    assert len(first) == 10


def test_advanced_limits_must_be_positive() -> None:
    with pytest.raises(ValueError, match="concurrency"):
        AdvancedInspectionLimits(candidates=1, runs=1, histories=1, task_queues=1, concurrency=0)

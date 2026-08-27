from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    BudgetExceededError,
    CancellationToken,
    DeadlineExceededError,
    RequestCancelledError,
    ResultTooLargeError,
)


def test_budget_allows_exact_limit_and_reports_exhaustion() -> None:
    now = datetime(2026, 8, 23, tzinfo=UTC)
    budget = BasicExecutionBudget(
        deadline=now + timedelta(seconds=10),
        limits=BasicBudgetLimits(temporal_calls=2, pages=1, history_events=3, output_bytes=4),
        clock=lambda: now,
    )

    budget.reserve_temporal_call("describe")
    budget.reserve_temporal_call("history")
    budget.consume_page("history")
    budget.consume_history_events(3)
    budget.consume_output_bytes(4)

    with pytest.raises(BudgetExceededError) as exc_info:
        budget.reserve_temporal_call("extra")

    assert exc_info.value.details == {
        "dimension": "temporal_calls",
        "limit": 2,
        "consumed": 2,
        "attempted": 1,
    }
    assert budget.snapshot().exhausted_dimension == "temporal_calls"


def test_budget_call_reservations_are_atomic() -> None:
    budget = BasicExecutionBudget(
        deadline=datetime.now(UTC) + timedelta(seconds=10),
        limits=BasicBudgetLimits(temporal_calls=20),
    )

    def reserve() -> bool:
        try:
            budget.reserve_temporal_call("concurrent")
            return True
        except BudgetExceededError:
            return False

    with ThreadPoolExecutor(max_workers=16) as executor:
        successes = list(executor.map(lambda _: reserve(), range(100)))

    assert sum(successes) == 20
    assert budget.snapshot().temporal_calls == 20


def test_budget_honors_deadline_and_cancellation() -> None:
    now = datetime(2026, 8, 23, tzinfo=UTC)
    expired = BasicExecutionBudget(deadline=now, clock=lambda: now)
    with pytest.raises(DeadlineExceededError):
        expired.ensure_active()

    cancellation = CancellationToken()
    cancelled = BasicExecutionBudget(
        deadline=now + timedelta(seconds=1), cancellation=cancellation, clock=lambda: now
    )
    cancellation.cancel()
    with pytest.raises(RequestCancelledError):
        cancelled.ensure_active()


def test_output_ceiling_uses_result_too_large_classification() -> None:
    now = datetime(2026, 8, 23, tzinfo=UTC)
    budget = BasicExecutionBudget(
        deadline=now + timedelta(seconds=1),
        limits=BasicBudgetLimits(output_bytes=10),
        clock=lambda: now,
    )

    with pytest.raises(ResultTooLargeError) as exc_info:
        budget.consume_output_bytes(11)

    assert exc_info.value.code == "result_too_large"
    assert budget.snapshot().exhausted_dimension == "output_bytes"

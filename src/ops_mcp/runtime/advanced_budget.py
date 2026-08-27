"""Advanced bounded-fan-out accounting for population inspection."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from threading import Lock
from typing import Generic, TypeVar

from ops_mcp.runtime.errors import BudgetExceededError

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True)
class AdvancedInspectionLimits:
    candidates: int
    runs: int
    histories: int
    task_queues: int
    concurrency: int

    def __post_init__(self) -> None:
        for name, value in (
            ("candidates", self.candidates),
            ("runs", self.runs),
            ("histories", self.histories),
            ("task_queues", self.task_queues),
            ("concurrency", self.concurrency),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class AdvancedInspectionSnapshot:
    candidates: int
    runs: int
    histories: int
    task_queues: int


class AdvancedInspectionBudget:
    """Atomic population counters separate from the basic Temporal-call budget."""

    def __init__(self, limits: AdvancedInspectionLimits) -> None:
        self.limits = limits
        self._lock = Lock()
        self._counters = {"candidates": 0, "runs": 0, "histories": 0, "task_queues": 0}

    def consume_candidates(self, count: int) -> None:
        self._consume("candidates", count)

    def consume_runs(self, count: int) -> None:
        self._consume("runs", count)

    def consume_histories(self, count: int) -> None:
        self._consume("histories", count)

    def consume_task_queues(self, count: int) -> None:
        self._consume("task_queues", count)

    def snapshot(self) -> AdvancedInspectionSnapshot:
        with self._lock:
            return AdvancedInspectionSnapshot(**self._counters)

    def _consume(self, dimension: str, count: int) -> None:
        if count < 0:
            raise ValueError("budget consumption must be non-negative")
        with self._lock:
            consumed = self._counters[dimension]
            limit = getattr(self.limits, dimension)
            if consumed + count > limit:
                raise BudgetExceededError(dimension, limit, consumed, count)
            self._counters[dimension] = consumed + count


@dataclass(frozen=True)
class FanoutResult(Generic[T, R]):
    item: T
    value: R | None = None
    error: Exception | None = None


async def bounded_fanout(
    items: Sequence[T],
    *,
    concurrency: int,
    operation: Callable[[T], Awaitable[R]],
) -> tuple[FanoutResult[T, R], ...]:
    """Run async work with a hard concurrency ceiling and stable input ordering."""

    if concurrency <= 0:
        raise ValueError("concurrency must be positive")
    semaphore = asyncio.Semaphore(concurrency)

    async def run(item: T) -> FanoutResult[T, R]:
        async with semaphore:
            try:
                return FanoutResult(item=item, value=await operation(item))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Partial-result policy is applied by the calling service.
                return FanoutResult(item=item, error=exc)

    return tuple(await asyncio.gather(*(run(item) for item in items)))


def deterministic_sample(
    items: Sequence[T],
    *,
    size: int,
    seed: str,
    identity: Callable[[T], str],
) -> tuple[T, ...]:
    """Select a reproducible hash-ranked sample, independent of input ordering."""

    if size < 0:
        raise ValueError("size must be non-negative")
    ranked = sorted(
        items,
        key=lambda item: (
            hashlib.sha256(f"{seed}\0{identity(item)}".encode()).digest(),
            identity(item),
        ),
    )
    return tuple(ranked[:size])

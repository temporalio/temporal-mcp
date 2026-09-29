"""Concurrency-safe request budgeting primitives."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event, Lock
from typing import Protocol

from ops_mcp.runtime.errors import (
    BudgetExceededError,
    DeadlineExceededError,
    RequestCancelledError,
    ResultTooLargeError,
)


@dataclass(frozen=True)
class BasicBudgetLimits:
    temporal_calls: int = 50
    pages: int = 50
    history_events: int = 2_000
    output_bytes: int = 1_000_000

    def __post_init__(self) -> None:
        for name, value in (
            ("temporal_calls", self.temporal_calls),
            ("pages", self.pages),
            ("history_events", self.history_events),
            ("output_bytes", self.output_bytes),
        ):
            if value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True)
class BudgetSnapshot:
    temporal_calls: int
    pages_read: int
    history_events_read: int
    output_bytes: int
    exhausted_dimension: str | None


class CancellationToken:
    """Small thread-safe cancellation seam independent of an async framework."""

    def __init__(self) -> None:
        self._event = Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise RequestCancelledError


class ExecutionBudget(Protocol):
    @property
    def deadline(self) -> datetime: ...

    def ensure_active(self) -> None: ...

    def remaining_seconds(self) -> float: ...

    def reserve_temporal_call(self, operation: str) -> None: ...

    def consume_page(self, operation: str) -> None: ...

    def consume_history_events(self, count: int) -> None: ...

    def remaining_history_events(self) -> int: ...

    def consume_output_bytes(self, count: int) -> None: ...

    def snapshot(self) -> BudgetSnapshot: ...

    def child(self) -> ExecutionBudget: ...


class BasicExecutionBudget:
    """Atomic counters and deadline shared by all components in one request."""

    def __init__(
        self,
        *,
        deadline: datetime,
        limits: BasicBudgetLimits | None = None,
        cancellation: CancellationToken | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if deadline.tzinfo is None:
            raise ValueError("deadline must be timezone-aware")
        self._deadline = deadline
        self._limits = limits or BasicBudgetLimits()
        self._cancellation = cancellation or CancellationToken()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = Lock()
        self._temporal_calls = 0
        self._pages_read = 0
        self._history_events_read = 0
        self._output_bytes = 0
        self._exhausted_dimension: str | None = None

    @property
    def deadline(self) -> datetime:
        return self._deadline

    @property
    def cancellation(self) -> CancellationToken:
        return self._cancellation

    @property
    def limits(self) -> BasicBudgetLimits:
        return self._limits

    def ensure_active(self) -> None:
        self._cancellation.raise_if_cancelled()
        if self._clock() >= self._deadline:
            raise DeadlineExceededError

    def remaining_seconds(self) -> float:
        self.ensure_active()
        return max(0.0, (self._deadline - self._clock()).total_seconds())

    def reserve_temporal_call(self, operation: str) -> None:
        del operation  # retained for future per-operation accounting
        self.ensure_active()
        self._consume("temporal_calls", 1)

    def consume_page(self, operation: str) -> None:
        del operation
        self.ensure_active()
        self._consume("pages", 1)

    def consume_history_events(self, count: int) -> None:
        self.ensure_active()
        self._consume("history_events", count)

    def remaining_history_events(self) -> int:
        self.ensure_active()
        with self._lock:
            return self._limits.history_events - self._history_events_read

    def consume_output_bytes(self, count: int) -> None:
        self.ensure_active()
        self._consume("output_bytes", count)

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            return BudgetSnapshot(
                temporal_calls=self._temporal_calls,
                pages_read=self._pages_read,
                history_events_read=self._history_events_read,
                output_bytes=self._output_bytes,
                exhausted_dimension=self._exhausted_dimension,
            )

    def child(self) -> ExecutionBudget:
        """Create local counters backed by this budget's aggregate enforcement."""

        return ScopedExecutionBudget(self)

    def _consume(self, dimension: str, count: int) -> None:
        if count < 0:
            raise ValueError("budget consumption must be non-negative")
        attribute = {
            "temporal_calls": "_temporal_calls",
            "pages": "_pages_read",
            "history_events": "_history_events_read",
            "output_bytes": "_output_bytes",
        }[dimension]
        limit = getattr(self._limits, dimension)
        with self._lock:
            consumed = getattr(self, attribute)
            if consumed + count > limit:
                self._exhausted_dimension = dimension
                if dimension == "output_bytes":
                    raise ResultTooLargeError(limit, consumed, count)
                raise BudgetExceededError(dimension, limit, consumed, count)
            setattr(self, attribute, consumed + count)


class ScopedExecutionBudget:
    """Per-operation accounting that atomically charges a shared parent budget."""

    def __init__(self, parent: ExecutionBudget) -> None:
        self._parent = parent
        self._lock = Lock()
        self._temporal_calls = 0
        self._pages_read = 0
        self._history_events_read = 0
        self._output_bytes = 0
        self._exhausted_dimension: str | None = None

    @property
    def deadline(self) -> datetime:
        return self._parent.deadline

    def ensure_active(self) -> None:
        self._parent.ensure_active()

    def remaining_seconds(self) -> float:
        return self._parent.remaining_seconds()

    def reserve_temporal_call(self, operation: str) -> None:
        self._charge("temporal_calls", 1, lambda: self._parent.reserve_temporal_call(operation))

    def consume_page(self, operation: str) -> None:
        self._charge("pages", 1, lambda: self._parent.consume_page(operation))

    def consume_history_events(self, count: int) -> None:
        self._charge(
            "history_events", count, lambda: self._parent.consume_history_events(count)
        )

    def remaining_history_events(self) -> int:
        return self._parent.remaining_history_events()

    def consume_output_bytes(self, count: int) -> None:
        self._charge("output_bytes", count, lambda: self._parent.consume_output_bytes(count))

    def snapshot(self) -> BudgetSnapshot:
        with self._lock:
            return BudgetSnapshot(
                temporal_calls=self._temporal_calls,
                pages_read=self._pages_read,
                history_events_read=self._history_events_read,
                output_bytes=self._output_bytes,
                exhausted_dimension=self._exhausted_dimension,
            )

    def child(self) -> ExecutionBudget:
        return ScopedExecutionBudget(self)

    def _charge(self, dimension: str, count: int, parent_charge: Callable[[], None]) -> None:
        if count < 0:
            raise ValueError("budget consumption must be non-negative")
        try:
            parent_charge()
        except (BudgetExceededError, ResultTooLargeError):
            with self._lock:
                self._exhausted_dimension = dimension
            raise
        attribute = {
            "temporal_calls": "_temporal_calls",
            "pages": "_pages_read",
            "history_events": "_history_events_read",
            "output_bytes": "_output_bytes",
        }[dimension]
        with self._lock:
            setattr(self, attribute, getattr(self, attribute) + count)

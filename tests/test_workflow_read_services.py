from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from ops_mcp.contracts.primitives import Completeness
from ops_mcp.contracts.workflow_reads import (
    DescribeTaskQueueRequest,
    DescribeWorkflowRequest,
    GetWorkflowResultRequest,
    HistorySliceRequest,
)
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    DecodedPayloadsFact,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    WorkflowCloseEventAttributes,
)
from ops_mcp.facts.task_queues import PollerFact, TaskQueueFact, TaskQueueKind
from ops_mcp.facts.workflows import PendingActivityFact, WorkflowExecutionFact
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.services.workflow_reads import DefaultWorkflowReadService
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_describe_workflow_reauthorizes_and_preserves_decode_status() -> None:
    context = _context()
    workflow = _workflow_fact()
    workflow_gateway = _WorkflowGateway(workflow)
    service = _service(workflow_gateway=workflow_gateway)

    response = await service.describe_workflow(
        context,
        DescribeWorkflowRequest(target_ref="orders-prod", workflow_id="order-1", run_id="run-1"),
    )

    assert response.schema_version == "1"
    assert response.answer.run_id == "run-1"
    assert response.answer.memo["secret"].decode_status == "decode_failed"
    pending = response.answer.pending_activities[0]
    assert pending.activity_task_queue == "catalog"
    assert pending.last_heartbeat_time is not None
    assert pending.heartbeat_details.values == ({"cursor": 42},)
    assert response.inspection.temporal_calls == 1
    assert response.inspection.completion is Completeness.COMPLETE
    assert context.budget.snapshot().output_bytes > 0


@pytest.mark.asyncio
async def test_failure_slice_is_explicitly_truncated_and_returns_token() -> None:
    context = _context()
    history_gateway = _HistoryGateway(
        HistoryReadFact(
            workflow_id="order-1",
            run_id="run-1",
            events=(
                _history_event(1, ActivityTaskEventAttributes(phase="scheduled")),
                _history_event(
                    2,
                    ActivityTaskEventAttributes(
                        phase="failed", failure=FailureFact(message="catalog 503")
                    ),
                ),
            ),
            reverse=False,
            pages_read=1,
            events_scanned=2,
            next_page_token=b"next",
            observed_at=_now(),
        )
    )
    service = _service(history_gateway=history_gateway)

    response = await service.get_workflow_history_slice(
        context,
        HistorySliceRequest(
            target_ref="orders-prod",
            workflow_id="order-1",
            run_id="run-1",
            failures_only=True,
            limit=2,
        ),
    )

    assert [event.event_id for event in response.answer.events] == [2]
    assert response.answer.inspected_event_id_range == (1, 2)
    assert response.answer.returned_event_id_range == (2, 2)
    assert response.inspection.completion is Completeness.TRUNCATED
    assert response.answer.continuation_token is not None
    assert history_gateway.read_args["max_events"] == 2


@pytest.mark.asyncio
async def test_result_does_not_wait_by_default_and_returns_terminal_failure() -> None:
    context = _context()
    closing = _history_event(
        10,
        WorkflowCloseEventAttributes(state="failed", failure=FailureFact(message="order failed")),
    )
    history_gateway = _HistoryGateway(None, closing=closing)
    service = _service(history_gateway=history_gateway)

    response = await service.get_workflow_result(
        context,
        GetWorkflowResultRequest(target_ref="orders-prod", workflow_id="order-1", run_id="run-1"),
    )

    assert response.answer.result.state == "failed"
    assert response.answer.result.failure
    assert response.answer.result.failure.message == "order failed"
    assert history_gateway.closing_args["wait"] is False
    assert history_gateway.closing_args["wait_timeout_seconds"] is None


@pytest.mark.asyncio
async def test_task_queue_keeps_workflow_and_activity_types_explicit() -> None:
    context = _context()
    queue_gateway = _TaskQueueGateway(
        TaskQueueFact(
            task_queue="activities",
            task_type=TaskQueueKind.ACTIVITY,
            pollers=(PollerFact("worker-1", _now(), 4.0),),
            approximate_backlog_count=3,
            approximate_backlog_age_seconds=5.0,
            tasks_add_rate=2.0,
            tasks_dispatch_rate=4.0,
            rate_limiting_active=False,
            observed_at=_now(),
        )
    )
    service = _service(task_queue_gateway=queue_gateway)

    response = await service.describe_task_queue(
        context,
        DescribeTaskQueueRequest(
            target_ref="orders-prod", task_queue="activities", task_type="activity"
        ),
    )

    assert response.answer.task_type == "activity"
    assert response.answer.currently_has_pollers
    assert queue_gateway.kind is TaskQueueKind.ACTIVITY


def test_history_slice_requires_exactly_one_selector() -> None:
    with pytest.raises(ValidationError):
        HistorySliceRequest(target_ref="orders-prod", workflow_id="order-1", run_id="run-1")
    with pytest.raises(ValidationError):
        HistorySliceRequest(
            target_ref="orders-prod",
            workflow_id="order-1",
            run_id="run-1",
            failures_only=True,
            last_n=5,
        )


class _Catalog:
    def __init__(self, target: ResolvedTarget) -> None:
        self.target = target

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        assert principal.subject == "tester"
        assert target_ref == self.target.target_ref
        return self.target


class _WorkflowGateway:
    def __init__(self, fact: WorkflowExecutionFact) -> None:
        self.fact = fact

    async def describe(self, context: RequestContext, *args: object) -> WorkflowExecutionFact:
        context.budget.reserve_temporal_call("describe")
        return self.fact


class _HistoryGateway:
    def __init__(
        self,
        read: HistoryReadFact | None,
        *,
        closing: HistoryEventFact | None = None,
    ) -> None:
        self.read_fact = read
        self.closing = closing
        self.read_args: dict[str, object] = {}
        self.closing_args: dict[str, object] = {}

    async def read(
        self, context: RequestContext, *args: object, **kwargs: object
    ) -> HistoryReadFact:
        assert self.read_fact is not None
        self.read_args = kwargs
        context.budget.reserve_temporal_call("history")
        context.budget.consume_page("history")
        context.budget.consume_history_events(self.read_fact.events_scanned)
        return self.read_fact

    async def closing_event(
        self, context: RequestContext, *args: object, **kwargs: object
    ) -> HistoryEventFact | None:
        self.closing_args = kwargs
        context.budget.reserve_temporal_call("closing")
        context.budget.consume_page("closing")
        if self.closing:
            context.budget.consume_history_events(1)
        return self.closing


class _TaskQueueGateway:
    def __init__(self, fact: TaskQueueFact) -> None:
        self.fact = fact
        self.kind: TaskQueueKind | None = None

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        task_queue: str,
        task_type: TaskQueueKind,
    ) -> TaskQueueFact:
        self.kind = task_type
        context.budget.reserve_temporal_call("task_queue")
        return self.fact


def _service(
    *,
    workflow_gateway: object | None = None,
    history_gateway: object | None = None,
    task_queue_gateway: object | None = None,
) -> DefaultWorkflowReadService:
    target = _target()
    return DefaultWorkflowReadService(
        _Catalog(target),
        workflow_gateway or _WorkflowGateway(_workflow_fact()),
        history_gateway or _HistoryGateway(None),
        task_queue_gateway
        or _TaskQueueGateway(
            TaskQueueFact(
                task_queue="orders",
                task_type=TaskQueueKind.WORKFLOW,
                pollers=(),
                approximate_backlog_count=0,
                approximate_backlog_age_seconds=0,
                tasks_add_rate=0,
                tasks_dispatch_rate=0,
                rate_limiting_active=False,
                observed_at=_now(),
            )
        ),
    )


def _workflow_fact() -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id="order-1",
        run_id="run-1",
        workflow_type="OrderWorkflow",
        status="FAILED",
        task_queue="orders",
        namespace="default",
        start_time=_now(),
        execution_time=_now(),
        close_time=_now(),
        history_length=10,
        history_size_bytes=100,
        first_run_id="run-1",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="order-1",
        root_run_id="run-1",
        memo={"secret": DecodedPayloadsFact(decode_status="decode_failed")},
        pending_activities=(
            PendingActivityFact(
                activity_id="enrich",
                activity_type="EnrichProduct",
                state="STARTED",
                attempt=1,
                maximum_attempts=3,
                scheduled_time=_now(),
                last_started_time=_now(),
                next_attempt_time=None,
                last_worker_identity="worker-1",
                last_failure=None,
                paused=False,
                activity_task_queue="catalog",
                last_heartbeat_time=_now(),
                heartbeat_details=DecodedPayloadsFact(values=({"cursor": 42},)),
            ),
        ),
        observed_at=_now(),
    )


def _history_event(event_id: int, attributes: object) -> HistoryEventFact:
    return HistoryEventFact(
        event_id=event_id,
        event_type=f"EVENT_{event_id}",
        event_time=_now(),
        task_id=event_id,
        attributes=attributes,
    )


def _target() -> ResolvedTarget:
    descriptor = ConnectionDescriptor(address="localhost:7233", namespace="default")
    return ResolvedTarget(
        target_ref="orders-prod",
        aliases=(),
        account="demo",
        cluster="local",
        namespace="default",
        connection=descriptor,
    )


def _context() -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="tester", groups=("test",)),
        budget=BasicExecutionBudget(deadline=_now() + timedelta(seconds=30)),
    )


def _now() -> datetime:
    return datetime.now(UTC)

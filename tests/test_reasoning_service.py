from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from ops_mcp.analysis.reasoning import DiagnosisAnalyzer
from ops_mcp.contracts.reasoning import (
    InspectWorkflowExecutionRequest,
    PlanWorkflowResetRequest,
)
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    SignalEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import PollerFact, TaskQueueFact, TaskQueueKind
from ops_mcp.facts.workflows import PendingActivityFact, WorkflowExecutionFact
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.services.reasoning import DefaultReasoningService
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget

NOW = datetime(2026, 8, 23, 12, tzinfo=UTC)


def _event(event_id: int, event_type: str, attributes: object) -> HistoryEventFact:
    return HistoryEventFact(
        event_id=event_id,
        event_type=event_type,
        event_time=NOW,
        task_id=event_id,
        attributes=attributes,  # type: ignore[arg-type]
    )


def _events() -> tuple[HistoryEventFact, ...]:
    return (
        _event(
            4,
            "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
            WorkflowTaskEventAttributes(
                phase="completed", scheduled_event_id=2, started_event_id=3
            ),
        ),
        _event(
            5,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled",
                activity_id="payment",
                activity_type="AuthorizePayment",
                task_queue="payments",
            ),
        ),
        _event(
            7,
            "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
            ActivityTaskEventAttributes(phase="completed", scheduled_event_id=5),
        ),
        _event(
            10,
            "EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
            WorkflowTaskEventAttributes(
                phase="completed", scheduled_event_id=8, started_event_id=9
            ),
        ),
        _event(
            11,
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            ActivityTaskEventAttributes(
                phase="scheduled",
                activity_id="enrich",
                activity_type="EnrichProduct",
                task_queue="catalog",
            ),
        ),
        _event(
            12,
            "EVENT_TYPE_ACTIVITY_TASK_FAILED",
            ActivityTaskEventAttributes(
                phase="failed",
                scheduled_event_id=11,
                retry_state="RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED",
                failure=FailureFact(
                    message="503 Service Unavailable",
                    application_type="CatalogServiceUnavailable",
                ),
            ),
        ),
        _event(
            13,
            "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
            SignalEventAttributes(signal_name="address-change"),
        ),
    )


def _workflow() -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id="order-88231",
        run_id="run-1",
        workflow_type="OrderWorkflow",
        status="WORKFLOW_EXECUTION_STATUS_FAILED",
        task_queue="orders",
        namespace="default",
        start_time=NOW,
        execution_time=NOW,
        close_time=NOW,
        history_length=len(_events()),
        history_size_bytes=100,
        first_run_id="run-1",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="order-88231",
        root_run_id="run-1",
        pending_activities=(
            PendingActivityFact(
                activity_id="enrich",
                activity_type="EnrichProduct",
                state="SCHEDULED",
                attempt=1,
                maximum_attempts=3,
                scheduled_time=NOW,
                last_started_time=None,
                next_attempt_time=None,
                last_worker_identity=None,
                last_failure=None,
                paused=False,
                activity_task_queue="catalog",
            ),
        ),
        observed_at=NOW,
    )


class _Catalog:
    def __init__(self) -> None:
        self.target = ResolvedTarget(
            target_ref="orders-prod",
            aliases=("orders prod",),
            account="demo",
            cluster="local",
            namespace="default",
            connection=ConnectionDescriptor(address="localhost:7233", namespace="default"),
        )

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        assert principal.subject == "tester"
        assert target_ref == "orders-prod"
        return self.target


class _WorkflowGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def describe(self, *args: object, **kwargs: object) -> WorkflowExecutionFact:
        self.calls += 1
        args[0].budget.reserve_temporal_call("DescribeWorkflowExecution")  # type: ignore[union-attr]
        return _workflow()


class _HistoryGateway:
    async def read(self, *args: object, **kwargs: object) -> HistoryReadFact:
        context = args[0]
        context.budget.reserve_temporal_call("GetWorkflowExecutionHistory")  # type: ignore[union-attr]
        context.budget.consume_page("history")  # type: ignore[union-attr]
        context.budget.consume_history_events(len(_events()))  # type: ignore[union-attr]
        return HistoryReadFact(
            workflow_id="order-88231",
            run_id="run-1",
            events=_events(),
            reverse=False,
            pages_read=1,
            events_scanned=len(_events()),
            next_page_token=None,
            observed_at=NOW,
        )


class _QueueGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, TaskQueueKind]] = []

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        task_queue: str,
        task_type: TaskQueueKind,
    ) -> TaskQueueFact:
        del target
        context.budget.reserve_temporal_call("DescribeTaskQueue")
        self.calls.append((task_queue, task_type))
        return TaskQueueFact(
            task_queue=task_queue,
            task_type=task_type,
            pollers=(PollerFact("worker", NOW, 1.0),),
            approximate_backlog_count=0,
            approximate_backlog_age_seconds=0,
            tasks_add_rate=0,
            tasks_dispatch_rate=0,
            rate_limiting_active=False,
            observed_at=NOW,
        )


def _context() -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="tester", groups=("demo-support",)),
        budget=BasicExecutionBudget(deadline=NOW + timedelta(hours=1), clock=lambda: NOW),
    )


@pytest.mark.asyncio
async def test_execution_inspection_authorizes_and_inspects_both_queue_types() -> None:
    queues = _QueueGateway()
    workflows = _WorkflowGateway()
    service = DefaultReasoningService(_Catalog(), workflows, _HistoryGateway(), queues)

    result = await service.inspect_workflow_execution(
        _context(),
        InspectWorkflowExecutionRequest(
            target_ref="orders-prod",
            workflow_id="order-88231",
            include_diagnostic_query=False,
        ),
    )

    assert workflows.calls == 1
    assert result.inspection.completion == "complete"
    assert set(queues.calls) == {
        ("orders", TaskQueueKind.WORKFLOW),
        ("catalog", TaskQueueKind.ACTIVITY),
    }
    assert any(item.code == "activity_retry_exhausted" for item in result.answer.retry_findings)


@pytest.mark.asyncio
async def test_inspect_execution_resolves_run_and_separates_current_dispatch() -> None:
    service = DefaultReasoningService(
        _Catalog(), _WorkflowGateway(), _HistoryGateway(), _QueueGateway()
    )

    result = await service.inspect_workflow_execution(
        _context(),
        InspectWorkflowExecutionRequest(
            target_ref="orders-prod",
            workflow_id="order-88231",
            include_diagnostic_query=False,
        ),
    )

    assert result.answer.workflow.run_id == "run-1"
    assert result.answer.workflow_wait_state == "pending_activity"
    assert result.answer.current_dispatch_state == "dispatch_available_now"
    assert "diagnostic_query:not_requested" in result.evidence_unavailable
    assert any("historical outage" in item for item in result.unsupported_conclusions)


async def test_queue_specific_failure_preserves_other_exact_run_evidence() -> None:
    class PartiallyFailingQueues(_QueueGateway):
        async def describe(self, context, target, task_queue, task_type):
            if task_queue == "catalog":
                raise OpsMcpError(
                    ErrorCode.INVALID_ARGUMENT,
                    "queue disappeared",
                    retryable=False,
                )
            return await super().describe(context, target, task_queue, task_type)

    result = await DefaultReasoningService(
        _Catalog(), _WorkflowGateway(), _HistoryGateway(), PartiallyFailingQueues()
    ).inspect_workflow_execution(
        _context(),
        InspectWorkflowExecutionRequest(
            target_ref="orders-prod",
            workflow_id="order-88231",
            run_id="run-1",
            include_diagnostic_query=False,
        ),
    )

    assert result.answer.workflow.run_id == "run-1"
    assert result.answer.task_queues
    assert result.inspection.completion == "partial"
    assert "task_queue_snapshot:activity:catalog" in result.evidence_unavailable


async def test_systemic_queue_failure_propagates() -> None:
    class SystemicFailure(_QueueGateway):
        async def describe(self, context, target, task_queue, task_type):
            raise OpsMcpError(
                ErrorCode.TEMPORAL_AUTHENTICATION_FAILED,
                "credentials rejected",
                retryable=False,
            )

    service = DefaultReasoningService(
        _Catalog(), _WorkflowGateway(), _HistoryGateway(), SystemicFailure()
    )

    with pytest.raises(OpsMcpError) as exc_info:
        await service.inspect_workflow_execution(
            _context(),
            InspectWorkflowExecutionRequest(
                target_ref="orders-prod",
                workflow_id="order-88231",
                run_id="run-1",
                include_diagnostic_query=False,
            ),
        )

    assert exc_info.value.code is ErrorCode.TEMPORAL_AUTHENTICATION_FAILED


def test_diagnosis_distinguishes_delay_from_current_activity_dispatch_blocker() -> None:
    workflow = replace(
        _workflow(),
        status="RUNNING",
        start_time=NOW - timedelta(hours=6),
        observed_at=NOW,
    )
    history = HistoryReadFact(
        workflow_id=workflow.workflow_id,
        run_id=workflow.run_id,
        events=(),
        reverse=False,
        pages_read=1,
        events_scanned=0,
        next_page_token=None,
        observed_at=NOW,
    )
    activity_queue = TaskQueueFact(
        task_queue="catalog",
        task_type=TaskQueueKind.ACTIVITY,
        pollers=(),
        approximate_backlog_count=4_200,
        approximate_backlog_age_seconds=300,
        tasks_add_rate=1,
        tasks_dispatch_rate=0,
        rate_limiting_active=False,
        observed_at=NOW,
    )

    analysis = DiagnosisAnalyzer().analyze(
        workflow,
        history,
        (activity_queue,),
        expected_duration_seconds=600,
    )

    delayed = next(
        item for item in analysis.findings if item.code == "workflow_duration_exceeds_expectation"
    )
    blocker = next(
        item for item in analysis.findings if item.code == "current_activity_dispatch_blocker"
    )
    assert "delay" in delayed.caveats[0]
    assert any("historical cause" in caveat for caveat in blocker.caveats)

    unrelated = replace(activity_queue, task_queue="other")
    unrelated_analysis = DiagnosisAnalyzer().analyze(
        workflow,
        history,
        (unrelated,),
        expected_duration_seconds=600,
    )
    assert not any(
        item.code == "current_activity_dispatch_blocker" for item in unrelated_analysis.findings
    )


@pytest.mark.asyncio
async def test_reset_service_returns_advisory_nonexecuted_single_line_preview() -> None:
    service = DefaultReasoningService(
        _Catalog(), _WorkflowGateway(), _HistoryGateway(), _QueueGateway()
    )

    result = await service.plan_workflow_reset(
        _context(),
        PlanWorkflowResetRequest(
            target_ref="orders-prod",
            workflow_id="order-88231",
            run_id="run-1",
            operator_rationale="catalog recovered\nmanual review",
        ),
    )

    assert result.recommendation is not None
    assert result.recommendation.event_id == 10
    assert result.command_preview is not None
    assert result.command_preview.executed is False
    assert "\n" not in result.command_preview.command
    assert "--event-id 10" in result.command_preview.command
    assert result.recheck_after == NOW + timedelta(minutes=15)
    point = next(item for item in result.valid_reset_points if item.event_id == 10)
    assert [item.activity_type for item in point.replay_impact.activities_replayed] == [
        "EnrichProduct"
    ]

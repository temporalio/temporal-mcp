"""Model-free Session 1 acceptance through the new typed service architecture."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ops_mcp.contracts import (
    CountWorkflowsRequest,
    DiagnoseWorkflowRequest,
    FilterWorkflowsRequest,
    PlanWorkflowResetRequest,
    ResolveTargetRequest,
    WorkflowField,
)
from ops_mcp.contracts.primitives import Completeness
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    SignalEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import PollerFact, TaskQueueFact, TaskQueueKind
from ops_mcp.facts.visibility import (
    CountPrecision,
    VisibilityCountFact,
    VisibilityPageFact,
    WorkflowVisibilityFact,
)
from ops_mcp.facts.workflows import WorkflowExecutionFact
from ops_mcp.runtime import BasicExecutionBudget, Principal, RequestContext
from ops_mcp.services import DefaultReasoningService, DefaultTargetService, DefaultVisibilityService
from ops_mcp.targets import ResolvedTarget, YamlTargetCatalog

NOW = datetime(2026, 8, 23, 12, tzinfo=UTC)


def _context() -> RequestContext:
    return RequestContext(
        request_id="session-1",
        principal=Principal(subject="support-person", groups=("demo-support",)),
        budget=BasicExecutionBudget(
            deadline=NOW + timedelta(minutes=1),
            clock=lambda: NOW,
        ),
    )


def _event(event_id: int, event_type: str, attributes: object) -> HistoryEventFact:
    return HistoryEventFact(
        event_id=event_id,
        event_type=event_type,
        event_time=NOW,
        task_id=event_id,
        attributes=attributes,  # type: ignore[arg-type]
    )


def _history_events() -> tuple[HistoryEventFact, ...]:
    catalog_failure = FailureFact(
        message="503 Service Unavailable from catalog service",
        application_type="CatalogServiceUnavailable",
    )
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
                task_queue="orders-demo",
            ),
        ),
        _event(
            6,
            "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            ActivityTaskEventAttributes(phase="started", scheduled_event_id=5, attempt=1),
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
                task_queue="orders-demo",
            ),
        ),
        _event(
            12,
            "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            ActivityTaskEventAttributes(phase="started", scheduled_event_id=11, attempt=3),
        ),
        _event(
            13,
            "EVENT_TYPE_ACTIVITY_TASK_FAILED",
            ActivityTaskEventAttributes(
                phase="failed",
                scheduled_event_id=11,
                started_event_id=12,
                retry_state="RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED",
                failure=catalog_failure,
            ),
        ),
        _event(
            14,
            "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
            SignalEventAttributes(signal_name="address-change"),
        ),
        _event(
            15,
            "EVENT_TYPE_WORKFLOW_TASK_FAILED",
            WorkflowTaskEventAttributes(
                phase="failed",
                scheduled_event_id=14,
                started_event_id=15,
                failure=FailureFact(message="workflow closed after activity failure"),
            ),
        ),
    )


def _workflow_fact() -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id="order-88231",
        run_id="run-session-1",
        workflow_type="OrderWorkflow",
        status="FAILED",
        task_queue="orders-demo",
        namespace="default",
        start_time=NOW - timedelta(minutes=20),
        execution_time=NOW - timedelta(minutes=20),
        close_time=NOW - timedelta(minutes=19),
        history_length=len(_history_events()),
        history_size_bytes=2_000,
        first_run_id="run-session-1",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="order-88231",
        root_run_id="run-session-1",
        observed_at=NOW,
    )


class _VisibilityGateway:
    async def list_workflows_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        page_size: int,
        page_token: bytes | None = None,
    ) -> VisibilityPageFact:
        del page_size, page_token
        context.budget.reserve_temporal_call("ListWorkflowExecutions")
        context.budget.consume_page("ListWorkflowExecutions")
        workflow = _workflow_fact()
        return VisibilityPageFact(
            namespace=target.namespace,
            query=query,
            rows=(
                WorkflowVisibilityFact(
                    namespace=target.namespace,
                    workflow_id=workflow.workflow_id,
                    run_id=workflow.run_id,
                    workflow_type=workflow.workflow_type,
                    status=workflow.status,
                    task_queue=workflow.task_queue,
                    start_time=workflow.start_time,
                    execution_time=workflow.execution_time,
                    close_time=workflow.close_time,
                    history_length=workflow.history_length,
                    parent_workflow_id=None,
                    parent_run_id=None,
                    root_workflow_id=workflow.root_workflow_id,
                    root_run_id=workflow.root_run_id,
                ),
            ),
            next_page_token=None,
        )

    async def count_workflows(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        group_by: object = None,
    ) -> VisibilityCountFact:
        del group_by
        context.budget.reserve_temporal_call("CountWorkflowExecutions")
        return VisibilityCountFact(
            namespace=target.namespace,
            query=query,
            group_by=None,
            count=12,
            groups=(),
            precision=CountPrecision.APPROXIMATE,
        )


class _WorkflowGateway:
    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str | None = None,
    ) -> WorkflowExecutionFact:
        del target
        assert workflow_id == "order-88231"
        assert run_id == "run-session-1"
        context.budget.reserve_temporal_call("DescribeWorkflowExecution")
        return _workflow_fact()


class _HistoryGateway:
    async def read(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        **kwargs: object,
    ) -> HistoryReadFact:
        del target, kwargs
        assert (workflow_id, run_id) == ("order-88231", "run-session-1")
        events = _history_events()
        context.budget.reserve_temporal_call("GetWorkflowExecutionHistory")
        context.budget.consume_page("GetWorkflowExecutionHistory")
        context.budget.consume_history_events(len(events))
        return HistoryReadFact(
            workflow_id=workflow_id,
            run_id=run_id,
            events=events,
            reverse=False,
            pages_read=1,
            events_scanned=len(events),
            next_page_token=None,
            observed_at=NOW,
        )


class _TaskQueueGateway:
    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        task_queue: str,
        task_type: TaskQueueKind,
    ) -> TaskQueueFact:
        del target
        context.budget.reserve_temporal_call("DescribeTaskQueue")
        return TaskQueueFact(
            task_queue=task_queue,
            task_type=task_type,
            pollers=(PollerFact("demo-worker", NOW, 1.0),),
            approximate_backlog_count=0,
            approximate_backlog_age_seconds=0,
            tasks_add_rate=0,
            tasks_dispatch_rate=1,
            rate_limiting_active=False,
            observed_at=NOW,
        )


@pytest.mark.asyncio
async def test_session_one_through_typed_services() -> None:
    catalog = YamlTargetCatalog.load("config/targets.example.yaml")
    targets = DefaultTargetService(catalog)
    visibility = DefaultVisibilityService(catalog, _VisibilityGateway())
    reasoning = DefaultReasoningService(
        catalog,
        _WorkflowGateway(),
        _HistoryGateway(),
        _TaskQueueGateway(),
    )

    resolved = targets.resolve_target(_context(), ResolveTargetRequest(human_name="orders prod"))
    assert resolved.target_ref == "orders-prod"
    assert "address" not in resolved.model_dump_json()

    filtered = await visibility.filter_workflows(
        _context(),
        FilterWorkflowsRequest(
            target_ref=resolved.target_ref,
            query="OrderId = '88231'",
            fields=(WorkflowField.WORKFLOW_ID, WorkflowField.RUN_ID, WorkflowField.STATUS),
            page_size=10,
        ),
    )
    assert filtered.answer.returned == 1
    failed = filtered.answer.workflows[0]
    assert failed.workflow_id == "order-88231"
    assert failed.run_id == "run-session-1"
    assert failed.status == "FAILED"

    diagnosis = await reasoning.diagnose_workflow(
        _context(),
        DiagnoseWorkflowRequest(
            target_ref=resolved.target_ref,
            workflow_id=failed.workflow_id,
            run_id=failed.run_id,
        ),
    )
    assert diagnosis.retry_assessment.assessment == "unknown"
    assert any(
        finding.code == "activity_retry_exhausted" and "503 Service Unavailable" in finding.summary
        for finding in diagnosis.findings
    )
    assert diagnosis.task_queues
    assert all(queue.observed_at == NOW for queue in diagnosis.task_queues)
    assert all(queue.historical_health_known is False for queue in diagnosis.task_queues)
    assert any(
        "does not establish health at failure time" in caveat
        for finding in diagnosis.findings
        for caveat in finding.caveats
    )

    count = await visibility.count_workflows(
        _context(),
        CountWorkflowsRequest(
            target_ref=resolved.target_ref,
            query="WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Completed'",
        ),
    )
    assert count.answer.count == 12
    assert count.answer.precision == "approximate"

    plan = await reasoning.plan_workflow_reset(
        _context(),
        PlanWorkflowResetRequest(
            target_ref=resolved.target_ref,
            workflow_id=failed.workflow_id,
            run_id=failed.run_id,
        ),
    )
    assert plan.history_inspection.completion is Completeness.COMPLETE
    assert plan.recommendation is not None
    assert plan.recommendation.event_id == 10
    recommended = next(
        point for point in plan.valid_reset_points if point.event_id == plan.recommendation.event_id
    )
    assert recommended.event_type == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"
    assert [
        activity.activity_type
        for activity in recommended.replay_impact.completed_activities_preserved
    ] == ["AuthorizePayment"]
    assert [
        activity.activity_type for activity in recommended.replay_impact.activities_replayed
    ] == ["EnrichProduct"]
    assert recommended.replay_impact.signals_reapplied == ("address-change",)
    assert plan.recheck_after == NOW + timedelta(minutes=15)
    assert plan.command_preview is not None
    assert plan.command_preview.executed is False
    assert "\n" not in plan.command_preview.command
    assert "--event-id 10" in plan.command_preview.command

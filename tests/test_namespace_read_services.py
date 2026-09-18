from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ops_mcp.contracts.namespace_reads import (
    DescribeScheduleRequest,
    ListApprovedWorkflowQueriesRequest,
    ListBatchOperationsRequest,
    ListSchedulesRequest,
    QueryWorkflowRequest,
)
from ops_mcp.contracts.primitives import Completeness
from ops_mcp.facts.namespace_reads import (
    BatchOperationDetailFact,
    BatchOperationPageFact,
    BatchOperationSummaryFact,
    QueryResultFact,
    ScheduleActionFact,
    ScheduleDescriptionFact,
    ScheduleSummaryFact,
)
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.services.namespace_reads import (
    ConfiguredQueryApprovalPolicy,
    DefaultNamespaceReadService,
)
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_list_schedules_filters_one_server_page_and_reports_truncation() -> None:
    gateway = _ScheduleGateway(
        (
            ScheduleSummaryFact("paused-cron", True, None, ("cron",), (), ()),
            ScheduleSummaryFact("active-cron", False, None, ("cron",), (), ()),
        ),
        next_token=b"next",
    )
    service = _service(schedules=gateway)

    response = await service.list_schedules(
        _context(),
        ListSchedulesRequest(
            target_ref="orders-prod", name_contains="paused", shape="cron", paused=True
        ),
    )

    assert [row.schedule_id for row in response.answer.schedules] == ["paused-cron"]
    assert response.answer.server_rows_scanned == 2
    assert response.inspection.completion is Completeness.TRUNCATED
    assert response.answer.continuation_token


@pytest.mark.asyncio
async def test_describe_schedule_returns_bounded_upcoming_times() -> None:
    fact = ScheduleDescriptionFact(
        schedule_id="daily",
        action=ScheduleActionFact("start_workflow", "OrderWorkflow", "daily-order", "orders"),
        paused=True,
        note="maintenance",
        limited_actions=False,
        remaining_actions=0,
        shapes=("interval",),
        cron_expressions=(),
        intervals_seconds=(3600,),
        calendar_count=0,
        time_zone_name="UTC",
        overlap_policy="SCHEDULE_OVERLAP_POLICY_SKIP",
        catchup_window_seconds=60,
        pause_on_failure=True,
        recent_action_times=(),
        next_action_times=(),
        upcoming_action_times=(_now() + timedelta(hours=1),),
        observed_at=_now(),
    )
    schedules = _ScheduleGateway((), description=fact)
    service = _service(schedules=schedules)
    start = _now()

    response = await service.describe_schedule(
        _context(),
        DescribeScheduleRequest(
            target_ref="orders-prod",
            schedule_id="daily",
            upcoming_count=1,
            upcoming_start=start,
            upcoming_end=start + timedelta(days=1),
        ),
    )

    assert response.answer.paused
    assert response.answer.action.workflow_type == "OrderWorkflow"
    assert len(response.answer.upcoming_action_times) == 1


@pytest.mark.asyncio
async def test_unapproved_query_is_rejected_before_gateway_call() -> None:
    queries = _QueryGateway()
    service = _service(queries=queries)

    with pytest.raises(OpsMcpError) as caught:
        await service.query_workflow(
            _context(),
            QueryWorkflowRequest(
                target_ref="orders-prod",
                workflow_id="order-1",
                run_id="run-1",
                query_name="arbitrary",
            ),
        )

    assert caught.value.code is ErrorCode.QUERY_NOT_APPROVED
    assert queries.calls == 0


@pytest.mark.asyncio
async def test_approved_query_returns_decoded_fact() -> None:
    queries = _QueryGateway()
    service = _service(queries=queries, approved=frozenset({"blocked_on"}))

    response = await service.query_workflow(
        _context(),
        QueryWorkflowRequest(
            target_ref="orders-prod",
            workflow_id="order-1",
            run_id="run-1",
            query_name="blocked_on",
        ),
    )

    assert response.answer.value == {"activity": "EnrichProduct"}
    assert response.inspection.temporal_calls == 1


@pytest.mark.asyncio
async def test_query_discovery_lists_approved_names_without_temporal_call() -> None:
    queries = _QueryGateway()
    service = _service(
        queries=queries,
        approved=frozenset({"custom_status", "__stack_trace"}),
    )

    response = await service.query_workflow(
        _context(),
        QueryWorkflowRequest(
            target_ref="orders-prod",
            workflow_id="order-1",
            run_id="run-1",
        ),
    )

    assert response.answer.kind == "discovery"
    assert [item.query_name for item in response.answer.approved_queries] == [
        "__stack_trace",
        "custom_status",
    ]
    assert response.answer.approved_queries[0].sensitivity == "sensitive"
    assert response.answer.support_status == "not_verified"
    assert response.inspection.temporal_calls == 0
    assert queries.calls == 0


def test_list_approved_queries_is_target_scoped_configuration_only() -> None:
    queries = _QueryGateway()
    service = _service(
        queries=queries,
        approved=frozenset({"custom_status", "__stack_trace"}),
    )

    response = service.list_approved_workflow_queries(
        _context(), ListApprovedWorkflowQueriesRequest(target_ref="orders-prod")
    )

    assert [item.query_name for item in response.answer.approved_queries] == [
        "__stack_trace",
        "custom_status",
    ]
    assert response.inspection.temporal_calls == 0
    assert queries.calls == 0


@pytest.mark.asyncio
async def test_batch_details_are_bounded_to_returned_filtered_rows() -> None:
    page = BatchOperationPageFact(
        operations=(
            BatchOperationSummaryFact("job-1", "RUNNING", "RESET", _now(), None),
            BatchOperationSummaryFact("job-2", "COMPLETED", "RESET", _now(), _now()),
        ),
        next_page_token=None,
        observed_at=_now(),
    )
    batches = _BatchGateway(page)
    service = _service(batches=batches)

    response = await service.list_batch_operations(
        _context(),
        ListBatchOperationsRequest(target_ref="orders-prod", state="RUNNING"),
    )

    assert [row.job_id for row in response.answer.operations] == ["job-1"]
    assert response.answer.operations[0].detail_status == "complete"
    assert batches.described == ["job-1"]


@pytest.mark.asyncio
async def test_batch_detail_propagates_systemic_temporal_errors() -> None:
    page = BatchOperationPageFact(
        operations=(BatchOperationSummaryFact("job-1", "RUNNING", "RESET", _now(), None),),
        next_page_token=None,
        observed_at=_now(),
    )

    class UnavailableBatchGateway(_BatchGateway):
        async def describe(
            self, context: RequestContext, target: ResolvedTarget, job_id: str
        ) -> BatchOperationDetailFact:
            raise OpsMcpError(
                ErrorCode.TEMPORAL_UNREACHABLE,
                "Temporal is unavailable",
                retryable=True,
            )

    service = _service(batches=UnavailableBatchGateway(page))

    with pytest.raises(OpsMcpError) as caught:
        await service.list_batch_operations(
            _context(), ListBatchOperationsRequest(target_ref="orders-prod")
        )

    assert caught.value.code is ErrorCode.TEMPORAL_UNREACHABLE


class _Catalog:
    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        assert principal.subject == "tester"
        assert target_ref == "orders-prod"
        return _target()


class _ScheduleGateway:
    def __init__(
        self,
        rows: tuple[ScheduleSummaryFact, ...],
        *,
        next_token: bytes | None = None,
        description: ScheduleDescriptionFact | None = None,
    ) -> None:
        self.rows = rows
        self.next_token = next_token
        self.description = description

    async def list_page(self, context: RequestContext, *args: object, **kwargs: object):
        context.budget.reserve_temporal_call("list_schedules")
        context.budget.consume_page("list_schedules")
        return self.rows, self.next_token

    async def describe(self, context: RequestContext, *args: object, **kwargs: object):
        assert self.description
        context.budget.reserve_temporal_call("describe_schedule")
        if kwargs["upcoming_count"]:
            context.budget.reserve_temporal_call("matching_times")
        return self.description


class _QueryGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def query(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        query_name: str,
    ) -> QueryResultFact:
        self.calls += 1
        context.budget.reserve_temporal_call("query")
        return QueryResultFact(
            workflow_id,
            run_id,
            query_name,
            {"activity": "EnrichProduct"},
            "decoded",
            _now(),
        )


class _BatchGateway:
    def __init__(self, page: BatchOperationPageFact) -> None:
        self.page = page
        self.described: list[str] = []

    async def list_page(self, context: RequestContext, *args: object, **kwargs: object):
        context.budget.reserve_temporal_call("list_batch")
        context.budget.consume_page("list_batch")
        return self.page

    async def describe(
        self, context: RequestContext, target: ResolvedTarget, job_id: str
    ) -> BatchOperationDetailFact:
        self.described.append(job_id)
        context.budget.reserve_temporal_call("describe_batch")
        return BatchOperationDetailFact(
            job_id,
            "RUNNING",
            "RESET",
            _now(),
            None,
            10,
            4,
            1,
            "tester",
            "demo",
            "ExecutionStatus = 'Running'",
        )


def _service(
    *,
    schedules: object | None = None,
    queries: object | None = None,
    batches: object | None = None,
    approved: frozenset[str] = frozenset(),
) -> DefaultNamespaceReadService:
    return DefaultNamespaceReadService(
        _Catalog(),
        schedules or _ScheduleGateway(()),
        queries or _QueryGateway(),
        batches or _BatchGateway(BatchOperationPageFact((), None, _now())),
        ConfiguredQueryApprovalPolicy({"orders-prod": approved}),
    )


def _target() -> ResolvedTarget:
    connection = ConnectionDescriptor("localhost:7233", "default")
    return ResolvedTarget("orders-prod", (), "demo", "local", "default", connection)


def _context() -> RequestContext:
    return RequestContext(
        "request-1",
        Principal("tester", ("test",)),
        BasicExecutionBudget(deadline=_now() + timedelta(seconds=30)),
    )


def _now() -> datetime:
    return datetime.now(UTC)

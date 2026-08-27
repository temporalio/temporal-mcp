from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.api.taskqueue.v1 import PollerInfo, TaskQueueStats
from temporalio.api.workflowservice.v1 import DescribeTaskQueueResponse

from ops_mcp.facts.task_queues import TaskQueueKind
from ops_mcp.gateways.task_queues import SdkTaskQueueGateway
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_describe_activity_queue_returns_current_snapshot_and_charges_call() -> None:
    poller = PollerInfo(identity="worker-1", rate_per_second=7.5)
    poller.last_access_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    stats = TaskQueueStats(
        approximate_backlog_count=4,
        tasks_add_rate=2.0,
        tasks_dispatch_rate=3.0,
        rate_limiting_active=True,
    )
    stats.approximate_backlog_age.FromTimedelta(timedelta(seconds=8))
    service = _TaskQueueService(DescribeTaskQueueResponse(pollers=[poller], stats=stats))
    context = _context()
    target = _target()

    gateway = SdkTaskQueueGateway(_Provider(SimpleNamespace(workflow_service=service)))
    result = await gateway.describe(context, target, "activities", TaskQueueKind.ACTIVITY)

    assert result.currently_has_pollers
    assert result.task_type is TaskQueueKind.ACTIVITY
    assert result.pollers[0].identity == "worker-1"
    assert result.approximate_backlog_age_seconds == 8
    assert context.budget.snapshot().temporal_calls == 1
    request, kwargs = service.calls[0]
    assert request.task_queue.name == "activities"
    assert request.report_stats and request.report_pollers
    assert kwargs["timeout"] <= timedelta(seconds=30)


class _TaskQueueService:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[object] = []

    async def describe_task_queue(self, request: object, **kwargs: object) -> object:
        self.calls.append((request, kwargs))
        return self.response


class _Provider:
    def __init__(self, client: object) -> None:
        self.client = client

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> object:
        return self.client


def _context() -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="tester", groups=("test",)),
        budget=BasicExecutionBudget(deadline=datetime.now(UTC) + timedelta(seconds=30)),
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

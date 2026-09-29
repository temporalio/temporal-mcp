"""Explicit workflow/activity task-queue description gateway."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.service import RPCError

from ops_mcp.clients import TemporalClientProvider, acquire_client
from ops_mcp.facts.task_queues import PollerFact, TaskQueueFact, TaskQueueKind
from ops_mcp.gateways.history import translate_rpc_error
from ops_mcp.runtime.context import RequestContext
from ops_mcp.targets.models import ResolvedTarget


class TemporalTaskQueueGateway(Protocol):
    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        task_queue: str,
        task_type: TaskQueueKind,
    ) -> TaskQueueFact: ...


class SdkTaskQueueGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        task_queue: str,
        task_type: TaskQueueKind,
    ) -> TaskQueueFact:
        if not task_queue:
            raise ValueError("task_queue is required")
        if not isinstance(task_type, TaskQueueKind):
            raise ValueError("task_type must be workflow or activity")
        client = await acquire_client(self._provider, context, target)
        context.budget.reserve_temporal_call("DescribeTaskQueue")
        try:
            response = await client.workflow_service.describe_task_queue(
                DescribeTaskQueueRequest(
                    namespace=target.namespace,
                    task_queue=TaskQueue(name=task_queue),
                    task_queue_type=(
                        TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW
                        if task_type is TaskQueueKind.WORKFLOW
                        else TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY
                    ),
                    report_stats=True,
                    report_pollers=True,
                ),
                retry=True,
                timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            raise translate_rpc_error(exc, operation="describe_task_queue") from exc
        stats = response.stats
        pollers = tuple(
            PollerFact(
                identity=poller.identity,
                last_access_time=(
                    poller.last_access_time.ToDatetime().replace(tzinfo=UTC)
                    if poller.HasField("last_access_time")
                    else None
                ),
                rate_per_second=poller.rate_per_second,
            )
            for poller in response.pollers
        )
        return TaskQueueFact(
            task_queue=task_queue,
            task_type=task_type,
            pollers=pollers,
            approximate_backlog_count=stats.approximate_backlog_count,
            approximate_backlog_age_seconds=(
                stats.approximate_backlog_age.ToTimedelta().total_seconds()
            ),
            tasks_add_rate=stats.tasks_add_rate,
            tasks_dispatch_rate=stats.tasks_dispatch_rate,
            rate_limiting_active=stats.rate_limiting_active,
            observed_at=datetime.now(UTC),
        )

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.api.activity.v1 import ActivityOptions
from temporalio.api.common.v1 import ActivityType, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import PendingActivityState, WorkflowExecutionStatus
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflow.v1 import PendingActivityInfo, WorkflowExecutionInfo
from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse
from temporalio.client import WorkflowExecutionDescription
from temporalio.converter import DataConverter

from ops_mcp.gateways.workflows import SdkWorkflowGateway
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_describe_returns_typed_workflow_and_pending_activity_facts() -> None:
    info = WorkflowExecutionInfo(
        execution=WorkflowExecution(workflow_id="order-1", run_id="run-1"),
        type=WorkflowType(name="OrderWorkflow"),
        status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
        task_queue="orders",
        history_length=12,
        history_size_bytes=1234,
        first_run_id="run-1",
    )
    info.start_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    raw = DescribeWorkflowExecutionResponse(
        workflow_execution_info=info,
        pending_activities=[
            PendingActivityInfo(
                activity_id="enrich",
                activity_type=ActivityType(name="EnrichProduct"),
                state=PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED,
                attempt=2,
                maximum_attempts=3,
                activity_options=ActivityOptions(task_queue=TaskQueue(name="catalog-activities")),
            )
        ],
    )
    heartbeat_time = datetime(2026, 1, 1, 1, tzinfo=UTC)
    raw.pending_activities[0].last_heartbeat_time.FromDatetime(heartbeat_time)
    raw.pending_activities[0].heartbeat_details.payloads.extend(
        await DataConverter.default.encode([{"cursor": 42}])
    )
    description = await WorkflowExecutionDescription._from_raw_description(
        raw, "default", DataConverter.default
    )
    handle = _Handle(description)
    provider = _Provider(SimpleNamespace(get_workflow_handle=lambda *args, **kwargs: handle))
    context = _context()

    result = await SdkWorkflowGateway(provider).describe(
        context, _target(), "order-1", run_id="run-1"
    )

    assert result.workflow_id == "order-1"
    assert result.run_id == "run-1"
    assert result.workflow_type == "OrderWorkflow"
    assert result.pending_activities[0].activity_type == "EnrichProduct"
    assert result.pending_activities[0].attempt == 2
    assert result.pending_activities[0].activity_task_queue == "catalog-activities"
    assert result.pending_activities[0].last_heartbeat_time == heartbeat_time
    assert result.pending_activities[0].heartbeat_details.values == ({"cursor": 42},)
    assert context.budget.snapshot().temporal_calls == 1
    assert handle.timeout is not None and handle.timeout <= timedelta(seconds=30)

    failed_decode = await SdkWorkflowGateway(
        _Provider(
            SimpleNamespace(
                get_workflow_handle=lambda *args, **kwargs: handle,
                data_converter=_FailingConverter(),
            )
        )
    ).describe(_context(), _target(), "order-1", run_id="run-1")
    assert failed_decode.pending_activities[0].heartbeat_details.decode_status == "decode_failed"


class _FailingConverter:
    async def decode(self, payloads: object) -> object:
        del payloads
        raise ValueError("intentionally undecodable")


class _Handle:
    def __init__(self, description: WorkflowExecutionDescription) -> None:
        self.description = description
        self.timeout: timedelta | None = None

    async def describe(self, *, rpc_timeout: timedelta) -> WorkflowExecutionDescription:
        self.timeout = rpc_timeout
        return self.description


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

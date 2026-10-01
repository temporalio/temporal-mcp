from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.api.common.v1 import ActivityType, WorkerVersionStamp, WorkflowExecution
from temporalio.api.enums.v1 import EventType, RetryState
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import History, HistoryEvent
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryResponse,
    GetWorkflowExecutionHistoryReverseResponse,
)
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    UnknownEventAttributes,
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.gateways.history import SdkHistoryGateway
from ops_mcp.runtime.budget import BasicBudgetLimits, BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


def _context(*, history_events: int = 20) -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="tester", groups=("test",)),
        budget=BasicExecutionBudget(
            deadline=datetime.now(UTC) + timedelta(seconds=30),
            limits=BasicBudgetLimits(temporal_calls=10, pages=10, history_events=history_events),
        ),
    )


def _target() -> ResolvedTarget:
    descriptor = ConnectionDescriptor(address="localhost:7233", namespace="default")
    return ResolvedTarget(
        target_ref="orders-prod",
        aliases=("orders prod",),
        account="demo",
        cluster="local",
        namespace="default",
        connection=descriptor,
    )


def _event(event_id: int, event_type: int, attribute_name: str, attributes: object) -> HistoryEvent:
    event = HistoryEvent(event_id=event_id, event_type=event_type, task_id=event_id)
    event.event_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    getattr(event, attribute_name).CopyFrom(attributes)
    return event


class _Provider:
    def __init__(self, client: object) -> None:
        self.client = client
        self.calls: list[tuple[str, ConnectionDescriptor]] = []

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> object:
        self.calls.append((target_ref, descriptor))
        return self.client


class _HistoryService:
    def __init__(self, forward: list[object], reverse: list[object] | None = None) -> None:
        self.forward = forward
        self.reverse = reverse or []
        self.forward_requests: list[object] = []
        self.reverse_requests: list[object] = []

    async def get_workflow_execution_history(self, request: object, **kwargs: object) -> object:
        self.forward_requests.append((request, kwargs))
        value = self.forward.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    async def get_workflow_execution_history_reverse(
        self, request: object, **kwargs: object
    ) -> object:
        self.reverse_requests.append((request, kwargs))
        return self.reverse.pop(0)


@pytest.mark.asyncio
async def test_forward_history_pages_are_charged_and_normalized() -> None:
    scheduled = _event(
        2,
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        "activity_task_scheduled_event_attributes",
        __import__(
            "temporalio.api.history.v1", fromlist=["ActivityTaskScheduledEventAttributes"]
        ).ActivityTaskScheduledEventAttributes(
            activity_id="enrich",
            activity_type=ActivityType(name="EnrichProduct"),
            task_queue=TaskQueue(name="activities"),
        ),
    )
    failure = Failure(
        message="catalog 503",
        application_failure_info=ApplicationFailureInfo(
            type="CatalogServiceUnavailable", non_retryable=False
        ),
    )
    failed = _event(
        3,
        EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
        "activity_task_failed_event_attributes",
        __import__(
            "temporalio.api.history.v1", fromlist=["ActivityTaskFailedEventAttributes"]
        ).ActivityTaskFailedEventAttributes(
            scheduled_event_id=2,
            started_event_id=2,
            retry_state=RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
            failure=failure,
        ),
    )
    unknown = _event(
        4,
        EventType.EVENT_TYPE_TIMER_STARTED,
        "timer_started_event_attributes",
        __import__(
            "temporalio.api.history.v1", fromlist=["TimerStartedEventAttributes"]
        ).TimerStartedEventAttributes(timer_id="timer-1"),
    )
    service = _HistoryService(
        [
            GetWorkflowExecutionHistoryResponse(
                history=History(events=[scheduled, failed]), next_page_token=b"two"
            ),
            GetWorkflowExecutionHistoryResponse(history=History(events=[unknown])),
        ]
    )
    client = SimpleNamespace(workflow_service=service, data_converter=DataConverter.default)
    context = _context()

    result = await SdkHistoryGateway(_Provider(client)).read(
        context, _target(), "order-1", "run-1", page_size=2
    )

    assert result.complete
    assert result.pages_read == 2
    assert [event.event_id for event in result.events] == [2, 3, 4]
    scheduled_fact = result.events[0].attributes
    assert isinstance(scheduled_fact, ActivityTaskEventAttributes)
    assert scheduled_fact.activity_type == "EnrichProduct"
    failed_fact = result.events[1].attributes
    assert isinstance(failed_fact, ActivityTaskEventAttributes)
    assert failed_fact.failure
    assert failed_fact.failure.application_type == "CatalogServiceUnavailable"
    assert isinstance(result.events[2].attributes, UnknownEventAttributes)
    assert context.budget.snapshot().temporal_calls == 2
    assert context.budget.snapshot().pages_read == 2
    assert context.budget.snapshot().history_events_read == 3
    assert service.forward_requests[1][0].next_page_token == b"two"


@pytest.mark.asyncio
@pytest.mark.parametrize(("delivered", "limit"), [(365, 200), (560, 500)])
async def test_server_overdelivery_resumes_same_page_without_skip_or_duplicate(
    delivered: int, limit: int
) -> None:
    events = [
        HistoryEvent(
            event_id=index,
            event_type=EventType.EVENT_TYPE_MARKER_RECORDED,
            task_id=index,
        )
        for index in range(1, delivered + 1)
    ]
    for event in events:
        event.event_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    response = GetWorkflowExecutionHistoryResponse(
        history=History(events=events), next_page_token=b"server-next"
    )
    service = _HistoryService([response, response])
    gateway = SdkHistoryGateway(
        _Provider(SimpleNamespace(workflow_service=service, data_converter=DataConverter.default))
    )

    first_context = _context(history_events=limit)
    first = await gateway.read(
        first_context,
        _target(),
        "order-1",
        "run-1",
        max_events=limit,
        page_size=limit,
    )
    assert [event.event_id for event in first.events] == list(range(1, limit + 1))
    assert first.next_page_token == b""
    assert first.next_page_offset == limit
    assert first.next_expected_event_id == limit + 1
    assert first_context.budget.snapshot().history_events_read == limit

    remaining = delivered - limit
    second_context = _context(history_events=remaining)
    second = await gateway.read(
        second_context,
        _target(),
        "order-1",
        "run-1",
        max_events=remaining,
        page_size=limit,
        next_page_token=first.next_page_token,
        page_offset=first.next_page_offset,
        expected_event_id=first.next_expected_event_id,
    )
    assert [event.event_id for event in second.events] == list(range(limit + 1, delivered + 1))
    assert second.next_page_token == b"server-next"
    assert second.next_page_offset == 0
    assert second_context.budget.snapshot().history_events_read == remaining


@pytest.mark.asyncio
async def test_reverse_overdelivery_resume_validates_descending_boundary() -> None:
    events = [
        HistoryEvent(
            event_id=index,
            event_type=EventType.EVENT_TYPE_MARKER_RECORDED,
            task_id=index,
        )
        for index in range(365, 0, -1)
    ]
    for event in events:
        event.event_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    response = GetWorkflowExecutionHistoryReverseResponse(history=History(events=events))
    service = _HistoryService([], [response, response])
    gateway = SdkHistoryGateway(
        _Provider(SimpleNamespace(workflow_service=service, data_converter=DataConverter.default))
    )

    first = await gateway.read(
        _context(history_events=200),
        _target(),
        "order-1",
        "run-1",
        max_events=200,
        reverse=True,
    )
    second = await gateway.read(
        _context(history_events=165),
        _target(),
        "order-1",
        "run-1",
        max_events=165,
        next_page_token=first.next_page_token,
        page_offset=first.next_page_offset,
        expected_event_id=first.next_expected_event_id,
        reverse=True,
    )

    combined = [event.event_id for event in first.events + second.events]
    assert combined == list(range(365, 0, -1))
    assert len(combined) == len(set(combined))


@pytest.mark.asyncio
async def test_overdelivery_resume_rejects_changed_history_boundary_without_charging_events() -> (
    None
):
    events = [
        HistoryEvent(
            event_id=index,
            event_type=EventType.EVENT_TYPE_MARKER_RECORDED,
            task_id=index,
        )
        for index in range(1, 5)
    ]
    for event in events:
        event.event_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    service = _HistoryService([GetWorkflowExecutionHistoryResponse(history=History(events=events))])
    gateway = SdkHistoryGateway(
        _Provider(SimpleNamespace(workflow_service=service, data_converter=DataConverter.default))
    )
    context = _context(history_events=2)

    with pytest.raises(OpsMcpError) as exc_info:
        await gateway.read(
            context,
            _target(),
            "order-1",
            "run-1",
            max_events=2,
            page_offset=2,
            expected_event_id=999,
        )

    assert exc_info.value.code is ErrorCode.INVALID_ARGUMENT
    assert context.budget.snapshot().history_events_read == 0


@pytest.mark.asyncio
async def test_history_rejects_native_page_above_resource_limit_without_charging_events() -> None:
    events = [HistoryEvent(event_id=index) for index in range(1, 10_002)]
    service = _HistoryService([GetWorkflowExecutionHistoryResponse(history=History(events=events))])
    gateway = SdkHistoryGateway(
        _Provider(SimpleNamespace(workflow_service=service, data_converter=DataConverter.default))
    )
    context = _context(history_events=2)

    with pytest.raises(OpsMcpError) as exc_info:
        await gateway.read(context, _target(), "order-1", "run-1", max_events=2)

    assert exc_info.value.code is ErrorCode.INTERNAL_ERROR
    assert context.budget.snapshot().history_events_read == 0


@pytest.mark.asyncio
async def test_workflow_task_started_preserves_worker_build_id() -> None:
    started = _event(
        8,
        EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
        "workflow_task_started_event_attributes",
        __import__(
            "temporalio.api.history.v1", fromlist=["WorkflowTaskStartedEventAttributes"]
        ).WorkflowTaskStartedEventAttributes(
            scheduled_event_id=7,
            identity="worker-v2",
            worker_version=WorkerVersionStamp(build_id="fulfillment-v2"),
        ),
    )
    service = _HistoryService(
        [GetWorkflowExecutionHistoryResponse(history=History(events=[started]))]
    )
    client = SimpleNamespace(workflow_service=service, data_converter=DataConverter.default)

    result = await SdkHistoryGateway(_Provider(client)).read(
        _context(), _target(), "fulfillment-1", "run-1"
    )

    attributes = result.events[0].attributes
    assert isinstance(attributes, WorkflowTaskEventAttributes)
    assert attributes.phase == "started"
    assert attributes.worker_build_id == "fulfillment-v2"


@pytest.mark.asyncio
async def test_reverse_closing_event_returns_terminal_fact_only() -> None:
    complete = _event(
        9,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
        "workflow_execution_completed_event_attributes",
        __import__(
            "temporalio.api.history.v1", fromlist=["WorkflowExecutionCompletedEventAttributes"]
        ).WorkflowExecutionCompletedEventAttributes(),
    )
    service = _HistoryService(
        [],
        [GetWorkflowExecutionHistoryReverseResponse(history=History(events=[complete]))],
    )
    client = SimpleNamespace(workflow_service=service, data_converter=DataConverter.default)

    result = await SdkHistoryGateway(_Provider(client)).closing_event(
        _context(), _target(), "order-1", "run-1"
    )

    assert result is not None
    assert isinstance(result.attributes, WorkflowCloseEventAttributes)
    assert result.attributes.state == "completed"
    request, kwargs = service.reverse_requests[0]
    assert request.execution == WorkflowExecution(workflow_id="order-1", run_id="run-1")
    assert kwargs["timeout"] <= timedelta(seconds=30)


@pytest.mark.asyncio
async def test_missing_exact_run_is_classified_without_transport_message() -> None:
    service = _HistoryService([RPCError("server details", RPCStatusCode.NOT_FOUND, b"")])
    client = SimpleNamespace(workflow_service=service, data_converter=DataConverter.default)

    with pytest.raises(OpsMcpError) as caught:
        await SdkHistoryGateway(_Provider(client)).read(
            _context(), _target(), "order-1", "missing-run"
        )

    assert caught.value.code is ErrorCode.RUN_NOT_FOUND
    assert "server details" not in caught.value.message

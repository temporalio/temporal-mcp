"""Authorization-aware services for direct workflow read capabilities."""

from __future__ import annotations

import base64
import binascii
from dataclasses import asdict
from typing import Protocol, TypeVar

from ops_mcp.contracts.primitives import Completeness, ContractModel, Inspection
from ops_mcp.contracts.workflow_reads import (
    DecodedPayloadsOutput,
    DescribeTaskQueueRequest,
    DescribeTaskQueueResponse,
    DescribeWorkflowRequest,
    DescribeWorkflowResponse,
    FailureOutput,
    GetWorkflowResultRequest,
    GetWorkflowResultResponse,
    HistoryEventOutput,
    HistorySliceAnswer,
    HistorySliceRequest,
    HistorySliceResponse,
    PendingActivityOutput,
    PendingChildOutput,
    PendingWorkflowTaskOutput,
    PollerOutput,
    TaskQueueDescriptionOutput,
    WorkflowDescriptionOutput,
    WorkflowResultAnswer,
    WorkflowResultOutput,
)
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    ContinueAsNewEventAttributes,
    DecodedPayloadsFact,
    FailureFact,
    HistoryEventFact,
    SignalEventAttributes,
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import TaskQueueKind
from ops_mcp.facts.workflows import WorkflowExecutionFact
from ops_mcp.gateways.history import TemporalHistoryGateway
from ops_mcp.gateways.task_queues import TemporalTaskQueueGateway
from ops_mcp.gateways.workflows import TemporalWorkflowGateway
from ops_mcp.runtime.budget import BudgetSnapshot
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import DeadlineExceededError, ErrorCode, OpsMcpError
from ops_mcp.targets.catalog import TargetCatalog
from ops_mcp.targets.models import ResolvedTarget


class WorkflowReadService(Protocol):
    """Cohesive direct-read surface implemented by one service object."""

    async def describe_workflow(
        self, context: RequestContext, request: DescribeWorkflowRequest
    ) -> DescribeWorkflowResponse: ...

    async def get_workflow_history_slice(
        self, context: RequestContext, request: HistorySliceRequest
    ) -> HistorySliceResponse: ...

    async def get_workflow_result(
        self, context: RequestContext, request: GetWorkflowResultRequest
    ) -> GetWorkflowResultResponse: ...

    async def describe_task_queue(
        self, context: RequestContext, request: DescribeTaskQueueRequest
    ) -> DescribeTaskQueueResponse: ...


class DefaultWorkflowReadService:
    """Concrete direct-read service; contains no Temporal SDK or MCP transport types."""

    def __init__(
        self,
        catalog: TargetCatalog,
        workflow_gateway: TemporalWorkflowGateway,
        history_gateway: TemporalHistoryGateway,
        task_queue_gateway: TemporalTaskQueueGateway,
    ) -> None:
        self._catalog = catalog
        self._workflows = workflow_gateway
        self._history = history_gateway
        self._task_queues = task_queue_gateway

    async def describe_workflow(
        self, context: RequestContext, request: DescribeWorkflowRequest
    ) -> DescribeWorkflowResponse:
        target = self._authorize(context, request.target_ref)
        before = context.budget.snapshot()
        fact = await self._workflows.describe(context, target, request.workflow_id, request.run_id)
        response = DescribeWorkflowResponse(
            target_ref=target.target_ref,
            answer=_workflow_output(fact),
            inspection=_inspection(context, before, Completeness.COMPLETE),
            warnings=_workflow_decode_warnings(fact),
        )
        return _charge_output(context, response)

    async def get_workflow_history_slice(
        self, context: RequestContext, request: HistorySliceRequest
    ) -> HistorySliceResponse:
        target = self._authorize(context, request.target_ref)
        before = context.budget.snapshot()
        reverse = request.last_n is not None
        scan_limit = request.last_n or request.limit
        read = await self._history.read(
            context,
            target,
            request.workflow_id,
            request.run_id,
            page_size=scan_limit,
            max_events=scan_limit,
            next_page_token=_decode_page_token(request.page_token),
            reverse=reverse,
        )
        scanned = tuple(sorted(read.events, key=lambda event: event.event_id))
        selected = tuple(event for event in scanned if _matches_selector(event, request))
        if request.event_types:
            allowed = frozenset(request.event_types)
            selected = tuple(event for event in selected if event.event_type in allowed)

        selector_complete = request.last_n is not None or _range_was_completed(scanned, request)
        completion = (
            Completeness.COMPLETE if read.complete or selector_complete else Completeness.TRUNCATED
        )
        next_token = None if selector_complete else _encode_page_token(read.next_page_token)
        truncation_warnings = (
            ("More history is available; continue with next_page_token.",)
            if completion is Completeness.TRUNCATED
            else ()
        )
        warnings = truncation_warnings + _history_decode_warnings(selected)
        response = HistorySliceResponse(
            target_ref=target.target_ref,
            answer=HistorySliceAnswer(
                workflow_id=request.workflow_id,
                run_id=request.run_id,
                events=tuple(_history_event_output(event) for event in selected),
                inspected_event_id_range=_event_range(scanned),
                returned_event_id_range=_event_range(selected),
                continuation_token=next_token,
            ),
            inspection=_inspection(
                context,
                before,
                completion,
                events_read=read.events_scanned,
                limit=scan_limit,
            ),
            warnings=warnings,
        )
        return _charge_output(context, response)

    async def get_workflow_result(
        self, context: RequestContext, request: GetWorkflowResultRequest
    ) -> GetWorkflowResultResponse:
        target = self._authorize(context, request.target_ref)
        before = context.budget.snapshot()
        bounded_wait_timeout = bool(
            request.wait
            and request.wait_timeout_seconds is not None
            and request.wait_timeout_seconds < context.budget.remaining_seconds()
        )
        try:
            event = await self._history.closing_event(
                context,
                target,
                request.workflow_id,
                request.run_id,
                wait=request.wait,
                wait_timeout_seconds=request.wait_timeout_seconds,
            )
        except DeadlineExceededError:
            if not bounded_wait_timeout:
                raise
            event = None

        result = _workflow_result(event)
        warnings = (
            ("The terminal payload could not be decoded.",)
            if result.result is not None and result.result.decode_status == "decode_failed"
            else ()
        )
        response = GetWorkflowResultResponse(
            target_ref=target.target_ref,
            answer=WorkflowResultAnswer(
                workflow_id=request.workflow_id,
                run_id=request.run_id,
                result=result,
            ),
            inspection=_inspection(
                context,
                before,
                Completeness.COMPLETE,
                events_read=1 if event is not None else 0,
                limit=1,
            ),
            warnings=warnings,
        )
        return _charge_output(context, response)

    async def describe_task_queue(
        self, context: RequestContext, request: DescribeTaskQueueRequest
    ) -> DescribeTaskQueueResponse:
        target = self._authorize(context, request.target_ref)
        before = context.budget.snapshot()
        queue = await self._task_queues.describe(
            context, target, request.task_queue, TaskQueueKind(request.task_type)
        )
        response = DescribeTaskQueueResponse(
            target_ref=target.target_ref,
            answer=TaskQueueDescriptionOutput(
                task_queue=queue.task_queue,
                task_type=queue.task_type.value,
                pollers=tuple(
                    PollerOutput(
                        identity=poller.identity,
                        last_access_time=poller.last_access_time,
                        rate_per_second=poller.rate_per_second,
                    )
                    for poller in queue.pollers
                ),
                poller_count=len(queue.pollers),
                currently_has_pollers=queue.currently_has_pollers,
                approximate_backlog_count=queue.approximate_backlog_count,
                approximate_backlog_age_seconds=queue.approximate_backlog_age_seconds,
                tasks_add_rate=queue.tasks_add_rate,
                tasks_dispatch_rate=queue.tasks_dispatch_rate,
                rate_limiting_active=queue.rate_limiting_active,
                observed_at=queue.observed_at,
            ),
            inspection=_inspection(context, before, Completeness.COMPLETE),
        )
        return _charge_output(context, response)

    def _authorize(self, context: RequestContext, target_ref: str) -> ResolvedTarget:
        return self._catalog.authorize(context.principal, target_ref)


def _workflow_output(fact: WorkflowExecutionFact) -> WorkflowDescriptionOutput:
    if fact.observed_at is None:
        raise ValueError("workflow fact must contain observed_at")
    return WorkflowDescriptionOutput(
        workflow_id=fact.workflow_id,
        run_id=fact.run_id,
        workflow_type=fact.workflow_type,
        status=fact.status,
        task_queue=fact.task_queue,
        namespace=fact.namespace,
        start_time=fact.start_time,
        execution_time=fact.execution_time,
        close_time=fact.close_time,
        history_length=fact.history_length,
        history_size_bytes=fact.history_size_bytes,
        first_run_id=fact.first_run_id,
        parent_workflow_id=fact.parent_workflow_id,
        parent_run_id=fact.parent_run_id,
        root_workflow_id=fact.root_workflow_id,
        root_run_id=fact.root_run_id,
        memo={key: _decoded_payloads(value) for key, value in fact.memo.items()},
        search_attributes=fact.search_attributes,
        pending_activities=tuple(
            PendingActivityOutput(
                activity_id=item.activity_id,
                activity_type=item.activity_type,
                state=item.state,
                attempt=item.attempt,
                maximum_attempts=item.maximum_attempts,
                scheduled_time=item.scheduled_time,
                last_started_time=item.last_started_time,
                next_attempt_time=item.next_attempt_time,
                last_worker_identity=item.last_worker_identity,
                last_failure=_failure_output(item.last_failure),
                paused=item.paused,
                activity_task_queue=item.activity_task_queue,
                last_heartbeat_time=item.last_heartbeat_time,
                heartbeat_details=_decoded_payloads(item.heartbeat_details),
            )
            for item in fact.pending_activities
        ),
        pending_children=tuple(
            PendingChildOutput(
                workflow_id=item.workflow_id,
                run_id=item.run_id,
                workflow_type=item.workflow_type,
                initiated_event_id=item.initiated_event_id,
                parent_close_policy=item.parent_close_policy,
            )
            for item in fact.pending_children
        ),
        pending_workflow_task=(
            PendingWorkflowTaskOutput(
                state=fact.pending_workflow_task.state,
                attempt=fact.pending_workflow_task.attempt,
                scheduled_time=fact.pending_workflow_task.scheduled_time,
                started_time=fact.pending_workflow_task.started_time,
            )
            if fact.pending_workflow_task
            else None
        ),
        observed_at=fact.observed_at,
    )


def _workflow_decode_warnings(fact: WorkflowExecutionFact) -> tuple[str, ...]:
    failed = tuple(
        key for key, value in fact.memo.items() if value.decode_status == "decode_failed"
    )
    return (f"Memo fields could not be decoded: {', '.join(sorted(failed))}",) if failed else ()


def _history_decode_warnings(events: tuple[HistoryEventFact, ...]) -> tuple[str, ...]:
    failed_event_ids: list[str] = []
    for event in events:
        attributes = event.attributes
        payload = None
        if isinstance(attributes, WorkflowCloseEventAttributes):
            payload = attributes.result
        elif isinstance(attributes, SignalEventAttributes):
            payload = attributes.input
        if payload is not None and payload.decode_status == "decode_failed":
            failed_event_ids.append(str(event.event_id))
    return (
        (f"Payloads could not be decoded for events: {', '.join(failed_event_ids)}",)
        if failed_event_ids
        else ()
    )


def _matches_selector(event: HistoryEventFact, request: HistorySliceRequest) -> bool:
    if request.failures_only:
        attributes = event.attributes
        return (
            (
                isinstance(attributes, ActivityTaskEventAttributes)
                and attributes.phase in {"failed", "timed_out"}
            )
            or (
                isinstance(attributes, WorkflowTaskEventAttributes)
                and attributes.phase in {"failed", "timed_out"}
            )
            or (
                isinstance(attributes, WorkflowCloseEventAttributes)
                and attributes.state in {"failed", "timed_out"}
            )
        )
    if request.event_id_range:
        return request.event_id_range.first <= event.event_id <= request.event_id_range.last
    return True


def _range_was_completed(
    scanned: tuple[HistoryEventFact, ...], request: HistorySliceRequest
) -> bool:
    return bool(
        request.event_id_range and scanned and scanned[-1].event_id >= request.event_id_range.last
    )


def _history_event_output(event: HistoryEventFact) -> HistoryEventOutput:
    return HistoryEventOutput(
        event_id=event.event_id,
        event_type=event.event_type,
        event_time=event.event_time,
        task_id=event.task_id,
        attributes=asdict(event.attributes),
    )


def _workflow_result(event: HistoryEventFact | None) -> WorkflowResultOutput:
    if event is None:
        return WorkflowResultOutput(state="running")
    attributes = event.attributes
    if isinstance(attributes, ContinueAsNewEventAttributes):
        return WorkflowResultOutput(
            state="continued_as_new",
            successor_run_id=attributes.new_execution_run_id,
            closing_event_id=event.event_id,
        )
    if not isinstance(attributes, WorkflowCloseEventAttributes):
        return WorkflowResultOutput(state="running")
    return WorkflowResultOutput(
        state=attributes.state,
        result=_decoded_payloads(attributes.result) if attributes.state == "completed" else None,
        failure=_failure_output(attributes.failure),
        successor_run_id=attributes.new_execution_run_id,
        closing_event_id=event.event_id,
    )


def _decoded_payloads(value: DecodedPayloadsFact) -> DecodedPayloadsOutput:
    return DecodedPayloadsOutput(decode_status=value.decode_status, values=value.values)


def _failure_output(value: FailureFact | None) -> FailureOutput | None:
    if value is None:
        return None
    return FailureOutput(
        message=value.message,
        source=value.source,
        stack_trace=value.stack_trace,
        application_type=value.application_type,
        non_retryable=value.non_retryable,
        timeout_type=value.timeout_type,
        cause=_failure_output(value.cause),
    )


def _event_range(events: tuple[HistoryEventFact, ...]) -> tuple[int, int] | None:
    return (events[0].event_id, events[-1].event_id) if events else None


def _inspection(
    context: RequestContext,
    before: BudgetSnapshot,
    completion: Completeness,
    *,
    events_read: int | None = None,
    limit: int | None = None,
) -> Inspection:
    after = context.budget.snapshot()
    return Inspection(
        completion=completion,
        temporal_calls=after.temporal_calls - before.temporal_calls,
        pages_read=after.pages_read - before.pages_read,
        events_read=events_read,
        limit=limit,
    )


ResponseModel = TypeVar("ResponseModel", bound=ContractModel)


def _charge_output(context: RequestContext, response: ResponseModel) -> ResponseModel:
    encoded = response.model_dump_json().encode("utf-8")
    context.budget.consume_output_bytes(len(encoded))
    return response


def _encode_page_token(value: bytes | None) -> str | None:
    return base64.urlsafe_b64encode(value).decode("ascii") if value else None


def _decode_page_token(value: str | None) -> bytes | None:
    if not value:
        return None
    try:
        return base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, ValueError, TypeError, binascii.Error) as exc:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The history page token is invalid",
            retryable=False,
        ) from exc

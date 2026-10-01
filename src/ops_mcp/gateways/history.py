"""Bounded Temporal workflow-history reads and normalization."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Protocol, cast

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType, HistoryEventFilterType
from temporalio.api.failure.v1 import Failure
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from ops_mcp.clients import TemporalClientProvider, acquire_client
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    ContinueAsNewEventAttributes,
    DecodedPayloadsFact,
    ExternalInteractionEventAttributes,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    JsonValue,
    SignalEventAttributes,
    UnknownEventAttributes,
    UpdateEventAttributes,
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.continuation import MAX_NATIVE_HISTORY_PAGE_EVENTS
from ops_mcp.runtime.errors import DeadlineExceededError, ErrorCode, OpsMcpError
from ops_mcp.targets.models import ResolvedTarget


class TemporalHistoryGateway(Protocol):
    async def read(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        *,
        page_size: int = 250,
        max_events: int | None = None,
        next_page_token: bytes | None = None,
        page_offset: int = 0,
        expected_event_id: int | None = None,
        reverse: bool = False,
    ) -> HistoryReadFact: ...

    async def closing_event(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        *,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
    ) -> HistoryEventFact | None: ...


class SdkHistoryGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def read(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        *,
        page_size: int = 250,
        max_events: int | None = None,
        next_page_token: bytes | None = None,
        page_offset: int = 0,
        expected_event_id: int | None = None,
        reverse: bool = False,
    ) -> HistoryReadFact:
        if not workflow_id or not run_id:
            raise ValueError("workflow_id and run_id are required")
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        if max_events is not None and max_events <= 0:
            raise ValueError("max_events must be positive when provided")
        if page_offset < 0 or page_offset > MAX_NATIVE_HISTORY_PAGE_EVENTS:
            raise ValueError("page_offset is outside the supported range")
        if (page_offset == 0) != (expected_event_id is None):
            raise ValueError("expected_event_id is required exactly when page_offset is set")
        client = await acquire_client(self._provider, context, target)
        token = next_page_token or b""
        events: list[HistoryEventFact] = []
        pages_read = 0
        resume_offset = page_offset
        resume_expected_event_id = expected_event_id
        budget_stopped = False
        stop = False
        while not stop:
            requested_remaining = None if max_events is None else max_events - len(events)
            budget_remaining = context.budget.remaining_history_events()
            remaining = (
                budget_remaining
                if requested_remaining is None
                else min(requested_remaining, budget_remaining)
            )
            if remaining is not None and remaining <= 0:
                budget_stopped = True
                break
            request_size = page_size if remaining is None else min(page_size, remaining)
            page_start_token = token
            context.budget.reserve_temporal_call(
                "GetWorkflowExecutionHistoryReverse" if reverse else "GetWorkflowExecutionHistory"
            )
            timeout = timedelta(seconds=context.budget.remaining_seconds())
            try:
                if reverse:
                    response = await client.workflow_service.get_workflow_execution_history_reverse(
                        GetWorkflowExecutionHistoryReverseRequest(
                            namespace=target.namespace,
                            execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                            maximum_page_size=request_size,
                            next_page_token=token,
                        ),
                        retry=True,
                        timeout=timeout,
                    )
                else:
                    response = await client.workflow_service.get_workflow_execution_history(
                        GetWorkflowExecutionHistoryRequest(
                            namespace=target.namespace,
                            execution=WorkflowExecution(workflow_id=workflow_id, run_id=run_id),
                            maximum_page_size=request_size,
                            next_page_token=token,
                            skip_archival=False,
                        ),
                        retry=True,
                        timeout=timeout,
                    )
            except RPCError as exc:
                raise translate_rpc_error(
                    exc,
                    exact_run=True,
                    operation=(
                        "get_workflow_execution_history_reverse"
                        if reverse
                        else "get_workflow_execution_history"
                    ),
                ) from exc
            context.budget.consume_page("workflow_history")
            pages_read += 1
            page = tuple(response.history.events)
            if len(page) > MAX_NATIVE_HISTORY_PAGE_EVENTS:
                raise OpsMcpError(
                    ErrorCode.INTERNAL_ERROR,
                    "Temporal returned an unexpectedly large native history page",
                    retryable=False,
                    details={
                        "limit": MAX_NATIVE_HISTORY_PAGE_EVENTS,
                        "returned": len(page),
                    },
                )
            if page_offset:
                if page_offset >= len(page) or page[page_offset].event_id != expected_event_id:
                    raise OpsMcpError(
                        ErrorCode.INVALID_ARGUMENT,
                        "The history continuation boundary is no longer valid",
                        retryable=False,
                    )
            available = page[page_offset:]
            take = len(available) if remaining is None else min(len(available), remaining)
            selected = available[:take]
            context.budget.consume_history_events(len(selected))
            events.extend(await _normalize_events(client.data_converter, selected))
            if take < len(available):
                resume_offset = page_offset + take
                resume_expected_event_id = page[resume_offset].event_id
                token = page_start_token
                stop = True
            else:
                token = response.next_page_token
                page_offset = 0
                resume_offset = 0
                resume_expected_event_id = None
                stop = not token or (max_events is not None and len(events) >= max_events)

        return HistoryReadFact(
            workflow_id=workflow_id,
            run_id=run_id,
            events=tuple(events),
            reverse=reverse,
            pages_read=pages_read,
            events_scanned=len(events),
            next_page_token=(token if resume_offset or budget_stopped else (token or None)),
            observed_at=datetime.now(UTC),
            next_page_offset=resume_offset,
            next_expected_event_id=resume_expected_event_id,
        )

    async def closing_event(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        *,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
    ) -> HistoryEventFact | None:
        if not workflow_id or not run_id:
            raise ValueError("workflow_id and run_id are required")
        client = await acquire_client(self._provider, context, target)
        context.budget.reserve_temporal_call(
            "GetWorkflowExecutionHistory" if wait else "GetWorkflowExecutionHistoryReverse"
        )
        remaining_seconds = context.budget.remaining_seconds()
        timeout = timedelta(
            seconds=(
                min(remaining_seconds, wait_timeout_seconds)
                if wait_timeout_seconds is not None
                else remaining_seconds
            )
        )
        execution = WorkflowExecution(workflow_id=workflow_id, run_id=run_id)
        try:
            if wait:
                response = await client.workflow_service.get_workflow_execution_history(
                    GetWorkflowExecutionHistoryRequest(
                        namespace=target.namespace,
                        execution=execution,
                        maximum_page_size=1,
                        wait_new_event=True,
                        history_event_filter_type=(
                            HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
                        ),
                    ),
                    retry=True,
                    timeout=timeout,
                )
            else:
                response = await client.workflow_service.get_workflow_execution_history_reverse(
                    GetWorkflowExecutionHistoryReverseRequest(
                        namespace=target.namespace,
                        execution=execution,
                        maximum_page_size=1,
                    ),
                    retry=True,
                    timeout=timeout,
                )
        except RPCError as exc:
            raise translate_rpc_error(
                exc,
                exact_run=True,
                operation=(
                    "get_workflow_execution_history"
                    if wait
                    else "get_workflow_execution_history_reverse"
                ),
            ) from exc
        context.budget.consume_page("workflow_closing_event")
        page = tuple(response.history.events)
        context.budget.consume_history_events(len(page))
        if not page:
            return None
        event = await _normalize_event(client.data_converter, page[0])
        return (
            event
            if isinstance(
                event.attributes,
                (WorkflowCloseEventAttributes, ContinueAsNewEventAttributes),
            )
            else None
        )


async def _normalize_events(
    converter: DataConverter, events: Iterable[HistoryEvent]
) -> list[HistoryEventFact]:
    return [await _normalize_event(converter, event) for event in events]


async def _normalize_event(converter: DataConverter, event: HistoryEvent) -> HistoryEventFact:
    attribute_name = event.WhichOneof("attributes")
    attributes = getattr(event, attribute_name) if attribute_name else None
    normalized: Any

    if attribute_name in {
        "workflow_task_started_event_attributes",
        "workflow_task_completed_event_attributes",
        "workflow_task_failed_event_attributes",
        "workflow_task_timed_out_event_attributes",
    }:
        phase = attribute_name.removeprefix("workflow_task_").removesuffix("_event_attributes")
        normalized = WorkflowTaskEventAttributes(
            phase=cast(Any, phase),
            scheduled_event_id=getattr(attributes, "scheduled_event_id", 0),
            started_event_id=getattr(attributes, "started_event_id", 0),
            identity=getattr(attributes, "identity", None) or None,
            worker_build_id=(
                attributes.worker_version.build_id
                if _has_message_field(attributes, "worker_version")
                and attributes.worker_version.build_id
                else None
            ),
            cause=_enum_field_name(attributes, "cause"),
            failure=(
                normalize_failure(attributes.failure)
                if _has_message_field(attributes, "failure")
                else None
            ),
        )
    elif attribute_name in {
        "activity_task_scheduled_event_attributes",
        "activity_task_started_event_attributes",
        "activity_task_completed_event_attributes",
        "activity_task_failed_event_attributes",
        "activity_task_timed_out_event_attributes",
    }:
        phase = attribute_name.removeprefix("activity_task_").removesuffix("_event_attributes")
        normalized = ActivityTaskEventAttributes(
            phase=cast(Any, phase),
            activity_id=getattr(attributes, "activity_id", None) or None,
            activity_type=_nested_name(attributes, "activity_type"),
            task_queue=_nested_name(attributes, "task_queue"),
            scheduled_event_id=getattr(attributes, "scheduled_event_id", 0),
            started_event_id=getattr(attributes, "started_event_id", 0),
            identity=getattr(attributes, "identity", None) or None,
            attempt=getattr(attributes, "attempt", None),
            retry_state=_enum_field_name(attributes, "retry_state"),
            failure=(
                normalize_failure(attributes.failure)
                if _has_message_field(attributes, "failure")
                else None
            ),
        )
    elif attribute_name in {
        "workflow_execution_completed_event_attributes",
        "workflow_execution_failed_event_attributes",
        "workflow_execution_canceled_event_attributes",
        "workflow_execution_terminated_event_attributes",
        "workflow_execution_timed_out_event_attributes",
    }:
        state = attribute_name.removeprefix("workflow_execution_").removesuffix("_event_attributes")
        result = DecodedPayloadsFact()
        if _has_message_field(attributes, "result"):
            result = await _decode_payloads(converter, attributes.result.payloads)
        normalized = WorkflowCloseEventAttributes(
            state=cast(Any, state),
            failure=(
                normalize_failure(attributes.failure)
                if _has_message_field(attributes, "failure")
                else None
            ),
            retry_state=_enum_field_name(attributes, "retry_state"),
            new_execution_run_id=getattr(attributes, "new_execution_run_id", None) or None,
            result=result,
        )
    elif attribute_name == "workflow_execution_signaled_event_attributes":
        normalized = SignalEventAttributes(
            signal_name=attributes.signal_name,
            identity=attributes.identity or None,
            input=await _decode_payloads(converter, attributes.input.payloads),
        )
    elif attribute_name and attribute_name.startswith("workflow_execution_update_"):
        phase = attribute_name.removeprefix("workflow_execution_update_").removesuffix(
            "_event_attributes"
        )
        normalized = UpdateEventAttributes(
            phase=cast(Any, phase),
            protocol_instance_id=getattr(attributes, "protocol_instance_id", None) or None,
            update_id=_find_update_id(attributes),
        )
    elif attribute_name == "workflow_execution_continued_as_new_event_attributes":
        normalized = ContinueAsNewEventAttributes(
            new_execution_run_id=attributes.new_execution_run_id,
            workflow_type=_nested_name(attributes, "workflow_type"),
            task_queue=_nested_name(attributes, "task_queue"),
        )
    elif attribute_name and (
        "child_workflow" in attribute_name or "external_workflow" in attribute_name
    ):
        execution = getattr(attributes, "workflow_execution", None)
        normalized = ExternalInteractionEventAttributes(
            interaction=attribute_name.removesuffix("_event_attributes"),
            workflow_id=(
                getattr(attributes, "workflow_id", None)
                or getattr(execution, "workflow_id", None)
                or None
            ),
            run_id=(
                getattr(attributes, "run_id", None) or getattr(execution, "run_id", None) or None
            ),
            workflow_type=_nested_name(attributes, "workflow_type"),
            initiated_event_id=getattr(attributes, "initiated_event_id", 0),
        )
    else:
        normalized = UnknownEventAttributes(
            attribute_name=attribute_name,
            metadata=_safe_message_metadata(attributes) if attributes is not None else {},
        )

    event_type = _event_type_name(event.event_type)
    return HistoryEventFact(
        event_id=event.event_id,
        event_type=event_type,
        event_time=event.event_time.ToDatetime().replace(tzinfo=UTC),
        task_id=event.task_id,
        attributes=normalized,
    )


def normalize_failure(failure: Failure) -> FailureFact:
    application_type = None
    non_retryable = None
    timeout_type = None
    if failure.HasField("application_failure_info"):
        application_type = failure.application_failure_info.type or None
        non_retryable = failure.application_failure_info.non_retryable
    if failure.HasField("timeout_failure_info"):
        timeout_type = _enum_field_name(failure.timeout_failure_info, "timeout_type")
    return FailureFact(
        message=failure.message,
        source=failure.source or None,
        stack_trace=failure.stack_trace or None,
        application_type=application_type,
        non_retryable=non_retryable,
        timeout_type=timeout_type,
        cause=normalize_failure(failure.cause) if failure.HasField("cause") else None,
    )


def translate_rpc_error(error: RPCError, *, operation: str, exact_run: bool = False) -> OpsMcpError:
    """Map SDK transport status to a stable, connection-safe internal error."""
    if error.status is RPCStatusCode.DEADLINE_EXCEEDED:
        return DeadlineExceededError()
    if error.status is RPCStatusCode.NOT_FOUND:
        return OpsMcpError(
            ErrorCode.RUN_NOT_FOUND if exact_run else ErrorCode.WORKFLOW_NOT_FOUND,
            "The requested workflow run was not found"
            if exact_run
            else "The requested workflow was not found",
            retryable=False,
        )
    if error.status is RPCStatusCode.UNAUTHENTICATED:
        return OpsMcpError(
            ErrorCode.TEMPORAL_AUTHENTICATION_FAILED,
            "Temporal rejected the server's credentials",
            retryable=False,
            details={"operation": operation, "grpc_status": "unauthenticated"},
        )
    if error.status is RPCStatusCode.PERMISSION_DENIED:
        return OpsMcpError(
            ErrorCode.TEMPORAL_PERMISSION_DENIED,
            "Temporal denied permission for the requested operation",
            retryable=False,
            details={"operation": operation, "grpc_status": "permission_denied"},
        )
    if error.status is RPCStatusCode.UNAVAILABLE:
        return OpsMcpError(
            ErrorCode.TEMPORAL_UNREACHABLE,
            "Temporal is currently unreachable",
            retryable=True,
        )
    if error.status is RPCStatusCode.INVALID_ARGUMENT:
        return OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "Temporal rejected the request arguments",
            retryable=False,
        )
    if error.status is RPCStatusCode.UNIMPLEMENTED:
        return OpsMcpError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            "The target does not support this Temporal capability",
            retryable=False,
        )
    return OpsMcpError(
        ErrorCode.INTERNAL_ERROR,
        "Temporal returned an unexpected error",
        retryable=error.status in {RPCStatusCode.RESOURCE_EXHAUSTED, RPCStatusCode.INTERNAL},
    )


async def _decode_payloads(
    converter: DataConverter, payloads: Iterable[Any]
) -> DecodedPayloadsFact:
    try:
        decoded = await converter.decode(list(payloads))
    except Exception:  # Converter implementations are third-party extension points.
        return DecodedPayloadsFact(decode_status="decode_failed")
    return DecodedPayloadsFact(values=tuple(_json_safe(value) for value in decoded))


def _json_safe(value: Any) -> JsonValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _nested_name(message: Message, field_name: str) -> str | None:
    value = getattr(message, field_name, None)
    return getattr(value, "name", None) or None


def _event_type_name(value: int) -> str:
    if not value:
        return "EVENT_TYPE_UNSPECIFIED"
    try:
        return EventType.Name(value)
    except ValueError:
        return f"EVENT_TYPE_UNKNOWN_{value}"


def _has_message_field(message: Message, field_name: str) -> bool:
    descriptor = message.DESCRIPTOR.fields_by_name.get(field_name)
    return bool(
        descriptor is not None
        and descriptor.type == FieldDescriptor.TYPE_MESSAGE
        and message.HasField(field_name)
    )


def _enum_field_name(message: Message, field_name: str) -> str | None:
    descriptor = message.DESCRIPTOR.fields_by_name.get(field_name)
    if descriptor is None or descriptor.enum_type is None:
        return None
    number = getattr(message, field_name)
    value = descriptor.enum_type.values_by_number.get(number)
    return value.name if value else None


def _find_update_id(message: Message) -> str | None:
    direct = getattr(message, "update_id", None)
    if direct:
        return direct
    meta = getattr(message, "meta", None)
    return getattr(meta, "update_id", None) or None


def _safe_message_metadata(message: Message) -> dict[str, JsonValue]:
    """Retain scalar topology metadata while excluding payload-bearing fields."""
    result: dict[str, JsonValue] = {}
    excluded = {"input", "result", "memo", "search_attributes", "header", "details"}
    for field, value in message.ListFields():
        if field.name in excluded:
            result[field.name] = "<payload omitted>"
        elif field.type == FieldDescriptor.TYPE_ENUM:
            enum_value = field.enum_type.values_by_number.get(int(value))
            result[field.name] = enum_value.name if enum_value else int(value)
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            full_name = field.message_type.full_name
            if full_name == "google.protobuf.Timestamp":
                result[field.name] = value.ToDatetime().replace(tzinfo=UTC).isoformat()
            elif full_name == "google.protobuf.Duration":
                result[field.name] = value.ToTimedelta().total_seconds()
            elif field.is_repeated:
                result[field.name] = ["<message>" for _ in value]
            else:
                result[field.name] = _safe_message_metadata(value)
        elif field.is_repeated:
            result[field.name] = [_json_safe(item) for item in value]
        else:
            result[field.name] = _json_safe(value)
    return result

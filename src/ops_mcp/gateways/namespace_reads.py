"""Temporal gateways for schedules, approved queries, and batch operations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Protocol

from temporalio.api.workflowservice.v1 import (
    DescribeBatchOperationRequest,
    ListBatchOperationsRequest,
    ListScheduleMatchingTimesRequest,
)
from temporalio.client import (
    ScheduleActionStartWorkflow,
    WorkflowQueryFailedError,
    WorkflowQueryRejectedError,
)
from temporalio.service import RPCError

from ops_mcp.clients.provider import TemporalClientProvider
from ops_mcp.facts.history import JsonValue
from ops_mcp.facts.namespace_reads import (
    BatchOperationDetailFact,
    BatchOperationPageFact,
    BatchOperationSummaryFact,
    QueryResultFact,
    ScheduleActionFact,
    ScheduleDescriptionFact,
    ScheduleSummaryFact,
)
from ops_mcp.gateways.history import translate_rpc_error
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.targets.models import ResolvedTarget


class ScheduleGateway(Protocol):
    async def list_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        page_size: int,
        page_token: bytes | None,
    ) -> tuple[tuple[ScheduleSummaryFact, ...], bytes | None]: ...

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        schedule_id: str,
        *,
        upcoming_start: datetime | None,
        upcoming_end: datetime | None,
        upcoming_count: int,
    ) -> ScheduleDescriptionFact: ...


class QueryGateway(Protocol):
    async def query(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        query_name: str,
    ) -> QueryResultFact: ...


class BatchGateway(Protocol):
    async def list_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        page_size: int,
        page_token: bytes | None,
    ) -> BatchOperationPageFact: ...

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        job_id: str,
    ) -> BatchOperationDetailFact: ...


class SdkScheduleGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def list_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        page_size: int,
        page_token: bytes | None,
    ) -> tuple[tuple[ScheduleSummaryFact, ...], bytes | None]:
        client = await self._provider.get_client(target.target_ref, target.connection)
        iterator = client.list_schedules(
            page_size=page_size,
            next_page_token=page_token,
            rpc_timeout=timedelta(seconds=context.budget.remaining_seconds()),
        )
        context.budget.reserve_temporal_call("ListSchedules")
        try:
            await iterator.fetch_next_page()
        except RPCError as exc:
            raise translate_rpc_error(exc) from exc
        context.budget.consume_page("schedules")
        facts = tuple(_schedule_summary(item) for item in iterator.current_page or ())
        return facts, iterator.next_page_token

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        schedule_id: str,
        *,
        upcoming_start: datetime | None,
        upcoming_end: datetime | None,
        upcoming_count: int,
    ) -> ScheduleDescriptionFact:
        client = await self._provider.get_client(target.target_ref, target.connection)
        context.budget.reserve_temporal_call("DescribeSchedule")
        try:
            description = await client.get_schedule_handle(schedule_id).describe(
                rpc_timeout=timedelta(seconds=context.budget.remaining_seconds())
            )
        except RPCError as exc:
            raise _schedule_error(exc) from exc

        upcoming: tuple[datetime, ...] = ()
        if upcoming_count:
            assert upcoming_start is not None and upcoming_end is not None
            context.budget.reserve_temporal_call("ListScheduleMatchingTimes")
            try:
                response = await client.workflow_service.list_schedule_matching_times(
                    ListScheduleMatchingTimesRequest(
                        namespace=target.namespace,
                        schedule_id=schedule_id,
                        start_time=_timestamp(upcoming_start),
                        end_time=_timestamp(upcoming_end),
                    ),
                    retry=True,
                    timeout=timedelta(seconds=context.budget.remaining_seconds()),
                )
            except RPCError as exc:
                raise _schedule_error(exc) from exc
            upcoming = tuple(
                value.ToDatetime().replace(tzinfo=UTC)
                for value in response.start_time[:upcoming_count]
            )

        schedule = description.schedule
        info = description.info
        spec = schedule.spec
        return ScheduleDescriptionFact(
            schedule_id=schedule_id,
            action=_schedule_action(schedule.action),
            paused=schedule.state.paused,
            note=schedule.state.note or None,
            limited_actions=schedule.state.limited_actions,
            remaining_actions=schedule.state.remaining_actions,
            shapes=_schedule_shapes(spec),
            cron_expressions=tuple(spec.cron_expressions),
            intervals_seconds=tuple(value.every.total_seconds() for value in spec.intervals),
            calendar_count=len(spec.calendars),
            time_zone_name=spec.time_zone_name or None,
            overlap_policy=schedule.policy.overlap.name,
            catchup_window_seconds=schedule.policy.catchup_window.total_seconds(),
            pause_on_failure=schedule.policy.pause_on_failure,
            recent_action_times=tuple(value.started_at for value in info.recent_actions),
            next_action_times=tuple(info.next_action_times),
            upcoming_action_times=upcoming,
            observed_at=datetime.now(UTC),
        )


class SdkQueryGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def query(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str,
        query_name: str,
    ) -> QueryResultFact:
        client = await self._provider.get_client(target.target_ref, target.connection)
        context.budget.reserve_temporal_call("QueryWorkflow")
        try:
            value = await client.get_workflow_handle(workflow_id, run_id=run_id).query(
                query_name,
                rpc_timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            if exc.status.name in {"INVALID_ARGUMENT", "FAILED_PRECONDITION"}:
                raise OpsMcpError(
                    ErrorCode.QUERY_FAILED,
                    "The workflow query failed",
                    retryable=False,
                ) from exc
            raise translate_rpc_error(exc, exact_run=True) from exc
        except (WorkflowQueryFailedError, WorkflowQueryRejectedError) as exc:
            raise OpsMcpError(
                ErrorCode.QUERY_FAILED,
                "The workflow query failed",
                retryable=False,
            ) from exc
        except Exception:  # Data converters are third-party extension points.
            return QueryResultFact(
                workflow_id=workflow_id,
                run_id=run_id,
                query_name=query_name,
                value=None,
                decode_status="decode_failed",
                observed_at=datetime.now(UTC),
            )
        return QueryResultFact(
            workflow_id=workflow_id,
            run_id=run_id,
            query_name=query_name,
            value=_json_safe(value),
            decode_status="decoded",
            observed_at=datetime.now(UTC),
        )


class SdkBatchGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def list_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        page_size: int,
        page_token: bytes | None,
    ) -> BatchOperationPageFact:
        client = await self._provider.get_client(target.target_ref, target.connection)
        context.budget.reserve_temporal_call("ListBatchOperations")
        try:
            response = await client.workflow_service.list_batch_operations(
                ListBatchOperationsRequest(
                    namespace=target.namespace,
                    page_size=page_size,
                    next_page_token=page_token or b"",
                ),
                retry=True,
                timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            raise translate_rpc_error(exc) from exc
        context.budget.consume_page("batch_operations")
        return BatchOperationPageFact(
            operations=tuple(_batch_summary(item) for item in response.operation_info),
            next_page_token=response.next_page_token or None,
            observed_at=datetime.now(UTC),
        )

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        job_id: str,
    ) -> BatchOperationDetailFact:
        client = await self._provider.get_client(target.target_ref, target.connection)
        context.budget.reserve_temporal_call("DescribeBatchOperation")
        try:
            value = await client.workflow_service.describe_batch_operation(
                DescribeBatchOperationRequest(namespace=target.namespace, job_id=job_id),
                retry=True,
                timeout=timedelta(seconds=context.budget.remaining_seconds()),
            )
        except RPCError as exc:
            raise _batch_error(exc) from exc
        return BatchOperationDetailFact(
            job_id=value.job_id,
            state=_enum_name(value, "state"),
            operation_type=_enum_name(value, "operation_type"),
            start_time=_optional_timestamp(value, "start_time"),
            close_time=_optional_timestamp(value, "close_time"),
            total_operation_count=value.total_operation_count,
            complete_operation_count=value.complete_operation_count,
            failure_operation_count=value.failure_operation_count,
            identity=value.identity or None,
            reason=value.reason or None,
            query=value.query or None,
        )


def _schedule_summary(value: Any) -> ScheduleSummaryFact:
    schedule = value.schedule
    info = value.info
    return ScheduleSummaryFact(
        schedule_id=value.id,
        paused=schedule.state.paused if schedule else None,
        note=(schedule.state.note or None) if schedule else None,
        shapes=_schedule_shapes(schedule.spec) if schedule else (),
        recent_action_times=(
            tuple(action.started_at for action in info.recent_actions) if info else ()
        ),
        next_action_times=tuple(info.next_action_times) if info else (),
    )


def _schedule_shapes(spec: Any) -> tuple[Any, ...]:
    values = []
    if spec.cron_expressions:
        values.append("cron")
    if spec.intervals:
        values.append("interval")
    if spec.calendars:
        values.append("calendar")
    return tuple(values)


def _schedule_action(value: Any) -> ScheduleActionFact:
    if isinstance(value, ScheduleActionStartWorkflow):
        workflow = value.workflow
        return ScheduleActionFact(
            action_type="start_workflow",
            workflow_type=(
                workflow
                if isinstance(workflow, str)
                else getattr(workflow, "__name__", str(workflow))
            ),
            workflow_id=value.id,
            task_queue=value.task_queue,
        )
    return ScheduleActionFact(
        action_type=type(value).__name__,
        workflow_type=None,
        workflow_id=None,
        task_queue=None,
    )


def _batch_summary(value: Any) -> BatchOperationSummaryFact:
    return BatchOperationSummaryFact(
        job_id=value.job_id,
        state=_enum_name(value, "state"),
        operation_type=_enum_name(value, "operation_type"),
        start_time=_optional_timestamp(value, "start_time"),
        close_time=_optional_timestamp(value, "close_time"),
    )


def _enum_name(message: Any, field_name: str) -> str:
    descriptor = message.DESCRIPTOR.fields_by_name[field_name]
    value = descriptor.enum_type.values_by_number.get(getattr(message, field_name))
    return value.name if value else "UNSPECIFIED"


def _optional_timestamp(message: Any, field_name: str) -> datetime | None:
    return (
        getattr(message, field_name).ToDatetime().replace(tzinfo=UTC)
        if message.HasField(field_name)
        else None
    )


def _timestamp(value: datetime):
    from google.protobuf.timestamp_pb2 import Timestamp

    result = Timestamp()
    result.FromDatetime(value)
    return result


def _json_safe(value: Any) -> JsonValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _schedule_error(error: RPCError) -> OpsMcpError:
    if error.status.name == "NOT_FOUND":
        return OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The requested schedule was not found",
            retryable=False,
        )
    return translate_rpc_error(error)


def _batch_error(error: RPCError) -> OpsMcpError:
    if error.status.name == "NOT_FOUND":
        return OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The requested batch operation was not found",
            retryable=False,
        )
    return translate_rpc_error(error)

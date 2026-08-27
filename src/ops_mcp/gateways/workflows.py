"""Temporal workflow-description gateway."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Protocol

from temporalio.client import WorkflowExecutionDescription
from temporalio.converter import DataConverter
from temporalio.service import RPCError

from ops_mcp.clients.provider import TemporalClientProvider
from ops_mcp.facts.history import DecodedPayloadsFact, JsonValue
from ops_mcp.facts.workflows import (
    PendingActivityFact,
    PendingChildFact,
    PendingWorkflowTaskFact,
    WorkflowExecutionFact,
)
from ops_mcp.gateways.history import normalize_failure, translate_rpc_error
from ops_mcp.runtime.context import RequestContext
from ops_mcp.targets.models import ResolvedTarget


class TemporalWorkflowGateway(Protocol):
    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str | None = None,
    ) -> WorkflowExecutionFact: ...


class SdkWorkflowGateway:
    def __init__(self, provider: TemporalClientProvider) -> None:
        self._provider = provider

    async def describe(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow_id: str,
        run_id: str | None = None,
    ) -> WorkflowExecutionFact:
        if not workflow_id:
            raise ValueError("workflow_id is required")
        client = await self._provider.get_client(target.target_ref, target.connection)
        context.budget.reserve_temporal_call("DescribeWorkflowExecution")
        try:
            description = await client.get_workflow_handle(workflow_id, run_id=run_id).describe(
                rpc_timeout=timedelta(seconds=context.budget.remaining_seconds())
            )
        except RPCError as exc:
            raise translate_rpc_error(exc, exact_run=run_id is not None) from exc
        return await _description_fact(
            target,
            description,
            getattr(client, "data_converter", DataConverter.default),
        )


async def _description_fact(
    target: ResolvedTarget, description: WorkflowExecutionDescription, data_converter: Any
) -> WorkflowExecutionFact:
    raw = description.raw_description
    info = raw.workflow_execution_info
    memo: dict[str, DecodedPayloadsFact] = {}
    for key in info.memo.fields:
        try:
            value = await description.memo_value(key)
        except Exception:  # Converter implementations are third-party extension points.
            memo[key] = DecodedPayloadsFact(decode_status="decode_failed")
        else:
            memo[key] = DecodedPayloadsFact(values=(_json_safe(value),))
    search_attributes = {
        str(key): _json_safe(value) for key, value in description.search_attributes.items()
    }

    pending_activities = tuple(
        [
            PendingActivityFact(
                activity_id=item.activity_id,
                activity_type=item.activity_type.name,
                state=_enum_field_name(item, "state") or "PENDING_ACTIVITY_STATE_UNSPECIFIED",
                attempt=item.attempt,
                maximum_attempts=item.maximum_attempts,
                scheduled_time=_timestamp(item, "scheduled_time"),
                last_started_time=_timestamp(item, "last_started_time"),
                next_attempt_time=_timestamp(item, "next_attempt_schedule_time"),
                last_worker_identity=item.last_worker_identity or None,
                last_failure=(
                    normalize_failure(item.last_failure) if item.HasField("last_failure") else None
                ),
                paused=item.paused,
                activity_task_queue=(
                    item.activity_options.task_queue.name
                    if item.HasField("activity_options")
                    and item.activity_options.HasField("task_queue")
                    and item.activity_options.task_queue.name
                    else None
                ),
                last_heartbeat_time=_timestamp(item, "last_heartbeat_time"),
                heartbeat_details=await _decode_payloads(
                    data_converter,
                    item.heartbeat_details.payloads if item.HasField("heartbeat_details") else (),
                ),
            )
            for item in raw.pending_activities
        ]
    )
    pending_children = tuple(
        PendingChildFact(
            workflow_id=item.workflow_id,
            run_id=item.run_id,
            workflow_type=item.workflow_type_name,
            initiated_event_id=item.initiated_id,
            parent_close_policy=_enum_field_name(item, "parent_close_policy")
            or "PARENT_CLOSE_POLICY_UNSPECIFIED",
        )
        for item in raw.pending_children
    )
    pending_workflow_task = None
    if raw.HasField("pending_workflow_task"):
        item = raw.pending_workflow_task
        pending_workflow_task = PendingWorkflowTaskFact(
            state=_enum_field_name(item, "state") or "PENDING_WORKFLOW_TASK_STATE_UNSPECIFIED",
            attempt=item.attempt,
            scheduled_time=_timestamp(item, "scheduled_time"),
            started_time=_timestamp(item, "started_time"),
        )

    return WorkflowExecutionFact(
        workflow_id=description.id,
        run_id=description.run_id,
        workflow_type=description.workflow_type,
        status=(
            description.status.name
            if description.status
            else "WORKFLOW_EXECUTION_STATUS_UNSPECIFIED"
        ),
        task_queue=description.task_queue,
        namespace=target.namespace,
        start_time=description.start_time,
        execution_time=description.execution_time,
        close_time=description.close_time,
        history_length=description.history_length,
        history_size_bytes=info.history_size_bytes,
        first_run_id=info.first_run_id or None,
        parent_workflow_id=description.parent_id,
        parent_run_id=description.parent_run_id,
        root_workflow_id=description.root_id,
        root_run_id=description.root_run_id,
        memo=memo,
        search_attributes=search_attributes,
        pending_activities=pending_activities,
        pending_children=pending_children,
        pending_workflow_task=pending_workflow_task,
        observed_at=datetime.now(UTC),
    )


def _timestamp(message: Any, name: str) -> datetime | None:
    if not message.HasField(name):
        return None
    return getattr(message, name).ToDatetime().replace(tzinfo=UTC)


def _enum_field_name(message: Any, field_name: str) -> str | None:
    descriptor = message.DESCRIPTOR.fields_by_name.get(field_name)
    if descriptor is None or descriptor.enum_type is None:
        return None
    value = descriptor.enum_type.values_by_number.get(getattr(message, field_name))
    return value.name if value else None


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


async def _decode_payloads(converter: Any, payloads: Any) -> DecodedPayloadsFact:
    try:
        decoded = await converter.decode(list(payloads))
    except Exception:  # Converter implementations are third-party extension points.
        return DecodedPayloadsFact(decode_status="decode_failed")
    return DecodedPayloadsFact(values=tuple(_json_safe(value) for value in decoded))

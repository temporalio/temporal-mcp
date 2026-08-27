from __future__ import annotations

import base64
import os
from dataclasses import asdict
from typing import Any

from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client

from ops_mcp.analysis import (
    build_reset_plan,
    duration_seconds,
    extract_failures,
    isoformat,
    likely_cause,
    workflow_status_label,
)
from ops_mcp.models import Target

MAX_HISTORY_EVENTS = 2_000


class TemporalGateway:
    def __init__(self) -> None:
        self._clients: dict[tuple[str, str, bool, str | None], Client] = {}

    async def client_for(self, target: Target) -> Client:
        key = (target.address, target.namespace, target.tls, target.api_key_env)
        if key in self._clients:
            return self._clients[key]
        api_key = os.getenv(target.api_key_env) if target.api_key_env else None
        if target.api_key_env and not api_key:
            raise RuntimeError(
                f"Target {target.id!r} requires environment variable {target.api_key_env!r}"
            )
        client = await Client.connect(
            target.address,
            namespace=target.namespace,
            tls=target.tls,
            api_key=api_key,
        )
        self._clients[key] = client
        return client

    async def filter_workflows(
        self,
        target: Target,
        query: str,
        fields: list[str],
        page_size: int,
        page_token: str | None,
    ) -> dict[str, Any]:
        client = await self.client_for(target)
        token = _decode_token(page_token)
        iterator = client.list_workflows(query or None, page_size=page_size, next_page_token=token)
        await iterator.fetch_next_page()
        executions = iterator.current_page or []
        allowed_fields = {
            "workflow_id",
            "run_id",
            "workflow_type",
            "status",
            "task_queue",
            "start_time",
            "close_time",
            "history_length",
            "parent_workflow_id",
        }
        unknown = set(fields) - allowed_fields
        if unknown:
            raise ValueError(f"Unsupported requested fields: {', '.join(sorted(unknown))}")
        rows = []
        for item in executions:
            all_values = {
                "workflow_id": item.id,
                "run_id": item.run_id,
                "workflow_type": item.workflow_type,
                "status": item.status.name if item.status else "UNSPECIFIED",
                "task_queue": item.task_queue,
                "start_time": isoformat(item.start_time),
                "close_time": isoformat(item.close_time),
                "history_length": item.history_length,
                "parent_workflow_id": item.parent_id,
            }
            rows.append({field: all_values[field] for field in fields})
        return {
            "target": target.id,
            "query": query,
            "workflows": rows,
            "returned": len(rows),
            "next_page_token": _encode_token(iterator.next_page_token),
        }

    async def count_workflows(
        self, target: Target, query: str, group_by: str | None
    ) -> dict[str, Any]:
        client = await self.client_for(target)
        group_field = {"status": "ExecutionStatus", "workflow_type": "WorkflowType"}.get(
            group_by or ""
        )
        final_query = query.strip()
        if group_field:
            final_query = f"{final_query} GROUP BY {group_field}".strip()
        result = await client.count_workflows(final_query or None)
        return {
            "target": target.id,
            "query": final_query,
            "count": result.count,
            "groups": [
                {
                    "values": [_json_value(value) for value in group.group_values],
                    "count": group.count,
                }
                for group in result.groups
            ],
        }

    async def diagnose_workflow(
        self, target: Target, workflow_id: str, run_id: str | None
    ) -> dict[str, Any]:
        client = await self.client_for(target)
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        description = await handle.describe()
        raw = description.raw_description
        info = raw.workflow_execution_info
        resolved_run_id = info.execution.run_id
        events, truncated = await self._history(client, workflow_id, resolved_run_id)
        failures = extract_failures(events)
        task_queue = info.task_queue
        health = await self._task_queue_health(client, target.namespace, task_queue)
        cause, worth_retrying = likely_cause(failures, health)
        pending = []
        for activity in raw.pending_activities:
            message = None
            if activity.HasField("last_failure"):
                from ops_mcp.analysis import flatten_failure

                message = flatten_failure(activity.last_failure)[0]
            pending.append(
                {
                    "activity_id": activity.activity_id,
                    "activity_type": activity.activity_type.name,
                    "attempt": activity.attempt,
                    "maximum_attempts": activity.maximum_attempts,
                    "last_failure": message,
                    "next_attempt": isoformat(activity.next_attempt_schedule_time)
                    if activity.HasField("next_attempt_schedule_time")
                    else None,
                }
            )
        return {
            "target": target.id,
            "workflow": {
                "workflow_id": info.execution.workflow_id,
                "run_id": resolved_run_id,
                "workflow_type": info.type.name,
                "status": workflow_status_label(info.status),
                "task_queue": task_queue,
                "start_time": isoformat(info.start_time),
                "close_time": isoformat(info.close_time) if info.HasField("close_time") else None,
                "history_length": info.history_length,
            },
            "pending_activities": pending,
            "failures": [asdict(failure) for failure in failures],
            "worker_health": health,
            "likely_cause": cause,
            "worth_retrying": worth_retrying,
            "history_inspection": {
                "events_inspected": len(events),
                "truncated": truncated,
                "hard_limit": MAX_HISTORY_EVENTS,
            },
        }

    async def plan_workflow_reset(
        self,
        target: Target,
        workflow_id: str,
        run_id: str,
        reason: str | None,
    ) -> dict[str, Any]:
        client = await self.client_for(target)
        events, truncated = await self._history(client, workflow_id, run_id)
        if truncated:
            raise ValueError(
                f"History exceeds the {MAX_HISTORY_EVENTS}-event safety ceiling; "
                "no reset plan was made"
            )
        plan = build_reset_plan(workflow_id, run_id, events, reason)
        plan["target"] = target.id
        plan["namespace"] = target.namespace
        return plan

    async def _history(
        self, client: Client, workflow_id: str, run_id: str
    ) -> tuple[list[Any], bool]:
        iterator = client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history_events(
            page_size=250
        )
        events = []
        async for event in iterator:
            events.append(event)
            if len(events) >= MAX_HISTORY_EVENTS:
                return events, iterator.next_page_token is not None
        return events, False

    async def _task_queue_health(
        self, client: Client, namespace: str, task_queue_name: str
    ) -> dict[str, Any]:
        response = await client.workflow_service.describe_task_queue(
            DescribeTaskQueueRequest(
                namespace=namespace,
                task_queue=TaskQueue(name=task_queue_name),
                task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                report_stats=True,
                report_pollers=True,
            )
        )
        stats = response.stats
        return {
            "task_queue": task_queue_name,
            "pollers_healthy": len(response.pollers) > 0,
            "poller_count": len(response.pollers),
            "poller_identities": sorted({poller.identity for poller in response.pollers}),
            "approximate_backlog_count": stats.approximate_backlog_count,
            "approximate_backlog_age_seconds": duration_seconds(stats.approximate_backlog_age),
        }


def _encode_token(value: bytes | None) -> str | None:
    return base64.urlsafe_b64encode(value).decode("ascii") if value else None


def _decode_token(value: str | None) -> bytes | None:
    if not value:
        return None
    try:
        return base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid page token") from exc


def _json_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)

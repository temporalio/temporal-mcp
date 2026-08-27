from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.api.batch.v1 import BatchOperationInfo
from temporalio.api.enums.v1 import BatchOperationState, BatchOperationType
from temporalio.api.workflowservice.v1 import ListBatchOperationsResponse

from ops_mcp.gateways.namespace_reads import (
    SdkBatchGateway,
    SdkQueryGateway,
    SdkScheduleGateway,
)
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


@pytest.mark.asyncio
async def test_schedule_list_normalizes_shape_and_charges_one_page() -> None:
    row = SimpleNamespace(
        id="daily",
        schedule=SimpleNamespace(
            state=SimpleNamespace(paused=True, note="maintenance"),
            spec=SimpleNamespace(cron_expressions=("0 0 * * *",), intervals=(), calendars=()),
        ),
        info=SimpleNamespace(recent_actions=(), next_action_times=()),
    )
    iterator = _ScheduleIterator((row,), b"next")
    client = SimpleNamespace(list_schedules=lambda **kwargs: iterator)
    context = _context()

    rows, token = await SdkScheduleGateway(_Provider(client)).list_page(
        context, _target(), page_size=10, page_token=None
    )

    assert rows[0].shapes == ("cron",)
    assert rows[0].paused is True
    assert token == b"next"
    assert context.budget.snapshot().temporal_calls == 1
    assert context.budget.snapshot().pages_read == 1


@pytest.mark.asyncio
async def test_query_gateway_calls_exact_run_with_no_arguments() -> None:
    handle = _QueryHandle()
    client = SimpleNamespace(
        get_workflow_handle=lambda workflow_id, run_id: (
            handle
            if (workflow_id, run_id) == ("order-1", "run-1")
            else pytest.fail("wrong identity")
        )
    )

    fact = await SdkQueryGateway(_Provider(client)).query(
        _context(), _target(), "order-1", "run-1", "blocked_on"
    )

    assert handle.query_name == "blocked_on"
    assert fact.value == {"activity": "EnrichProduct"}


@pytest.mark.asyncio
async def test_batch_list_uses_raw_service_and_normalizes_enum_values() -> None:
    item = BatchOperationInfo(
        job_id="job-1",
        state=BatchOperationState.BATCH_OPERATION_STATE_RUNNING,
        operation_type=BatchOperationType.BATCH_OPERATION_TYPE_RESET,
    )
    item.start_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    service = _BatchService(
        ListBatchOperationsResponse(operation_info=[item], next_page_token=b"next")
    )
    context = _context()

    page = await SdkBatchGateway(_Provider(SimpleNamespace(workflow_service=service))).list_page(
        context, _target(), page_size=5, page_token=None
    )

    assert page.operations[0].state == "BATCH_OPERATION_STATE_RUNNING"
    assert page.operations[0].operation_type == "BATCH_OPERATION_TYPE_RESET"
    assert page.next_page_token == b"next"
    request, kwargs = service.calls[0]
    assert request.page_size == 5
    assert kwargs["timeout"] <= timedelta(seconds=30)
    assert context.budget.snapshot().temporal_calls == 1


class _Provider:
    def __init__(self, client: object) -> None:
        self.client = client

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> object:
        return self.client


class _ScheduleIterator:
    def __init__(self, rows: tuple[object, ...], token: bytes | None) -> None:
        self.current_page = rows
        self.next_page_token = token

    async def fetch_next_page(self) -> None:
        return None


class _QueryHandle:
    def __init__(self) -> None:
        self.query_name: str | None = None

    async def query(self, query_name: str, **kwargs: object) -> object:
        self.query_name = query_name
        assert "args" not in kwargs and "arg" not in kwargs
        assert kwargs["rpc_timeout"] <= timedelta(seconds=30)
        return {"activity": "EnrichProduct"}


class _BatchService:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[object] = []

    async def list_batch_operations(self, request: object, **kwargs: object) -> object:
        self.calls.append((request, kwargs))
        return self.response


def _context() -> RequestContext:
    return RequestContext(
        "request-1",
        Principal("tester", ("test",)),
        BasicExecutionBudget(deadline=datetime.now(UTC) + timedelta(seconds=30)),
    )


def _target() -> ResolvedTarget:
    connection = ConnectionDescriptor("localhost:7233", "default")
    return ResolvedTarget("orders-prod", (), "demo", "local", "default", connection)

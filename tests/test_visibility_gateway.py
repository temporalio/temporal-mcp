from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import ListSearchAttributesResponse
from temporalio.client import (
    Client,
    WorkflowExecutionCount,
    WorkflowExecutionCountAggregationGroup,
    WorkflowExecutionStatus,
)
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes
from temporalio.service import RPCError, RPCStatusCode

from ops_mcp.facts import CountPrecision, SearchAttributeSource, VisibilityGroupBy
from ops_mcp.gateways import SdkVisibilityGateway
from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    ErrorCode,
    OpsMcpError,
    Principal,
    RequestContext,
)
from ops_mcp.targets import ConnectionDescriptor, ResolvedTarget


class FakeProvider:
    def __init__(self, client: object) -> None:
        self.client = client
        self.calls: list[tuple[str, ConnectionDescriptor]] = []

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> Client:
        self.calls.append((target_ref, descriptor))
        return cast(Client, self.client)

    async def invalidate(self, descriptor: ConnectionDescriptor | None = None) -> None:
        del descriptor

    async def shutdown(self) -> None:
        pass


class FakeIterator:
    def __init__(
        self,
        rows: list[object],
        *,
        next_page_token: bytes | None,
        error: RPCError | None = None,
    ) -> None:
        self.current_page: list[object] | None = None
        self.next_page_token = next_page_token
        self._rows = rows
        self._error = error
        self.fetches = 0

    async def fetch_next_page(self) -> None:
        self.fetches += 1
        if self._error:
            raise self._error
        self.current_page = self._rows


class FakeOperatorService:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[tuple[object, dict[str, Any]]] = []

    async def list_search_attributes(self, request: object, **kwargs: Any) -> object:
        self.calls.append((request, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class FakeClient:
    def __init__(
        self,
        *,
        iterator: FakeIterator | None = None,
        count: WorkflowExecutionCount | Exception | None = None,
        schema: ListSearchAttributesResponse | Exception | None = None,
    ) -> None:
        self.iterator = iterator
        self.count = count
        self.operator_service = FakeOperatorService(schema)
        self.list_calls: list[tuple[str | None, dict[str, Any]]] = []
        self.count_calls: list[tuple[str | None, dict[str, Any]]] = []

    def list_workflows(self, query: str | None, **kwargs: Any) -> FakeIterator:
        self.list_calls.append((query, kwargs))
        assert self.iterator is not None
        return self.iterator

    async def count_workflows(self, query: str | None, **kwargs: Any) -> WorkflowExecutionCount:
        self.count_calls.append((query, kwargs))
        assert self.count is not None
        if isinstance(self.count, Exception):
            raise self.count
        return self.count


def _context(*, temporal_calls: int = 10, pages: int = 10) -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="demo", groups=("demo-support",)),
        budget=BasicExecutionBudget(
            deadline=datetime.now(UTC) + timedelta(seconds=30),
            limits=BasicBudgetLimits(temporal_calls=temporal_calls, pages=pages),
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


def _workflow_row() -> object:
    return SimpleNamespace(
        namespace="default",
        id="order-88231",
        run_id="run-1",
        workflow_type="OrderWorkflow",
        status=WorkflowExecutionStatus.FAILED,
        task_queue="orders",
        start_time=datetime(2026, 8, 23, 10, tzinfo=UTC),
        execution_time=datetime(2026, 8, 23, 10, tzinfo=UTC),
        close_time=datetime(2026, 8, 23, 10, 1, tzinfo=UTC),
        history_length=42,
        parent_id=None,
        parent_run_id=None,
        root_id="order-88231",
        root_run_id="run-1",
        typed_search_attributes=TypedSearchAttributes(
            [
                SearchAttributePair(SearchAttributeKey.for_keyword("Team"), "ledger"),
                SearchAttributePair(
                    SearchAttributeKey.for_keyword_list("Domains"),
                    ["payments", "ledger"],
                ),
            ]
        ),
    )


async def test_list_workflows_fetches_exactly_one_page_and_preserves_token() -> None:
    iterator = FakeIterator([_workflow_row()], next_page_token=b"\x00opaque\xff")
    client = FakeClient(iterator=iterator)
    provider = FakeProvider(client)
    context = _context()

    result = await SdkVisibilityGateway(provider).list_workflows_page(
        context,
        _target(),
        query="  OrderId = '88231'  ",
        page_size=25,
        page_token=b"previous-token",
    )

    assert iterator.fetches == 1
    assert client.list_calls[0][0] == "  OrderId = '88231'  "
    assert client.list_calls[0][1]["page_size"] == 25
    assert client.list_calls[0][1]["next_page_token"] == b"previous-token"
    assert client.list_calls[0][1]["rpc_timeout"] <= timedelta(seconds=30)
    assert result.next_page_token == b"\x00opaque\xff"
    assert result.query == "  OrderId = '88231'  "
    assert result.rows[0].workflow_id == "order-88231"
    assert result.rows[0].run_id == "run-1"
    assert result.rows[0].status == "FAILED"
    assert result.rows[0].search_attributes == {
        "Domains": ("payments", "ledger"),
        "Team": "ledger",
    }
    assert context.budget.snapshot().temporal_calls == 1
    assert context.budget.snapshot().pages_read == 1
    assert provider.calls == [("orders-prod", _target().connection)]


async def test_list_workflows_passes_target_specific_query_syntax_through_unchanged() -> None:
    query = "WorkflowType = 'OrderWorkflow' ORDER BY StartTime DESC"
    client = FakeClient(iterator=FakeIterator([], next_page_token=None))

    result = await SdkVisibilityGateway(FakeProvider(client)).list_workflows_page(
        _context(), _target(), query=query, page_size=10
    )

    assert client.list_calls[0][0] == query
    assert result.query == query


@pytest.mark.parametrize("page_size", [0, 201])
async def test_list_workflows_rejects_page_sizes_outside_contract(page_size: int) -> None:
    provider = FakeProvider(FakeClient(iterator=FakeIterator([], next_page_token=None)))

    with pytest.raises(ValueError, match="page_size"):
        await SdkVisibilityGateway(provider).list_workflows_page(
            _context(), _target(), query="", page_size=page_size
        )

    assert provider.calls == []


async def test_count_uses_server_group_by_and_marks_sdk_precision_approximate() -> None:
    client = FakeClient(
        count=WorkflowExecutionCount(
            count=12,
            groups=[WorkflowExecutionCountAggregationGroup(count=12, group_values=["Completed"])],
        )
    )
    context = _context()

    result = await SdkVisibilityGateway(FakeProvider(client)).count_workflows(
        context,
        _target(),
        query="WorkflowType = 'OrderWorkflow'",
        group_by=VisibilityGroupBy.STATUS,
    )

    assert client.count_calls[0][0] == ("WorkflowType = 'OrderWorkflow' GROUP BY ExecutionStatus")
    assert result.count == 12
    assert result.groups[0].values == ("Completed",)
    assert result.precision is CountPrecision.APPROXIMATE
    assert result.group_by is VisibilityGroupBy.STATUS
    assert context.budget.snapshot().temporal_calls == 1
    assert context.budget.snapshot().pages_read == 0


async def test_schema_reads_operator_service_and_preserves_server_types() -> None:
    response = ListSearchAttributesResponse(
        system_attributes={"WorkflowId": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD},
        custom_attributes={"OrderId": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD},
        storage_schema={"OrderId": "keyword"},
    )
    client = FakeClient(schema=response)
    context = _context()

    result = await SdkVisibilityGateway(FakeProvider(client)).get_visibility_schema(
        context, _target()
    )

    request, kwargs = client.operator_service.calls[0]
    assert request.namespace == "default"
    assert kwargs["retry"] is True
    assert kwargs["timeout"] <= timedelta(seconds=30)
    assert [(field.name, field.source, field.value_type.name) for field in result.fields] == [
        ("OrderId", SearchAttributeSource.CUSTOM, "KEYWORD"),
        ("WorkflowId", SearchAttributeSource.SYSTEM, "KEYWORD"),
    ]
    assert result.storage_schema[0].name == "OrderId"
    assert context.budget.snapshot().temporal_calls == 1


async def test_invalid_query_returns_safe_temporal_rejection_feedback() -> None:
    rpc_error = RPCError(
        "invalid query: operation is not supported: 'ORDER BY' clause",
        RPCStatusCode.INVALID_ARGUMENT,
        b"",
    )
    client = FakeClient(iterator=FakeIterator([], next_page_token=None, error=rpc_error))

    with pytest.raises(OpsMcpError) as exc_info:
        await SdkVisibilityGateway(FakeProvider(client)).list_workflows_page(
            _context(), _target(), query="bad", page_size=10
        )

    assert exc_info.value.code is ErrorCode.INVALID_VISIBILITY_QUERY
    assert exc_info.value.message == "Temporal rejected the visibility query"
    assert exc_info.value.details == {
        "rejection_reason": "invalid query: operation is not supported: 'ORDER BY' clause"
    }


@pytest.mark.parametrize(
    ("server_message", "forbidden"),
    [
        ("query mentions SECRET_API_KEY and has bad syntax", "SECRET_API_KEY"),
        ("password='correct horse' caused parse failure", "correct horse"),
        ("backend https://user:pass@temporal.example.test/api rejected query", "temporal.example"),
        ("backend localhost:7233 rejected query", "localhost:7233"),
        ("Authorization: Bearer abc.def.ghi was invalid", "abc.def.ghi"),
    ],
)
async def test_invalid_query_rejection_reason_redacts_sensitive_material(
    server_message: str,
    forbidden: str,
) -> None:
    rpc_error = RPCError(server_message, RPCStatusCode.INVALID_ARGUMENT, b"")
    client = FakeClient(iterator=FakeIterator([], next_page_token=None, error=rpc_error))

    with pytest.raises(OpsMcpError) as exc_info:
        await SdkVisibilityGateway(FakeProvider(client)).list_workflows_page(
            _context(), _target(), query="bad", page_size=10
        )

    assert exc_info.value.details is not None
    assert forbidden not in exc_info.value.details["rejection_reason"]
    assert "[REDACTED]" in exc_info.value.details["rejection_reason"]


async def test_invalid_query_rejection_reason_is_single_line_and_bounded() -> None:
    rpc_error = RPCError(
        "invalid\x00 query\r\n" + ("unsupported clause " * 100),
        RPCStatusCode.INVALID_ARGUMENT,
        b"",
    )
    client = FakeClient(iterator=FakeIterator([], next_page_token=None, error=rpc_error))

    with pytest.raises(OpsMcpError) as exc_info:
        await SdkVisibilityGateway(FakeProvider(client)).list_workflows_page(
            _context(), _target(), query="bad", page_size=10
        )

    assert exc_info.value.details is not None
    reason = exc_info.value.details["rejection_reason"]
    assert len(reason) == 400
    assert reason.endswith("…")
    assert "\n" not in reason
    assert "\x00" not in reason


async def test_schema_unimplemented_is_classified_as_unsupported() -> None:
    rpc_error = RPCError("unknown method", RPCStatusCode.UNIMPLEMENTED, b"")
    client = FakeClient(schema=rpc_error)

    with pytest.raises(OpsMcpError) as exc_info:
        await SdkVisibilityGateway(FakeProvider(client)).get_visibility_schema(
            _context(), _target()
        )

    assert exc_info.value.code is ErrorCode.UNSUPPORTED_CAPABILITY

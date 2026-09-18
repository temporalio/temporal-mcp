from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from ops_mcp.contracts import (
    CountWorkflowsRequest,
    GetVisibilitySchemaRequest,
    ResolveTargetRequest,
    SearchWorkflowRunsRequest,
    WorkflowCountGroupBy,
    WorkflowField,
)
from ops_mcp.facts import (
    CountPrecision,
    IndexedValueTypeFact,
    SearchAttributeFieldFact,
    SearchAttributeSource,
    VisibilityCountFact,
    VisibilityCountGroupFact,
    VisibilityGroupBy,
    VisibilityPageFact,
    VisibilitySchemaFact,
    WorkflowVisibilityFact,
)
from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    ErrorCode,
    OpsMcpError,
    Principal,
    RequestContext,
    ResultTooLargeError,
)
from ops_mcp.services import DefaultTargetService, DefaultVisibilityService
from ops_mcp.targets import ConnectionDescriptor, ResolvedTarget, TargetCatalog


class RecordingCatalog:
    def __init__(self, target: ResolvedTarget) -> None:
        self.target = target
        self.resolutions: list[tuple[Principal, str]] = []
        self.authorizations: list[tuple[Principal, str]] = []

    def resolve(self, principal: Principal, human_name: str) -> ResolvedTarget:
        self.resolutions.append((principal, human_name))
        return self.target

    def list_authorized(self, principal: Principal) -> tuple[ResolvedTarget, ...]:
        return (self.target,)

    def match_authorized(self, principal: Principal, human_name: str) -> tuple[ResolvedTarget, ...]:
        self.resolutions.append((principal, human_name))
        return (self.target,)

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        self.authorizations.append((principal, target_ref))
        return self.target


class FakeVisibilityGateway:
    def __init__(
        self,
        *,
        page: VisibilityPageFact | None = None,
        count: VisibilityCountFact | None = None,
        schema: VisibilitySchemaFact | None = None,
    ) -> None:
        self.page = page
        self.count = count
        self.schema = schema
        self.list_args: list[tuple[ResolvedTarget, str, int, bytes | None]] = []
        self.count_args: list[tuple[ResolvedTarget, str, VisibilityGroupBy | None]] = []
        self.schema_args: list[ResolvedTarget] = []

    async def list_workflows_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        page_size: int,
        page_token: bytes | None = None,
    ) -> VisibilityPageFact:
        context.budget.reserve_temporal_call("list")
        context.budget.consume_page("list")
        self.list_args.append((target, query, page_size, page_token))
        assert self.page is not None
        return self.page

    async def count_workflows(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        group_by: VisibilityGroupBy | None = None,
    ) -> VisibilityCountFact:
        context.budget.reserve_temporal_call("count")
        self.count_args.append((target, query, group_by))
        assert self.count is not None
        return self.count

    async def get_visibility_schema(
        self,
        context: RequestContext,
        target: ResolvedTarget,
    ) -> VisibilitySchemaFact:
        context.budget.reserve_temporal_call("schema")
        self.schema_args.append(target)
        assert self.schema is not None
        return self.schema


def _target() -> ResolvedTarget:
    return ResolvedTarget(
        target_ref="orders-prod",
        aliases=("orders prod", "production orders"),
        account="local-demo",
        cluster="temporal-dev",
        namespace="default",
        connection=ConnectionDescriptor(address="localhost:7233", namespace="default"),
    )


def _context(*, output_bytes: int = 1_000_000) -> RequestContext:
    return RequestContext(
        request_id="request-1",
        principal=Principal(subject="demo", groups=("demo-support",)),
        budget=BasicExecutionBudget(
            deadline=datetime.now(UTC) + timedelta(seconds=30),
            limits=BasicBudgetLimits(output_bytes=output_bytes),
        ),
    )


def _workflow() -> WorkflowVisibilityFact:
    return WorkflowVisibilityFact(
        namespace="default",
        workflow_id="order-88231",
        run_id="run-1",
        workflow_type="OrderWorkflow",
        status="FAILED",
        task_queue="orders",
        start_time=datetime(2026, 8, 23, 10, tzinfo=UTC),
        execution_time=datetime(2026, 8, 23, 10, tzinfo=UTC),
        close_time=None,
        history_length=42,
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id="order-88231",
        root_run_id="run-1",
    )


def test_resolve_target_uses_context_identity_and_exposes_no_connection() -> None:
    catalog = RecordingCatalog(_target())
    context = _context()

    result = DefaultTargetService(catalog).resolve_target(
        context, ResolveTargetRequest(human_name="orders prod")
    )

    assert catalog.resolutions == [(context.principal, "orders prod")]
    assert result.answer.display_name == "orders prod"
    assert result.answer.resolved_from == "orders prod"
    assert result.inspection.temporal_calls == 0
    assert "localhost" not in result.model_dump_json()
    assert context.budget.snapshot().output_bytes > 0


async def test_filter_reauthorizes_projects_exact_fields_and_round_trips_token() -> None:
    catalog = RecordingCatalog(_target())
    gateway = FakeVisibilityGateway(
        page=VisibilityPageFact(
            namespace="default",
            query="OrderId = '88231'",
            rows=(_workflow(),),
            next_page_token=b"\x00opaque\xff",
        )
    )
    context = _context()

    result = await DefaultVisibilityService(catalog, gateway).search_workflow_runs(
        context,
        SearchWorkflowRunsRequest(
            target_ref="orders-prod",
            query="OrderId = '88231'",
            fields=(
                WorkflowField.WORKFLOW_ID,
                WorkflowField.RUN_ID,
                WorkflowField.CLOSE_TIME,
            ),
            page_size=10,
            continuation_token="cHJldmlvdXM=",
        ),
    )

    assert catalog.authorizations == [(context.principal, "orders-prod")]
    assert gateway.list_args[0][3] == b"previous"
    assert result.answer.continuation_token == "AG9wYXF1Zf8="
    assert result.answer.returned == 1
    assert result.answer.workflows[0].model_dump() == {
        "workflow_id": "order-88231",
        "run_id": "run-1",
        "close_time": None,
    }
    assert result.inspection.temporal_calls == 1
    assert result.inspection.pages_read == 1
    assert result.inspection.completion == "complete"


async def test_filter_projects_only_requested_registered_custom_search_attributes() -> None:
    catalog = RecordingCatalog(_target())
    row = replace(
        _workflow(),
        search_attributes={"Team": "ledger", "LongValue": "x" * 300, "Hidden": "no"},
    )
    gateway = FakeVisibilityGateway(
        page=VisibilityPageFact(namespace="default", query="", rows=(row,), next_page_token=None),
        schema=VisibilitySchemaFact(
            namespace="default",
            fields=(
                SearchAttributeFieldFact(
                    name="Team",
                    source=SearchAttributeSource.CUSTOM,
                    value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
                ),
                SearchAttributeFieldFact(
                    name="LongValue",
                    source=SearchAttributeSource.CUSTOM,
                    value_type=IndexedValueTypeFact(code=1, name="TEXT"),
                ),
            ),
            storage_schema=(),
        ),
    )

    result = await DefaultVisibilityService(catalog, gateway).search_workflow_runs(
        _context(),
        SearchWorkflowRunsRequest(
            target_ref="orders-prod",
            query="",
            fields=(WorkflowField.WORKFLOW_ID,),
            custom_search_attributes=("Team", "LongValue"),
        ),
    )

    attributes = result.answer.workflows[0].search_attributes
    assert result.answer.custom_search_attributes == ("Team", "LongValue")
    assert attributes[0].model_dump() == {
        "name": "Team",
        "value_type": "keyword",
        "status": "present",
        "value": "ledger",
        "value_sha256": None,
    }
    assert attributes[1].status == "truncated"
    assert len(attributes[1].value) == 256
    assert len(attributes[1].value_sha256 or "") == 64
    assert result.inspection.temporal_calls == 2


async def test_filter_rejects_unknown_custom_projection_before_listing() -> None:
    gateway = FakeVisibilityGateway(
        schema=VisibilitySchemaFact(namespace="default", fields=(), storage_schema=())
    )

    with pytest.raises(OpsMcpError) as exc_info:
        await DefaultVisibilityService(RecordingCatalog(_target()), gateway).search_workflow_runs(
            _context(),
            SearchWorkflowRunsRequest(
                target_ref="orders-prod",
                query="",
                fields=(WorkflowField.WORKFLOW_ID,),
                custom_search_attributes=("Missing",),
            ),
        )

    assert exc_info.value.code is ErrorCode.INVALID_ARGUMENT
    assert exc_info.value.details == {"unknown_custom_search_attributes": ["Missing"]}
    assert gateway.list_args == []


async def test_filter_rejects_invalid_public_token_after_reauthorization() -> None:
    catalog = RecordingCatalog(_target())
    gateway = FakeVisibilityGateway()
    context = _context()

    with pytest.raises(OpsMcpError) as exc_info:
        await DefaultVisibilityService(catalog, gateway).search_workflow_runs(
            context,
            SearchWorkflowRunsRequest(
                target_ref="orders-prod",
                query="",
                fields=(WorkflowField.WORKFLOW_ID,),
                continuation_token="not base64!",
            ),
        )

    assert exc_info.value.code is ErrorCode.INVALID_ARGUMENT
    assert catalog.authorizations == [(context.principal, "orders-prod")]
    assert gateway.list_args == []


async def test_filter_enforces_request_output_budget() -> None:
    catalog = RecordingCatalog(_target())
    gateway = FakeVisibilityGateway(
        page=VisibilityPageFact(
            namespace="default",
            query="",
            rows=(_workflow(),),
            next_page_token=None,
        )
    )
    context = _context(output_bytes=1)

    with pytest.raises(ResultTooLargeError):
        await DefaultVisibilityService(catalog, gateway).search_workflow_runs(
            context,
            SearchWorkflowRunsRequest(
                target_ref="orders-prod",
                query="",
                fields=(WorkflowField.WORKFLOW_ID,),
            ),
        )

    assert context.budget.snapshot().exhausted_dimension == "output_bytes"


def test_filter_contract_rejects_duplicate_fields() -> None:
    with pytest.raises(ValidationError, match="duplicates"):
        SearchWorkflowRunsRequest(
            target_ref="orders-prod",
            query="",
            fields=(WorkflowField.RUN_ID, WorkflowField.RUN_ID),
        )


async def test_count_reauthorizes_normalizes_status_and_preserves_approximation() -> None:
    catalog = RecordingCatalog(_target())
    gateway = FakeVisibilityGateway(
        count=VisibilityCountFact(
            namespace="default",
            query="WorkflowType = 'OrderWorkflow' GROUP BY ExecutionStatus",
            group_by=VisibilityGroupBy.STATUS,
            count=12,
            groups=(VisibilityCountGroupFact(values=("ContinuedAsNew",), count=12),),
            precision=CountPrecision.APPROXIMATE,
        )
    )
    context = _context()

    result = await DefaultVisibilityService(catalog, gateway).count_workflows(
        context,
        CountWorkflowsRequest(
            target_ref="orders-prod",
            query="WorkflowType = 'OrderWorkflow'",
            group_by=WorkflowCountGroupBy.STATUS,
        ),
    )

    assert catalog.authorizations == [(context.principal, "orders-prod")]
    assert gateway.count_args[0][2] is VisibilityGroupBy.STATUS
    assert result.answer.groups[0].key == "CONTINUED_AS_NEW"
    assert result.answer.precision == "approximate"
    assert result.inspection.temporal_calls == 1
    assert result.inspection.pages_read == 0


async def test_visibility_schema_maps_validated_types_and_warns_on_unknown_type() -> None:
    catalog = RecordingCatalog(_target())
    gateway = FakeVisibilityGateway(
        schema=VisibilitySchemaFact(
            namespace="default",
            fields=(
                SearchAttributeFieldFact(
                    name="WorkflowId",
                    source=SearchAttributeSource.SYSTEM,
                    value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
                ),
                SearchAttributeFieldFact(
                    name="FutureField",
                    source=SearchAttributeSource.CUSTOM,
                    value_type=IndexedValueTypeFact(code=99, name="FUTURE"),
                ),
            ),
            storage_schema=(),
        )
    )
    context = _context()

    result = await DefaultVisibilityService(catalog, gateway).get_visibility_schema(
        context, GetVisibilitySchemaRequest(target_ref="orders-prod")
    )

    future, workflow_id = result.answer.fields
    assert future.name == "FutureField"
    assert future.value_type == "unknown"
    assert workflow_id.name == "WorkflowId"
    assert workflow_id.value_type == "keyword"
    assert "operators" not in workflow_id.model_dump()
    assert "compatibility" not in result.answer.model_dump()
    assert len(result.warnings) == 1
    assert result.inspection.temporal_calls == 1


def test_service_protocol_dependencies_are_structural() -> None:
    catalog: TargetCatalog = RecordingCatalog(_target())
    assert catalog.authorize(_context().principal, "orders-prod").target_ref == "orders-prod"

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from ops_mcp.contracts import (
    BatchSearchWorkflowRunsRequest,
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
    SearchAttributeSchemaFact,
    VisibilityCountFact,
    VisibilityCountGroupFact,
    VisibilityGroupBy,
    VisibilityPageFact,
    WorkflowVisibilityFact,
)
from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    ContinuationCodec,
    ContinuationState,
    ErrorCode,
    OpsMcpError,
    Principal,
    RequestContext,
    ResultTooLargeError,
    TargetNotAuthorizedError,
)
from ops_mcp.services import (
    DefaultTargetService,
    DefaultVisibilityService,
    SearchAttributeSchemaResolver,
)
from ops_mcp.services.visibility import _search_scope
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
        schema: SearchAttributeSchemaFact | None = None,
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

    async def get_legal_search_attributes(
        self,
        context: RequestContext,
        target: ResolvedTarget,
    ) -> SearchAttributeSchemaFact:
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
    codec = ContinuationCodec(b"v" * 32)
    request = SearchWorkflowRunsRequest(
        target_ref="orders-prod",
        query="OrderId = '88231'",
        fields=(
            WorkflowField.WORKFLOW_ID,
            WorkflowField.RUN_ID,
            WorkflowField.CLOSE_TIME,
        ),
        page_size=10,
    )
    scope = _search_scope(request)
    previous = codec.encode(
        ContinuationState(
            tool="search_workflow_runs",
            target_ref="orders-prod",
            scope_digest=codec.scope_digest(scope),
            sample_seed="seed",
            candidate_page_token="cHJldmlvdXM=",
        )
    )

    result = await DefaultVisibilityService(
        catalog, gateway, continuations=codec
    ).search_workflow_runs(
        context,
        request.model_copy(update={"continuation_token": previous}),
    )

    assert catalog.authorizations == [(context.principal, "orders-prod")]
    assert gateway.list_args[0][3] == b"previous"
    assert result.answer.continuation_token is not None
    resumed = codec.decode(
        result.answer.continuation_token,
        tool="search_workflow_runs",
        target_ref="orders-prod",
        query=scope,
    )
    assert resumed.candidate_page_token == "AG9wYXF1Zf8="
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
        schema=SearchAttributeSchemaFact(
            namespace="default",
            fields=(
                SearchAttributeFieldFact(
                    name="Team",
                    value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
                ),
                SearchAttributeFieldFact(
                    name="LongValue",
                    value_type=IndexedValueTypeFact(code=1, name="TEXT"),
                ),
            ),
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


async def test_system_projection_avoids_schema_rpc() -> None:
    row = replace(
        _workflow(),
        search_attributes={"TemporalReportedProblems": ("workflow-task-failed",)},
    )
    gateway = FakeVisibilityGateway(
        page=VisibilityPageFact(namespace="default", query="", rows=(row,), next_page_token=None)
    )

    result = await DefaultVisibilityService(
        RecordingCatalog(_target()), gateway
    ).search_workflow_runs(
        _context(),
        SearchWorkflowRunsRequest(
            target_ref="orders-prod",
            query="TemporalReportedProblems = 'workflow-task-failed'",
            fields=(WorkflowField.WORKFLOW_ID,),
            system_search_attributes=("TemporalReportedProblems",),
        ),
    )

    assert gateway.schema_args == []
    assert result.inspection.temporal_calls == 1
    assert result.answer.workflows[0].search_attributes[0].value == ("workflow-task-failed",)


async def test_system_projection_rejected_from_custom_parameter_before_schema_rpc() -> None:
    gateway = FakeVisibilityGateway()

    with pytest.raises(OpsMcpError) as caught:
        await DefaultVisibilityService(RecordingCatalog(_target()), gateway).search_workflow_runs(
            _context(),
            SearchWorkflowRunsRequest(
                target_ref="orders-prod",
                query="",
                custom_search_attributes=("TemporalReportedProblems",),
            ),
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert gateway.schema_args == []
    assert gateway.list_args == []


@pytest.mark.parametrize("operation", ["search", "count"])
async def test_unterminated_visibility_literal_is_rejected_before_rpc(operation: str) -> None:
    gateway = FakeVisibilityGateway()
    service = DefaultVisibilityService(RecordingCatalog(_target()), gateway)
    malformed = 'ExecutionStatus = "TimedOut" AND CloseTime > "2026-09-29T18:55:09Z'

    with pytest.raises(OpsMcpError) as caught:
        if operation == "search":
            await service.search_workflow_runs(
                _context(),
                SearchWorkflowRunsRequest(target_ref="orders-prod", query=malformed),
            )
        else:
            await service.count_workflows(
                _context(), CountWorkflowsRequest(target_ref="orders-prod", query=malformed)
            )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert "unterminated" in caught.value.message
    assert gateway.list_args == []
    assert gateway.count_args == []


async def test_balanced_datetime_visibility_literal_passes_through() -> None:
    query = 'ExecutionStatus = "TimedOut" AND CloseTime > "2026-09-29T18:55:09Z"'
    gateway = FakeVisibilityGateway(
        count=VisibilityCountFact(
            namespace="default",
            query=query,
            group_by=None,
            count=2,
            groups=(),
            precision=CountPrecision.APPROXIMATE,
        )
    )

    await DefaultVisibilityService(RecordingCatalog(_target()), gateway).count_workflows(
        _context(), CountWorkflowsRequest(target_ref="orders-prod", query=query)
    )

    assert gateway.count_args[0][1] == query


async def test_filter_rejects_unknown_custom_projection_before_listing() -> None:
    gateway = FakeVisibilityGateway(
        schema=SearchAttributeSchemaFact(namespace="default", fields=())
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


async def test_schema_resolver_caches_single_flight_and_expires() -> None:
    schema = SearchAttributeSchemaFact(
        namespace="default",
        fields=(
            SearchAttributeFieldFact(
                name="Team",
                value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
            ),
        ),
    )

    class DelayedGateway(FakeVisibilityGateway):
        async def get_legal_search_attributes(self, context, target):
            await asyncio.sleep(0)
            return await super().get_legal_search_attributes(context, target)

    now = [1.0]
    gateway = DelayedGateway(schema=schema)
    resolver = SearchAttributeSchemaResolver(gateway, ttl_seconds=10, clock=lambda: now[0])
    target = _target()
    first, second = await asyncio.gather(
        resolver.custom_fields(_context(), target, ("Team",)),
        resolver.custom_fields(_context(), target, ("Team",)),
    )
    assert first == second
    assert len(gateway.schema_args) == 1

    now[0] = 12.0
    await resolver.custom_fields(_context(), target, ("Team",))
    assert len(gateway.schema_args) == 2


async def test_schema_resolver_isolates_credential_generations() -> None:
    schema = SearchAttributeSchemaFact(namespace="default", fields=())
    gateway = FakeVisibilityGateway(schema=schema)
    resolver = SearchAttributeSchemaResolver(gateway)
    first = _target()
    second = replace(
        first,
        connection=replace(first.connection, credential_generation=1),
    )

    for target in (first, second):
        with pytest.raises(OpsMcpError):
            await resolver.custom_fields(_context(), target, ("Missing",))

    assert len(gateway.schema_args) == 2


async def test_schema_resolver_single_flights_concurrent_failure() -> None:
    class FailingGateway:
        def __init__(self) -> None:
            self.calls = 0
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def get_legal_search_attributes(self, context, target):
            self.calls += 1
            self.started.set()
            await self.release.wait()
            raise OpsMcpError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "unsupported",
                retryable=False,
            )

    gateway = FailingGateway()
    resolver = SearchAttributeSchemaResolver(gateway)
    lookups = [
        asyncio.create_task(resolver.custom_fields(_context(), _target(), ("Team",)))
        for _ in range(3)
    ]
    await gateway.started.wait()
    await asyncio.sleep(0)
    gateway.release.set()
    results = await asyncio.gather(*lookups, return_exceptions=True)

    assert gateway.calls == 1
    assert all(
        isinstance(result, OpsMcpError)
        and result.code is ErrorCode.UNSUPPORTED_CAPABILITY
        for result in results
    )
    assert resolver._inflight == {}


async def test_schema_cache_ttl_starts_after_slow_lookup_completes() -> None:
    now = [0.0]
    schema = SearchAttributeSchemaFact(
        namespace="default",
        fields=(
            SearchAttributeFieldFact(
                name="Team",
                value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
            ),
        ),
    )

    class SlowGateway(FakeVisibilityGateway):
        async def get_legal_search_attributes(self, context, target):
            now[0] = 100.0
            return await super().get_legal_search_attributes(context, target)

    gateway = SlowGateway(schema=schema)
    resolver = SearchAttributeSchemaResolver(gateway, ttl_seconds=10, clock=lambda: now[0])
    await resolver.custom_fields(_context(), _target(), ("Team",))
    now[0] = 105.0
    await resolver.custom_fields(_context(), _target(), ("Team",))

    assert len(gateway.schema_args) == 1


async def test_schema_resolver_never_accepts_predefined_or_unknown_temporal_as_custom() -> None:
    schema = SearchAttributeSchemaFact(
        namespace="default",
        fields=tuple(
            SearchAttributeFieldFact(
                name=name,
                value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
            )
            for name in ("WorkflowId", "ExecutionStatus", "TemporalFutureField", "Team")
        ),
    )
    resolver = SearchAttributeSchemaResolver(FakeVisibilityGateway(schema=schema))

    resolved = await resolver.custom_fields(_context(), _target(), ("Team",))
    assert tuple(resolved) == ("Team",)
    for name in ("WorkflowId", "ExecutionStatus", "TemporalFutureField"):
        with pytest.raises(OpsMcpError):
            await resolver.custom_fields(_context(), _target(), (name,))


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


async def test_batch_search_preserves_success_and_authorization_failure() -> None:
    class PartialCatalog(RecordingCatalog):
        def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
            self.authorizations.append((principal, target_ref))
            if target_ref == "denied":
                raise TargetNotAuthorizedError(target_ref)
            return self.target

    catalog = PartialCatalog(_target())
    gateway = FakeVisibilityGateway(
        page=VisibilityPageFact(namespace="default", query="", rows=(), next_page_token=None)
    )

    result = await DefaultVisibilityService(catalog, gateway).batch_search_workflow_runs(
        _context(),
        BatchSearchWorkflowRunsRequest(target_refs=("orders-prod", "denied"), query=""),
    )

    assert result.items[0].response is not None
    assert result.items[0].error is None
    assert result.items[1].response is None
    assert result.items[1].error == {
        "code": "target_not_authorized",
        "message": "The caller is not authorized for target 'denied'",
        "retryable": False,
    }
    assert len(gateway.list_args) == 1


async def test_batch_search_reports_per_item_accounting_under_concurrency() -> None:
    class ConcurrentGateway(FakeVisibilityGateway):
        def __init__(self) -> None:
            super().__init__()
            self.active = 0
            self.maximum_active = 0

        async def list_workflows_page(self, context, target, **kwargs):
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            await asyncio.sleep(0.01)
            context.budget.reserve_temporal_call("list")
            context.budget.consume_page("list")
            self.active -= 1
            return VisibilityPageFact(
                namespace=target.namespace,
                query=kwargs["query"],
                rows=(),
                next_page_token=None,
            )

    gateway = ConcurrentGateway()
    result = await DefaultVisibilityService(
        RecordingCatalog(_target()), gateway
    ).batch_search_workflow_runs(
        _context(),
        BatchSearchWorkflowRunsRequest(
            target_refs=tuple(f"target-{index}" for index in range(8)), query=""
        ),
    )

    assert gateway.maximum_active == 4
    assert [item.response.inspection.temporal_calls for item in result.items] == [1] * 8
    assert [item.response.inspection.pages_read for item in result.items] == [1] * 8


async def test_batch_search_validates_shared_query_before_authorization() -> None:
    catalog = RecordingCatalog(_target())

    with pytest.raises(OpsMcpError, match="unterminated"):
        await DefaultVisibilityService(catalog, FakeVisibilityGateway()).batch_search_workflow_runs(
            _context(),
            BatchSearchWorkflowRunsRequest(
                target_refs=("orders-prod", "another"), query='WorkflowType = "broken'
            ),
        )

    assert catalog.authorizations == []


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
        schema=SearchAttributeSchemaFact(
            namespace="default",
            fields=(
                SearchAttributeFieldFact(
                    name="WorkflowId",
                    value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
                ),
                SearchAttributeFieldFact(
                    name="FutureField",
                    value_type=IndexedValueTypeFact(code=99, name="FUTURE"),
                ),
            ),
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


@pytest.mark.parametrize("custom_name", ("ActivityId", "ActivityType"))
async def test_activity_names_are_legal_custom_projections(custom_name: str) -> None:
    schema = SearchAttributeSchemaFact(
        namespace="default",
        fields=(
            SearchAttributeFieldFact(
                name=custom_name,
                value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
            ),
        ),
    )
    gateway = FakeVisibilityGateway(
        schema=schema,
        page=VisibilityPageFact(namespace="default", query="", rows=(), next_page_token=None),
    )
    service = DefaultVisibilityService(RecordingCatalog(_target()), gateway)

    public = await service.get_visibility_schema(
        _context(), GetVisibilitySchemaRequest(target_ref="orders-prod")
    )
    search = await service.search_workflow_runs(
        _context(),
        SearchWorkflowRunsRequest(
            target_ref="orders-prod",
            query="",
            fields=(WorkflowField.WORKFLOW_ID,),
            custom_search_attributes=(custom_name,),
        ),
    )

    assert public.answer.fields[0].model_dump() == {
        "name": custom_name,
        "value_type": "keyword",
        "query_examples": (f"{custom_name} = 'value'",),
    }
    assert search.answer.custom_search_attributes == (custom_name,)
    assert len(gateway.schema_args) == 1


def test_service_protocol_dependencies_are_structural() -> None:
    catalog: TargetCatalog = RecordingCatalog(_target())
    assert catalog.authorize(_context().principal, "orders-prod").target_ref == "orders-prod"

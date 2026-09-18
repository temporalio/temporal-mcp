"""Tool services for bounded Temporal workflow-visibility reads."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime
from typing import Protocol

from ops_mcp.contracts.primitives import Completeness, ContractModel, Inspection
from ops_mcp.contracts.visibility import (
    CountWorkflowsAnswer,
    CountWorkflowsRequest,
    CountWorkflowsResponse,
    GetVisibilitySchemaRequest,
    GetVisibilitySchemaResponse,
    PublicIndexedValueType,
    PublicSearchAttributeSource,
    SearchWorkflowRunsAnswer,
    SearchWorkflowRunsRequest,
    SearchWorkflowRunsResponse,
    VisibilityField,
    VisibilitySchemaAnswer,
    WorkflowCountGroup,
    WorkflowCountGroupBy,
    WorkflowField,
    WorkflowProjectionRow,
)
from ops_mcp.facts import (
    SearchAttributeFieldFact,
    VisibilityCountGroupFact,
    VisibilityGroupBy,
    WorkflowVisibilityFact,
)
from ops_mcp.gateways import VisibilityGateway
from ops_mcp.runtime import BudgetSnapshot, ErrorCode, OpsMcpError, RequestContext
from ops_mcp.services.search_attributes import (
    project_search_attribute,
    public_indexed_value_type,
    validate_custom_search_attributes,
)
from ops_mcp.targets import TargetCatalog


class VisibilityService(Protocol):
    async def search_workflow_runs(
        self,
        context: RequestContext,
        request: SearchWorkflowRunsRequest,
    ) -> SearchWorkflowRunsResponse: ...

    async def count_workflows(
        self,
        context: RequestContext,
        request: CountWorkflowsRequest,
    ) -> CountWorkflowsResponse: ...

    async def get_visibility_schema(
        self,
        context: RequestContext,
        request: GetVisibilitySchemaRequest,
    ) -> GetVisibilitySchemaResponse: ...


class DefaultVisibilityService:
    def __init__(self, target_catalog: TargetCatalog, gateway: VisibilityGateway) -> None:
        self._target_catalog = target_catalog
        self._gateway = gateway

    async def search_workflow_runs(
        self,
        context: RequestContext,
        request: SearchWorkflowRunsRequest,
    ) -> SearchWorkflowRunsResponse:
        before = context.budget.snapshot()
        target = self._target_catalog.authorize(context.principal, request.target_ref)
        custom_schema: dict[str, SearchAttributeFieldFact] = {}
        if request.custom_search_attributes:
            schema = await self._gateway.get_visibility_schema(context, target)
            custom_schema = validate_custom_search_attributes(
                request.custom_search_attributes, schema.fields
            )
        page = await self._gateway.list_workflows_page(
            context,
            target,
            query=request.query,
            page_size=request.page_size,
            page_token=_decode_page_token(request.continuation_token),
        )
        rows = tuple(
            _project_workflow(
                row,
                request.fields,
                request.custom_search_attributes,
                custom_schema,
            )
            for row in page.rows
        )
        response = SearchWorkflowRunsResponse(
            target_ref=target.target_ref,
            answer=SearchWorkflowRunsAnswer(
                query=page.query,
                custom_search_attributes=request.custom_search_attributes,
                workflows=rows,
                returned=len(rows),
                continuation_token=_encode_page_token(page.next_page_token),
            ),
            inspection=_inspection(context.budget.snapshot(), before, pages_limit=1),
        )
        _charge_output(context, response)
        return response

    async def count_workflows(
        self,
        context: RequestContext,
        request: CountWorkflowsRequest,
    ) -> CountWorkflowsResponse:
        before = context.budget.snapshot()
        target = self._target_catalog.authorize(context.principal, request.target_ref)
        gateway_group = (
            VisibilityGroupBy(request.group_by.value) if request.group_by is not None else None
        )
        fact = await self._gateway.count_workflows(
            context,
            target,
            query=request.query,
            group_by=gateway_group,
        )
        response = CountWorkflowsResponse(
            target_ref=target.target_ref,
            answer=CountWorkflowsAnswer(
                query=fact.query,
                group_by=request.group_by,
                count=fact.count,
                groups=tuple(_public_count_group(group, request.group_by) for group in fact.groups),
                precision=fact.precision.value,
            ),
            inspection=_inspection(context.budget.snapshot(), before),
        )
        _charge_output(context, response)
        return response

    async def get_visibility_schema(
        self,
        context: RequestContext,
        request: GetVisibilitySchemaRequest,
    ) -> GetVisibilitySchemaResponse:
        before = context.budget.snapshot()
        target = self._target_catalog.authorize(context.principal, request.target_ref)
        schema = await self._gateway.get_visibility_schema(context, target)
        warnings: list[str] = []
        fields = []
        for fact in schema.fields:
            field, warning = _public_schema_field(fact)
            fields.append(field)
            if warning:
                warnings.append(warning)
        response = GetVisibilitySchemaResponse(
            target_ref=target.target_ref,
            answer=VisibilitySchemaAnswer(
                namespace=schema.namespace,
                fields=tuple(sorted(fields, key=lambda field: (field.name, field.source.value))),
            ),
            inspection=_inspection(context.budget.snapshot(), before),
            warnings=tuple(warnings),
        )
        _charge_output(context, response)
        return response


def _inspection(
    after: BudgetSnapshot,
    before: BudgetSnapshot,
    *,
    pages_limit: int | None = None,
) -> Inspection:
    return Inspection(
        completion=Completeness.COMPLETE,
        temporal_calls=after.temporal_calls - before.temporal_calls,
        pages_read=after.pages_read - before.pages_read,
        limit=pages_limit,
    )


def _charge_output(context: RequestContext, response: ContractModel) -> None:
    serialized = response.model_dump_json().encode("utf-8")
    context.budget.consume_output_bytes(len(serialized))


def _project_workflow(
    fact: WorkflowVisibilityFact,
    fields: tuple[WorkflowField, ...],
    custom_names: tuple[str, ...] = (),
    custom_schema: dict[str, SearchAttributeFieldFact] | None = None,
) -> WorkflowProjectionRow:
    values: dict[str, object] = {
        WorkflowField.WORKFLOW_ID: fact.workflow_id,
        WorkflowField.RUN_ID: fact.run_id,
        WorkflowField.WORKFLOW_TYPE: fact.workflow_type,
        WorkflowField.STATUS: fact.status,
        WorkflowField.TASK_QUEUE: fact.task_queue,
        WorkflowField.START_TIME: fact.start_time,
        WorkflowField.EXECUTION_TIME: fact.execution_time,
        WorkflowField.CLOSE_TIME: fact.close_time,
        WorkflowField.HISTORY_LENGTH: fact.history_length,
        WorkflowField.PARENT_WORKFLOW_ID: fact.parent_workflow_id,
        WorkflowField.PARENT_RUN_ID: fact.parent_run_id,
        WorkflowField.ROOT_WORKFLOW_ID: fact.root_workflow_id,
        WorkflowField.ROOT_RUN_ID: fact.root_run_id,
    }
    projected = {field.value: values[field] for field in fields}
    if custom_names:
        schema = custom_schema or {}
        projected["search_attributes"] = tuple(
            project_search_attribute(name, schema[name], fact.search_attributes)
            for name in custom_names
        )
    return WorkflowProjectionRow(**projected)


def _encode_page_token(token: bytes | None) -> str | None:
    return base64.urlsafe_b64encode(token).decode("ascii") if token else None


def _decode_page_token(token: str | None) -> bytes | None:
    if token is None:
        return None
    try:
        return base64.b64decode(token.encode("ascii"), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The page token is invalid",
            retryable=False,
        ) from exc


def _public_count_group(
    fact: VisibilityCountGroupFact,
    group_by: WorkflowCountGroupBy | None,
) -> WorkflowCountGroup:
    if group_by is None or len(fact.values) != 1:
        raise OpsMcpError(
            ErrorCode.INTERNAL_ERROR,
            "Temporal returned an unexpected workflow count grouping",
            retryable=False,
        )
    value = fact.values[0]
    if isinstance(value, datetime):
        key = value.isoformat()
    elif isinstance(value, tuple):
        key = ",".join(value)
    else:
        key = str(value)
    if group_by is WorkflowCountGroupBy.STATUS:
        key = _normalize_status(key)
    return WorkflowCountGroup(key=key, count=fact.count)


def _normalize_status(status: str) -> str:
    return {
        "running": "RUNNING",
        "completed": "COMPLETED",
        "failed": "FAILED",
        "canceled": "CANCELED",
        "terminated": "TERMINATED",
        "continuedasnew": "CONTINUED_AS_NEW",
        "continued_as_new": "CONTINUED_AS_NEW",
        "timedout": "TIMED_OUT",
        "timed_out": "TIMED_OUT",
    }.get(status.replace(" ", "").casefold(), status)


def _public_schema_field(
    fact: SearchAttributeFieldFact,
) -> tuple[VisibilityField, str | None]:
    value_type = public_indexed_value_type(fact)
    source = PublicSearchAttributeSource(fact.source.value)
    field = VisibilityField(
        name=fact.name,
        source=source,
        value_type=value_type,
        query_examples=_query_examples(fact.name, value_type),
    )
    warning = None
    if value_type is PublicIndexedValueType.UNKNOWN:
        warning = (
            f"Search attribute {fact.name!r} uses unrecognized indexed type "
            f"{fact.value_type.name!r}"
        )
    return field, warning


def _query_examples(name: str, value_type: PublicIndexedValueType) -> tuple[str, ...]:
    if value_type in {PublicIndexedValueType.KEYWORD, PublicIndexedValueType.TEXT}:
        return (f"{name} = 'value'",)
    if value_type is PublicIndexedValueType.DATETIME:
        return (f"{name} >= '2026-01-01T00:00:00Z'",)
    if value_type in {PublicIndexedValueType.INT, PublicIndexedValueType.DOUBLE}:
        return (f"{name} = 1",)
    if value_type is PublicIndexedValueType.BOOL:
        return (f"{name} = true",)
    if value_type is PublicIndexedValueType.KEYWORD_LIST:
        return (f"'{name}' IN ['value']",)
    return ()

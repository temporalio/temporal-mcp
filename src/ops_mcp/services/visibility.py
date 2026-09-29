"""Tool services for bounded Temporal workflow-visibility reads."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from traceback import extract_tb
from typing import Protocol

from ops_mcp.contracts.primitives import Completeness, ContractModel, Inspection
from ops_mcp.contracts.visibility import (
    BatchSearchWorkflowRunsItem,
    BatchSearchWorkflowRunsRequest,
    BatchSearchWorkflowRunsResponse,
    CountWorkflowsAnswer,
    CountWorkflowsRequest,
    CountWorkflowsResponse,
    GetVisibilitySchemaRequest,
    GetVisibilitySchemaResponse,
    PublicIndexedValueType,
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
from ops_mcp.runtime import (
    BudgetSnapshot,
    ContinuationCodec,
    ContinuationState,
    ErrorCode,
    OpsMcpError,
    RequestContext,
)
from ops_mcp.services.search_attributes import (
    SYSTEM_SEARCH_ATTRIBUTES,
    SearchAttributeSchemaResolver,
    project_search_attribute,
    public_indexed_value_type,
)
from ops_mcp.targets import TargetCatalog

_LOGGER = logging.getLogger("ops_mcp.tools")
_SAFE_TARGET_REF = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


class VisibilityService(Protocol):
    async def batch_search_workflow_runs(
        self,
        context: RequestContext,
        request: BatchSearchWorkflowRunsRequest,
    ) -> BatchSearchWorkflowRunsResponse: ...

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
    def __init__(
        self,
        target_catalog: TargetCatalog,
        gateway: VisibilityGateway,
        *,
        continuations: ContinuationCodec | None = None,
        schema_resolver: SearchAttributeSchemaResolver | None = None,
    ) -> None:
        self._target_catalog = target_catalog
        self._gateway = gateway
        self._continuations = continuations or ContinuationCodec()
        self._schema_resolver = schema_resolver or SearchAttributeSchemaResolver(gateway)

    async def batch_search_workflow_runs(
        self,
        context: RequestContext,
        request: BatchSearchWorkflowRunsRequest,
    ) -> BatchSearchWorkflowRunsResponse:
        # Reject shared malformed input once, before authorizing or contacting any target.
        _validate_visibility_query(request.query)
        _validate_projection_names(
            request.custom_search_attributes, request.system_search_attributes
        )
        semaphore = asyncio.Semaphore(4)

        async def one(target_ref: str) -> BatchSearchWorkflowRunsItem:
            async with semaphore:
                child_context = RequestContext(
                    request_id=context.request_id,
                    principal=context.principal,
                    budget=context.budget.child(),
                    started_monotonic=context.started_monotonic,
                )
                try:
                    response = await self.search_workflow_runs(
                        child_context,
                        SearchWorkflowRunsRequest(
                            target_ref=target_ref,
                            query=request.query,
                            fields=request.fields,
                            custom_search_attributes=request.custom_search_attributes,
                            system_search_attributes=request.system_search_attributes,
                            page_size=request.page_size,
                        ),
                    )
                    return BatchSearchWorkflowRunsItem(target_ref=target_ref, response=response)
                except OpsMcpError as exc:
                    return BatchSearchWorkflowRunsItem(target_ref=target_ref, error=exc.as_dict())
                except Exception as exc:
                    _log_unexpected_batch_error(context.request_id, target_ref, exc)
                    return BatchSearchWorkflowRunsItem(
                        target_ref=target_ref,
                        error=OpsMcpError(
                            ErrorCode.INTERNAL_ERROR,
                            "The target search failed unexpectedly",
                            retryable=False,
                        ).as_dict(),
                    )

        response = BatchSearchWorkflowRunsResponse(
            items=tuple(await asyncio.gather(*(one(ref) for ref in request.target_refs))),
            observed_at=datetime.now(UTC),
        )
        nested_bytes = sum(
            len(item.response.model_dump_json().encode("utf-8"))
            for item in response.items
            if item.response is not None
        )
        total_bytes = len(response.model_dump_json().encode("utf-8"))
        context.budget.consume_output_bytes(max(0, total_bytes - nested_bytes))
        return response

    async def search_workflow_runs(
        self,
        context: RequestContext,
        request: SearchWorkflowRunsRequest,
    ) -> SearchWorkflowRunsResponse:
        before = context.budget.snapshot()
        _validate_visibility_query(request.query)
        target = self._target_catalog.authorize(context.principal, request.target_ref)
        _validate_projection_names(
            request.custom_search_attributes, request.system_search_attributes
        )
        custom_schema: dict[str, SearchAttributeFieldFact] = {}
        if request.custom_search_attributes:
            custom_schema = await self._schema_resolver.custom_fields(
                context, target, request.custom_search_attributes
            )
        scope = _search_scope(request)
        native_page_token = None
        if request.continuation_token:
            state = self._continuations.decode(
                request.continuation_token,
                tool="search_workflow_runs",
                target_ref=target.target_ref,
                query=scope,
            )
            native_page_token = _decode_page_token(state.candidate_page_token)
        page = await self._gateway.list_workflows_page(
            context,
            target,
            query=request.query,
            page_size=request.page_size,
            page_token=native_page_token,
        )
        rows = tuple(
            _project_workflow(
                row,
                request.fields,
                request.custom_search_attributes,
                custom_schema,
                request.system_search_attributes,
            )
            for row in page.rows
        )
        response = SearchWorkflowRunsResponse(
            target_ref=target.target_ref,
            answer=SearchWorkflowRunsAnswer(
                query=page.query,
                custom_search_attributes=request.custom_search_attributes,
                system_search_attributes=request.system_search_attributes,
                workflows=rows,
                returned=len(rows),
                continuation_token=_visibility_continuation(
                    self._continuations, target.target_ref, scope, page.next_page_token
                ),
                observed_at=datetime.now(UTC),
            ),
            inspection=_inspection(context.budget.snapshot(), before, pages_limit=1),
            warnings=_visibility_warnings(page.rows, request.fields),
        )
        _charge_output(context, response)
        return response

    async def count_workflows(
        self,
        context: RequestContext,
        request: CountWorkflowsRequest,
    ) -> CountWorkflowsResponse:
        before = context.budget.snapshot()
        _validate_visibility_query(request.query)
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
                observed_at=datetime.now(UTC),
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
        schema = await self._schema_resolver.schema(context, target)
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
                fields=tuple(sorted(fields, key=lambda field: field.name)),
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
    system_names: tuple[str, ...] = (),
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
        WorkflowField.HISTORY_LENGTH: (
            None
            if fact.history_length == 0 and fact.start_time is not None and fact.close_time is None
            else fact.history_length
        ),
        WorkflowField.PARENT_WORKFLOW_ID: fact.parent_workflow_id,
        WorkflowField.PARENT_RUN_ID: fact.parent_run_id,
        WorkflowField.ROOT_WORKFLOW_ID: fact.root_workflow_id,
        WorkflowField.ROOT_RUN_ID: fact.root_run_id,
    }
    projected = {field.value: values[field] for field in fields}
    if custom_names or system_names:
        schema = custom_schema or {}
        custom = tuple(
            project_search_attribute(name, schema[name], fact.search_attributes)
            for name in custom_names
        )
        system = tuple(
            project_search_attribute(name, SYSTEM_SEARCH_ATTRIBUTES[name], fact.search_attributes)
            for name in system_names
        )
        projected["search_attributes"] = custom + system
    return WorkflowProjectionRow(**projected)


def _validate_projection_names(
    custom_names: tuple[str, ...], system_names: tuple[str, ...]
) -> None:
    misuse = tuple(name for name in custom_names if name in SYSTEM_SEARCH_ATTRIBUTES)
    if misuse:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "System search attributes must be requested through system_search_attributes",
            retryable=False,
            details={"system_search_attributes": list(misuse)},
        )
    unknown_system = tuple(name for name in system_names if name not in SYSTEM_SEARCH_ATTRIBUTES)
    if unknown_system:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "Requested system search attributes are not supported for projection",
            retryable=False,
            details={"unsupported_system_search_attributes": list(unknown_system)},
        )


def _validate_visibility_query(query: str) -> None:
    quote: str | None = None
    opened_at = -1
    index = 0
    while index < len(query):
        character = query[index]
        if quote is None and character in {"'", '"'}:
            quote = character
            opened_at = index
        elif quote == character:
            if index + 1 < len(query) and query[index + 1] == character:
                index += 1
            elif index == 0 or query[index - 1] != "\\":
                quote = None
        index += 1
    if quote is not None:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "Visibility query contains an unterminated quoted literal",
            retryable=False,
            details={
                "quote_position": opened_at,
                "hint": f"Close the {quote} quoted literal before submitting the query",
            },
        )


def _search_scope(request: SearchWorkflowRunsRequest) -> str:
    serialized = json.dumps(
        {
            "query": request.query,
            "fields": [field.value for field in request.fields],
            "custom": list(request.custom_search_attributes),
            "system": list(request.system_search_attributes),
            "page_size": request.page_size,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(serialized).hexdigest()


def _visibility_continuation(
    codec: ContinuationCodec, target_ref: str, scope: str, token: bytes | None
) -> str | None:
    if not token:
        return None
    return codec.encode(
        ContinuationState(
            tool="search_workflow_runs",
            target_ref=target_ref,
            scope_digest=codec.scope_digest(scope),
            sample_seed=codec.stable_seed(
                tool="search_workflow_runs", target_ref=target_ref, scope=scope
            ),
            candidate_page_token=base64.urlsafe_b64encode(token).decode("ascii"),
        )
    )


def _visibility_warnings(
    rows: tuple[WorkflowVisibilityFact, ...], fields: tuple[WorkflowField, ...]
) -> tuple[str, ...]:
    if WorkflowField.HISTORY_LENGTH in fields and any(
        row.history_length == 0 and row.start_time is not None and row.close_time is None
        for row in rows
    ):
        return (
            "Temporal reported history_length=0 for an open execution; it is returned as unknown.",
        )
    return ()


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


def _log_unexpected_batch_error(request_id: str, target_ref: str, exc: Exception) -> None:
    safe_target = target_ref if _SAFE_TARGET_REF.fullmatch(target_ref) else "invalid"
    _LOGGER.error(
        "Unexpected batch target failure",
        extra={
            "request_id": request_id,
            "target_ref": safe_target,
            "operation": "search_workflow_runs",
            "traceback": [
                {"file": item.filename, "line": item.lineno, "function": item.name}
                for item in extract_tb(exc.__traceback__)
            ],
        },
    )


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
    field = VisibilityField(
        name=fact.name,
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

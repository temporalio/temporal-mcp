"""Bounded Temporal workflow-visibility reads."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Protocol

from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.workflowservice.v1 import GetSearchAttributesRequest
from temporalio.client import Client, WorkflowExecution
from temporalio.service import RPCError, RPCStatusCode

from ops_mcp.clients import TemporalClientProvider, acquire_client
from ops_mcp.facts.visibility import (
    CountPrecision,
    IndexedValueTypeFact,
    SearchAttributeFieldFact,
    SearchAttributeSchemaFact,
    VisibilityCountFact,
    VisibilityCountGroupFact,
    VisibilityGroupBy,
    VisibilityPageFact,
    VisibilityScalar,
    WorkflowVisibilityFact,
)
from ops_mcp.runtime import DeadlineExceededError, ErrorCode, OpsMcpError, RequestContext
from ops_mcp.targets import ResolvedTarget

MAX_VISIBILITY_PAGE_SIZE = 200
MAX_VISIBILITY_REJECTION_REASON_LENGTH = 400

_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]+")
_CONNECTION_URI = re.compile(r"(?i)\b(?:grpc|grpcs|http|https|temporal)://\S+")
_HOST_AND_PORT = re.compile(
    r"(?i)(?<![\w.-])(?:localhost|(?:\d{1,3}\.){3}\d{1,3}|"
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\.[a-z]{2,})\s*:\s*\d{1,5}\b"
)
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)\b(?:[\w-]*(?:api[_ -]?key|access[_ -]?token|refresh[_ -]?token|"
    r"client[_ -]?secret|password|passwd|authorization|credential|secret)[\w-]*)\b"
    r"\s*(?:=|:|\bis\b)\s*(?:bearer\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_SENSITIVE_IDENTIFIER = re.compile(
    r"(?i)\b[\w-]*(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|"
    r"password|passwd|authorization|credential|secret)[\w-]*\b"
)
_PEM_MATERIAL = re.compile(r"(?is)-----BEGIN [^-\r\n]+-----.*?(?:-----END [^-\r\n]+-----|\Z)")


class VisibilityGateway(Protocol):
    async def get_legal_search_attributes(
        self,
        context: RequestContext,
        target: ResolvedTarget,
    ) -> SearchAttributeSchemaFact: ...

    async def list_workflows_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        page_size: int,
        page_token: bytes | None = None,
    ) -> VisibilityPageFact: ...

    async def count_workflows(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        group_by: VisibilityGroupBy | None = None,
    ) -> VisibilityCountFact: ...

class SdkVisibilityGateway:
    """Pinned-SDK implementation using one Temporal call per gateway method."""

    def __init__(self, client_provider: TemporalClientProvider) -> None:
        self._client_provider = client_provider

    async def get_legal_search_attributes(
        self, context: RequestContext, target: ResolvedTarget
    ) -> SearchAttributeSchemaFact:
        client = await self._client(context, target, "get_search_attributes")
        try:
            response = await client.workflow_service.get_search_attributes(
                GetSearchAttributesRequest(),
                retry=True,
                timeout=_remaining_timeout(context),
            )
        except RPCError as exc:
            raise _translate_rpc_error(
                exc, capability_read=True, operation="get_search_attributes"
            ) from exc
        return SearchAttributeSchemaFact(
            namespace=target.namespace,
            fields=tuple(
                sorted(
                    (
                        _search_attribute_fact(name, value)
                        for name, value in response.keys.items()
                    ),
                    key=lambda item: item.name,
                )
            ),
        )

    async def list_workflows_page(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        page_size: int,
        page_token: bytes | None = None,
    ) -> VisibilityPageFact:
        if not 1 <= page_size <= MAX_VISIBILITY_PAGE_SIZE:
            raise ValueError(
                f"page_size must be between 1 and {MAX_VISIBILITY_PAGE_SIZE}, inclusive"
            )
        client = await self._client(context, target, "list_workflow_executions")
        context.budget.consume_page("list_workflow_executions")
        timeout = _remaining_timeout(context)
        iterator = client.list_workflows(
            query,
            page_size=page_size,
            next_page_token=page_token,
            rpc_timeout=timeout,
        )
        try:
            await iterator.fetch_next_page()
        except RPCError as exc:
            raise _translate_rpc_error(
                exc, visibility_query=True, operation="list_workflow_executions"
            ) from exc

        rows = tuple(_workflow_fact(item) for item in (iterator.current_page or ()))
        return VisibilityPageFact(
            namespace=target.namespace,
            query=query,
            rows=rows,
            next_page_token=iterator.next_page_token,
        )

    async def count_workflows(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        *,
        query: str,
        group_by: VisibilityGroupBy | None = None,
    ) -> VisibilityCountFact:
        client = await self._client(context, target, "count_workflow_executions")
        effective_query = _grouped_query(query, group_by)
        try:
            result = await client.count_workflows(
                effective_query or None,
                rpc_timeout=_remaining_timeout(context),
            )
        except RPCError as exc:
            raise _translate_rpc_error(
                exc, visibility_query=True, operation="count_workflow_executions"
            ) from exc

        return VisibilityCountFact(
            namespace=target.namespace,
            query=effective_query,
            group_by=group_by,
            count=result.count,
            groups=tuple(
                VisibilityCountGroupFact(
                    values=tuple(_visibility_scalar(value) for value in group.group_values),
                    count=group.count,
                )
                for group in result.groups
            ),
            # temporalio 1.31 documents this aggregate as approximate.
            precision=CountPrecision.APPROXIMATE,
        )

    async def _client(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        operation: str,
    ) -> Client:
        context.budget.reserve_temporal_call(operation)
        return await acquire_client(self._client_provider, context, target)


def _workflow_fact(item: WorkflowExecution) -> WorkflowVisibilityFact:
    typed_search_attributes = getattr(item, "typed_search_attributes", ())
    return WorkflowVisibilityFact(
        namespace=item.namespace,
        workflow_id=item.id,
        run_id=item.run_id,
        workflow_type=item.workflow_type,
        status=item.status.name if item.status is not None else None,
        task_queue=item.task_queue,
        start_time=item.start_time,
        execution_time=item.execution_time,
        close_time=item.close_time,
        history_length=item.history_length,
        parent_workflow_id=item.parent_id,
        parent_run_id=item.parent_run_id,
        root_workflow_id=item.root_id,
        root_run_id=item.root_run_id,
        search_attributes={
            pair.key.name: _visibility_scalar(pair.value) for pair in typed_search_attributes
        },
    )


def _grouped_query(query: str, group_by: VisibilityGroupBy | None) -> str:
    base = query.strip()
    if group_by is None:
        return base
    server_field = {VisibilityGroupBy.STATUS: "ExecutionStatus"}[group_by]
    return f"{base} GROUP BY {server_field}".strip()


def _visibility_scalar(value: object) -> VisibilityScalar:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value
    if isinstance(value, tuple):
        return tuple(str(item) for item in value)
    if isinstance(value, list):
        return tuple(str(item) for item in value)
    return str(value)


def _search_attribute_fact(
    name: str,
    value: int,
) -> SearchAttributeFieldFact:
    try:
        enum_name = IndexedValueType.Name(value)
    except ValueError:
        enum_name = f"UNKNOWN_{value}"
    normalized_name = enum_name.removeprefix("INDEXED_VALUE_TYPE_")
    return SearchAttributeFieldFact(
        name=name,
        value_type=IndexedValueTypeFact(code=value, name=normalized_name),
    )


def _remaining_timeout(context: RequestContext) -> timedelta:
    return timedelta(seconds=context.budget.remaining_seconds())


def _translate_rpc_error(
    error: RPCError,
    *,
    operation: str,
    visibility_query: bool = False,
    capability_read: bool = False,
) -> OpsMcpError:
    if error.status == RPCStatusCode.DEADLINE_EXCEEDED:
        return DeadlineExceededError()
    if error.status is RPCStatusCode.UNAUTHENTICATED:
        return OpsMcpError(
            ErrorCode.TEMPORAL_AUTHENTICATION_FAILED,
            "Temporal rejected the configured target credentials",
            retryable=False,
            details={"operation": operation, "grpc_status": "unauthenticated"},
        )
    if error.status is RPCStatusCode.PERMISSION_DENIED:
        return OpsMcpError(
            ErrorCode.TEMPORAL_PERMISSION_DENIED,
            "Temporal denied permission for the visibility operation",
            retryable=False,
            details={"operation": operation, "grpc_status": "permission_denied"},
        )
    if error.status == RPCStatusCode.NOT_FOUND:
        return OpsMcpError(
            ErrorCode.NAMESPACE_NOT_FOUND,
            "The configured Temporal namespace was not found",
            retryable=False,
        )
    if visibility_query and error.status == RPCStatusCode.INVALID_ARGUMENT:
        rejection_reason = _sanitize_visibility_rejection(error.message)
        return OpsMcpError(
            ErrorCode.INVALID_VISIBILITY_QUERY,
            "Temporal rejected the visibility query",
            retryable=False,
            details={"rejection_reason": rejection_reason},
        )
    if capability_read and error.status == RPCStatusCode.UNIMPLEMENTED:
        return OpsMcpError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            "The target does not expose its visibility schema",
            retryable=False,
        )
    if error.status == RPCStatusCode.UNAVAILABLE:
        return OpsMcpError(
            ErrorCode.TEMPORAL_UNREACHABLE,
            "The configured Temporal target is unavailable",
            retryable=True,
        )
    return OpsMcpError(
        ErrorCode.INTERNAL_ERROR,
        "Temporal could not complete the visibility operation",
        retryable=error.status in {RPCStatusCode.CANCELLED, RPCStatusCode.RESOURCE_EXHAUSTED},
    )


def _sanitize_visibility_rejection(message: str) -> str:
    """Keep parser feedback useful without forwarding connection or credential material."""

    sanitized = _PEM_MATERIAL.sub("[REDACTED]", message)
    sanitized = _CONTROL_CHARACTERS.sub(" ", sanitized)
    sanitized = _CONNECTION_URI.sub("[REDACTED]", sanitized)
    sanitized = _HOST_AND_PORT.sub("[REDACTED]", sanitized)
    sanitized = _SENSITIVE_ASSIGNMENT.sub("[REDACTED]", sanitized)
    sanitized = _SENSITIVE_IDENTIFIER.sub("[REDACTED]", sanitized)
    sanitized = " ".join(sanitized.split()).strip()
    if not sanitized:
        return "Temporal did not provide a safe rejection reason"
    if len(sanitized) <= MAX_VISIBILITY_REJECTION_REASON_LENGTH:
        return sanitized
    return sanitized[: MAX_VISIBILITY_REJECTION_REASON_LENGTH - 1].rstrip() + "…"

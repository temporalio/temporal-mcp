"""Validation and bounded projection for registered custom search attributes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from ops_mcp.contracts.visibility import (
    PublicIndexedValueType,
    SearchAttributeProjection,
)
from ops_mcp.facts import (
    IndexedValueTypeFact,
    SearchAttributeFieldFact,
    SearchAttributeSchemaFact,
)
from ops_mcp.runtime import ErrorCode, OpsMcpError, RequestContext
from ops_mcp.targets import ResolvedTarget

MAX_PROJECTED_STRING_LENGTH = 256
MAX_PROJECTED_KEYWORD_LIST_ITEMS = 20

# Policy for the custom-only projection/grouping inputs. GetSearchAttributes itself returns all
# legal names without provenance, so public schema discovery does not apply this classification.
PREDEFINED_SEARCH_ATTRIBUTE_NAMES = frozenset(
    {
        "BatcherNamespace",
        "BatcherUser",
        "BinaryChecksums",
        "BuildIds",
        "CloseTime",
        "ExecutionDuration",
        "ExecutionStatus",
        "ExecutionTime",
        "HistoryLength",
        "HistorySizeBytes",
        "ParentRunId",
        "ParentWorkflowId",
        "RootRunId",
        "RootWorkflowId",
        "RunId",
        "StartTime",
        "StateTransitionCount",
        "TaskQueue",
        "TemporalChangeVersion",
        "TemporalNamespaceDivision",
        "TemporalPauseInfo",
        "TemporalReportedProblems",
        "TemporalSchedulePaused",
        "TemporalScheduledById",
        "TemporalScheduledStartTime",
        "TemporalUsedWorkerDeploymentVersions",
        "TemporalWorkerDeployment",
        "TemporalWorkerDeploymentVersion",
        "TemporalWorkflowVersioningBehavior",
        "WorkflowId",
        "WorkflowType",
    }
)


def _is_predefined_search_attribute(name: str) -> bool:
    return name in PREDEFINED_SEARCH_ATTRIBUTE_NAMES or name.startswith("Temporal")

SYSTEM_SEARCH_ATTRIBUTES = {
    "TemporalReportedProblems": SearchAttributeFieldFact(
        name="TemporalReportedProblems",
        value_type=IndexedValueTypeFact(code=7, name="KEYWORD_LIST"),
    )
}


class SearchAttributeSchemaGateway(Protocol):
    async def get_legal_search_attributes(
        self, context: RequestContext, target: ResolvedTarget
    ) -> SearchAttributeSchemaFact: ...


class SearchAttributeSchemaResolver:
    """Cache the legal Search Attribute names/types exposed by WorkflowService."""

    def __init__(
        self,
        gateway: SearchAttributeSchemaGateway,
        *,
        ttl_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        self._gateway = gateway
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._cache: dict[object, tuple[float, SearchAttributeSchemaFact]] = {}
        self._inflight: dict[object, asyncio.Task[SearchAttributeSchemaFact]] = {}

    async def custom_fields(
        self, context: RequestContext, target: ResolvedTarget, requested: tuple[str, ...]
    ) -> dict[str, SearchAttributeFieldFact]:
        if not requested:
            return {}
        schema = await self.schema(context, target)
        return validate_custom_search_attributes(requested, schema.fields)

    async def schema(
        self, context: RequestContext, target: ResolvedTarget
    ) -> SearchAttributeSchemaFact:
        key = target.connection
        now = self._clock()
        cached = self._cache.get(key)
        if cached is not None and now - cached[0] < self._ttl_seconds:
            return cached[1]
        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(self._load_schema(key, context, target))
            self._inflight[key] = task
            task.add_done_callback(
                lambda completed, cache_key=key: self._clear_inflight(cache_key, completed)
            )
        return await asyncio.shield(task)

    def _clear_inflight(
        self, key: object, task: asyncio.Task[SearchAttributeSchemaFact]
    ) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]

    async def _load_schema(
        self, key: object, context: RequestContext, target: ResolvedTarget
    ) -> SearchAttributeSchemaFact:
        schema = await self._gateway.get_legal_search_attributes(context, target)
        self._cache[key] = (self._clock(), schema)
        return schema

def validate_custom_search_attributes(
    requested: tuple[str, ...],
    fields: tuple[SearchAttributeFieldFact, ...],
) -> dict[str, SearchAttributeFieldFact]:
    """Return requested custom fields, rejecting unregistered/system names explicitly."""

    custom = {
        field.name: field
        for field in fields
        if not _is_predefined_search_attribute(field.name)
    }
    unknown = tuple(name for name in requested if name not in custom)
    if unknown:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "Requested custom search attributes are not registered on this target",
            retryable=False,
            details={"unknown_custom_search_attributes": list(unknown)},
        )
    return {name: custom[name] for name in requested}


def project_search_attribute(
    name: str,
    field: SearchAttributeFieldFact,
    values: dict[str, object],
) -> SearchAttributeProjection:
    """Project one decoded value with explicit absence/truncation status."""

    value_type = public_indexed_value_type(field)
    if name not in values or values[name] is None:
        return SearchAttributeProjection(
            name=name,
            value_type=value_type,
            status="not_set",
        )
    value = values[name]
    bounded, truncated = _bounded_value(value)
    if bounded is None:
        return SearchAttributeProjection(
            name=name,
            value_type=value_type,
            status="unsupported",
            value_sha256=_value_hash(value),
        )
    return SearchAttributeProjection(
        name=name,
        value_type=value_type,
        status="truncated" if truncated else "present",
        value=bounded,
        value_sha256=_value_hash(value) if truncated else None,
    )


def public_indexed_value_type(
    field: SearchAttributeFieldFact,
) -> PublicIndexedValueType:
    try:
        return PublicIndexedValueType(field.value_type.name.casefold())
    except ValueError:
        return PublicIndexedValueType.UNKNOWN


def _bounded_value(
    value: object,
) -> tuple[str | int | float | bool | datetime | tuple[str, ...] | None, bool]:
    if isinstance(value, datetime) or isinstance(value, (int, float, bool)):
        return value, False
    if isinstance(value, str):
        return value[:MAX_PROJECTED_STRING_LENGTH], len(value) > MAX_PROJECTED_STRING_LENGTH
    if isinstance(value, tuple) and all(isinstance(item, str) for item in value):
        bounded = tuple(
            item[:MAX_PROJECTED_STRING_LENGTH] for item in value[:MAX_PROJECTED_KEYWORD_LIST_ITEMS]
        )
        truncated = len(value) > MAX_PROJECTED_KEYWORD_LIST_ITEMS or any(
            len(item) > MAX_PROJECTED_STRING_LENGTH for item in value
        )
        return bounded, truncated
    return None, False


def _value_hash(value: object) -> str:
    serializable: object = value.isoformat() if isinstance(value, datetime) else value
    encoded = json.dumps(serializable, sort_keys=True, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()

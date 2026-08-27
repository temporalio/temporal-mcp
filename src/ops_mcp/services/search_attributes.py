"""Validation and bounded projection for registered custom search attributes."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from ops_mcp.contracts.visibility import (
    PublicIndexedValueType,
    SearchAttributeProjection,
)
from ops_mcp.facts import SearchAttributeFieldFact, SearchAttributeSource
from ops_mcp.runtime import ErrorCode, OpsMcpError

MAX_PROJECTED_STRING_LENGTH = 256
MAX_PROJECTED_KEYWORD_LIST_ITEMS = 20


def validate_custom_search_attributes(
    requested: tuple[str, ...],
    fields: tuple[SearchAttributeFieldFact, ...],
) -> dict[str, SearchAttributeFieldFact]:
    """Return requested custom fields, rejecting unregistered/system names explicitly."""

    custom = {field.name: field for field in fields if field.source is SearchAttributeSource.CUSTOM}
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

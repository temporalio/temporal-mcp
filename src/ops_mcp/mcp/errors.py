"""Safe translation from internal errors to the MCP transport."""

from __future__ import annotations

import json
import re
from typing import NoReturn

from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ValidationError

from ops_mcp.runtime import ErrorCode, OpsMcpError


def raise_request_validation_error(exc: ValidationError) -> NoReturn:
    fields = []
    for item in exc.errors(include_input=False, include_url=False):
        field = {"location": ".".join(map(str, item["loc"])), "type": item["type"]}
        expected = (item.get("ctx") or {}).get("expected")
        if isinstance(expected, str):
            allowed_values = tuple(dict.fromkeys(re.findall(r"'([^']+)'", expected)))
            if allowed_values:
                field["allowed_values"] = allowed_values
        fields.append(field)
    details = {
        "fields": fields
    }
    raise _tool_error(
        OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The tool arguments are invalid",
            retryable=False,
            details=details,
        )
    ) from exc


def raise_invalid_argument(message: str, *, allowed_values: tuple[str, ...] = ()) -> NoReturn:
    details = {"allowed_values": allowed_values} if allowed_values else None
    raise _tool_error(
        OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            message,
            retryable=False,
            details=details,
        )
    )


def raise_service_error(exc: Exception) -> NoReturn:
    if isinstance(exc, OpsMcpError):
        raise _tool_error(exc) from exc
    raise _tool_error(
        OpsMcpError(
            ErrorCode.INTERNAL_ERROR,
            "The tool could not complete because of an internal error",
            retryable=False,
        )
    ) from exc


def _tool_error(exc: OpsMcpError) -> ToolError:
    return ToolError(json.dumps(exc.as_dict(), sort_keys=True, separators=(",", ":")))

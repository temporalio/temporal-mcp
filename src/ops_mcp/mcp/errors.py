"""Safe translation from internal errors to the MCP transport."""

from __future__ import annotations

import json
import logging
import re
from time import monotonic
from traceback import extract_tb
from typing import NoReturn

from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ValidationError

from ops_mcp.runtime import ErrorCode, OpsMcpError
from ops_mcp.runtime.context import RequestContext

_LOGGER = logging.getLogger("ops_mcp.tools")
_SAFE_TARGET_REF = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


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
    details = {"fields": fields}
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


def raise_service_error(
    exc: Exception,
    *,
    context: RequestContext | None = None,
    tool_name: str = "unknown",
    target_ref: str | None = None,
) -> NoReturn:
    if isinstance(exc, OpsMcpError):
        raise _tool_error(exc) from exc
    request_id = context.request_id if context is not None else "unavailable"
    elapsed_ms = (
        max(0.0, (monotonic() - context.started_monotonic) * 1_000) if context is not None else 0.0
    )
    safe_target = (
        target_ref if target_ref is not None and _SAFE_TARGET_REF.fullmatch(target_ref) else None
    )
    fields = {
        "request_id": request_id,
        "tool_name": tool_name,
        "elapsed_ms": round(elapsed_ms, 3),
        "error_code": ErrorCode.INTERNAL_ERROR.value,
    }
    if safe_target is not None:
        fields["target_ref"] = safe_target
    fields["traceback"] = [
        {"file": frame.filename, "line": frame.lineno, "function": frame.name}
        for frame in extract_tb(exc.__traceback__)
    ]
    message = json.dumps(
        {"event": "unexpected_tool_failure", **fields},
        sort_keys=True,
        separators=(",", ":"),
    )
    _LOGGER.error(message, extra=fields)
    raise _tool_error(
        OpsMcpError(
            ErrorCode.INTERNAL_ERROR,
            "The tool could not complete because of an internal error",
            retryable=False,
            details={"request_id": request_id},
        )
    ) from exc


def _tool_error(exc: OpsMcpError) -> ToolError:
    return ToolError(json.dumps(exc.as_dict(), sort_keys=True, separators=(",", ":")))

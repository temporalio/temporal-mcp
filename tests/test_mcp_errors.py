import json
import logging

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from ops_mcp.mcp.errors import raise_service_error
from ops_mcp.runtime import (
    BasicExecutionBudget,
    OpsMcpError,
    Principal,
    RequestContext,
)
from ops_mcp.runtime.errors import ErrorCode


def _context() -> RequestContext:
    from datetime import UTC, datetime, timedelta

    return RequestContext(
        request_id="request-correlation-1",
        principal=Principal(subject="tester", groups=("support",)),
        budget=BasicExecutionBudget(deadline=datetime.now(UTC) + timedelta(minutes=1)),
    )


def test_unexpected_error_is_correlated_and_redacted(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR, logger="ops_mcp.tools")

    with pytest.raises(ToolError) as exc_info:
        try:
            raise RuntimeError("api-key=super-secret raw-query=private")
        except RuntimeError as exc:
            raise_service_error(
                exc,
                context=_context(),
                tool_name="inspect_workflow_execution",
                target_ref="orders-prod",
            )

    payload = json.loads(str(exc_info.value))
    record = caplog.records[-1]
    rendered = json.loads(record.getMessage())
    assert payload["details"]["request_id"] == "request-correlation-1"
    assert rendered["event"] == "unexpected_tool_failure"
    assert rendered["request_id"] == "request-correlation-1"
    assert rendered["tool_name"] == "inspect_workflow_execution"
    assert rendered["error_code"] == "internal_error"
    assert rendered["target_ref"] == "orders-prod"
    assert rendered["elapsed_ms"] >= 0
    assert any(
        frame["function"] == "test_unexpected_error_is_correlated_and_redacted"
        for frame in rendered["traceback"]
    )
    assert "super-secret" not in caplog.text
    assert "private" not in caplog.text
    assert "super-secret" not in str(exc_info.value)


def test_expected_error_is_not_logged_as_internal(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR, logger="ops_mcp.tools")
    expected = OpsMcpError(
        ErrorCode.TEMPORAL_UNREACHABLE,
        "Temporal is unavailable",
        retryable=True,
    )

    with pytest.raises(ToolError) as exc_info:
        raise_service_error(
            expected,
            context=_context(),
            tool_name="check_connection",
            target_ref="orders-prod",
        )

    assert json.loads(str(exc_info.value))["code"] == "temporal_unreachable"
    assert caplog.records == []

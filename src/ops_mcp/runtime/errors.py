"""Typed internal errors with stable public classifications."""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    TARGET_NOT_FOUND = "target_not_found"
    TARGET_NOT_AUTHORIZED = "target_not_authorized"
    TEMPORAL_UNREACHABLE = "temporal_unreachable"
    TEMPORAL_AUTHENTICATION_FAILED = "temporal_authentication_failed"
    NAMESPACE_NOT_FOUND = "namespace_not_found"
    INVALID_VISIBILITY_QUERY = "invalid_visibility_query"
    WORKFLOW_NOT_FOUND = "workflow_not_found"
    RUN_NOT_FOUND = "run_not_found"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    CODEC_UNREACHABLE = "codec_unreachable"
    PAYLOAD_DECODE_FAILED = "payload_decode_failed"
    QUERY_NOT_APPROVED = "query_not_approved"
    QUERY_FAILED = "query_failed"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    BUDGET_EXHAUSTED = "budget_exhausted"
    RESULT_TOO_LARGE = "result_too_large"
    INCOMPLETE_HISTORY = "incomplete_history"
    UNSAFE_TO_RECOMMEND = "unsafe_to_recommend"
    INTERNAL_ERROR = "internal_error"


class OpsMcpError(Exception):
    """Base error safe for an adapter to translate into a structured tool error."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = dict(details) if details else None

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "code": self.code.value,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.details:
            value["details"] = self.details
        return value


class DeadlineExceededError(OpsMcpError):
    def __init__(self) -> None:
        super().__init__(
            ErrorCode.DEADLINE_EXCEEDED,
            "The request deadline was exceeded",
            retryable=True,
        )


class RequestCancelledError(OpsMcpError):
    def __init__(self) -> None:
        super().__init__(
            ErrorCode.DEADLINE_EXCEEDED,
            "The request was cancelled",
            retryable=True,
            details={"reason": "cancelled"},
        )


class BudgetExceededError(OpsMcpError):
    def __init__(self, dimension: str, limit: int, consumed: int, attempted: int) -> None:
        super().__init__(
            ErrorCode.BUDGET_EXHAUSTED,
            f"The request's {dimension} budget was exhausted",
            retryable=False,
            details={
                "dimension": dimension,
                "limit": limit,
                "consumed": consumed,
                "attempted": attempted,
            },
        )


class ResultTooLargeError(OpsMcpError):
    def __init__(self, limit: int, consumed: int, attempted: int) -> None:
        super().__init__(
            ErrorCode.RESULT_TOO_LARGE,
            "The result exceeds the request's output-size limit",
            retryable=False,
            details={"limit": limit, "consumed": consumed, "attempted": attempted},
        )


class InvalidConfigurationError(OpsMcpError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INTERNAL_ERROR, message, retryable=False)


class TargetNotFoundError(OpsMcpError):
    def __init__(self, human_name: str) -> None:
        super().__init__(
            ErrorCode.TARGET_NOT_FOUND,
            f"No target named {human_name!r} is configured",
            retryable=False,
        )


class TargetNotAuthorizedError(OpsMcpError):
    def __init__(self, target: str) -> None:
        super().__init__(
            ErrorCode.TARGET_NOT_AUTHORIZED,
            f"The caller is not authorized for target {target!r}",
            retryable=False,
        )


class MissingCredentialError(OpsMcpError):
    def __init__(self, target_ref: str) -> None:
        super().__init__(
            ErrorCode.TEMPORAL_AUTHENTICATION_FAILED,
            f"Credentials are not configured for target {target_ref!r}",
            retryable=False,
            details={"reason": "credential_missing"},
        )


class ProviderClosedError(OpsMcpError):
    def __init__(self) -> None:
        super().__init__(
            ErrorCode.INTERNAL_ERROR,
            "The Temporal client provider is shut down",
            retryable=False,
        )

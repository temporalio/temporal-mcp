"""Request identity and execution context seams."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Protocol
from uuid import uuid4

from ops_mcp.runtime.budget import BasicBudgetLimits, BasicExecutionBudget, ExecutionBudget


@dataclass(frozen=True)
class Principal:
    subject: str
    groups: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.subject:
            raise ValueError("principal subject must not be empty")


class IdentityProvider(Protocol):
    def current_principal(self) -> Principal: ...


class DemoIdentityProvider:
    """Unauthenticated POC identity source; replace at the transport edge later."""

    def __init__(
        self,
        group_env: str = "OPS_MCP_GROUP",
        legacy_group_env: str | None = "OPS_MCP_DEMO_GROUP",
    ) -> None:
        self._group_env = group_env
        self._legacy_group_env = legacy_group_env

    def current_principal(self) -> Principal:
        group = os.getenv(self._group_env)
        if group is None and self._legacy_group_env is not None:
            group = os.getenv(self._legacy_group_env)
        group = group or "operators"
        return Principal(subject="unauthenticated-demo", groups=(group,))


@dataclass(frozen=True)
class RequestContext:
    request_id: str
    principal: Principal
    budget: ExecutionBudget
    started_monotonic: float = field(default_factory=monotonic)

    @property
    def deadline(self) -> datetime:
        return self.budget.deadline


class RequestContextFactory:
    def __init__(
        self,
        identity_provider: IdentityProvider,
        *,
        default_timeout: timedelta = timedelta(seconds=30),
        default_limits: BasicBudgetLimits | None = None,
    ) -> None:
        if default_timeout <= timedelta(0):
            raise ValueError("default_timeout must be positive")
        self._identity_provider = identity_provider
        self._default_timeout = default_timeout
        self._default_limits = default_limits or BasicBudgetLimits()

    def create(
        self,
        *,
        timeout: timedelta | None = None,
        request_id: str | None = None,
        limits: BasicBudgetLimits | None = None,
    ) -> RequestContext:
        effective_timeout = timeout or self._default_timeout
        if effective_timeout <= timedelta(0):
            raise ValueError("timeout must be positive")
        deadline = datetime.now(UTC) + effective_timeout
        budget = BasicExecutionBudget(deadline=deadline, limits=limits or self._default_limits)
        return RequestContext(
            request_id=request_id or str(uuid4()),
            principal=self._identity_provider.current_principal(),
            budget=budget,
            started_monotonic=monotonic(),
        )

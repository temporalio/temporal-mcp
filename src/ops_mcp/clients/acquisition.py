"""Request-bounded acquisition of process-scoped Temporal clients."""

from __future__ import annotations

import asyncio

from temporalio.client import Client

from ops_mcp.clients.provider import TemporalClientProvider
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import DeadlineExceededError
from ops_mcp.targets.models import ResolvedTarget


async def acquire_client(
    provider: TemporalClientProvider,
    context: RequestContext,
    target: ResolvedTarget,
) -> Client:
    """Wait only through this request's deadline without cancelling the shared attempt."""
    timeout = context.budget.remaining_seconds()
    try:
        async with asyncio.timeout(timeout):
            return await provider.get_client(target.target_ref, target.connection)
    except TimeoutError as exc:
        raise DeadlineExceededError from exc

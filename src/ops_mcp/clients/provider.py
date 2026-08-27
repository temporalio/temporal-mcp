"""Concurrency-safe Temporal SDK client construction and reuse."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Protocol, cast

from temporalio.client import Client
from temporalio.service import TLSConfig

from ops_mcp.runtime.errors import MissingCredentialError, ProviderClosedError
from ops_mcp.targets.models import ConnectionDescriptor

ClientConnector = Callable[..., Awaitable[Client]]
CredentialResolver = Callable[[str], str | None]


class TemporalClientProvider(Protocol):
    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> Client: ...

    async def invalidate(self, descriptor: ConnectionDescriptor | None = None) -> None: ...

    async def shutdown(self) -> None: ...


class CachingTemporalClientProvider:
    """Process-scoped client cache for one asyncio event loop.

    Temporal Python clients are not thread-safe and must remain on the event loop where they were
    connected. The MCP application composition root must therefore create and use this provider on
    its serving loop.

    temporalio 1.31 has no public client close API. Invalidation and shutdown drop owned
    references; shutdown also permanently rejects new acquisitions.
    """

    def __init__(
        self,
        *,
        connector: ClientConnector | None = None,
        credential_resolver: CredentialResolver = os.getenv,
        identity: str = "ops-mcp",
    ) -> None:
        self._connector = connector or cast(ClientConnector, Client.connect)
        self._credential_resolver = credential_resolver
        self._identity = identity
        self._clients: dict[ConnectionDescriptor, Client] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> Client:
        async with self._lock:
            if self._closed:
                raise ProviderClosedError
            cached = self._clients.get(descriptor)
            if cached is not None:
                return cached
            api_key = self._resolve_api_key(target_ref, descriptor)
            tls: bool | TLSConfig = descriptor.tls
            if descriptor.server_name:
                tls = TLSConfig(domain=descriptor.server_name)
            client = await self._connector(
                descriptor.address,
                namespace=descriptor.namespace,
                tls=tls,
                api_key=api_key,
                identity=self._identity,
            )
            self._clients[descriptor] = client
            return client

    async def invalidate(self, descriptor: ConnectionDescriptor | None = None) -> None:
        async with self._lock:
            if descriptor is None:
                self._clients.clear()
            else:
                self._clients.pop(descriptor, None)

    async def shutdown(self) -> None:
        async with self._lock:
            self._closed = True
            self._clients.clear()

    @property
    def closed(self) -> bool:
        return self._closed

    def _resolve_api_key(self, target_ref: str, descriptor: ConnectionDescriptor) -> str | None:
        if descriptor.api_key_env is None:
            return None
        api_key = self._credential_resolver(descriptor.api_key_env)
        if not api_key:
            raise MissingCredentialError(target_ref)
        return api_key

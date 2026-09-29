"""Concurrency-safe Temporal SDK client construction and reuse."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import Protocol, cast

from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode, TLSConfig

from ops_mcp.runtime.errors import (
    ErrorCode,
    MissingCredentialError,
    OpsMcpError,
    ProviderClosedError,
)
from ops_mcp.targets.models import ConnectionDescriptor

ClientConnector = Callable[..., Awaitable[Client]]
CredentialResolver = Callable[[str], str | None]
CredentialFileReader = Callable[[str], bytes]


def _read_credential_file(path: str) -> bytes:
    return Path(path).read_bytes()


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
        credential_file_reader: CredentialFileReader = _read_credential_file,
        identity: str = "ops-mcp",
        connection_attempt_timeout: float = 15.0,
    ) -> None:
        if connection_attempt_timeout <= 0:
            raise ValueError("connection_attempt_timeout must be positive")
        self._connector = connector or cast(ClientConnector, Client.connect)
        self._credential_resolver = credential_resolver
        self._credential_file_reader = credential_file_reader
        self._identity = identity
        self._connection_attempt_timeout = connection_attempt_timeout
        self._clients: dict[ConnectionDescriptor, Client] = {}
        self._connections: dict[ConnectionDescriptor, asyncio.Task[Client]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def get_client(self, target_ref: str, descriptor: ConnectionDescriptor) -> Client:
        async with self._lock:
            if self._closed:
                raise ProviderClosedError
            cached = self._clients.get(descriptor)
            if cached is not None:
                return cached
            connection = self._connections.get(descriptor)
            if connection is None:
                connection = asyncio.create_task(self._connection_attempt(target_ref, descriptor))
                connection.add_done_callback(_consume_task_exception)
                self._connections[descriptor] = connection

        client = await asyncio.shield(connection)

        async with self._lock:
            if self._closed:
                raise ProviderClosedError
            return self._clients.get(descriptor, client)

    async def invalidate(self, descriptor: ConnectionDescriptor | None = None) -> None:
        async with self._lock:
            if descriptor is None:
                self._clients.clear()
                pending = tuple(self._connections.values())
                self._connections.clear()
            else:
                self._clients.pop(descriptor, None)
                connection = self._connections.pop(descriptor, None)
                pending = (connection,) if connection is not None else ()
        await _cancel_all(pending)

    async def shutdown(self) -> None:
        async with self._lock:
            self._closed = True
            self._clients.clear()
            pending = tuple(self._connections.values())
            self._connections.clear()
        await _cancel_all(pending)

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

    def _read_tls_file(self, target_ref: str, path: str | None) -> bytes | None:
        if path is None:
            return None
        try:
            contents = self._credential_file_reader(path)
        except OSError as exc:
            raise MissingCredentialError(target_ref) from exc
        if not contents:
            raise MissingCredentialError(target_ref)
        return contents

    def _tls_config(self, target_ref: str, descriptor: ConnectionDescriptor) -> bool | TLSConfig:
        if not descriptor.tls:
            return False
        if not any(
            (
                descriptor.server_name,
                descriptor.client_cert_path,
                descriptor.client_key_path,
                descriptor.server_ca_cert_path,
            )
        ):
            return True
        return TLSConfig(
            server_root_ca_cert=self._read_tls_file(target_ref, descriptor.server_ca_cert_path),
            domain=descriptor.server_name,
            client_cert=self._read_tls_file(target_ref, descriptor.client_cert_path),
            client_private_key=self._read_tls_file(target_ref, descriptor.client_key_path),
        )

    async def _connect(self, target_ref: str, descriptor: ConnectionDescriptor) -> Client:
        api_key = self._resolve_api_key(target_ref, descriptor)
        return await self._connector(
            descriptor.address,
            namespace=descriptor.namespace,
            tls=self._tls_config(target_ref, descriptor),
            api_key=api_key,
            identity=self._identity,
        )

    async def _connection_attempt(
        self, target_ref: str, descriptor: ConnectionDescriptor
    ) -> Client:
        current = asyncio.current_task()
        client: Client | None = None
        try:
            try:
                async with asyncio.timeout(self._connection_attempt_timeout):
                    client = await self._connect(target_ref, descriptor)
            except TimeoutError as exc:
                raise OpsMcpError(
                    ErrorCode.TEMPORAL_UNREACHABLE,
                    "The Temporal connection attempt timed out",
                    retryable=True,
                    details={"reason": "connection_attempt_timeout"},
                ) from exc
            except OpsMcpError:
                raise
            except Exception as exc:
                raise _translate_connection_error(exc) from exc
            return client
        finally:
            async with self._lock:
                if self._connections.get(descriptor) is current:
                    self._connections.pop(descriptor, None)
                    if client is not None and not self._closed:
                        self._clients[descriptor] = client


async def _cancel_all(tasks: tuple[asyncio.Task[Client], ...]) -> None:
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _consume_task_exception(task: asyncio.Task[Client]) -> None:
    """Retrieve provider-owned task failures after all request waiters have left."""
    if not task.cancelled():
        with suppress(BaseException):
            task.exception()


def _translate_connection_error(exc: Exception) -> OpsMcpError:
    if isinstance(exc, OpsMcpError):
        return exc
    if isinstance(exc, RPCError):
        if exc.status is RPCStatusCode.UNAUTHENTICATED:
            return OpsMcpError(
                ErrorCode.TEMPORAL_AUTHENTICATION_FAILED,
                "Temporal rejected the configured target credentials",
                retryable=False,
                details={"operation": "connect", "grpc_status": "unauthenticated"},
            )
        if exc.status is RPCStatusCode.PERMISSION_DENIED:
            return OpsMcpError(
                ErrorCode.TEMPORAL_PERMISSION_DENIED,
                "Temporal denied the configured target permission for this operation",
                retryable=False,
                details={"operation": "connect", "grpc_status": "permission_denied"},
            )
        if exc.status in {
            RPCStatusCode.UNAVAILABLE,
            RPCStatusCode.DEADLINE_EXCEEDED,
            RPCStatusCode.CANCELLED,
        }:
            return OpsMcpError(
                ErrorCode.TEMPORAL_UNREACHABLE,
                "The configured Temporal target is unavailable",
                retryable=True,
            )
    if isinstance(exc, (ConnectionError, OSError)):
        return OpsMcpError(
            ErrorCode.TEMPORAL_UNREACHABLE,
            "The configured Temporal target is unavailable",
            retryable=True,
        )
    return OpsMcpError(
        ErrorCode.INTERNAL_ERROR,
        "Temporal client setup failed unexpectedly",
        retryable=False,
    )

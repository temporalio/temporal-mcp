import asyncio
from datetime import timedelta
from typing import Any, cast

import pytest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode, TLSConfig

from ops_mcp.clients import CachingTemporalClientProvider, acquire_client
from ops_mcp.runtime import (
    DeadlineExceededError,
    ErrorCode,
    MissingCredentialError,
    OpsMcpError,
    Principal,
    ProviderClosedError,
    RequestContextFactory,
)
from ops_mcp.targets import ConnectionDescriptor, ResolvedTarget


class FakeConnector:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def __call__(self, address: str, **kwargs: Any) -> Client:
        await asyncio.sleep(0)
        client = cast(Client, object())
        self.calls.append((address, kwargs))
        return client


async def test_provider_reuses_one_concurrently_created_client() -> None:
    connector = FakeConnector()
    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="localhost:7233", namespace="default")

    clients = await asyncio.gather(
        *(provider.get_client("orders-prod", descriptor) for _ in range(20))
    )

    assert all(client is clients[0] for client in clients)
    assert len(connector.calls) == 1
    assert connector.calls[0][1]["namespace"] == "default"


async def test_provider_classifies_missing_credentials_without_leaking_env_name() -> None:
    provider = CachingTemporalClientProvider(credential_resolver=lambda _: None)
    descriptor = ConnectionDescriptor(
        address="cloud.example:7233",
        namespace="orders",
        tls=True,
        api_key_env="SECRET_TEMPORAL_KEY",
    )

    with pytest.raises(MissingCredentialError) as exc_info:
        await provider.get_client("orders-prod", descriptor)

    assert exc_info.value.code == "temporal_authentication_failed"
    assert "SECRET_TEMPORAL_KEY" not in str(exc_info.value)


async def test_provider_builds_mtls_config_from_server_side_files() -> None:
    connector = FakeConnector()
    credentials = {
        "/run/secrets/temporal-client.pem": b"client cert",
        "/run/secrets/temporal-client.key": b"client key",
        "/run/secrets/temporal-ca.pem": b"server ca",
    }
    provider = CachingTemporalClientProvider(
        connector=connector,
        credential_file_reader=credentials.__getitem__,
    )
    descriptor = ConnectionDescriptor(
        address="temporal.example:7233",
        namespace="orders",
        tls=True,
        server_name="temporal.internal",
        client_cert_path="/run/secrets/temporal-client.pem",
        client_key_path="/run/secrets/temporal-client.key",
        server_ca_cert_path="/run/secrets/temporal-ca.pem",
    )

    await provider.get_client("orders-prod", descriptor)

    tls = connector.calls[0][1]["tls"]
    assert isinstance(tls, TLSConfig)
    assert tls.client_cert == b"client cert"
    assert tls.client_private_key == b"client key"
    assert tls.server_root_ca_cert == b"server ca"
    assert tls.domain == "temporal.internal"


async def test_provider_classifies_unreadable_mtls_file_without_leaking_path() -> None:
    def unreadable(_: str) -> bytes:
        raise OSError("private path details")

    provider = CachingTemporalClientProvider(credential_file_reader=unreadable)
    descriptor = ConnectionDescriptor(
        address="temporal.example:7233",
        namespace="orders",
        tls=True,
        client_cert_path="/secret/client.pem",
        client_key_path="/secret/client.key",
    )

    with pytest.raises(MissingCredentialError) as exc_info:
        await provider.get_client("orders-prod", descriptor)

    assert exc_info.value.code == ErrorCode.TEMPORAL_AUTHENTICATION_FAILED
    assert "/secret/" not in str(exc_info.value)


async def test_provider_invalidation_and_shutdown() -> None:
    connector = FakeConnector()
    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="localhost:7233", namespace="default")

    first = await provider.get_client("orders-prod", descriptor)
    await provider.invalidate(descriptor)
    second = await provider.get_client("orders-prod", descriptor)
    await provider.shutdown()

    assert first is not second
    assert len(connector.calls) == 2
    assert provider.closed is True
    with pytest.raises(ProviderClosedError):
        await provider.get_client("orders-prod", descriptor)


async def test_provider_connects_distinct_targets_without_global_serialization() -> None:
    both_started = asyncio.Event()
    release = asyncio.Event()
    starts = 0

    async def connector(address: str, **kwargs: Any) -> Client:
        nonlocal starts
        starts += 1
        if starts == 2:
            both_started.set()
        await release.wait()
        return cast(Client, object())

    provider = CachingTemporalClientProvider(connector=connector)
    first = ConnectionDescriptor(address="first:7233", namespace="default")
    second = ConnectionDescriptor(address="second:7233", namespace="default")
    tasks = (
        asyncio.create_task(provider.get_client("first", first)),
        asyncio.create_task(provider.get_client("second", second)),
    )

    await asyncio.wait_for(both_started.wait(), timeout=0.5)
    release.set()
    await asyncio.gather(*tasks)


async def test_client_acquisition_obeys_request_deadline() -> None:
    never_connect = asyncio.Event()

    async def connector(address: str, **kwargs: Any) -> Client:
        await never_connect.wait()
        return cast(Client, object())

    class Identity:
        def current_principal(self) -> Principal:
            return Principal(subject="tester", groups=("support",))

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="slow:7233", namespace="default")
    target = ResolvedTarget("slow", (), "acme", "prod", "default", descriptor)
    context = RequestContextFactory(Identity(), default_timeout=timedelta(milliseconds=10)).create()

    with pytest.raises(DeadlineExceededError):
        await acquire_client(provider, context, target)
    await provider.shutdown()


async def test_shared_connection_failure_is_classified_once() -> None:
    starts = 0

    async def connector(address: str, **kwargs: Any) -> Client:
        nonlocal starts
        starts += 1
        raise OSError("connection refused")

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="offline:7233", namespace="default")
    results = await asyncio.gather(
        *(provider.get_client("offline", descriptor) for _ in range(5)),
        return_exceptions=True,
    )

    assert starts == 1
    assert all(getattr(item, "code", None) is ErrorCode.TEMPORAL_UNREACHABLE for item in results)


async def test_authentication_failure_is_classified() -> None:
    async def connector(address: str, **kwargs: Any) -> Client:
        raise RPCError("denied", RPCStatusCode.UNAUTHENTICATED, b"")

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="cloud:7233", namespace="default")

    with pytest.raises(Exception) as exc_info:
        await provider.get_client("cloud", descriptor)

    assert getattr(exc_info.value, "code", None) is ErrorCode.TEMPORAL_AUTHENTICATION_FAILED


async def test_arbitrary_runtime_error_is_not_classified_as_unreachable() -> None:
    async def connector(address: str, **kwargs: Any) -> Client:
        raise RuntimeError("connector programming error")

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="broken:7233", namespace="default")

    with pytest.raises(OpsMcpError) as exc_info:
        await provider.get_client("broken", descriptor)

    assert getattr(exc_info.value, "code", None) is ErrorCode.INTERNAL_ERROR
    assert getattr(exc_info.value, "retryable", None) is False


async def test_provider_attempt_timeout_allows_later_recovery() -> None:
    never = asyncio.Event()
    calls = 0

    async def connector(address: str, **kwargs: Any) -> Client:
        nonlocal calls
        calls += 1
        if calls == 1:
            await never.wait()
        return cast(Client, object())

    provider = CachingTemporalClientProvider(
        connector=connector,
        connection_attempt_timeout=0.01,
    )
    descriptor = ConnectionDescriptor(address="slow:7233", namespace="default")

    with pytest.raises(Exception) as exc_info:
        await provider.get_client("slow", descriptor)
    recovered = await provider.get_client("slow", descriptor)

    assert getattr(exc_info.value, "code", None) is ErrorCode.TEMPORAL_UNREACHABLE
    assert recovered is not None
    assert calls == 2


async def test_one_request_deadline_does_not_cancel_shared_attempt() -> None:
    release = asyncio.Event()

    async def connector(address: str, **kwargs: Any) -> Client:
        await release.wait()
        return cast(Client, object())

    class Identity:
        def current_principal(self) -> Principal:
            return Principal(subject="tester", groups=("support",))

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="shared:7233", namespace="default")
    target = ResolvedTarget("shared", (), "acme", "prod", "default", descriptor)
    short = RequestContextFactory(Identity(), default_timeout=timedelta(milliseconds=10)).create()
    patient = asyncio.create_task(provider.get_client("shared", descriptor))

    with pytest.raises(DeadlineExceededError):
        await acquire_client(provider, short, target)
    release.set()

    assert await patient is not None


async def test_invalidation_during_active_attempt_cancels_without_caching() -> None:
    started = asyncio.Event()
    release_replacement = asyncio.Event()
    calls = 0

    async def connector(address: str, **kwargs: Any) -> Client:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await asyncio.Event().wait()
        await release_replacement.wait()
        return cast(Client, object())

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="invalidate:7233", namespace="default")
    first = asyncio.create_task(provider.get_client("invalidate", descriptor))
    await asyncio.wait_for(started.wait(), timeout=0.5)

    await provider.invalidate(descriptor)

    with pytest.raises(asyncio.CancelledError):
        await first
    assert descriptor not in provider._connections
    assert descriptor not in provider._clients

    replacement = asyncio.create_task(provider.get_client("invalidate", descriptor))
    release_replacement.set()
    assert await replacement is not None
    assert calls == 2
    assert descriptor not in provider._connections
    assert descriptor in provider._clients


async def test_shutdown_during_active_attempt_cancels_and_rejects_later_requests() -> None:
    started = asyncio.Event()

    async def connector(address: str, **kwargs: Any) -> Client:
        started.set()
        await asyncio.Event().wait()
        return cast(Client, object())

    provider = CachingTemporalClientProvider(connector=connector)
    descriptor = ConnectionDescriptor(address="shutdown:7233", namespace="default")
    pending = asyncio.create_task(provider.get_client("shutdown", descriptor))
    await asyncio.wait_for(started.wait(), timeout=0.5)

    await provider.shutdown()

    with pytest.raises(asyncio.CancelledError):
        await pending
    assert provider.closed is True
    assert provider._connections == {}
    assert provider._clients == {}
    with pytest.raises(ProviderClosedError):
        await provider.get_client("shutdown", descriptor)

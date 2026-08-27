import asyncio
from typing import Any, cast

import pytest
from temporalio.client import Client

from ops_mcp.clients import CachingTemporalClientProvider
from ops_mcp.runtime import MissingCredentialError, ProviderClosedError
from ops_mcp.targets import ConnectionDescriptor


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

from __future__ import annotations

import os

from temporalio.client import Client


async def connect() -> Client:
    address = os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    api_key = os.getenv("TEMPORAL_API_KEY")
    tls = os.getenv("TEMPORAL_TLS", "false").casefold() in {"1", "true", "yes"}
    return await Client.connect(address, namespace=namespace, api_key=api_key, tls=tls)

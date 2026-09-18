"""Internal target and connection types that never cross the MCP boundary directly."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConnectionDescriptor:
    address: str
    namespace: str
    tls: bool = False
    api_key_env: str | None = None
    server_name: str | None = None
    credential_generation: int = 0

    def __post_init__(self) -> None:
        if not self.address:
            raise ValueError("connection address must not be empty")
        if not self.namespace:
            raise ValueError("connection namespace must not be empty")
        if self.api_key_env == "":
            raise ValueError("api_key_env must be non-empty when provided")
        if self.server_name and not self.tls:
            raise ValueError("server_name requires TLS")
        if self.credential_generation < 0:
            raise ValueError("credential_generation must be non-negative")


@dataclass(frozen=True)
class ResolvedTarget:
    target_ref: str
    aliases: tuple[str, ...]
    account: str
    cluster: str
    namespace: str
    connection: ConnectionDescriptor

    def __post_init__(self) -> None:
        for name, value in (
            ("target_ref", self.target_ref),
            ("account", self.account),
            ("cluster", self.cluster),
            ("namespace", self.namespace),
        ):
            if not value:
                raise ValueError(f"{name} must not be empty")

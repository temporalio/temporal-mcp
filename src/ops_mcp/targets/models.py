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
    client_cert_path: str | None = None
    client_key_path: str | None = None
    server_ca_cert_path: str | None = None

    def __post_init__(self) -> None:
        if not self.address:
            raise ValueError("connection address must not be empty")
        if not self.namespace:
            raise ValueError("connection namespace must not be empty")
        if self.api_key_env == "":
            raise ValueError("api_key_env must be non-empty when provided")
        tls_paths = (
            self.client_cert_path,
            self.client_key_path,
            self.server_ca_cert_path,
        )
        if any(path == "" for path in tls_paths):
            raise ValueError("TLS certificate paths must be non-empty when provided")
        if (self.client_cert_path is None) != (self.client_key_path is None):
            raise ValueError("client_cert_path and client_key_path must be configured together")
        if (self.server_name or any(tls_paths)) and not self.tls:
            raise ValueError("TLS options require TLS")
        if self.credential_generation < 0:
            raise ValueError("credential_generation must be non-negative")


@dataclass(frozen=True)
class ResolvedTarget:
    target_ref: str
    aliases: tuple[str, ...]
    account: str | None
    cluster: str | None
    namespace: str
    connection: ConnectionDescriptor

    def __post_init__(self) -> None:
        for name, value in (
            ("target_ref", self.target_ref),
            ("namespace", self.namespace),
        ):
            if not value:
                raise ValueError(f"{name} must not be empty")
        for name, value in (("account", self.account), ("cluster", self.cluster)):
            if value is not None and not value:
                raise ValueError(f"{name} must not be empty when provided")
